from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

try:
    from everpro_api_source import fetch_everpro_api_payloads, normalize_everpro_api_orders
    from mengantar_api_source import fetch_mengantar_api_records
    from mengantar_login import login_and_refresh_mengantar_cookie
    from returns_config import env_bool
    from returns_mart import refresh_returns_marts_sql
    from returns_storage import (
        df_to_postgres,
        ensure_schema,
        get_db_connection,
        read_raw_payload,
        write_raw_payload,
    )
    from spx_api_source import fetch_spx_api_records
    from spx_login import login_and_refresh_spx_cookies
except ImportError:  # pragma: no cover - package import path for tests
    from dags.everpro_api_source import fetch_everpro_api_payloads, normalize_everpro_api_orders
    from dags.mengantar_api_source import fetch_mengantar_api_records
    from dags.mengantar_login import login_and_refresh_mengantar_cookie
    from dags.returns_config import env_bool
    from dags.returns_mart import refresh_returns_marts_sql
    from dags.returns_storage import (
        df_to_postgres,
        ensure_schema,
        get_db_connection,
        read_raw_payload,
        write_raw_payload,
    )
    from dags.spx_api_source import fetch_spx_api_records
    from dags.spx_login import login_and_refresh_spx_cookies

RAW_SCHEMA = "raw"
STAGING_SCHEMA = "staging"
MART_SCHEMA = "mart"

RAW_SPX_API_TABLE = "spx_api_order_payloads"
RAW_EVERPRO_API_TABLE = "everpro_api_order_payloads"
RAW_MENGANTAR_API_TABLE = "mengantar_api_order_payloads"
STG_RETURN_SHIPMENTS_TABLE = "stg_return_shipments"

RETURNS_WEEKLY_TABLE = "fact_returns_weekly"
RETURNS_REASON_TABLE = "fact_return_reason_weekly"
RETURNS_DRIVER_TABLE = "fact_return_driver_weekly"

CANCEL_KEYWORDS = ("cancel", "canceled", "cancelled", "batal", "dibatalkan")
FINAL_SUCCESS_STATUSES = {"delivered"}
FINAL_RETURN_STATUSES = {"returned", "rts"}
FINAL_FAILED_STATUSES = {"damaged", "lost", "undelivered"}


def _contains_word(text: str, keywords: Any) -> bool:
    # Word-boundary match instead of plain substring: "delivered" must not match
    # inside "undelivered", the way `"delivered" in "undelivered"` would.
    return any(re.search(rf"\b{re.escape(keyword)}\b", text) for keyword in keywords)


NORMALIZED_ORDER_COLUMNS = [
    "source_system",
    "order_id",
    "event_date",
    "province",
    "city",
    "expedition",
    "service_type",
    "payment_method",
    "raw_payment_method",
    "cod_type",
    "order_value",
    "cod_value",
    "shipping_fee",
    "return_flag",
    "return_reason",
]


def _default_fetch_range(today: Optional[date] = None) -> tuple[str, str]:
    current = today or date.today()
    start_date = date(current.year, 1, 1)
    end_date = current
    return start_date.isoformat(), end_date.isoformat()


def _get_fetch_range(today: Optional[date] = None) -> tuple[str, str]:
    default_start, default_end = _default_fetch_range(today=today)
    start_date = os.getenv("RETURNS_FETCH_START_DATE", "").strip() or default_start
    end_date = os.getenv("RETURNS_FETCH_END_DATE", "").strip() or default_end
    start_dt = pd.to_datetime(start_date, errors="coerce")
    end_dt = pd.to_datetime(end_date, errors="coerce")
    if pd.isna(start_dt):
        raise ValueError(f"Invalid RETURNS_FETCH_START_DATE: {start_date}")
    if pd.isna(end_dt):
        raise ValueError(f"Invalid RETURNS_FETCH_END_DATE: {end_date}")
    if start_dt > end_dt:
        raise ValueError(
            f"RETURNS_FETCH_START_DATE must be <= RETURNS_FETCH_END_DATE, got {start_date} > {end_date}"
        )
    return start_date, end_date


def _to_datetime(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        # unix seconds
        try:
            return datetime.fromtimestamp(value)
        except (OSError, OverflowError, ValueError):
            return None
    try:
        return pd.to_datetime(value, errors="coerce").to_pydatetime()
    except Exception:
        return None


def _normalize_text(value: Any, fallback: str = "No Value") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback


def _normalize_service_type(value: Any) -> str:
    text = _normalize_text(value)
    lowered = text.lower()
    if "eco" in lowered or "hemat" in lowered:
        return "Eco"
    if "standard" in lowered or "regular" in lowered:
        return "Standard"
    return text


def _has_cancel_keyword(*values: Any) -> bool:
    text = " ".join("" if value is None else str(value) for value in values).lower()
    return _contains_word(text, CANCEL_KEYWORDS)


def _is_final_non_cancel_status(status: Any) -> bool:
    status_text = _normalize_text(status, fallback="").lower()
    final_statuses = FINAL_SUCCESS_STATUSES | FINAL_RETURN_STATUSES | FINAL_FAILED_STATUSES
    return _contains_word(status_text, final_statuses)


def _is_eligible_shipment(source_system: Any, delivery_status: Any, is_cancelled: Any) -> int:
    if int(is_cancelled or 0) == 1:
        return 0
    source_text = _normalize_text(source_system, fallback="").lower()
    status_text = _normalize_text(delivery_status, fallback="").lower()
    if source_text == "everpro_api":
        return 1 if _contains_word(status_text, {"delivered", "returned"}) else 0
    return 1 if _is_final_non_cancel_status(delivery_status) else 0


def _to_number(value: Any) -> float:
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return 0.0


def _get_enabled_return_sources() -> List[str]:
    enabled_sources: List[str] = []

    # Preferred explicit config.
    if env_bool("SPX_API_SOURCE_ENABLED", default=False):
        enabled_sources.append("spx_api")
    if env_bool("EVERPRO_API_SOURCE_ENABLED", default=False):
        enabled_sources.append("everpro_api")
    if env_bool("MENGANTAR_API_SOURCE_ENABLED", default=False):
        enabled_sources.append("mengantar_api")

    if enabled_sources:
        return enabled_sources

    # Backward-compatible fallback.
    raw_value = os.getenv("API2_SOURCE_MODE", "").strip().lower()
    if raw_value:
        parts = [part.strip() for part in raw_value.split(",")]
        return [part for part in parts if part]

    # Safe default for older envs that only configured SPX.
    return ["spx_api"]


def refresh_spx_login_if_enabled() -> None:
    if not env_bool("SPX_WEB_LOGIN_ENABLED", default=False):
        return
    login_and_refresh_spx_cookies()


def refresh_mengantar_login_if_enabled() -> None:
    if not env_bool("MENGANTAR_WEB_LOGIN_ENABLED", default=False):
        return
    login_and_refresh_mengantar_cookie()


def extract_spx_api_raw() -> None:
    q_start, q_end = _get_fetch_range()
    enabled_sources = _get_enabled_return_sources()
    if "spx_api" not in enabled_sources:
        write_raw_payload(
            RAW_SPX_API_TABLE, [], RAW_SCHEMA, fetch_start_date=q_start, fetch_end_date=q_end
        )
        return
    records = fetch_spx_api_records(q_start, q_end)
    write_raw_payload(
        RAW_SPX_API_TABLE, records, RAW_SCHEMA, fetch_start_date=q_start, fetch_end_date=q_end
    )


def extract_everpro_api_raw() -> None:
    q_start, q_end = _get_fetch_range()
    enabled_sources = _get_enabled_return_sources()
    if "everpro_api" not in enabled_sources:
        write_raw_payload(
            RAW_EVERPRO_API_TABLE, [], RAW_SCHEMA, fetch_start_date=q_start, fetch_end_date=q_end
        )
        return
    payloads = fetch_everpro_api_payloads(q_start, q_end)
    write_raw_payload(
        RAW_EVERPRO_API_TABLE, payloads, RAW_SCHEMA, fetch_start_date=q_start, fetch_end_date=q_end
    )


def extract_mengantar_api_raw() -> None:
    q_start, q_end = _get_fetch_range()
    enabled_sources = _get_enabled_return_sources()
    if "mengantar_api" not in enabled_sources:
        write_raw_payload(
            RAW_MENGANTAR_API_TABLE, [], RAW_SCHEMA, fetch_start_date=q_start, fetch_end_date=q_end
        )
        return
    records = fetch_mengantar_api_records(q_start, q_end)
    write_raw_payload(
        RAW_MENGANTAR_API_TABLE, records, RAW_SCHEMA, fetch_start_date=q_start, fetch_end_date=q_end
    )


def _normalize_api2_source_data(
    source_mode: str, data: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    if source_mode == "spx_api":
        records = []
        for record in data:
            normalized = dict(record)
            normalized["return_flag"] = int(normalized.get("return_flag", 0))
            normalized["order_value"] = _to_number(normalized.get("order_value"))
            normalized["cod_value"] = _to_number(normalized.get("cod_value"))
            normalized["shipping_fee"] = _to_number(normalized.get("shipping_fee"))
            normalized["service_type"] = _normalize_service_type(normalized.get("service_type"))
            normalized["return_reason"] = _normalize_text(
                normalized.get("return_reason"), fallback="No Reason Provided"
            )
            normalized["province"] = _normalize_text(normalized.get("province"))
            normalized["city"] = _normalize_text(normalized.get("city"))
            normalized["payment_method"] = _normalize_text(normalized.get("payment_method"))
            normalized["raw_payment_method"] = _normalize_text(
                normalized.get("raw_payment_method", normalized.get("payment_method"))
            )
            normalized["cod_type"] = _normalize_text(normalized.get("cod_type"), fallback="NON-COD")
            normalized["expedition"] = _normalize_text(normalized.get("expedition"), fallback="SPX")
            records.append(normalized)
        return records
    if source_mode == "everpro_api":
        return normalize_everpro_api_orders(data)
    if source_mode == "mengantar_api":
        records = []
        for record in data:
            normalized = dict(record)
            normalized["return_flag"] = int(normalized.get("return_flag", 0))
            normalized["order_value"] = _to_number(normalized.get("order_value"))
            normalized["cod_value"] = _to_number(normalized.get("cod_value"))
            normalized["shipping_fee"] = _to_number(normalized.get("shipping_fee"))
            normalized["service_type"] = _normalize_service_type(normalized.get("service_type"))
            normalized["return_reason"] = _normalize_text(
                normalized.get("return_reason"), fallback="No Reason Provided"
            )
            normalized["province"] = _normalize_text(normalized.get("province"))
            normalized["city"] = _normalize_text(normalized.get("city"))
            normalized["payment_method"] = _normalize_text(normalized.get("payment_method"))
            normalized["raw_payment_method"] = _normalize_text(
                normalized.get("raw_payment_method", normalized.get("payment_method"))
            )
            normalized["cod_type"] = _normalize_text(normalized.get("cod_type"), fallback="NON-COD")
            normalized["expedition"] = _normalize_text(normalized.get("expedition"), fallback="JNE")
            records.append(normalized)
        return records
    raise ValueError(f"Unsupported normalization source mode: {source_mode}")


def build_returns_reporting_tables() -> None:
    conn = get_db_connection()
    ensure_schema(conn, RAW_SCHEMA)
    ensure_schema(conn, STAGING_SCHEMA)
    ensure_schema(conn, MART_SCHEMA)

    spx_payloads = read_raw_payload(RAW_SPX_API_TABLE, RAW_SCHEMA)
    everpro_payloads = read_raw_payload(RAW_EVERPRO_API_TABLE, RAW_SCHEMA)
    mengantar_payloads = read_raw_payload(RAW_MENGANTAR_API_TABLE, RAW_SCHEMA)

    api2_orders: List[Dict[str, Any]] = []
    api2_orders.extend(_normalize_api2_source_data("spx_api", spx_payloads))
    api2_orders.extend(_normalize_api2_source_data("everpro_api", everpro_payloads))
    api2_orders.extend(_normalize_api2_source_data("mengantar_api", mengantar_payloads))
    returns_raw = pd.DataFrame(api2_orders)
    if returns_raw.empty:
        returns_raw = pd.DataFrame(
            columns=NORMALIZED_ORDER_COLUMNS
            + [
                "delivery_status",
                "raw_delivery_status",
                "failed_reason",
                "delay_reason",
                "sender_province",
            ]
        )
    if "delivery_status" not in returns_raw.columns:
        returns_raw["delivery_status"] = ""
    if "return_reason" not in returns_raw.columns:
        returns_raw["return_reason"] = ""
    if "failed_reason" not in returns_raw.columns:
        returns_raw["failed_reason"] = ""
    if "delay_reason" not in returns_raw.columns:
        returns_raw["delay_reason"] = ""
    returns_raw["is_cancelled"] = returns_raw.apply(
        lambda row: (
            1
            if _has_cancel_keyword(
                row.get("delivery_status"),
                row.get("return_reason"),
                row.get("failed_reason"),
                row.get("delay_reason"),
            )
            else 0
        ),
        axis=1,
    )
    returns_raw["is_final_status"] = returns_raw["delivery_status"].apply(
        lambda value: 1 if _is_final_non_cancel_status(value) else 0
    )
    returns_raw["eligible_shipment_flag"] = returns_raw.apply(
        lambda row: _is_eligible_shipment(
            row.get("source_system"),
            row.get("delivery_status"),
            row.get("is_cancelled"),
        ),
        axis=1,
    )

    returns_raw = returns_raw.drop_duplicates(subset=["source_system", "order_id"], keep="last")
    df_to_postgres(
        returns_raw,
        STG_RETURN_SHIPMENTS_TABLE,
        conn,
        STAGING_SCHEMA,
        replace=False,
        unique_keys=["source_system", "order_id"],
    )

    event_dates = pd.to_datetime(returns_raw["event_date"], errors="coerce")
    affected_weeks_df = pd.DataFrame(
        {
            "year": event_dates.dt.year,
            "week_of_year": event_dates.dt.isocalendar().week.astype("Int64"),
        }
    ).dropna()
    affected_weeks = [
        (int(row["year"]), int(row["week_of_year"]))
        for _, row in affected_weeks_df.drop_duplicates().iterrows()
    ]
    refresh_returns_marts_sql(
        conn,
        staging_schema=STAGING_SCHEMA,
        mart_schema=MART_SCHEMA,
        staging_table=STG_RETURN_SHIPMENTS_TABLE,
        weekly_table=RETURNS_WEEKLY_TABLE,
        reason_table=RETURNS_REASON_TABLE,
        driver_table=RETURNS_DRIVER_TABLE,
        affected_weeks=affected_weeks,
    )
    conn.close()


def validate_returns_outputs() -> None:
    enabled_sources = _get_enabled_return_sources()
    min_staging_rows = int(os.getenv("VALIDATION_MIN_STAGING_ROWS", "100"))
    min_eligible_rows = int(os.getenv("VALIDATION_MIN_ELIGIBLE_ROWS", "10"))
    max_spx_no_value_ratio = float(os.getenv("VALIDATION_MAX_SPX_NO_VALUE_RATIO", "0.05"))

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        f"""
        SELECT
            COUNT(*)::bigint AS total_rows,
            COALESCE(SUM(COALESCE(NULLIF(eligible_shipment_flag::text, ''), '0')::bigint), 0)::bigint AS eligible_rows
        FROM "{STAGING_SCHEMA}"."{STG_RETURN_SHIPMENTS_TABLE}"
        """
    )
    total_rows, eligible_rows = cur.fetchone()
    if total_rows < min_staging_rows:
        raise ValueError(
            f"Validation failed: staging rows too low. got={total_rows}, min={min_staging_rows}"
        )
    if eligible_rows < min_eligible_rows:
        raise ValueError(
            f"Validation failed: eligible shipment rows too low. got={eligible_rows}, min={min_eligible_rows}"
        )

    for source_system in enabled_sources:
        cur.execute(
            f"""
            SELECT COUNT(*)::bigint
            FROM "{STAGING_SCHEMA}"."{STG_RETURN_SHIPMENTS_TABLE}"
            WHERE source_system = %s
            """,
            (source_system,),
        )
        source_rows = cur.fetchone()[0]
        if source_rows <= 0:
            raise ValueError(
                f"Validation failed: no rows loaded for enabled source '{source_system}'"
            )

    if "spx_api" in enabled_sources:
        cur.execute(
            f"""
            SELECT
                COUNT(*)::double precision AS total_rows,
                AVG(CASE WHEN province = 'No Value' THEN 1.0 ELSE 0.0 END) AS province_no_value_ratio,
                AVG(CASE WHEN city = 'No Value' THEN 1.0 ELSE 0.0 END) AS city_no_value_ratio,
                AVG(CASE WHEN service_type = 'No Value' THEN 1.0 ELSE 0.0 END) AS service_type_no_value_ratio
            FROM "{STAGING_SCHEMA}"."{STG_RETURN_SHIPMENTS_TABLE}"
            WHERE source_system = 'spx_api'
            """
        )
        _, province_ratio, city_ratio, service_type_ratio = cur.fetchone()
        ratios = {
            "province": float(province_ratio or 0.0),
            "city": float(city_ratio or 0.0),
            "service_type": float(service_type_ratio or 0.0),
        }
        bad_dims = [
            f"{name}={value:.2%}"
            for name, value in ratios.items()
            if value > max_spx_no_value_ratio
        ]
        if bad_dims:
            raise ValueError(
                "Validation failed: SPX No Value ratio too high for "
                + ", ".join(bad_dims)
                + f" (max={max_spx_no_value_ratio:.2%})"
            )

    for table_name in [RETURNS_WEEKLY_TABLE, RETURNS_REASON_TABLE, RETURNS_DRIVER_TABLE]:
        cur.execute(f'SELECT COUNT(*)::bigint FROM "{MART_SCHEMA}"."{table_name}"')
        mart_rows = cur.fetchone()[0]
        if mart_rows <= 0:
            raise ValueError(f"Validation failed: mart table '{table_name}' is empty")

    cur.close()
    conn.close()


with DAG(
    dag_id="returns_api_weekly",
    description="Weekly return shipment pipeline for SPX, Everpro, and Mengantar API sources",
    schedule="0 19 * * 1",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    max_active_runs=1,
    max_active_tasks=1,
    tags=["etl", "returns", "spx", "everpro", "mengantar", "analytics"],
) as dag:
    refresh_spx_login = PythonOperator(
        task_id="refresh_spx_login",
        python_callable=refresh_spx_login_if_enabled,
    )

    extract_spx_api = PythonOperator(
        task_id="extract_spx_api_shipments",
        python_callable=extract_spx_api_raw,
    )

    extract_everpro_api = PythonOperator(
        task_id="extract_everpro_api_orders",
        python_callable=extract_everpro_api_raw,
    )

    refresh_mengantar_login = PythonOperator(
        task_id="refresh_mengantar_login",
        python_callable=refresh_mengantar_login_if_enabled,
    )

    extract_mengantar_api = PythonOperator(
        task_id="extract_mengantar_api_orders",
        python_callable=extract_mengantar_api_raw,
    )

    build_reporting_tables = PythonOperator(
        task_id="build_returns_reporting_tables",
        python_callable=build_returns_reporting_tables,
    )

    validate_outputs = PythonOperator(
        task_id="validate_returns_outputs",
        python_callable=validate_returns_outputs,
    )

    refresh_spx_login >> extract_spx_api
    refresh_mengantar_login >> extract_mengantar_api
    (
        [extract_spx_api, extract_everpro_api, extract_mengantar_api]
        >> build_reporting_tables
        >> validate_outputs
    )
