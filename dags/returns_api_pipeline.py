from __future__ import annotations

import json
import os
import time
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import io
import requests
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

from spx_web_source import fetch_spx_export_records

RAW_SCHEMA = "raw"
STAGING_SCHEMA = "staging"
MART_SCHEMA = "mart"

RAW_SPX_WEB_TABLE = "spx_web_order_payloads"
RAW_EVERPRO_API_TABLE = "everpro_api_order_payloads"
STG_RETURN_SHIPMENTS_TABLE = "stg_return_shipments"

RETURNS_WEEKLY_TABLE = "fact_returns_weekly"
RETURNS_REASON_TABLE = "fact_return_reason_weekly"
RETURNS_DRIVER_TABLE = "fact_return_driver_weekly"

CANCEL_KEYWORDS = ("cancel", "canceled", "cancelled", "batal", "dibatalkan")
FINAL_SUCCESS_STATUSES = {"delivered"}
FINAL_RETURN_STATUSES = {"returned"}
FINAL_FAILED_STATUSES = {"damaged", "lost"}

NORMALIZED_ORDER_COLUMNS = [
    "source_system",
    "order_id",
    "event_date",
    "province",
    "city",
    "expedition",
    "service_type",
    "payment_method",
    "cod_type",
    "order_value",
    "cod_value",
    "shipping_fee",
    "return_flag",
    "return_reason",
]


def _env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _env_bool(name: str, default: bool = False) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _current_quarter_range(today: Optional[date] = None) -> tuple[str, str]:
    current = today or date.today()
    quarter = (current.month - 1) // 3 + 1
    start_month = (quarter - 1) * 3 + 1
    start_date = date(current.year, start_month, 1)
    if quarter == 4:
        end_date = date(current.year, 12, 31)
    else:
        next_quarter_start = date(current.year, start_month + 3, 1)
        end_date = next_quarter_start - pd.Timedelta(days=1)
    return start_date.isoformat(), end_date.isoformat()


def _get_nested(obj: Dict[str, Any], path: Iterable[str], default: Any = None) -> Any:
    current: Any = obj
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


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
    return any(keyword in text for keyword in CANCEL_KEYWORDS)


def _is_final_non_cancel_status(status: Any) -> bool:
    status_text = _normalize_text(status, fallback="").lower()
    final_statuses = FINAL_SUCCESS_STATUSES | FINAL_RETURN_STATUSES | FINAL_FAILED_STATUSES
    return status_text in final_statuses


def _is_eligible_shipment(source_system: Any, delivery_status: Any, is_cancelled: Any) -> int:
    if int(is_cancelled or 0) == 1:
        return 0
    source_text = _normalize_text(source_system, fallback="").lower()
    status_text = _normalize_text(delivery_status, fallback="").lower()
    if source_text == "everpro_api":
        return 1 if status_text in {"delivered", "returned"} else 0
    return 1 if _is_final_non_cancel_status(delivery_status) else 0


def _to_number(value: Any) -> float:
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return 0.0


def _ensure_schema(conn: psycopg2.extensions.connection, schema: str) -> None:
    cur = conn.cursor()
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    conn.commit()
    cur.close()


def _df_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    conn: psycopg2.extensions.connection,
    schema: str,
    *,
    replace: bool = True,
    unique_keys: Optional[List[str]] = None,
) -> None:
    cur = conn.cursor()
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    column_defs = []
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            col_type = "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            col_type = "DOUBLE PRECISION"
        else:
            col_type = "TEXT"
        column_defs.append(f'"{col}" {col_type}')

    if replace:
        cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"')
        cur.execute(f'CREATE TABLE "{schema}"."{table_name}" ({", ".join(column_defs)})')
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        cur.copy_expert(f'COPY "{schema}"."{table_name}" FROM STDIN WITH CSV HEADER', buffer)
        conn.commit()
        cur.close()
        return

    # incremental upsert
    temp_table = f"{table_name}__staging"
    cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{temp_table}"')
    cur.execute(f'CREATE TABLE "{schema}"."{temp_table}" ({", ".join(column_defs)})')

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    cur.copy_expert(f'COPY "{schema}"."{temp_table}" FROM STDIN WITH CSV HEADER', buffer)

    # ensure target exists
    cur.execute(
        f'CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" ({", ".join(column_defs)})'
    )

    # if schema mismatch (new columns), rebuild target table
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table_name),
    )
    existing_cols = [row[0] for row in cur.fetchall()]
    if existing_cols != list(df.columns):
        cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"')
        cur.execute(f'CREATE TABLE "{schema}"."{table_name}" ({", ".join(column_defs)})')

    if not unique_keys:
        raise ValueError("unique_keys required for incremental upsert")

    key_match = " AND ".join([f't."{k}" = s."{k}"' for k in unique_keys])
    cur.execute(f'DELETE FROM "{schema}"."{table_name}" t USING "{schema}"."{temp_table}" s WHERE {key_match}')
    cur.execute(f'INSERT INTO "{schema}"."{table_name}" SELECT * FROM "{schema}"."{temp_table}"')
    cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{temp_table}"')
    conn.commit()
    cur.close()


def _fetch_paged(url: str, params: Dict[str, Any], headers: Dict[str, str]) -> List[Dict[str, Any]]:
    page = int(params.get("page", 1))
    limit = int(params.get("limit", 100))
    max_pages = int(os.getenv("API_MAX_PAGES", "50"))
    sleep_seconds = float(os.getenv("API_RATE_SLEEP", "1.0"))
    max_retries = int(os.getenv("API_MAX_RETRIES", "5"))
    max_network_retries = int(os.getenv("API_NETWORK_MAX_RETRIES", str(max_retries)))
    fatal_on_5xx = os.getenv("API_FATAL_ON_5XX", "false").lower() == "true"
    results: List[Dict[str, Any]] = []

    def _request_page(request_params: Dict[str, Any]) -> requests.Response:
        last_error: Optional[Exception] = None
        for attempt in range(max_network_retries + 1):
            try:
                return requests.get(url, params=request_params, headers=headers, timeout=60)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                last_error = exc
                if attempt >= max_network_retries:
                    break
                retry_delay = min(sleep_seconds * (2 ** attempt), 20.0)
                print(
                    f"Retrying API request after network error on page={request_params.get('page')} "
                    f"attempt={attempt + 1}/{max_network_retries} delay={retry_delay:.1f}s error={exc}"
                )
                time.sleep(retry_delay)
        assert last_error is not None
        raise last_error

    while True:
        if page > max_pages:
            break
        params["page"] = page
        params["limit"] = limit
        resp = _request_page(params.copy())
        if resp.status_code == 429:
            # simple backoff on rate limit
            for attempt in range(max_retries):
                time.sleep(sleep_seconds * (attempt + 1))
                resp = _request_page(params.copy())
                if resp.status_code != 429:
                    break
        if 500 <= resp.status_code < 600:
            # retry on server errors; if still failing, either stop or skip page
            last_status = resp.status_code
            for attempt in range(max_retries):
                time.sleep(sleep_seconds * (attempt + 1))
                resp = _request_page(params.copy())
                if resp.status_code < 500:
                    break
                last_status = resp.status_code
            if 500 <= resp.status_code < 600:
                if fatal_on_5xx:
                    resp.raise_for_status()
                # stop paging but keep what we already collected
                break
        resp.raise_for_status()
        payload = resp.json()
        results.append(payload)

        data_list = _extract_api_list(payload)
        if not data_list or len(data_list) < limit:
            break
        page += 1
        time.sleep(sleep_seconds)
    return results


def _extract_api_list(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    # SPX exported payload fallback
    orders = _get_nested(payload, ["data", "data", "orders"])
    if isinstance(orders, list):
        return orders
    # Everpro: data.orders
    orders = _get_nested(payload, ["data", "orders"])
    if isinstance(orders, list):
        return orders
    # Nested payload fallback
    items = _get_nested(payload, ["data", "data", "list"])
    if isinstance(items, list):
        return items
    return []


def _get_enabled_return_sources() -> List[str]:
    enabled_sources: List[str] = []

    # Preferred explicit config.
    if _env_bool("SPX_WEB_SOURCE_ENABLED", default=False):
        enabled_sources.append("spx_web")
    if _env_bool("EVERPRO_API_SOURCE_ENABLED", default=False):
        enabled_sources.append("everpro_api")

    if enabled_sources:
        return enabled_sources

    # Backward-compatible fallback.
    raw_value = os.getenv("API2_SOURCE_MODE", "").strip().lower()
    if raw_value:
        parts = [part.strip() for part in raw_value.split(",")]
        return [part for part in parts if part]

    # Safe default for older envs that only configured SPX.
    return ["spx_web"]


def _everpro_headers() -> Dict[str, str]:
    token = _env("EVERPRO_API_TOKEN").strip()
    return {
        "accept": "application/json, text/plain, */*",
        "authorization": f"Bearer {token}",
        "referer": "https://customer.everpro.id/order",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
        ),
    }


def _fetch_everpro_orders(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    base_url = os.getenv("EVERPRO_API_BASE_URL", "https://customer.everpro.id").strip().rstrip("/")
    api_url = f"{base_url}/api/logistic/v2/public/orders"
    headers = _everpro_headers()
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    params = {
        "page": int(os.getenv("EVERPRO_API_PAGE", "1")),
        "limit": int(os.getenv("EVERPRO_API_LIMIT", os.getenv("API2_LIMIT", "100"))),
        "sort_by": "",
        "order_status": os.getenv("EVERPRO_ORDER_STATUS", "all"),
        "status": "",
        "filter_order": "",
        "courier": "",
        "start_date": start_date,
        "end_date": end_date,
        "start_epoch": int(start_dt.timestamp()),
        "end_epoch": int(end_dt.timestamp()),
        "created_by": "",
        "shipment_type": "",
        "payment_type": "",
        "sender": "",
        "dropshipper_name": "",
        "receiver": "",
        "no_ref": "",
        "origin_postal_code": "",
        "destination_postal_code": "",
        "origin_city": "",
        "destination_city": "",
        "over_sla_status": "",
        "return_lost_confirmation": "",
        "channel": "",
    }
    return _fetch_paged(api_url, params, headers)


def _table_exists(conn: psycopg2.extensions.connection, schema: str, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table_name),
    )
    exists = cur.fetchone() is not None
    cur.close()
    return exists


def _write_raw_payload(table_name: str, payload_obj: Any) -> None:
    db_host = _env("DB_HOST")
    db_port = _env("DB_PORT")
    db_name = _env("DB_NAME")
    db_user = _env("DB_USER")
    db_password = _env("DB_PASSWORD")
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
    )
    _ensure_schema(conn, RAW_SCHEMA)
    payload_df = pd.DataFrame(
        [{"run_ts": datetime.utcnow().isoformat(), "payload": json.dumps(payload_obj, ensure_ascii=False)}]
    )
    _df_to_postgres(payload_df, table_name, conn, RAW_SCHEMA, replace=True)
    conn.close()


def _read_raw_payload(table_name: str) -> List[Dict[str, Any]]:
    db_host = _env("DB_HOST")
    db_port = _env("DB_PORT")
    db_name = _env("DB_NAME")
    db_user = _env("DB_USER")
    db_password = _env("DB_PASSWORD")
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
    )
    if not _table_exists(conn, RAW_SCHEMA, table_name):
        conn.close()
        return []
    cur = conn.cursor()
    cur.execute(f'SELECT payload FROM "{RAW_SCHEMA}"."{table_name}" ORDER BY run_ts DESC LIMIT 1')
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return []
    payload_obj = json.loads(row[0])
    return payload_obj if isinstance(payload_obj, list) else []


def extract_spx_web_raw() -> None:
    q_start, q_end = _current_quarter_range()
    enabled_sources = _get_enabled_return_sources()
    if "spx_web" not in enabled_sources:
        _write_raw_payload(RAW_SPX_WEB_TABLE, [])
        return
    records = fetch_spx_export_records(
        q_start,
        q_end,
        headless=os.getenv("SPX_WEB_HEADLESS", "true").lower() == "true",
        keep_download=False,
        output_dir=os.getenv("SPX_WEB_DOWNLOAD_DIR"),
    )
    _write_raw_payload(RAW_SPX_WEB_TABLE, records)


def extract_everpro_api_raw() -> None:
    q_start, q_end = _current_quarter_range()
    enabled_sources = _get_enabled_return_sources()
    if "everpro_api" not in enabled_sources:
        _write_raw_payload(RAW_EVERPRO_API_TABLE, [])
        return
    payloads = _fetch_everpro_orders(q_start, q_end)
    _write_raw_payload(RAW_EVERPRO_API_TABLE, payloads)


def _build_everpro_status_map(payloads: List[Dict[str, Any]]) -> Dict[str, str]:
    status_map: Dict[str, str] = {}
    for payload in payloads:
        statuses = _get_nested(payload, ["data", "statuses"], default=[])
        if not isinstance(statuses, list):
            continue
        for status in statuses:
            if not isinstance(status, dict):
                continue
            status_id = status.get("id")
            status_name = str(status.get("name") or "").strip()
            if status_id is None or not status_name:
                continue
            status_map[str(status_id)] = status_name
    return status_map


def _normalize_everpro_orders(payloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    status_map = _build_everpro_status_map(payloads)
    final_success_statuses = {"COMPLETED"}
    final_return_statuses = {"RETURN", "REJECTED"}
    final_failure_statuses = {"LOST / BROKEN"}
    for payload in payloads:
        for order in _extract_api_list(payload):
            shipper = order.get("shipper", {}) or {}
            shipper_addr = shipper.get("address_detail", {}) or {}
            receiver = order.get("receiver", {}) or {}
            receiver_addr = receiver.get("address_detail", {}) or {}
            shipment = order.get("shipment", {}) or {}
            logistic = order.get("logistic", {}) or {}
            cod = order.get("cod", {}) or {}

            event_dt = _to_datetime(order.get("created_at"))
            rts_reasons = order.get("rts_reasons") or []
            if isinstance(rts_reasons, list):
                return_reason = " | ".join(str(reason).strip() for reason in rts_reasons if str(reason).strip())
            else:
                return_reason = _normalize_text(rts_reasons, fallback="")
            rts_status = _normalize_text(order.get("rts_status"), fallback="")
            shipment_status = _normalize_text(shipment.get("status"), fallback="")
            everpro_status_name = status_map.get(shipment_status, shipment_status).strip().upper()

            if everpro_status_name in final_success_statuses:
                normalized_status = "Delivered"
            elif everpro_status_name in final_return_statuses:
                normalized_status = "Returned"
            elif everpro_status_name == "CANCELLED":
                normalized_status = "Cancelled"
            elif everpro_status_name in final_failure_statuses:
                normalized_status = "Lost"
            else:
                normalized_status = everpro_status_name.title() if everpro_status_name else "Unknown"

            is_return = normalized_status == "Returned"
            failed_reason = ""
            if normalized_status in {"Returned", "Lost"}:
                failed_reason = return_reason or rts_status or everpro_status_name

            items.append(
                {
                    "source_system": "everpro_api",
                    "order_id": _normalize_text(order.get("awb_number") or order.get("shipment_order_no")),
                    "event_date": event_dt.date() if event_dt else None,
                    "province": _normalize_text(receiver_addr.get("province")),
                    "city": _normalize_text(receiver_addr.get("city")),
                    "expedition": _normalize_text(logistic.get("name")),
                    "service_type": _normalize_service_type(
                        logistic.get("rate_type_name") or logistic.get("rate_name") or shipment.get("type")
                    ),
                    "payment_method": "COD" if order.get("is_cod") else "NON-COD",
                    "cod_type": "COD" if order.get("is_cod") else "NON-COD",
                    "order_value": _to_number(cod.get("total") or order.get("package", {}).get("price")),
                    "cod_value": _to_number(cod.get("total")) if order.get("is_cod") else 0.0,
                    "shipping_fee": _to_number(shipment.get("total_price") or shipment.get("price")),
                    "return_flag": 1 if is_return else 0,
                    "return_reason": return_reason or rts_status or failed_reason or "No Reason Provided",
                    "delivery_status": normalized_status,
                    "raw_delivery_status": shipment_status,
                    "failed_reason": failed_reason,
                    "delay_reason": "",
                    "sender_province": _normalize_text(shipper_addr.get("province")),
                }
            )
    return items


def _normalize_api2_source_data(source_mode: str, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if source_mode == "spx_web":
        records = []
        for record in data:
            normalized = dict(record)
            normalized["return_flag"] = int(normalized.get("return_flag", 0))
            normalized["order_value"] = _to_number(normalized.get("order_value"))
            normalized["cod_value"] = _to_number(normalized.get("cod_value"))
            normalized["shipping_fee"] = _to_number(normalized.get("shipping_fee"))
            normalized["service_type"] = _normalize_service_type(normalized.get("service_type"))
            normalized["return_reason"] = _normalize_text(normalized.get("return_reason"), fallback="No Reason Provided")
            normalized["province"] = _normalize_text(normalized.get("province"))
            normalized["city"] = _normalize_text(normalized.get("city"))
            normalized["payment_method"] = _normalize_text(normalized.get("payment_method"))
            normalized["cod_type"] = _normalize_text(normalized.get("cod_type"), fallback="NON-COD")
            normalized["expedition"] = _normalize_text(normalized.get("expedition"), fallback="SPX")
            records.append(normalized)
        return records
    if source_mode == "everpro_api":
        return _normalize_everpro_orders(data)
    raise ValueError(f"Unsupported normalization source mode: {source_mode}")


def build_returns_reporting_tables() -> None:
    db_host = _env("DB_HOST")
    db_port = _env("DB_PORT")
    db_name = _env("DB_NAME")
    db_user = _env("DB_USER")
    db_password = _env("DB_PASSWORD")

    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
    )
    _ensure_schema(conn, RAW_SCHEMA)
    _ensure_schema(conn, STAGING_SCHEMA)
    _ensure_schema(conn, MART_SCHEMA)

    spx_payloads = _read_raw_payload(RAW_SPX_WEB_TABLE)
    everpro_payloads = _read_raw_payload(RAW_EVERPRO_API_TABLE)

    api2_orders: List[Dict[str, Any]] = []
    api2_orders.extend(_normalize_api2_source_data("spx_web", spx_payloads))
    api2_orders.extend(_normalize_api2_source_data("everpro_api", everpro_payloads))
    returns_raw = pd.DataFrame(api2_orders)
    if returns_raw.empty:
        returns_raw = pd.DataFrame(
            columns=NORMALIZED_ORDER_COLUMNS
            + ["delivery_status", "raw_delivery_status", "failed_reason", "delay_reason", "sender_province"]
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
        lambda row: 1
        if _has_cancel_keyword(
            row.get("delivery_status"),
            row.get("return_reason"),
            row.get("failed_reason"),
            row.get("delay_reason"),
        )
        else 0,
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

    # staging table
    _df_to_postgres(returns_raw, STG_RETURN_SHIPMENTS_TABLE, conn, STAGING_SCHEMA, replace=True)

    # Return rate denominator includes only final non-cancel shipments.
    returns_raw = returns_raw[returns_raw["eligible_shipment_flag"] == 1].copy()

    returns_raw["event_date"] = pd.to_datetime(returns_raw["event_date"], errors="coerce")
    returns_raw["year"] = returns_raw["event_date"].dt.year
    returns_raw["week_of_year"] = returns_raw["event_date"].dt.isocalendar().week.astype("Int64")

    returns_weekly = (
        returns_raw.groupby(
            [
                "year",
                "week_of_year",
                "province",
                "city",
                "expedition",
                "service_type",
                "payment_method",
                "cod_type",
            ],
            dropna=False,
        )
        .agg(
            total_shipments=("order_id", "count"),
            total_returns=("return_flag", "sum"),
            total_order_value=("order_value", "sum"),
            total_cod_value=("cod_value", "sum"),
            total_shipping_fee=("shipping_fee", "sum"),
        )
        .reset_index()
        .sort_values(["year", "week_of_year", "province", "city", "expedition"])
    )
    returns_weekly["return_rate"] = returns_weekly.apply(
        lambda row: (row["total_returns"] / row["total_shipments"]) if row["total_shipments"] else 0.0,
        axis=1,
    )

    returns_reason_weekly = (
        returns_raw.groupby(
            ["year", "week_of_year", "province", "city", "expedition", "service_type", "return_reason"],
            dropna=False,
        )
        .agg(
            total_shipments=("order_id", "count"),
            total_returns=("return_flag", "sum"),
        )
        .reset_index()
        .sort_values(["year", "week_of_year", "province", "city", "expedition", "return_reason"])
    )
    returns_reason_weekly["return_rate"] = returns_reason_weekly.apply(
        lambda row: (row["total_returns"] / row["total_shipments"]) if row["total_shipments"] else 0.0,
        axis=1,
    )

    driver_frames = []
    for driver_name in ["service_type"]:
        grouped = (
            returns_raw.groupby(
                ["year", "week_of_year", "province", "city", "expedition", driver_name],
                dropna=False,
            )
            .agg(
                total_shipments=("order_id", "count"),
                total_returns=("return_flag", "sum"),
                total_order_value=("order_value", "sum"),
            )
            .reset_index()
        )
        grouped["group_total_shipments"] = grouped.groupby(
            ["year", "week_of_year", "province", "city", "expedition"], dropna=False
        )["total_shipments"].transform("sum")
        grouped["shipments_share"] = grouped.apply(
            lambda row: (row["total_shipments"] / row["group_total_shipments"]) if row["group_total_shipments"] else 0.0,
            axis=1,
        )
        grouped["driver_type"] = driver_name
        grouped["driver_value"] = grouped[driver_name].astype(str)
        grouped["return_rate"] = grouped.apply(
            lambda row: (row["total_returns"] / row["total_shipments"]) if row["total_shipments"] else 0.0,
            axis=1,
        )
        driver_frames.append(
            grouped[
                [
                    "year",
                    "week_of_year",
                    "province",
                    "city",
                    "expedition",
                    "driver_type",
                    "driver_value",
                    "total_shipments",
                    "total_returns",
                    "total_order_value",
                    "shipments_share",
                    "return_rate",
                ]
            ]
        )

    returns_driver_weekly = pd.concat(driver_frames, ignore_index=True).sort_values(
        ["year", "week_of_year", "province", "city", "expedition", "driver_type", "return_rate"],
        ascending=[True, True, True, True, True, True, False],
    )
    returns_driver_weekly["rank_in_group"] = (
        returns_driver_weekly.groupby(
            ["year", "week_of_year", "province", "city", "expedition", "driver_type"],
            dropna=False,
        )["return_rate"]
        .rank(method="dense", ascending=False)
        .astype(int)
    )

    # Store to DB only (replace tables each run)
    _df_to_postgres(
        returns_weekly,
        RETURNS_WEEKLY_TABLE,
        conn,
        MART_SCHEMA,
        replace=False,
        unique_keys=[
            "year",
            "week_of_year",
            "province",
            "city",
            "expedition",
            "service_type",
            "payment_method",
            "cod_type",
        ],
    )
    _df_to_postgres(
        returns_reason_weekly,
        RETURNS_REASON_TABLE,
        conn,
        MART_SCHEMA,
        replace=False,
        unique_keys=[
            "year",
            "week_of_year",
            "province",
            "city",
            "expedition",
            "service_type",
            "return_reason",
        ],
    )
    _df_to_postgres(
        returns_driver_weekly,
        RETURNS_DRIVER_TABLE,
        conn,
        MART_SCHEMA,
        replace=False,
        unique_keys=[
            "year",
            "week_of_year",
            "province",
            "city",
            "expedition",
            "driver_type",
            "driver_value",
        ],
    )
    conn.close()


def validate_returns_outputs() -> None:
    _env("DB_HOST")
    _env("DB_PORT")
    _env("DB_NAME")
    _env("DB_USER")
    _env("DB_PASSWORD")


with DAG(
    dag_id="returns_api_weekly",
    description="Weekly return shipment pipeline for SPX web scraping and Everpro API sources",
    schedule="0 1 * * 1",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "returns", "spx", "everpro", "analytics"],
) as dag:
    extract_spx_web = PythonOperator(
        task_id="extract_spx_web_shipments",
        python_callable=extract_spx_web_raw,
    )

    extract_everpro_api = PythonOperator(
        task_id="extract_everpro_api_orders",
        python_callable=extract_everpro_api_raw,
    )

    build_reporting_tables = PythonOperator(
        task_id="build_returns_reporting_tables",
        python_callable=build_returns_reporting_tables,
    )

    validate_outputs = PythonOperator(
        task_id="validate_returns_outputs",
        python_callable=validate_returns_outputs,
    )

    [extract_spx_web, extract_everpro_api] >> build_reporting_tables >> validate_outputs
