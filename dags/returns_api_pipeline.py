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

RAW_API1_TABLE = "api1_payloads"
RAW_API2_TABLE = "api2_payloads"
STG_API1_ORDERS = "stg_api1_orders"
STG_API2_ORDERS = "stg_api2_orders"

RETURNS_WEEKLY_TABLE = "fact_returns_weekly"
RETURNS_REASON_TABLE = "fact_return_reason_weekly"
RETURNS_DRIVER_TABLE = "fact_return_driver_weekly"

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


def _to_number(value: Any) -> float:
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return 0.0


def _format_decimal_comma(series: pd.Series) -> pd.Series:
    return series.map(lambda x: f"{x:.6f}".replace(".", ",") if pd.notna(x) else "")


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
    fatal_on_5xx = os.getenv("API_FATAL_ON_5XX", "false").lower() == "true"
    results: List[Dict[str, Any]] = []

    while True:
        if page > max_pages:
            break
        params["page"] = page
        params["limit"] = limit
        resp = requests.get(url, params=params, headers=headers, timeout=60)
        if resp.status_code == 429:
            # simple backoff on rate limit
            for attempt in range(max_retries):
                time.sleep(sleep_seconds * (attempt + 1))
                resp = requests.get(url, params=params, headers=headers, timeout=60)
                if resp.status_code != 429:
                    break
        if 500 <= resp.status_code < 600:
            # retry on server errors; if still failing, either stop or skip page
            last_status = resp.status_code
            for attempt in range(max_retries):
                time.sleep(sleep_seconds * (attempt + 1))
                resp = requests.get(url, params=params, headers=headers, timeout=60)
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
    # API1: data.data.orders
    orders = _get_nested(payload, ["data", "data", "orders"])
    if isinstance(orders, list):
        return orders
    # API2: data.data.list
    items = _get_nested(payload, ["data", "data", "list"])
    if isinstance(items, list):
        return items
    return []


def _get_api2_source_mode() -> str:
    return os.getenv("API2_SOURCE_MODE", "api").strip().lower()


def _fetch_api2_source_data(start_date: str, end_date: str) -> Dict[str, Any]:
    source_mode = _get_api2_source_mode()
    if source_mode == "spx_web":
        records = fetch_spx_export_records(
            start_date,
            end_date,
            headless=os.getenv("SPX_WEB_HEADLESS", "true").lower() == "true",
            keep_download=False,
            output_dir=os.getenv("SPX_WEB_DOWNLOAD_DIR"),
        )
        return {"source_mode": source_mode, "data": records}

    # Legacy API2 path intentionally disabled.
    # api2_url = _env("API2_URL")
    # api2_token = os.getenv("API2_TOKEN", "")
    # api2_params = {
    #     "page": int(os.getenv("API2_PAGE", "1")),
    #     "limit": int(os.getenv("API2_LIMIT", "100")),
    #     "start_date": os.getenv("API2_START_DATE", start_date),
    #     "end_date": os.getenv("API2_END_DATE", end_date),
    # }
    # api2_headers = {"Authorization": api2_token} if api2_token else {}
    # api2_payloads = _fetch_paged(api2_url, api2_params, api2_headers)
    # return {"source_mode": source_mode, "data": api2_payloads}
    raise ValueError("API2_SOURCE_MODE must be 'spx_web'; legacy API2 mode is disabled.")


def _decode_api2_payload_blob(payload_text: str) -> tuple[str, List[Dict[str, Any]]]:
    payload_obj = json.loads(payload_text)
    if isinstance(payload_obj, dict) and "source_mode" in payload_obj and "data" in payload_obj:
        return str(payload_obj["source_mode"]), list(payload_obj["data"])
    return "api", list(payload_obj)


def fetch_api_raw() -> None:
    q_start, q_end = _current_quarter_range()

    # Legacy API1 path intentionally disabled.
    # api1_url = _env("API1_URL")
    # api1_token = os.getenv("API1_TOKEN", "")
    # api1_params = {
    #     "page": int(os.getenv("API1_PAGE", "1")),
    #     "limit": int(os.getenv("API1_LIMIT", "100")),
    #     "order_status": os.getenv("API1_ORDER_STATUS", "all"),
    #     "filter_order": os.getenv("API1_FILTER_ORDER", ""),
    #     "courier": os.getenv("API1_COURIER", ""),
    #     "start_date": os.getenv("API1_START_DATE", q_start),
    #     "end_date": os.getenv("API1_END_DATE", q_end),
    # }
    # api1_headers = {"Authorization": api1_token} if api1_token else {}
    # api1_payloads = _fetch_paged(api1_url, api1_params, api1_headers)
    api1_payloads: List[Dict[str, Any]] = []
    api2_payloads = _fetch_api2_source_data(q_start, q_end)

    # persist raw payloads to DB (raw schema)
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
    api1_df = pd.DataFrame(
        [{"run_ts": datetime.utcnow().isoformat(), "payload": json.dumps(api1_payloads, ensure_ascii=False)}]
    )
    api2_df = pd.DataFrame(
        [{"run_ts": datetime.utcnow().isoformat(), "payload": json.dumps(api2_payloads, ensure_ascii=False)}]
    )
    _df_to_postgres(api1_df, RAW_API1_TABLE, conn, RAW_SCHEMA, replace=True)
    _df_to_postgres(api2_df, RAW_API2_TABLE, conn, RAW_SCHEMA, replace=True)
    conn.close()


def _normalize_api1_orders(payloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    orders: List[Dict[str, Any]] = []
    for payload in payloads:
        for order in _extract_api_list(payload):
            shipment = order.get("shipment", {}) or {}
            logistic = order.get("logistic", {}) or shipment.get("logistic", {}) or {}
            receiver = order.get("receiver", {}) or {}
            receiver_addr = receiver.get("address_detail", {}) or {}

            status_name = _normalize_text(shipment.get("status_name") or order.get("status_name"))
            status_upper = status_name.upper()
            # API1 return detection: keyword-based with explicit exclusions.
            return_keywords = [
                "RETURN",
                "RETUR",
                "RTS",
                "RETURN TO SENDER",
                "RECEIVER REFUSED TO ACCEPT",
                "CANCELLED BY COURIER (REFUSED)",
                "DELIVERY FAILED - OUT OF DELIVERY ATTEMPTS",
                "RETURN - ITEM DAMAGED",
                "CONSIGNEE UNREACHABLE - RETURN",
                "GAGAL ANTAR",
                "DITOLAK",
            ]
            non_return_keywords = [
                "IN PROCESS RETURN",
                "PEMBELI MENJADWALKAN ULANG WAKTU PENGIRIMAN",
                "PENJUAL BELUM SELESAI MENYIAPKAN PESANAN",
            ]
            is_return = any(k in status_upper for k in return_keywords) and not any(
                k in status_upper for k in non_return_keywords
            )
            if status_upper in ("RETURN", "RETUR"):
                status_name = "No Reason Provided"
            created_at = _to_datetime(order.get("created_at"))
            pickup_time = _to_datetime(shipment.get("pickup_time_date") or shipment.get("pickup_time"))
            event_dt = created_at or pickup_time

            is_cod = order.get("is_cod")
            cod_value = _to_number(order.get("cod")) if order.get("cod") is not None else _to_number(shipment.get("cod"))
            payment_method = _normalize_text(shipment.get("payment_method") or order.get("payment_method") or shipment.get("payment_method_rts"))

            orders.append(
                {
                    "source_system": "api1",
                    "order_id": _normalize_text(order.get("awb_number") or order.get("shipment_order_no")),
                    "event_date": event_dt.date() if event_dt else None,
                    "province": _normalize_text(receiver_addr.get("province")),
                    "city": _normalize_text(receiver_addr.get("city")),
                    "expedition": _normalize_text(logistic.get("name") or shipment.get("logistic_name")),
                    "service_type": _normalize_service_type(
                        logistic.get("rate_name") or logistic.get("rate_type_name")
                    ),
                    "payment_method": payment_method,
                    "cod_type": "COD" if is_cod or cod_value > 0 else "NON-COD",
                    "order_value": _to_number(shipment.get("total_price") or shipment.get("total") or order.get("total")),
                    "cod_value": cod_value,
                    "shipping_fee": _to_number(shipment.get("fee") or shipment.get("shipping_fee")),
                    "return_flag": 1 if is_return else 0,
                    "return_reason": status_name if is_return else "No Reason Provided",
                }
            )
    return orders


def _normalize_api2_orders(payloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for payload in payloads:
        for order in _extract_api_list(payload):
            base_info = order.get("base_info", {}) or {}
            order_info = order.get("order_info", {}) or {}
            deliver_info = order.get("deliver_info", {}) or {}
            tracking_info = order.get("tracking_info", {}) or {}
            finance_info = order.get("finance_info", {}) or {}

            event_dt = _to_datetime(order_info.get("ctime"))
            returning_start = tracking_info.get("returning_start_time")
            returned_time = tracking_info.get("returned_time")
            return_flag = 1 if (returning_start or returned_time) else 0
            raw_reason = tracking_info.get("latest_tracking_reason")
            if raw_reason in (None, "", "0", 0):
                raw_reason = tracking_info.get("reason_code")
            reason_text = _normalize_text(raw_reason)
            if reason_text in ("0", "unknown", "Unknown", "None"):
                reason_text = "No Reason Provided"
            if reason_text.isdigit():
                reason_text = f"Reason Code {reason_text}"

            items.append(
                {
                    "source_system": "api2",
                    "order_id": _normalize_text(order_info.get("order_sn") or order_info.get("order_id")),
                    "event_date": event_dt.date() if event_dt else None,
                    "province": _normalize_text(deliver_info.get("deliver_state")),
                    "city": _normalize_text(deliver_info.get("deliver_city")),
                    "expedition": _normalize_text(base_info.get("three_pl_name") or "SPX"),
                    "service_type": _normalize_service_type(base_info.get("product_name")),
                    "payment_method": _normalize_text(_get_nested(order, ["fulfillment_info", "payment_method_rts"]) or "unknown"),
                    "cod_type": "COD" if _to_number(finance_info.get("cod_service_fee") or order.get("cod_amount")) > 0 else "NON-COD",
                    "order_value": _to_number(finance_info.get("actual_shipping_fee") or finance_info.get("estimated_shipping_fee") or order.get("cod_amount")),
                    "cod_value": _to_number(order.get("cod_amount") or finance_info.get("cod_service_fee")),
                    "shipping_fee": _to_number(finance_info.get("actual_shipping_fee")),
                    "return_flag": return_flag,
                    "return_reason": reason_text if return_flag else "No Reason Provided",
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
    return _normalize_api2_orders(data)


def build_returns_mart() -> None:
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

    # load raw payloads (latest row)
    cur = conn.cursor()
    cur.execute(f'SELECT payload FROM "{RAW_SCHEMA}"."{RAW_API1_TABLE}" ORDER BY run_ts DESC LIMIT 1')
    api1_payloads = json.loads(cur.fetchone()[0])
    cur.execute(f'SELECT payload FROM "{RAW_SCHEMA}"."{RAW_API2_TABLE}" ORDER BY run_ts DESC LIMIT 1')
    api2_source_mode, api2_payloads = _decode_api2_payload_blob(cur.fetchone()[0])
    cur.close()

    # Legacy API1 normalization intentionally disabled.
    # api1_orders = _normalize_api1_orders(api1_payloads)
    api1_orders: List[Dict[str, Any]] = []
    api2_orders = _normalize_api2_source_data(api2_source_mode, api2_payloads)
    returns_raw = pd.DataFrame(api2_orders)

    # staging tables
    _df_to_postgres(pd.DataFrame(api1_orders, columns=NORMALIZED_ORDER_COLUMNS), STG_API1_ORDERS, conn, STAGING_SCHEMA, replace=True)
    _df_to_postgres(pd.DataFrame(api2_orders), STG_API2_ORDERS, conn, STAGING_SCHEMA, replace=True)

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


def quality_check_outputs() -> None:
    _env("DB_HOST")
    _env("DB_PORT")
    _env("DB_NAME")
    _env("DB_USER")
    _env("DB_PASSWORD")


with DAG(
    dag_id="returns_api_weekly",
    description="Fetch API data and build weekly return analytics for Tableau",
    schedule="0 1 * * 1",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "api", "returns", "tableau"],
) as dag:
    fetch_raw = PythonOperator(
        task_id="fetch_api_raw",
        python_callable=fetch_api_raw,
    )

    build_mart = PythonOperator(
        task_id="build_returns_mart",
        python_callable=build_returns_mart,
    )

    validate = PythonOperator(
        task_id="quality_check_outputs",
        python_callable=quality_check_outputs,
    )

    fetch_raw >> build_mart >> validate
