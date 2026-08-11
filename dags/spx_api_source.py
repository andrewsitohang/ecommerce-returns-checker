from __future__ import annotations

import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

try:
    from returns_config import env
except ImportError:  # pragma: no cover - package import path for tests
    from dags.returns_config import env


EXPORT_COLUMNS = {
    "tracking_no": "Nomer Resi",
    "customer_reference_no": "No Referensi Pelanggan",
    "created_at": "Waktu dibuat",
    "recipient_region": "Provinsi/Kota/Kecamatan Penerima",
    "payment_role": "Payment Role",
    "cod_collection_flag": "Koleksi COD",
    "cod_amount": "Jumlah COD",
    "parcel_value": "Nilai Parcel",
    "estimated_shipping_fee": "Estimasi Ongkir",
    "actual_shipping_fee": "Ongkir Aktual",
    "delay_reason": "Alasan Pengiriman Tertunda",
    "returned_to_sender_at": "Waktu pengembalian ke pengirim",
    "failed_reason": "Alasan pengiriman gagal",
    "original_pickup_option": "Opsi Penjemputan Awal",
    "actual_pickup_option": "Opsi Penjemputan Aktual",
    "delivery_status": "Status Pengiriman",
}

DELIVERED_STATUSES = {"delivered", "terkirim", "pesanan telah sampai"}
RETURN_STATUSES = {
    "returned",
    "returning",
    "return to sender",
    "returned to sender",
    "dikembalikan",
    "pengembalian",
    "retur",
}
FAILED_FINAL_STATUSES = {"damaged", "lost", "rusak", "hilang"}
IN_PROGRESS_STATUSES = {
    "in transit",
    "delivering",
    "pickup on hold",
    "on hold",
    "dalam pengiriman",
    "sedang dikirim",
    "tertunda",
}
CANCEL_KEYWORDS = ("cancel", "canceled", "cancelled", "batal", "dibatalkan")
PAYMENT_ROLE_LABELS = {
    "1": "Sender Paid",
}


def _text(value: Any, fallback: str = "No Value") -> str:
    if value is None:
        return fallback
    try:
        if pd.isna(value):
            return fallback
    except TypeError:
        pass
    text = str(value).strip()
    if text.lower() in {"nan", "nat", "none", "null"}:
        return fallback
    return text if text else fallback


def _to_datetime(value: Any) -> Optional[datetime]:
    if value is None or str(value).strip() == "":
        return None
    if isinstance(value, (int, float)) or str(value).strip().isdigit():
        try:
            number = float(value)
            if number <= 0:
                return None
            if number > 10_000_000_000:
                number = number / 1000
            return datetime.fromtimestamp(number)
        except (OSError, OverflowError, ValueError):
            return None
    try:
        text_value = str(value).strip()
        dayfirst = not (len(text_value) >= 10 and text_value[4] == "-" and text_value[7] == "-")
        parsed = pd.to_datetime(text_value, errors="coerce", dayfirst=dayfirst)
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def _to_number(value: Any) -> float:
    if value is None:
        return 0.0
    text = str(value).strip()
    if text in ("", "-", "No Value", "nan", "NaN"):
        return 0.0
    try:
        return float(text.replace(",", ""))
    except Exception:
        return 0.0


def _dig_value(obj: Any, names: List[str]) -> Any:
    if not isinstance(obj, dict):
        return None
    lowered = {name.lower() for name in names}
    for key, value in obj.items():
        if str(key).lower() in lowered:
            return value
    for value in obj.values():
        if isinstance(value, dict):
            nested = _dig_value(value, names)
            if nested is not None:
                return nested
    return None


def _dig_text(obj: Any, names: List[str], fallback: str = "No Value") -> str:
    return _text(_dig_value(obj, names), fallback=fallback)


def _split_region(value: Any) -> tuple[str, str]:
    text = _text(value)
    if text == "No Value":
        return "No Value", "No Value"
    parts = [part.strip() for part in text.split("/") if part.strip()]
    province = parts[0] if len(parts) >= 1 else "No Value"
    city = parts[1] if len(parts) >= 2 else "No Value"
    return province, city


def _infer_cod_type(payment_role: Any) -> tuple[str, str]:
    payment_text = _text(payment_role)
    lowered = payment_text.lower()
    if "non" in lowered and "cod" in lowered:
        return payment_text, "NON-COD"
    if "cod" in lowered:
        return payment_text, "COD"
    return payment_text, "NON-COD"


def _humanize_payment_role(payment_role: Any) -> str:
    payment_text = _text(payment_role)
    if payment_text == "No Value":
        return payment_text
    return PAYMENT_ROLE_LABELS.get(payment_text, f"Payment Role {payment_text}")


def _build_region(item: Dict[str, Any], prefix: str) -> str:
    direct = _dig_value(
        item,
        [
            f"{prefix}_region",
            f"{prefix}_area",
            f"{prefix}_location",
            f"{prefix}_address_region",
            f"{prefix}_full_address",
        ],
    )
    if direct:
        return _text(direct)
    parts = [
        _dig_text(item, [f"{prefix}_province", "province"], fallback=""),
        _dig_text(item, [f"{prefix}_city", "city"], fallback=""),
        _dig_text(item, [f"{prefix}_district", "district"], fallback=""),
    ]
    parts = [part for part in parts if part and part != "No Value"]
    return " / ".join(parts) if parts else "No Value"


def _service_type_label(value: Any) -> str:
    text = _text(value)
    lowered = text.lower()
    if "economy" in lowered or "eco" in lowered or "hemat" in lowered:
        return "Economy"
    if "standard" in lowered or "regular" in lowered or "reguler" in lowered:
        return "Reguler"
    return text


def _extract_list(payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    candidates = [
        ["data", "list"],
        ["data", "data", "list"],
        ["data", "orders"],
        ["data", "data", "orders"],
        ["data", "records"],
        ["data", "data", "records"],
        ["result", "list"],
    ]
    for path in candidates:
        current: Any = payload
        for key in path:
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(key)
        if isinstance(current, list):
            return [item for item in current if isinstance(item, dict)]
    for value in payload.values():
        if isinstance(value, list) and all(isinstance(item, dict) for item in value):
            return value
        nested = _extract_list(value)
        if nested:
            return nested
    return []


def _payload_total(payload: Any) -> Optional[int]:
    value = (
        _dig_value(payload, ["total", "total_count", "count"])
        if isinstance(payload, dict)
        else None
    )
    try:
        return int(value)
    except Exception:
        return None


def _contains_word(text: str, keywords: Any) -> bool:
    # Word-boundary match instead of plain substring: "delivered" must not match
    # inside "undelivered", the way `"delivered" in "undelivered"` would.
    return any(re.search(rf"\b{re.escape(keyword)}\b", text) for keyword in keywords)


def _infer_return_flag_and_reason(
    status_text: str,
    failed_reason: str,
    delay_reason: str,
    returned_at: Optional[datetime],
) -> tuple[int, str]:
    status_lower = status_text.lower()
    reason_text = f"{failed_reason} {delay_reason}".lower()

    if _contains_word(f"{status_lower} {reason_text}", CANCEL_KEYWORDS):
        return 0, "Cancelled"
    if _contains_word(status_lower, DELIVERED_STATUSES):
        return 0, "No Reason Provided"
    if (
        bool(returned_at)
        or _contains_word(status_lower, RETURN_STATUSES)
        or _contains_word(status_lower, {"return"})
    ):
        return 1, failed_reason or delay_reason or status_text or "No Reason Provided"
    if _contains_word(status_lower, FAILED_FINAL_STATUSES):
        return 1, failed_reason or delay_reason or status_text or "No Reason Provided"
    is_in_progress = _contains_word(status_lower, IN_PROGRESS_STATUSES)
    if failed_reason and not is_in_progress:
        return 1, failed_reason
    if failed_reason:
        return 0, failed_reason
    return 0, "No Reason Provided"


def _normalize_order(item: Dict[str, Any]) -> Dict[str, Any]:
    province = _dig_text(
        item,
        [
            "deliver_state",
            "recipient_state",
            "receiver_state",
            "recipient_province",
            "receiver_province",
            "province",
        ],
    )
    city = _dig_text(
        item,
        [
            "deliver_city",
            "recipient_city",
            "receiver_city",
            "city",
        ],
    )
    if province == "No Value" or city == "No Value":
        fallback_province, fallback_city = _split_region(_build_region(item, "recipient"))
        if province == "No Value":
            province = fallback_province
        if city == "No Value":
            city = fallback_city
    service_type = _service_type_label(
        _dig_value(
            item,
            [
                "service_type",
                "product_name",
                "service_name",
                "pickup_option",
                "original_pickup_option",
                "actual_pickup_option",
                EXPORT_COLUMNS["original_pickup_option"],
                EXPORT_COLUMNS["actual_pickup_option"],
            ],
        )
    )
    created_at = _to_datetime(
        _dig_value(
            item,
            [
                "created_at",
                "create_time",
                "created_time",
                "ctime",
                "order_create_time",
                EXPORT_COLUMNS["created_at"],
            ],
        )
    )
    returned_at = _to_datetime(
        _dig_value(
            item,
            [
                "returned_to_sender_at",
                "returned_time",
                "return_time",
                "returning_start_time",
                "rts_time",
                EXPORT_COLUMNS["returned_to_sender_at"],
            ],
        )
    )
    status_group = _dig_text(item, ["tracking_code_group_name"], fallback="")
    status_subgroup = _dig_text(item, ["tracking_code_subgroup_name"], fallback="")
    delivery_status = _dig_text(
        item,
        [
            "delivery_status",
            "tracking_status",
            "status",
            "order_status",
            "shipment_status",
            EXPORT_COLUMNS["delivery_status"],
        ],
        fallback="",
    )
    if not delivery_status and (status_group or status_subgroup):
        delivery_status = " / ".join(part for part in [status_group, status_subgroup] if part)
    failed_reason = _dig_text(
        item,
        [
            "failed_reason",
            "failure_reason",
            "delivery_failed_reason",
            "latest_tracking_reason",
            EXPORT_COLUMNS["failed_reason"],
        ],
        fallback="",
    )
    delay_reason = _dig_text(
        item,
        [
            "delay_reason",
            "onhold_reason",
            "delivery_onhold_reason",
            "delivery_on_hold_reason",
            EXPORT_COLUMNS["delay_reason"],
        ],
        fallback="",
    )
    return_flag, return_reason = _infer_return_flag_and_reason(
        delivery_status,
        failed_reason,
        delay_reason,
        returned_at,
    )

    cod_amount = _to_number(
        _dig_value(item, ["cod_amount", "cod_value", EXPORT_COLUMNS["cod_amount"]])
    )
    cod_flag = _dig_text(
        item, ["cod_collection_flag", "is_cod", EXPORT_COLUMNS["cod_collection_flag"]], fallback=""
    )
    payment_role = _dig_value(
        item, ["payment_role", "payment_method", "payment_type", EXPORT_COLUMNS["payment_role"]]
    )
    raw_payment_method, payment_cod_type = _infer_cod_type(payment_role)
    cod_type = (
        "COD"
        if cod_amount > 0 or str(cod_flag).lower() in {"1", "true", "y", "yes", "cod"}
        else payment_cod_type
    )
    payment_method = cod_type
    estimated_shipping_fee = _to_number(
        _dig_value(
            item,
            ["estimated_shipping_fee", "shipping_fee", EXPORT_COLUMNS["estimated_shipping_fee"]],
        )
    )
    actual_shipping_fee = _to_number(
        _dig_value(
            item, ["actual_shipping_fee", "actual_fee", EXPORT_COLUMNS["actual_shipping_fee"]]
        )
    )

    return {
        "source_system": "spx_api",
        "order_id": _dig_text(
            item,
            [
                "tracking_no",
                "tracking_number",
                "tracking_code",
                "shipment_id",
                "shipment_no",
                "order_no",
                "order_id",
                EXPORT_COLUMNS["tracking_no"],
            ],
        ),
        "event_date": created_at.date().isoformat() if created_at else None,
        "province": province,
        "city": city,
        "expedition": "SPX",
        "service_type": service_type,
        "payment_method": payment_method,
        "raw_payment_method": _humanize_payment_role(raw_payment_method),
        "cod_type": cod_type,
        "order_value": _to_number(
            _dig_value(
                item,
                [
                    "parcel_value",
                    "order_value",
                    "item_value",
                    "express_insured_value",
                    EXPORT_COLUMNS["parcel_value"],
                ],
            )
        ),
        "cod_value": cod_amount,
        "shipping_fee": actual_shipping_fee if actual_shipping_fee > 0 else estimated_shipping_fee,
        "return_flag": return_flag,
        "return_reason": return_reason,
        "customer_reference_no": _dig_text(
            item,
            [
                "customer_reference_no",
                "reference_no",
                "ref_no",
                EXPORT_COLUMNS["customer_reference_no"],
            ],
        ),
        "delivery_status": delivery_status,
        "failed_reason": failed_reason,
        "delay_reason": delay_reason,
        "created_at": created_at.isoformat() if created_at else None,
        "returned_to_sender_at": returned_at.isoformat() if returned_at else None,
        "raw_spx_api_order": item,
    }


def _headers() -> Dict[str, str]:
    token = env("SPX_API_SPX_TOKEN", "").strip()
    sid = env("SPX_API_SPX_SID", "").strip()
    if not token or not sid:
        raise ValueError("SPX API requires both SPX_API_SPX_TOKEN and SPX_API_SPX_SID.")
    cookie = (
        f"spx_token={token}; spx_sid={sid}; login_type=1; "
        "login_status=true; nss_sys_type=true; nss_cid=ID"
    )
    return {
        "accept": "application/json, text/plain, */*",
        "accept-language": os.getenv("SPX_API_ACCEPT_LANGUAGE", "en-US,en;q=0.5"),
        "content-type": "application/json;charset=UTF-8",
        "cookie": cookie,
        "origin": "https://spx.co.id",
        "referer": os.getenv("SPX_API_REFERER", "https://spx.co.id/spx-admin/order/trackings"),
        "user-agent": os.getenv(
            "SPX_API_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
        ),
    }


def _request_page(
    session: requests.Session,
    api_url: str,
    headers: Dict[str, str],
    body: Dict[str, Any],
) -> requests.Response:
    sleep_seconds = float(os.getenv("API_RATE_SLEEP", "1.0"))
    max_retries = int(os.getenv("API_MAX_RETRIES", "5"))
    timeout_seconds = int(os.getenv("SPX_API_TIMEOUT_SECONDS", "60"))
    response: Optional[requests.Response] = None
    for attempt in range(max_retries + 1):
        try:
            response = session.post(api_url, headers=headers, json=body, timeout=timeout_seconds)
            if response.status_code != 429 and response.status_code < 500:
                return response
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            if attempt >= max_retries:
                raise
        time.sleep(min(sleep_seconds * (attempt + 1), 20.0))
    assert response is not None
    return response


def fetch_spx_api_records(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    api_url = os.getenv(
        "SPX_API_LIST_ALL_ORDER_URL",
        "https://spx.co.id/shipment/order/logistic/order/list_all_order",
    ).strip()
    start_dt = _to_datetime(start_date)
    end_dt = _to_datetime(end_date)
    if not start_dt or not end_dt:
        raise ValueError(
            f"Invalid SPX API date range: start_date={start_date}, end_date={end_date}"
        )

    page_size = int(os.getenv("SPX_API_PAGE_SIZE", "100"))
    max_pages = int(os.getenv("API_MAX_PAGES", "50"))
    sleep_seconds = float(os.getenv("API_RATE_SLEEP", "1.0"))
    end_dt = end_dt.replace(hour=23, minute=59, second=59)

    session = requests.Session()
    records: List[Dict[str, Any]] = []
    for page_no in range(1, max_pages + 1):
        body = {
            "issue_type_list": [],
            "page_size": page_size,
            "offset": (page_no - 1) * page_size,
            "date_range_list": [
                {
                    "type": int(os.getenv("SPX_API_DATE_RANGE_TYPE", "1")),
                    "start": int(start_dt.timestamp()),
                    "end": int(end_dt.timestamp()),
                }
            ],
            "tracking_code_info_list": [],
            "language": os.getenv("SPX_API_LANGUAGE", "id"),
        }
        response = _request_page(session, api_url, _headers(), body)
        if response.status_code == 401:
            raise RuntimeError(
                "SPX API returned 401 Unauthorized. Refresh SPX API token and sid, then restart Airflow."
            )
        response.raise_for_status()

        payload = response.json()
        if isinstance(payload, dict) and payload.get("retcode") not in (0, None):
            raise RuntimeError(
                f"SPX API returned an error payload on page {page_no} despite HTTP "
                f"{response.status_code}: retcode={payload.get('retcode')} "
                f"message={payload.get('message')!r} detail={payload.get('detail')!r}. "
                "This is an SPX-side backend error (not an auth/cookie problem); the task "
                "will retry per the DAG's retry policy."
            )
        page_items = _extract_list(payload)
        records.extend(_normalize_order(item) for item in page_items)

        total = _payload_total(payload)
        if not page_items or len(page_items) < page_size:
            break
        if total is not None and len(records) >= total:
            break
        time.sleep(sleep_seconds)
    else:
        print(
            f"WARNING: SPX API fetch stopped after hitting API_MAX_PAGES={max_pages} "
            f"({len(records)} records collected so far) for range {start_date}..{end_date}. "
            "The last page was still full, so there may be more data beyond this cap; "
            "increase API_MAX_PAGES if the true order count is expected to be higher."
        )

    deduped_records: List[Dict[str, Any]] = []
    seen_order_ids: set[str] = set()
    for record in records:
        order_id = str(record.get("order_id") or "")
        if not order_id or order_id == "No Value" or order_id in seen_order_ids:
            continue
        seen_order_ids.add(order_id)
        deduped_records.append(record)
    return deduped_records
