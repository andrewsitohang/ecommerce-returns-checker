from __future__ import annotations

import json
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
        parsed = pd.to_datetime(str(value).strip(), errors="coerce")
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
        ["data", "results"],
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
    value = _dig_value(payload, ["total", "totalCount", "total_count", "count"]) if isinstance(payload, dict) else None
    try:
        return int(value)
    except Exception:
        return None


DELIVERED_STATUSES = {"delivered", "terkirim", "diterima"}
RETURN_STATUSES = {"returned", "return", "return to sender", "rts", "retur", "dikembalikan"}
CANCEL_KEYWORDS = ("cancel", "canceled", "cancelled", "batal", "dibatalkan")


def _contains_word(text: str, keywords) -> bool:
    # Word-boundary match instead of plain substring: "delivered" must not match
    # inside "undelivered", the way `"delivered" in "undelivered"` would.
    return any(re.search(rf"\b{re.escape(keyword)}\b", text) for keyword in keywords)


def _infer_return_flag_and_reason(status_text: str) -> tuple[int, str]:
    status_lower = status_text.lower()
    if _contains_word(status_lower, CANCEL_KEYWORDS):
        return 0, "Cancelled"
    if _contains_word(status_lower, DELIVERED_STATUSES):
        return 0, "No Reason Provided"
    if _contains_word(status_lower, RETURN_STATUSES):
        return 1, status_text or "No Reason Provided"
    return 0, "No Reason Provided"


def _normalize_order(item: Dict[str, Any]) -> Dict[str, Any]:
    province = _dig_text(item, ["receiver_region", "receiver_province", "province", "destination_province"])
    city = _dig_text(item, ["receiver_city", "city", "destination_city"])
    service_type = _service_type_label(_dig_value(item, ["plan", "service_type", "product"]))
    created_at = _to_datetime(_dig_value(item, ["createdAt", "created_at", "order_date"]))
    status_text = _dig_text(item, ["status", "ticket_status", "delivery_status"], fallback="")
    return_flag, return_reason = _infer_return_flag_and_reason(status_text)
    cod_value = _to_number(_dig_value(item, ["cod_value", "cod_amount", "cod"]))

    return {
        "source_system": "mengantar_api",
        "order_id": _dig_text(item, ["sttNumber", "stt_number", "tracking", "tracking_number", "order_id"]),
        "event_date": created_at.date().isoformat() if created_at else None,
        "province": province,
        "city": city,
        "expedition": _dig_text(item, ["courier"], fallback="JNE"),
        "service_type": service_type,
        "payment_method": "COD" if cod_value > 0 else "NON-COD",
        "raw_payment_method": "COD" if cod_value > 0 else "NON-COD",
        "cod_type": "COD" if cod_value > 0 else "NON-COD",
        "order_value": _to_number(_dig_value(item, ["order_value", "item_value", "goods_amount"])),
        "cod_value": cod_value,
        "shipping_fee": _to_number(_dig_value(item, ["shipping_fee", "actual_shipping_fee", "price"])),
        "return_flag": return_flag,
        "return_reason": return_reason,
        "delivery_status": status_text,
        "created_at": created_at.isoformat() if created_at else None,
        "raw_mengantar_api_order": item,
    }


def _headers() -> Dict[str, str]:
    cookie = env("MENGANTAR_API").strip()
    return {
        "accept": "*/*",
        "accept-language": os.getenv("MENGANTAR_API_ACCEPT_LANGUAGE", "en-US,en;q=0.8"),
        "content-type": "application/json",
        "cookie": cookie,
        "origin": "https://app.mengantar.com",
        "referer": os.getenv("MENGANTAR_API_REFERER", "https://app.mengantar.com/tracking/search"),
        "user-agent": os.getenv(
            "MENGANTAR_API_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
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
    timeout_seconds = int(os.getenv("MENGANTAR_API_TIMEOUT_SECONDS", "60"))
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


def fetch_mengantar_api_records(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    api_url = os.getenv(
        "MENGANTAR_API_ORDER_URL",
        "https://app.mengantar.com/api/order/advanced",
    ).strip()
    start_dt = _to_datetime(start_date)
    end_dt = _to_datetime(end_date)
    if not start_dt or not end_dt:
        raise ValueError(f"Invalid Mengantar API date range: start_date={start_date}, end_date={end_date}")
    end_dt = end_dt.replace(hour=23, minute=59, second=59)

    page_size = int(os.getenv("MENGANTAR_API_PAGE_SIZE", "50"))
    max_pages = int(os.getenv("API_MAX_PAGES", "50"))
    sleep_seconds = float(os.getenv("API_RATE_SLEEP", "1.0"))
    courier = os.getenv("MENGANTAR_API_COURIER", "all")
    plan = os.getenv("MENGANTAR_API_PLAN", "Standart JNE")

    date_range = json.dumps(
        {
            "startDate": start_dt.strftime("%Y-%m-%dT00:00:00.000Z"),
            "endDate": end_dt.strftime("%Y-%m-%dT23:59:59.999Z"),
        }
    )

    session = requests.Session()
    records: List[Dict[str, Any]] = []
    for page_no in range(1, max_pages + 1):
        body = {
            "trackingSearch": True,
            "page": page_no,
            "size": page_size,
            "ticketFilter": "all",
            "receiverFilter": [],
            "dateRange": date_range,
            "courier": courier,
            "dateType": "createdAt",
            "attemptsFilter": "all",
            "ticketMultiStatus": "",
            "pickupDateFilter": "",
            "status": "{}",
            "cod": json.dumps({"COD": False, "NON_COD": False}),
            "tracking": "",
            "order": "",
            "keyword": "",
            "isArchived": "all",
            "plan": plan,
            "sort": "",
            "asc": -1,
            "address_id": "all",
            "sub_user_id": "all",
            "assignee": "all",
            "printedFilter": "all",
            "dontIncludeAssignee": False,
            "sttNumber": "",
            "returnReceipt": "",
            "reseller_id": "all",
        }
        response = _request_page(session, api_url, _headers(), body)
        if response.status_code == 401:
            raise RuntimeError(
                "Mengantar API returned 401 Unauthorized. Refresh MENGANTAR_API, then restart Airflow."
            )
        response.raise_for_status()

        payload = response.json()
        page_items = _extract_list(payload)
        records.extend(_normalize_order(item) for item in page_items)

        # The API's own "total"/"count" fields are only populated when the
        # request body also sets getCount=True, in which case "data" comes
        # back empty instead (it's a separate count-only query mode). So a
        # short page is the only reliable end-of-results signal here.
        total = _payload_total(payload)
        if not page_items or len(page_items) < page_size:
            break
        if total is not None and total > 0 and len(records) >= total:
            break
        time.sleep(sleep_seconds)

    deduped_records: List[Dict[str, Any]] = []
    seen_order_ids: set[str] = set()
    for record in records:
        order_id = str(record.get("order_id") or "")
        if not order_id or order_id == "No Value" or order_id in seen_order_ids:
            continue
        seen_order_ids.add(order_id)
        deduped_records.append(record)
    return deduped_records
