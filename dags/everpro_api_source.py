from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests

try:
    from returns_config import env
except ImportError:  # pragma: no cover - package import path for tests
    from dags.returns_config import env


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


def _extract_api_list(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    orders = _get_nested(payload, ["data", "orders"])
    if isinstance(orders, list):
        return orders
    items = _get_nested(payload, ["data", "data", "list"])
    if isinstance(items, list):
        return items
    return []


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
                retry_delay = min(sleep_seconds * (2**attempt), 20.0)
                print(
                    f"Retrying API request after network error on page={request_params.get('page')} "
                    f"attempt={attempt + 1}/{max_network_retries} delay={retry_delay:.1f}s error={exc}"
                )
                time.sleep(retry_delay)
        assert last_error is not None
        raise last_error

    while True:
        if page > max_pages:
            print(
                f"WARNING: Everpro API fetch stopped after hitting API_MAX_PAGES={max_pages} "
                f"({len(results)} pages collected so far). The last page was still full, so there "
                "may be more data beyond this cap; increase API_MAX_PAGES if the true order count "
                "is expected to be higher."
            )
            break
        params["page"] = page
        params["limit"] = limit
        resp = _request_page(params.copy())
        if resp.status_code == 429:
            for attempt in range(max_retries):
                time.sleep(sleep_seconds * (attempt + 1))
                resp = _request_page(params.copy())
                if resp.status_code != 429:
                    break
        if 500 <= resp.status_code < 600:
            for attempt in range(max_retries):
                time.sleep(sleep_seconds * (attempt + 1))
                resp = _request_page(params.copy())
                if resp.status_code < 500:
                    break
            if 500 <= resp.status_code < 600:
                if fatal_on_5xx:
                    resp.raise_for_status()
                break
        if resp.status_code == 401:
            raise RuntimeError(
                f"API returned 401 Unauthorized on page={params.get('page')}. "
                "Refresh the source access token in EVERPRO_API_TOKEN, then recreate/restart Airflow services."
            )
        resp.raise_for_status()
        payload = resp.json()
        results.append(payload)
        data_list = _extract_api_list(payload)
        if not data_list or len(data_list) < limit:
            break
        page += 1
        time.sleep(sleep_seconds)
    return results


def _everpro_headers() -> Dict[str, str]:
    token = env("EVERPRO_API_TOKEN").strip()
    return {
        "accept": "application/json, text/plain, */*",
        "authorization": f"Bearer {token}",
        "referer": "https://customer.everpro.id/order",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
        ),
    }


def fetch_everpro_api_payloads(start_date: str, end_date: str) -> List[Dict[str, Any]]:
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


def normalize_everpro_api_orders(payloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
                return_reason = " | ".join(
                    str(reason).strip() for reason in rts_reasons if str(reason).strip()
                )
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
                normalized_status = (
                    everpro_status_name.title() if everpro_status_name else "Unknown"
                )

            is_return = normalized_status == "Returned"
            failed_reason = ""
            if normalized_status in {"Returned", "Lost"}:
                failed_reason = return_reason or rts_status or everpro_status_name

            items.append(
                {
                    "source_system": "everpro_api",
                    "order_id": _normalize_text(
                        order.get("awb_number") or order.get("shipment_order_no")
                    ),
                    "event_date": event_dt.date() if event_dt else None,
                    "province": _normalize_text(receiver_addr.get("province")),
                    "city": _normalize_text(receiver_addr.get("city")),
                    "expedition": _normalize_text(logistic.get("name")),
                    "service_type": _normalize_service_type(
                        logistic.get("rate_type_name")
                        or logistic.get("rate_name")
                        or shipment.get("type")
                    ),
                    "payment_method": "COD" if order.get("is_cod") else "NON-COD",
                    "raw_payment_method": "COD" if order.get("is_cod") else "NON-COD",
                    "cod_type": "COD" if order.get("is_cod") else "NON-COD",
                    "order_value": _to_number(
                        cod.get("total") or order.get("package", {}).get("price")
                    ),
                    "cod_value": _to_number(cod.get("total")) if order.get("is_cod") else 0.0,
                    "shipping_fee": _to_number(
                        shipment.get("total_price") or shipment.get("price")
                    ),
                    "return_flag": 1 if is_return else 0,
                    "return_reason": return_reason
                    or rts_status
                    or failed_reason
                    or "No Reason Provided",
                    "delivery_status": normalized_status,
                    "raw_delivery_status": shipment_status,
                    "failed_reason": failed_reason,
                    "delay_reason": "",
                    "sender_province": _normalize_text(shipper_addr.get("province")),
                }
            )
    return items
