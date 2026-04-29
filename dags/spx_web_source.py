from __future__ import annotations

import json
import os
import tempfile
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


SPX_DOWNLOAD_DEBUG_VERSION = "task-panel-v6"


EXPORT_COLUMNS = {
    "tracking_no": "Nomer Resi",
    "customer_reference_no": "No Referensi Pelanggan",
    "created_at": "Waktu dibuat",
    "recipient_name": "Nama Penerima",
    "recipient_phone": "Nomer Telepon Penerima",
    "recipient_region": "Provinsi/Kota/Kecamatan Penerima",
    "recipient_address": "Alamat Detail Penerima",
    "pickup_region": "Provinsi/Kota/Kecamatan Penjemputan",
    "pickup_address": "Alamat Detail Penjemputan",
    "scheduled_pickup_at": "Waktu Penjemputan Terjadwal",
    "actual_pickup_at": "Waktu Penjemputan/Drop Off Aktual",
    "delivered_at": "Waktu Terkirim",
    "payment_role": "Payment Role",
    "original_pickup_option": "Opsi Penjemputan Awal",
    "actual_pickup_option": "Opsi Penjemputan Aktual",
    "cod_collection_flag": "Koleksi COD",
    "cod_amount": "Jumlah COD",
    "parcel_value": "Nilai Parcel",
    "estimated_shipping_fee": "Estimasi Ongkir",
    "actual_shipping_fee": "Ongkir Aktual",
    "delayed_at": "Waktu Penundaan Pengiriman",
    "delay_reason": "Alasan Pengiriman Tertunda",
    "returned_to_sender_at": "Waktu pengembalian ke pengirim",
    "failed_reason": "Alasan pengiriman gagal",
    "creation_method": "Metode Pembuatan",
    "created_by": "Dibuat oleh",
    "delivery_status": "Status Pengiriman",
}

EXPORT_COLUMN_ALIASES = {
    "tracking_no": ["Nomer Resi", "Tracking No."],
    "customer_reference_no": ["No Referensi Pelanggan", "Customer Reference No."],
    "created_at": ["Waktu dibuat", "Create Time"],
    "recipient_name": ["Nama Penerima", "Recipient Name"],
    "recipient_phone": ["Nomer Telepon Penerima", "Recipient Phone Number"],
    "recipient_region": ["Provinsi/Kota/Kecamatan Penerima"],
    "recipient_address": ["Alamat Detail Penerima", "Recipient Detail Address"],
    "pickup_region": ["Provinsi/Kota/Kecamatan Penjemputan"],
    "pickup_address": ["Alamat Detail Penjemputan", "Sender Detail Address"],
    "scheduled_pickup_at": ["Waktu Penjemputan Terjadwal", "Scheduled Pickup Time"],
    "actual_pickup_at": ["Waktu Penjemputan/Drop Off Aktual", "Actual Pickup/Drop Off Time"],
    "delivered_at": ["Waktu Terkirim", "Delivered Time"],
    "payment_role": ["Payment Role"],
    "original_pickup_option": ["Opsi Penjemputan Awal", "Original pickup option", "Original Pickup Option"],
    "actual_pickup_option": ["Opsi Penjemputan Aktual", "Actual pickup option"],
    "cod_collection_flag": ["Koleksi COD", "COD Collection(Y/N)"],
    "cod_amount": ["Jumlah COD", "COD Amount"],
    "parcel_value": ["Nilai Parcel", "Parcel Value"],
    "estimated_shipping_fee": ["Estimasi Ongkir", "Estimated Shipping Fee"],
    "actual_shipping_fee": ["Ongkir Aktual", "Actual Shipping Fee"],
    "delayed_at": ["Waktu Penundaan Pengiriman", "Delivery OnHold Times"],
    "delay_reason": ["Alasan Pengiriman Tertunda", "Delivery OnHold Reason"],
    "returned_to_sender_at": ["Waktu pengembalian ke pengirim", "Returning Start Time"],
    "failed_reason": ["Alasan pengiriman gagal", "Delivery failed Reason"],
    "creation_method": ["Metode Pembuatan", "Create Method"],
    "created_by": ["Dibuat oleh", "Order Creator"],
    "delivery_status": ["Status Pengiriman", "Tracking Status"],
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
    try:
        dt = pd.to_datetime(value, errors="coerce", dayfirst=True)
    except Exception:
        return None
    if pd.isna(dt):
        return None
    return dt.to_pydatetime()


def _to_date_text(value: Any) -> str:
    dt = _to_datetime(value)
    if not dt:
        return str(value).strip()
    return dt.strftime("%Y-%m-%d")


def _normalize_export_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(col).strip() for col in df.columns]
    if df.empty:
        return df

    first_row = [str(value).strip() for value in df.iloc[0].tolist()]
    known_aliases = {alias for aliases in EXPORT_COLUMN_ALIASES.values() for alias in aliases}
    recognized = sum(1 for value in first_row if value in known_aliases)
    if recognized >= 5:
        df.columns = first_row
        df = df.iloc[1:].reset_index(drop=True)

    for field_name, aliases in EXPORT_COLUMN_ALIASES.items():
        canonical = EXPORT_COLUMNS[field_name]
        if canonical in df.columns:
            continue
        for alias in aliases:
            if alias in df.columns:
                df = df.rename(columns={alias: canonical})
                break

    if EXPORT_COLUMNS["recipient_region"] not in df.columns:
        province = df.get("Recipient Province")
        city = df.get("Recipient City")
        district = df.get("Recipient District")
        if province is not None and city is not None and district is not None:
            df[EXPORT_COLUMNS["recipient_region"]] = (
                province.fillna("").astype(str).str.strip()
                + " / "
                + city.fillna("").astype(str).str.strip()
                + " / "
                + district.fillna("").astype(str).str.strip()
            )

    if EXPORT_COLUMNS["pickup_region"] not in df.columns:
        province = df.get("Sender Province")
        city = df.get("Sender City")
        district = df.get("Sender District")
        if province is not None and city is not None and district is not None:
            df[EXPORT_COLUMNS["pickup_region"]] = (
                province.fillna("").astype(str).str.strip()
                + " / "
                + city.fillna("").astype(str).str.strip()
                + " / "
                + district.fillna("").astype(str).str.strip()
            )

    return df


def _selector_candidates(env_name: str, defaults: List[str]) -> List[str]:
    configured = os.getenv(env_name, "").strip()
    if not configured:
        return defaults
    candidates = [item.strip() for item in configured.split(",") if item.strip()]
    for selector in defaults:
        if selector not in candidates:
            candidates.append(selector)
    return candidates


def _set_input_value(locator: Any, value: str) -> None:
    locator.scroll_into_view_if_needed()
    try:
        locator.click()
    except Exception:
        pass
    try:
        locator.fill(value)
        return
    except Exception:
        pass
    try:
        locator.evaluate(
            """(el, val) => {
                el.value = val;
                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
                el.dispatchEvent(new Event('blur', { bubbles: true }));
            }""",
            value,
        )
        return
    except Exception:
        pass
    locator.press("Control+A")
    locator.type(value)


def _fill_first_matching(page: Any, selectors: List[str], value: str, timeout_ms: int) -> str:
    last_error: Optional[Exception] = None
    per_selector_timeout = max(2000, timeout_ms // max(len(selectors), 1))
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            locator.wait_for(state="visible", timeout=per_selector_timeout)
            _set_input_value(locator, value)
            return selector
        except Exception as exc:
            last_error = exc
    raise TimeoutError(f"No visible input found for selectors: {selectors}") from last_error


def _write_debug_artifacts(page: Any, output_dir: Path, prefix: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        page.screenshot(path=str(output_dir / f"{prefix}.png"), full_page=True)
    except Exception:
        pass
    try:
        (output_dir / f"{prefix}.html").write_text(page.content(), encoding="utf-8")
    except Exception:
        pass


def _write_control_debug(page: Any, output_dir: Path, prefix: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    lines: List[str] = [f"debug_version\t{SPX_DOWNLOAD_DEBUG_VERSION}"]
    selectors = ["button", "a", "[role='button']"]
    for selector in selectors:
        locator = page.locator(selector)
        try:
            count = min(locator.count(), 200)
        except Exception:
            count = 0
        for idx in range(count):
            item = locator.nth(idx)
            try:
                if not item.is_visible():
                    continue
                text = item.inner_text().strip()
                if not text:
                    continue
                lowered = text.lower()
                if not any(token in lowered for token in ["unduh", "download", "ekspor", "export", "berhasil", "paket"]):
                    continue
                html = item.evaluate("(el) => el.outerHTML")
                context = item.evaluate(
                    """(el) => {
                        const container = el.closest('li, tr, [role="dialog"], [class*="popover"], [class*="dropdown"], [class*="drawer"], div');
                        if (!container) return '';
                        const text = (container.innerText || '').replace(/\\s+/g, ' ').trim();
                        return text.slice(0, 500);
                    }"""
                )
                lines.append(f"{selector}\t{text}\tcontext={context}\t{html}")
            except Exception:
                continue
    (output_dir / f"{prefix}.txt").write_text("\n".join(lines), encoding="utf-8")


def _write_task_panel_debug(page: Any, output_dir: Path, prefix: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    selectors = [
        ".ssc-popover-popper",
        ".ssc-popover-content",
        "[class*='popover-popper']",
        "[class*='popover-content']",
        "div[class*='popover']",
        ".ssc-popover",
        "div[class*='dropdown']",
        "div[class*='drawer']",
        "[role='dialog']",
    ]
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if not locator.is_visible():
                continue
            text = locator.evaluate(
                "(el) => (el.innerText || '').replace(/\\s+/g, ' ').trim().slice(0, 4000)"
            )
            html = locator.evaluate("(el) => el.outerHTML")
            (output_dir / f"{prefix}.txt").write_text(f"selector={selector}\ntext={text}\n\n{html}", encoding="utf-8")
            return
        except Exception:
            continue


def _wait_for_first_visible(page: Any, selectors: List[str], timeout_ms: int) -> Any:
    if not selectors:
        raise ValueError("selectors must not be empty")
    per_selector_timeout = max(3000, timeout_ms // max(len(selectors), 1))
    last_error: Optional[Exception] = None
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            locator.wait_for(state="visible", timeout=per_selector_timeout)
            return locator
        except Exception as exc:
            last_error = exc
    raise TimeoutError(f"No visible element found for selectors: {selectors}") from last_error


def _is_login_url(url: str) -> bool:
    return "authenticate/login" in url or "account.spx.co.id" in url


def _wait_for_page_ready(page: Any, timeout_ms: int, *, allow_networkidle_timeout: bool = True) -> None:
    page.wait_for_load_state("load", timeout=timeout_ms)
    try:
        page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 10000))
    except Exception:
        if not allow_networkidle_timeout:
            raise


def _page_contains_any_text(page: Any, texts: List[str]) -> bool:
    for text in texts:
        try:
            if page.locator(f"text={text}").first.count() > 0:
                return True
        except Exception:
            continue
    return False


def _wait_for_any_visible_text(page: Any, texts: List[str], timeout_ms: int) -> None:
    if not texts:
        raise ValueError("texts must not be empty")
    per_text_timeout = max(3000, timeout_ms // max(len(texts), 1))
    last_error: Optional[Exception] = None
    for text in texts:
        locator = page.locator(f"text={text}").first
        try:
            locator.wait_for(state="visible", timeout=per_text_timeout)
            return
        except Exception as exc:
            last_error = exc
    raise TimeoutError(f"No visible text found for: {texts}") from last_error


def _click_export_result_download(page: Any, timeout_ms: int) -> None:
    ready_texts = [
        "Ekspor Pelacakan Paket Berhasil",
        "Mengekspor Paket",
        "Unduh",
    ]
    _wait_for_any_visible_text(page, ready_texts, min(timeout_ms, 15000))

    buttons = page.locator("button:has-text('Unduh'), a:has-text('Unduh')")
    deadline = time.time() + (timeout_ms / 1000.0)
    while time.time() < deadline:
        visible_buttons = []
        try:
            count = buttons.count()
        except Exception:
            count = 0
        for idx in range(count):
            candidate = buttons.nth(idx)
            try:
                if candidate.is_visible():
                    visible_buttons.append(candidate)
            except Exception:
                continue
        if visible_buttons:
            button = visible_buttons[-1]
            try:
                button.scroll_into_view_if_needed()
            except Exception:
                pass
            try:
                button.click(force=True)
            except Exception:
                button.evaluate("(el) => el.click()")
            return
        time.sleep(1)
    raise TimeoutError("Unable to find export result download button after opening SPX export panel.")


def _click_successful_export_download(page: Any, timeout_ms: int) -> bool:
    try:
        _wait_for_any_visible_text(page, ["Ekspor Pelacakan Paket Berhasil"], min(timeout_ms, 10000))
    except Exception:
        return False
    success_buttons = page.locator(
        "xpath=(//*[contains(@class,'desc') and contains(normalize-space(.), 'Ekspor Pelacakan Paket Berhasil')]"
        "/ancestor::*[contains(@class,'content')][1]//*[self::button or self::a][contains(normalize-space(.), 'Unduh')])"
    )
    try:
        count = success_buttons.count()
    except Exception:
        count = 0
    for idx in range(count):
        button = success_buttons.nth(idx)
        try:
            if not button.is_visible():
                continue
            button.scroll_into_view_if_needed()
            try:
                button.click(force=True)
            except Exception:
                button.evaluate("(el) => el.click()")
            return True
        except Exception:
            continue
    return False


def _open_task_panel_download(
    page: Any,
    timeout_ms: int,
    latest_task_id: Optional[int] = None,
    debug_dir: Optional[Path] = None,
    debug_events: Optional[List[str]] = None,
) -> bool:
    def _debug(message: str) -> None:
        if debug_events is not None:
            debug_events.append(message)

    task_button_selectors = [
        "[data-chain='navTaskBtn']",
        ".ssc-popover-reference:has(.task-icon-container)",
        ".task-icon-container-unread",
        ".task-icon-container",
        "section[data-chain='navTaskBtn']",
    ]
    task_panel_selectors = [
        ".ssc-popover-popper",
        ".ssc-popover-content",
        "[class*='popover-popper']",
        "[class*='popover-content']",
        "div[class*='popover']:has-text('Unduh')",
        "div[class*='popover']:has-text('Ekspor')",
        "div[class*='popover']:has-text('Mengekspor')",
        "div[class*='dropdown']",
        "div[class*='drawer']",
        "[role='dialog']",
    ]
    download_button_selectors = [
        ".ssc-popover-popper button:has-text('Unduh')",
        ".ssc-popover-popper a:has-text('Unduh')",
        ".ssc-popover-content button:has-text('Unduh')",
        ".ssc-popover-content a:has-text('Unduh')",
        "[class*='popover-popper'] button:has-text('Unduh')",
        "[class*='popover-popper'] a:has-text('Unduh')",
        "[class*='popover-content'] button:has-text('Unduh')",
        "[class*='popover-content'] a:has-text('Unduh')",
        ".ssc-popover button.ssc-button-small:has-text('Unduh')",
        ".ssc-popover a.ssc-button-small:has-text('Unduh')",
        "div[class*='popover'] button.ssc-button-small:has-text('Unduh')",
        "div[class*='popover'] a.ssc-button-small:has-text('Unduh')",
        "div[class*='dropdown'] button.ssc-button-small:has-text('Unduh')",
        "div[class*='dropdown'] a.ssc-button-small:has-text('Unduh')",
        "div[class*='drawer'] button.ssc-button-small:has-text('Unduh')",
        "div[class*='drawer'] a.ssc-button-small:has-text('Unduh')",
        "button.ssc-button-small:has-text('Unduh')",
        "a.ssc-button-small:has-text('Unduh')",
    ]
    try:
        _debug(f"task_panel_open_attempt\t{latest_task_id or 'unknown'}")
        task_button = _wait_for_first_visible(page, task_button_selectors, min(timeout_ms, 10000))
        try:
            task_button.hover()
            page.wait_for_timeout(500)
        except Exception:
            pass
        task_button.click(force=True)
        page.wait_for_timeout(1000)
    except Exception as exc:
        _debug(f"task_panel_open_failed\t{type(exc).__name__}")
        return False

    try:
        try:
            _wait_for_first_visible(page, task_panel_selectors, min(timeout_ms, 10000))
        except Exception:
            task_button.click(force=True)
            page.wait_for_timeout(1000)
            _wait_for_first_visible(page, task_panel_selectors, min(timeout_ms, 10000))
        _debug("task_panel_opened")
    except Exception as exc:
        _debug(f"task_panel_wait_failed\t{type(exc).__name__}")
        return False

    if latest_task_id is not None:
        try:
            _wait_for_any_visible_text(page, [str(latest_task_id)], 3000)
        except Exception:
            pass
    try:
        _wait_for_any_visible_text(
            page,
            ["Unduh", "Ekspor Pelacakan Paket Berhasil", "Mengekspor Paket", "export", "Export"],
            5000,
        )
    except Exception:
        pass

    if debug_dir is not None:
        try:
            _write_task_panel_debug(page, debug_dir, "spx_task_panel_debug")
        except Exception as exc:
            _debug(f"task_panel_debug_failed\t{type(exc).__name__}")

    if latest_task_id is not None:
        task_specific_buttons = page.locator(
            "xpath=("
            f"//*[contains(normalize-space(.), '{latest_task_id}')]"
            "/ancestor::*[self::li or self::div or self::tr][1]"
            "//*[contains(@class,'ssc-button-small') and contains(normalize-space(.), 'Unduh')]"
            ")"
        )
        try:
            count = min(task_specific_buttons.count(), 5)
        except Exception:
            count = 0
        for idx in range(count):
            button = task_specific_buttons.nth(idx)
            try:
                if not button.is_visible():
                    continue
                button.scroll_into_view_if_needed()
                try:
                    button.click(force=True)
                except Exception:
                    button.evaluate("(el) => el.click()")
                _debug(f"task_panel_click_task_button\t{latest_task_id}\t{idx}")
                return True
            except Exception:
                continue

    for selector in download_button_selectors:
        locator = page.locator(selector)
        try:
            count = min(locator.count(), 20)
        except Exception:
            count = 0
        for idx in range(count):
            button = locator.nth(idx)
            try:
                if not button.is_visible():
                    continue
                button.scroll_into_view_if_needed()
                try:
                    button.click(force=True)
                except Exception:
                    button.evaluate("(el) => el.click()")
                _debug(f"task_panel_click_button\t{selector}\t{idx}")
                return True
            except Exception:
                continue
    _debug("task_panel_download_button_not_found")
    return False


def _guess_export_extension(content_type: str, url: str) -> str:
    lowered_type = (content_type or "").lower()
    lowered_url = (url or "").lower()
    if "csv" in lowered_type or lowered_url.endswith(".csv"):
        return ".csv"
    if (
        "spreadsheetml" in lowered_type
        or "excel" in lowered_type
        or lowered_url.endswith(".xlsx")
    ):
        return ".xlsx"
    if lowered_url.endswith(".xls"):
        return ".xls"
    return ".xlsx"


def _looks_like_export_response(response: Any) -> bool:
    try:
        request = response.request
        resource_type = str(getattr(request, "resource_type", "")).lower()
    except Exception:
        resource_type = ""
    if resource_type and resource_type not in {"fetch", "xhr", "document", "other"}:
        return False
    try:
        headers = {str(k).lower(): str(v) for k, v in response.headers.items()}
    except Exception:
        headers = {}
    content_type = headers.get("content-type", "").lower()
    content_disposition = headers.get("content-disposition", "").lower()
    url = str(getattr(response, "url", "")).lower()
    if "json" in content_type:
        return False
    if any(token in url for token in ["/task", "task_status", "task-id", "task_id"]):
        return False
    if "attachment" in content_disposition:
        return True
    if any(token in content_type for token in ["spreadsheet", "excel", "csv"]):
        return True
    if any(token in url for token in ["/download", "/export", ".xlsx", ".xls", ".csv"]):
        return True
    return False


def _save_response_export(response: Any, download_dir: Path) -> Optional[Path]:
    if not _looks_like_export_response(response):
        return None
    try:
        payload = response.body()
    except Exception:
        return None
    if not payload:
        return None
    try:
        decoded = json.loads(payload.decode("utf-8"))
        if isinstance(decoded, dict) and "task_id" in decoded and "task_status" in decoded:
            return None
    except Exception:
        pass
    try:
        headers = {str(k).lower(): str(v) for k, v in response.headers.items()}
    except Exception:
        headers = {}
    extension = _guess_export_extension(headers.get("content-type", ""), str(getattr(response, "url", "")))
    export_path = download_dir / f"spx_export_fallback{extension}"
    export_path.write_bytes(payload)
    return export_path


def _walk_json_strings(value: Any) -> List[str]:
    values: List[str] = []
    if isinstance(value, dict):
        for nested in value.values():
            values.extend(_walk_json_strings(nested))
    elif isinstance(value, list):
        for nested in value:
            values.extend(_walk_json_strings(nested))
    elif isinstance(value, str):
        values.append(value.strip())
    return values


def _normalize_candidate_url(value: str) -> Optional[str]:
    text = str(value).strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        return text
    if text.startswith("/"):
        return f"https://spx.co.id{text}"
    return None


def _extract_export_download_urls(payload: Any) -> List[str]:
    candidates: List[str] = []
    seen: set[str] = set()
    for value in _walk_json_strings(payload):
        normalized = _normalize_candidate_url(value)
        if not normalized:
            continue
        lowered = normalized.lower()
        if not any(token in lowered for token in ["download", "export", ".xlsx", ".xls", ".csv"]):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(normalized)
    return candidates


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
    if payment_text == "No Value":
        return payment_text, "NON-COD"
    return payment_text, "NON-COD"


def _to_number(value: Any) -> float:
    if value is None:
        return 0.0
    text = str(value).strip()
    if text in ("", "-", "No Value", "nan", "NaN"):
        return 0.0
    text = text.replace(",", "")
    try:
        return float(text)
    except Exception:
        return 0.0


def _infer_cod_fields(row: pd.Series) -> tuple[str, float]:
    cod_amount = _to_number(row.get(EXPORT_COLUMNS["cod_amount"]))
    cod_flag = _text(row.get(EXPORT_COLUMNS["cod_collection_flag"]), fallback="").lower()
    is_cod = cod_amount > 0 or cod_flag in ("y", "yes", "cod", "true")
    return ("COD" if is_cod else "NON-COD"), cod_amount


def _infer_return_flag_and_reason(row: pd.Series) -> tuple[int, str]:
    returned_at = _text(row.get(EXPORT_COLUMNS["returned_to_sender_at"]), fallback="")
    failed_reason = _text(row.get(EXPORT_COLUMNS["failed_reason"]), fallback="")
    delay_reason = _text(row.get(EXPORT_COLUMNS["delay_reason"]), fallback="")
    status_text = _text(row.get(EXPORT_COLUMNS["delivery_status"]), fallback="")

    cancel_keywords = ("cancel", "canceled", "cancelled", "batal", "dibatalkan")
    status_lower = status_text.lower()
    reason_text = f"{failed_reason} {delay_reason}".lower()
    delivered_statuses = {"delivered"}
    return_statuses = {"returned", "returning"}
    failed_final_statuses = {"damaged", "lost"}
    in_progress_statuses = {"in transit", "delivering", "pickup on hold", "on hold"}

    if any(key in f"{status_lower} {reason_text}" for key in cancel_keywords):
        return 0, "Cancelled"

    if status_lower in delivered_statuses:
        return 0, "No Reason Provided"

    if bool(returned_at) or status_lower in return_statuses:
        return 1, failed_reason or delay_reason or status_text or "No Reason Provided"

    if status_lower in failed_final_statuses:
        return 1, failed_reason or delay_reason or status_text or "No Reason Provided"

    if failed_reason and status_lower not in in_progress_statuses:
        return 1, failed_reason

    if failed_reason:
        return 0, failed_reason
    return 0, "No Reason Provided"


def load_spx_export_records(path: str | Path) -> List[Dict[str, Any]]:
    export_path = Path(path)
    suffix = export_path.suffix.lower()
    raw_head = export_path.read_bytes()[:4096]
    head_lower = raw_head.lower()

    if head_lower.lstrip().startswith(b"{") and b'"task_id"' in head_lower and b'"task_status"' in head_lower:
        raise ValueError(
            f"SPX captured export task metadata instead of the exported file: {export_path.name}"
        )

    if raw_head.startswith(b"PK") and suffix in (".xlsx", ".xls", ".zip", ""):
        df = pd.read_excel(export_path, engine="openpyxl")
    elif raw_head.startswith(b"\xd0\xcf\x11\xe0"):
        df = pd.read_excel(export_path)
    elif head_lower.lstrip().startswith(b"<!doctype html") or head_lower.lstrip().startswith(b"<html"):
        tables = pd.read_html(export_path)
        if not tables:
            raise ValueError(f"SPX export HTML does not contain tables: {export_path.name}")
        df = tables[0]
    elif b"," in raw_head or suffix == ".csv":
        df = pd.read_csv(export_path)
    else:
        try:
            if zipfile.is_zipfile(export_path):
                df = pd.read_excel(export_path, engine="openpyxl")
            else:
                df = pd.read_excel(export_path, engine="openpyxl")
        except Exception as exc:
            raise ValueError(
                f"Unsupported or invalid SPX export format: {export_path.name}. "
                f"First bytes={raw_head[:16]!r}"
            ) from exc

    df = _normalize_export_dataframe(df)
    df.columns = [str(col).strip() for col in df.columns]
    missing = [name for name in EXPORT_COLUMNS.values() if name not in df.columns]
    if missing:
        raise ValueError(f"SPX export missing required columns: {missing}")

    records: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        province, city = _split_region(row.get(EXPORT_COLUMNS["recipient_region"]))
        created_at = _to_datetime(row.get(EXPORT_COLUMNS["created_at"]))
        returned_at = _to_datetime(row.get(EXPORT_COLUMNS["returned_to_sender_at"]))
        payment_method, _ = _infer_cod_type(row.get(EXPORT_COLUMNS["payment_role"]))
        cod_type, cod_amount = _infer_cod_fields(row)
        return_flag, return_reason = _infer_return_flag_and_reason(row)
        estimated_shipping_fee = _to_number(row.get(EXPORT_COLUMNS["estimated_shipping_fee"]))
        actual_shipping_fee = _to_number(row.get(EXPORT_COLUMNS["actual_shipping_fee"]))
        parcel_value = _to_number(row.get(EXPORT_COLUMNS["parcel_value"]))
        shipping_fee = actual_shipping_fee if actual_shipping_fee > 0 else estimated_shipping_fee
        service_type = _text(row.get(EXPORT_COLUMNS["original_pickup_option"]), fallback="")
        if service_type in ("", "No Value"):
            service_type = _text(row.get(EXPORT_COLUMNS["actual_pickup_option"]))

        records.append(
            {
                "source_system": "spx_web",
                "order_id": _text(row.get(EXPORT_COLUMNS["tracking_no"])),
                "event_date": created_at.date().isoformat() if created_at else None,
                "province": province,
                "city": city,
                "expedition": "SPX",
                "service_type": service_type,
                "payment_method": payment_method,
                "cod_type": cod_type,
                "order_value": parcel_value,
                "cod_value": cod_amount,
                "shipping_fee": shipping_fee,
                "return_flag": return_flag,
                "return_reason": return_reason,
                "customer_reference_no": _text(row.get(EXPORT_COLUMNS["customer_reference_no"])),
                "delivery_status": _text(row.get(EXPORT_COLUMNS["delivery_status"])),
                "failed_reason": _text(row.get(EXPORT_COLUMNS["failed_reason"]), fallback=""),
                "delay_reason": _text(row.get(EXPORT_COLUMNS["delay_reason"]), fallback=""),
                "created_at": created_at.isoformat() if created_at else None,
                "returned_to_sender_at": returned_at.isoformat() if returned_at else None,
            }
        )
    return records


def fetch_spx_export_records(
    start_date: str,
    end_date: str,
    *,
    headless: Optional[bool] = None,
    keep_download: bool = False,
    output_dir: Optional[str] = None,
) -> List[Dict[str, Any]]:
    login_url = os.getenv("SPX_WEB_LOGIN_URL")
    tracking_url = os.getenv("SPX_WEB_TRACKING_URL")
    username = os.getenv("SPX_WEB_USERNAME")
    password = os.getenv("SPX_WEB_PASSWORD")
    if not all([login_url, tracking_url, username, password]):
        raise ValueError(
            "Missing SPX web configuration. Required: SPX_WEB_LOGIN_URL, SPX_WEB_TRACKING_URL, SPX_WEB_USERNAME, SPX_WEB_PASSWORD"
        )

    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Playwright is not installed. Install with 'pip install playwright' and 'playwright install chromium'."
        ) from exc

    browser_headless = headless if headless is not None else os.getenv("SPX_WEB_HEADLESS", "true").lower() == "true"
    login_wait_for = os.getenv("SPX_WEB_POST_LOGIN_WAIT_FOR", "networkidle")
    open_login_selectors = _selector_candidates(
        "SPX_WEB_OPEN_LOGIN_SELECTOR",
        ["a:has-text('Masuk')", "button:has-text('Masuk')", "a:has-text('Login')", "button:has-text('Login')"],
    )
    staff_login_switch_selectors = _selector_candidates(
        "SPX_WEB_STAFF_LOGIN_SWITCH_SELECTOR",
        [
            "a:has-text('Masuk di sini')",
            "text=Untuk login Staf",
        ],
    )
    username_selectors = _selector_candidates(
        "SPX_WEB_USERNAME_SELECTOR",
        [
            "input[name='phone']",
            "input[name='loginId']",
            "input[name='username']",
            "input[name='email']",
            "input[placeholder*='Telepon']",
            "input[placeholder*='nomor']",
            "input[placeholder*='Email']",
            "input[placeholder*='Username']",
            "input[type='text']",
        ],
    )
    password_selectors = _selector_candidates(
        "SPX_WEB_PASSWORD_SELECTOR",
        ["input[name='password']", "input[placeholder*='Password']", "input[type='password']"],
    )
    submit_selectors = _selector_candidates(
        "SPX_WEB_SUBMIT_SELECTOR",
        ["button[type='submit']", "button:has-text('Masuk')", "button:has-text('Login')"],
    )
    download_selectors = _selector_candidates(
        "SPX_WEB_DOWNLOAD_SELECTOR",
        ["button:has-text('Unduh')", "a:has-text('Unduh')", "button:has-text('Download')", "a:has-text('Download')"],
    )
    download_dialog_selectors = _selector_candidates(
        "SPX_WEB_DOWNLOAD_DIALOG_SELECTOR",
        [
            "[role='dialog']",
            "div[class*='modal']",
            "div[class*='dialog']",
            "div[class*='popover']",
            "div[class*='drawer']",
            "text=Mengekspor Paket",
            "text=Ekspor Pelacakan Paket Berhasil",
        ],
    )
    download_result_selectors = _selector_candidates(
        "SPX_WEB_DOWNLOAD_RESULT_SELECTOR",
        [
            "[role='dialog'] button:has-text('Unduh')",
            "div[class*='modal'] button:has-text('Unduh')",
            "div[class*='popover'] button:has-text('Unduh')",
            "div[class*='drawer'] button:has-text('Unduh')",
        ],
    )
    tracking_table_selectors = _selector_candidates(
        "SPX_WEB_TRACKING_TABLE_SELECTOR",
        ["table", "[role='table']", "div[class*='table']"],
    )
    tracking_ready_selectors = _selector_candidates(
        "SPX_WEB_TRACKING_READY_SELECTOR",
        [
            "h1:has-text('Lacak Paket')",
            "text=Lacak Paket",
            "button:has-text('Unduh')",
            "a:has-text('Unduh')",
            "text=Nomer Resi",
        ],
    )
    start_date_selectors = _selector_candidates(
        "SPX_WEB_START_DATE_SELECTOR",
        [
            "xpath=(//input[contains(@value,'/')])[1]",
            "xpath=(//input[@type='text'])[2]",
            "xpath=(//input[contains(@placeholder,'Tanggal')])[1]",
        ],
    )
    end_date_selectors = _selector_candidates(
        "SPX_WEB_END_DATE_SELECTOR",
        [
            "xpath=(//input[contains(@value,'/')])[2]",
            "xpath=(//input[@type='text'])[3]",
            "xpath=(//input[contains(@placeholder,'Tanggal')])[2]",
        ],
    )
    apply_selectors = _selector_candidates(
        "SPX_WEB_APPLY_FILTER_SELECTOR",
        ["button:has-text('Cari')"],
    )
    timeout_ms = int(os.getenv("SPX_WEB_TIMEOUT_MS", "120000"))

    target_dir_ctx = tempfile.TemporaryDirectory() if output_dir is None else None
    download_dir = Path(output_dir or target_dir_ctx.name)
    download_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=browser_headless)
        context = browser.new_context(
            accept_downloads=True,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/135.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()
        page.set_default_timeout(timeout_ms)
        captured_export_path: Optional[Path] = None
        capture_enabled = False
        network_debug_events: List[str] = []
        network_debug_events.append(f"debug_version\t{SPX_DOWNLOAD_DEBUG_VERSION}")
        export_task_ready = False
        latest_export_task_id: Optional[int] = None

        def _flush_network_debug() -> None:
            if not network_debug_events:
                return
            debug_path = download_dir / "spx_network_debug.log"
            debug_path.write_text("\n".join(network_debug_events), encoding="utf-8")

        def _handle_request(request: Any) -> None:
            try:
                url = request.url
                lowered_url = url.lower()
                if not any(token in lowered_url for token in ["task", "export", "download", "file_id", "task_id", ".xlsx", ".csv"]):
                    return
                method = request.method
                post_data = request.post_data or ""
                if post_data:
                    post_data = post_data[:1000]
                network_debug_events.append(
                    f"request\t{method}\t{url}\t{post_data}"
                )
            except Exception:
                return

        def _handle_export_task_payload(payload: Dict[str, Any], source: str) -> None:
            nonlocal captured_export_path, export_task_ready, latest_export_task_id
            task_list = payload.get("data", {}).get("list", [])
            if task_list:
                current_task = task_list[0]
                latest_export_task_id = current_task.get("task_id")
                if current_task.get("task_status") == 4:
                    export_task_ready = True
                    network_debug_events.append(
                        f"task_ready\t{latest_export_task_id}\t{current_task.get('file_id')}\t{current_task.get('success_count')}"
                    )
            download_urls = _extract_export_download_urls(payload)
            network_debug_events.append(
                f"{source}_task_payload\t{json.dumps(payload, ensure_ascii=True)[:4000]}"
            )
            if download_urls:
                network_debug_events.append(
                    f"task_urls\t{json.dumps(download_urls, ensure_ascii=True)}"
                )
            for download_url in download_urls:
                download_response = context.request.get(download_url, timeout=min(timeout_ms, 30000))
                if not download_response.ok:
                    network_debug_events.append(
                        f"task_download_failed\t{download_response.status}\t{download_url}"
                    )
                    continue
                payload_bytes = download_response.body()
                if not payload_bytes:
                    network_debug_events.append(f"task_download_empty\t{download_url}")
                    continue
                extension = _guess_export_extension(
                    download_response.headers.get("content-type", ""),
                    download_url,
                )
                export_path = download_dir / f"spx_export_task{extension}"
                export_path.write_bytes(payload_bytes)
                captured_export_path = export_path
                network_debug_events.append(
                    f"task_download_saved\t{download_response.status}\t{download_url}\t{export_path.name}"
                )
                return

        def _handle_response(response: Any) -> None:
            nonlocal captured_export_path, export_task_ready, latest_export_task_id
            url = response.url
            content_type = response.headers.get("content-type", "")
            lowered_url = url.lower()
            if any(token in lowered_url for token in ["task", "export", "download", ".xlsx", ".csv"]):
                network_debug_events.append(
                    f"response\t{response.status}\t{content_type}\t{url}"
                )
            if capture_enabled and "export_task_list" in lowered_url:
                try:
                    payload = json.loads(response.body().decode("utf-8"))
                    _handle_export_task_payload(payload, "network")
                except Exception as exc:
                    network_debug_events.append(f"task_parse_error\t{url}\t{exc}")
            if not capture_enabled or captured_export_path is not None:
                return
            export_path = _save_response_export(response, download_dir)
            if export_path is not None:
                captured_export_path = export_path

        def _poll_export_task_list() -> None:
            try:
                result = page.evaluate(
                    """async () => {
                        const response = await fetch(
                            'https://spx.co.id/shipment/order/logistic/order/v2/export_task_list',
                            {
                                method: 'POST',
                                credentials: 'include',
                                headers: {'content-type': 'application/json'},
                                body: JSON.stringify({pageno: 1, count: 1000})
                            }
                        );
                        return {status: response.status, text: await response.text()};
                    }"""
                )
                network_debug_events.append(
                    f"manual_task_poll\t{result.get('status')}"
                )
                if int(result.get("status", 0)) != 200:
                    return
                payload = json.loads(result.get("text", "{}"))
                _handle_export_task_payload(payload, "manual")
            except Exception as exc:
                network_debug_events.append(f"manual_task_poll_error\t{exc}")

        page.on("request", _handle_request)
        page.on("response", _handle_response)

        def _perform_login_flow() -> None:
            if _page_contains_any_text(page, ["Untuk login Staf", "Log Masuk Admin"]):
                try:
                    _wait_for_first_visible(page, staff_login_switch_selectors, timeout_ms).click()
                    _wait_for_page_ready(page, timeout_ms)
                except Exception as exc:
                    _write_debug_artifacts(page, download_dir, "spx_staff_switch_debug")
                    raise RuntimeError(
                        f"Unable to switch from admin login to staff login. Current URL: {page.url}. "
                        f"Checked staff switch selectors={staff_login_switch_selectors}. Debug files saved under {download_dir}."
                    ) from exc

            for selector in open_login_selectors:
                locator = page.locator(selector).first
                try:
                    locator.wait_for(state="visible", timeout=3000)
                    locator.click()
                    _wait_for_page_ready(page, timeout_ms)
                    break
                except Exception:
                    continue

            try:
                username_locator = _wait_for_first_visible(page, username_selectors, timeout_ms)
                password_locator = _wait_for_first_visible(page, password_selectors, timeout_ms)
                submit_locator = _wait_for_first_visible(page, submit_selectors, timeout_ms)
            except Exception as exc:
                _write_debug_artifacts(page, download_dir, "spx_login_debug")
                raise RuntimeError(
                    f"Unable to find SPX login form. Current URL: {page.url}. "
                    f"Checked username selectors={username_selectors}, password selectors={password_selectors}, submit selectors={submit_selectors}. "
                    f"Debug files saved under {download_dir}."
                ) from exc

            username_locator.fill(username)
            password_locator.fill(password)
            submit_locator.click()
            page.wait_for_load_state(login_wait_for)

            try:
                page.wait_for_url(lambda url: not _is_login_url(url), timeout=timeout_ms)
            except Exception as exc:
                _write_debug_artifacts(page, download_dir, "spx_login_failed")
                raise RuntimeError(
                    f"SPX login did not complete. Still on URL: {page.url}. Debug files saved under {download_dir}."
                ) from exc

            page.goto(tracking_url, wait_until="domcontentloaded")
            _wait_for_page_ready(page, timeout_ms)

        page.goto(tracking_url, wait_until="domcontentloaded")
        _wait_for_page_ready(page, timeout_ms)
        try:
            page.evaluate("(dir) => { window.__spxDownloadDebugDir = dir; }", str(download_dir))
        except Exception:
            pass

        for _ in range(2):
            if not _is_login_url(page.url):
                break
            _perform_login_flow()

        if _is_login_url(page.url):
            _write_debug_artifacts(page, download_dir, "spx_login_failed")
            raise RuntimeError(
                f"SPX login did not complete after retries. Still on URL: {page.url}. Debug files saved under {download_dir}."
            )

        def _apply_date_filter() -> None:
            _fill_first_matching(page, start_date_selectors, _to_date_text(start_date), timeout_ms)
            _fill_first_matching(page, end_date_selectors, _to_date_text(end_date), timeout_ms)
            try:
                _wait_for_first_visible(page, apply_selectors, timeout_ms).click()
                _wait_for_page_ready(page, timeout_ms)
            except Exception:
                pass

        if start_date_selectors and end_date_selectors:
            last_date_filter_error: Optional[Exception] = None
            for attempt in range(2):
                try:
                    _apply_date_filter()
                    last_date_filter_error = None
                    break
                except Exception as exc:
                    last_date_filter_error = exc
                    if _is_login_url(page.url) and attempt == 0:
                        network_debug_events.append("date_filter_login_redirect")
                        _perform_login_flow()
                        continue
                    break
            if last_date_filter_error is not None:
                _write_debug_artifacts(page, download_dir, "spx_date_filter_debug")
                _flush_network_debug()
                raise RuntimeError(
                    f"Unable to apply SPX date filter. Current URL: {page.url}. "
                    f"Checked start selectors={start_date_selectors}, end selectors={end_date_selectors}, "
                    f"apply selectors={apply_selectors}. "
                    f"Debug files saved under {download_dir}."
                ) from last_date_filter_error

        if _is_login_url(page.url):
            _perform_login_flow()

        try:
            _wait_for_first_visible(page, tracking_ready_selectors, timeout_ms)
        except Exception as exc:
            if _is_login_url(page.url):
                try:
                    _perform_login_flow()
                    _wait_for_first_visible(page, tracking_ready_selectors, timeout_ms)
                except Exception as retry_exc:
                    _write_debug_artifacts(page, download_dir, "spx_login_failed")
                    raise RuntimeError(
                        f"SPX session returned to login during tracking load. Current URL: {page.url}. "
                        f"Debug files saved under {download_dir}."
                    ) from retry_exc
            else:
                _write_debug_artifacts(page, download_dir, "spx_tracking_debug")
                raise RuntimeError(
                    f"Unable to find SPX tracking table. Current URL: {page.url}. "
                    f"Checked ready selectors={tracking_ready_selectors} and table selectors={tracking_table_selectors}. "
                    f"Debug files saved under {download_dir}."
                ) from exc

        export_path: Optional[Path] = None
        try:
            _wait_for_first_visible(page, download_selectors, timeout_ms).click()
            try:
                _wait_for_first_visible(page, download_dialog_selectors, timeout_ms)
            except Exception:
                pass

            def _click_result_download() -> None:
                if _click_successful_export_download(page, timeout_ms):
                    return
                if _open_task_panel_download(
                    page,
                    timeout_ms,
                    latest_export_task_id,
                    download_dir,
                    network_debug_events,
                ):
                    return
                if export_task_ready:
                    network_debug_events.append(
                        f"initial_task_ready_skip_main_download\t{latest_export_task_id or 'unknown'}"
                    )
                    return
                if latest_export_task_id:
                    network_debug_events.append(
                        f"initial_task_pending_skip_main_download\t{latest_export_task_id}"
                    )
                    return
                try:
                    _wait_for_first_visible(page, download_result_selectors, timeout_ms).click()
                except Exception:
                    _click_export_result_download(page, timeout_ms)

            try:
                captured_export_path = None
                capture_enabled = True
                with page.expect_download(timeout=min(timeout_ms, 60000)) as download_info:
                    _click_result_download()
                download = download_info.value
                export_path = download_dir / download.suggested_filename
                download.save_as(str(export_path))
            except PlaywrightTimeoutError:
                captured_export_path = None
                capture_enabled = True
                export_ready_timeout_s = int(os.getenv("SPX_EXPORT_READY_TIMEOUT_SECONDS", "600"))
                deadline = time.time() + export_ready_timeout_s
                next_retry_at = 0.0
                while time.time() < deadline and export_path is None:
                    if captured_export_path is not None and captured_export_path.exists():
                        export_path = captured_export_path
                        break
                    if time.time() >= next_retry_at:
                        try:
                            if export_task_ready:
                                network_debug_events.append(
                                    f"retry_task_panel_download\t{latest_export_task_id or 'unknown'}"
                                )
                                _open_task_panel_download(
                                    page,
                                    min(timeout_ms, 10000),
                                    latest_export_task_id,
                                    download_dir,
                                    network_debug_events,
                                )
                                next_retry_at = time.time() + 5
                                continue
                            if latest_export_task_id:
                                network_debug_events.append(
                                    f"retry_wait_task_pending\t{latest_export_task_id}"
                                )
                                _poll_export_task_list()
                                next_retry_at = time.time() + 5
                                continue
                            _click_result_download()
                        except Exception:
                            pass
                        next_retry_at = time.time() + 5
                    time.sleep(1)
                if export_path is None:
                    raise
            finally:
                capture_enabled = False
        except PlaywrightTimeoutError as exc:
            _write_debug_artifacts(page, download_dir, "spx_download_debug")
            _write_control_debug(page, download_dir, "spx_download_controls")
            _flush_network_debug()
            raise TimeoutError(
                f"Timed out waiting for SPX export download. Checked download selectors={download_selectors}, "
                f"dialog selectors={download_dialog_selectors}, result selectors={download_result_selectors}. "
                f"Debug files saved under {download_dir}."
            ) from exc
        except Exception as exc:
            _write_debug_artifacts(page, download_dir, "spx_download_debug")
            _write_control_debug(page, download_dir, "spx_download_controls")
            _flush_network_debug()
            raise TimeoutError(
                f"Unable to navigate SPX export dialog. Checked download selectors={download_selectors}, "
                f"dialog selectors={download_dialog_selectors}, result selectors={download_result_selectors}. "
                f"Debug files saved under {download_dir}."
            ) from exc
        if export_path is None:
            _write_debug_artifacts(page, download_dir, "spx_download_debug")
            _write_control_debug(page, download_dir, "spx_download_controls")
            _flush_network_debug()
            raise TimeoutError(f"SPX export completed without a detectable file. Debug files saved under {download_dir}.")

        context.close()
        browser.close()

    records = load_spx_export_records(export_path)
    if not keep_download:
        export_path.unlink(missing_ok=True)
    if target_dir_ctx is not None:
        target_dir_ctx.cleanup()
    return records
