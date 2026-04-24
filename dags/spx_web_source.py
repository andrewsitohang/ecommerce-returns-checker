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
    text = str(value).strip()
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
    return [item.strip() for item in configured.split(",") if item.strip()]


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

    return_keywords = ("return", "retur", "gagal", "failed", "ditolak")
    status_lower = status_text.lower()
    is_return = bool(returned_at) or bool(failed_reason) or any(key in status_lower for key in return_keywords)

    if failed_reason:
        return 1 if is_return else 0, failed_reason
    if delay_reason and is_return:
        return 1, delay_reason
    if status_text and is_return:
        return 1, status_text
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

        def _handle_response(response: Any) -> None:
            nonlocal captured_export_path
            if not capture_enabled or captured_export_path is not None:
                return
            export_path = _save_response_export(response, download_dir)
            if export_path is not None:
                captured_export_path = export_path

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

        for _ in range(2):
            if not _is_login_url(page.url):
                break
            _perform_login_flow()

        if _is_login_url(page.url):
            _write_debug_artifacts(page, download_dir, "spx_login_failed")
            raise RuntimeError(
                f"SPX login did not complete after retries. Still on URL: {page.url}. Debug files saved under {download_dir}."
            )

        if start_date_selectors and end_date_selectors:
            try:
                used_start = _fill_first_matching(page, start_date_selectors, _to_date_text(start_date), timeout_ms)
                used_end = _fill_first_matching(page, end_date_selectors, _to_date_text(end_date), timeout_ms)
                try:
                    _wait_for_first_visible(page, apply_selectors, timeout_ms).click()
                    _wait_for_page_ready(page, timeout_ms)
                except Exception:
                    pass
            except Exception as exc:
                _write_debug_artifacts(page, download_dir, "spx_date_filter_debug")
                raise RuntimeError(
                    f"Unable to apply SPX date filter. "
                    f"Checked start selectors={start_date_selectors}, end selectors={end_date_selectors}, "
                    f"apply selectors={apply_selectors}. "
                    f"Debug files saved under {download_dir}."
                ) from exc

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
                deadline = time.time() + min(timeout_ms, 90000) / 1000.0
                next_retry_at = 0.0
                while time.time() < deadline and export_path is None:
                    if captured_export_path is not None and captured_export_path.exists():
                        export_path = captured_export_path
                        break
                    if time.time() >= next_retry_at:
                        try:
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
            raise TimeoutError(
                f"Timed out waiting for SPX export download. Checked download selectors={download_selectors}, "
                f"dialog selectors={download_dialog_selectors}, result selectors={download_result_selectors}. "
                f"Debug files saved under {download_dir}."
            ) from exc
        except Exception as exc:
            _write_debug_artifacts(page, download_dir, "spx_download_debug")
            raise TimeoutError(
                f"Unable to navigate SPX export dialog. Checked download selectors={download_selectors}, "
                f"dialog selectors={download_dialog_selectors}, result selectors={download_result_selectors}. "
                f"Debug files saved under {download_dir}."
            ) from exc
        if export_path is None:
            _write_debug_artifacts(page, download_dir, "spx_download_debug")
            raise TimeoutError(f"SPX export completed without a detectable file. Debug files saved under {download_dir}.")

        context.close()
        browser.close()

    records = load_spx_export_records(export_path)
    if not keep_download:
        export_path.unlink(missing_ok=True)
    if target_dir_ctx is not None:
        target_dir_ctx.cleanup()
    return records
