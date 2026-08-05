from __future__ import annotations

import os
from pathlib import Path
from typing import Any, List, Optional

try:
    from returns_config import env
except ImportError:  # pragma: no cover - package import path for tests
    from dags.returns_config import env


def _selector_candidates(env_name: str, defaults: List[str]) -> List[str]:
    raw = os.getenv(env_name, "").strip()
    if raw:
        return [selector.strip() for selector in raw.split("||") if selector.strip()]
    return defaults


def _wait_for_first_visible(page: Any, selectors: List[str], timeout_ms: int) -> Any:
    last_exc: Optional[Exception] = None
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            locator.wait_for(state="visible", timeout=timeout_ms)
            return locator
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            continue
    raise RuntimeError(f"None of the selectors were visible: {selectors}") from last_exc


def _write_debug_artifacts(page: Any, prefix: str) -> str:
    debug_dir = Path(
        os.getenv("MENGANTAR_WEB_DEBUG_DIR", "/opt/airflow/data/mengantar_login_debug")
    )
    try:
        debug_dir.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(debug_dir / f"{prefix}.png"), full_page=True)
        (debug_dir / f"{prefix}.html").write_text(page.content(), encoding="utf-8")
        return str(debug_dir)
    except Exception as exc:  # noqa: BLE001
        return f"(failed to write debug artifacts: {exc})"


def _is_login_url(url: str) -> bool:
    return "/login" in url


def login_and_refresh_mengantar_cookie() -> None:
    """Log into Mengantar with a real browser (email/password) and persist the
    full resulting cookie header (including the Cloudflare cf_clearance cookie)
    to the secret file that mengantar_api_source.py reads on every fetch."""
    login_url = os.getenv("MENGANTAR_WEB_LOGIN_URL", "https://app.mengantar.com/login")
    email = env("MENGANTAR_WEB_EMAIL")
    password = env("MENGANTAR_WEB_PASSWORD")

    cookie_file = os.getenv("MENGANTAR_API_FILE", "/opt/airflow/data/session/mengantar_api")

    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Playwright is not installed. Install with 'pip install playwright' and 'playwright install chromium'."
        ) from exc

    headless = os.getenv("MENGANTAR_WEB_HEADLESS", "true").lower() == "true"
    timeout_ms = int(os.getenv("MENGANTAR_WEB_TIMEOUT_MS", "120000"))

    email_selectors = _selector_candidates(
        "MENGANTAR_WEB_EMAIL_SELECTOR",
        ["input[type='email']", "input[name='email']"],
    )
    password_selectors = _selector_candidates(
        "MENGANTAR_WEB_PASSWORD_SELECTOR",
        ["input[type='password']", "input[name='password']"],
    )
    submit_selectors = _selector_candidates(
        "MENGANTAR_WEB_SUBMIT_SELECTOR",
        ["button[type='submit']", "button:has-text('Masuk')"],
    )

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=headless,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()
        page.set_default_timeout(timeout_ms)
        try:
            page.goto(login_url, wait_until="networkidle", timeout=timeout_ms)
            page.wait_for_timeout(3000)  # allow the Cloudflare challenge to resolve

            try:
                email_locator = _wait_for_first_visible(page, email_selectors, timeout_ms)
                password_locator = _wait_for_first_visible(page, password_selectors, timeout_ms)
                submit_locator = _wait_for_first_visible(page, submit_selectors, timeout_ms)
            except Exception as exc:
                debug_dir = _write_debug_artifacts(page, "mengantar_login_form_not_found")
                raise RuntimeError(
                    f"Could not find the Mengantar login form. Current URL: {page.url}. "
                    f"Debug screenshot/HTML saved under {debug_dir}."
                ) from exc

            email_locator.fill(email)
            password_locator.fill(password)
            submit_locator.click()

            try:
                page.wait_for_url(lambda url: not _is_login_url(url), timeout=timeout_ms)
            except Exception as exc:
                debug_dir = _write_debug_artifacts(page, "mengantar_login_failed")
                raise RuntimeError(
                    f"Mengantar login did not complete. Still on URL: {page.url}. "
                    f"Debug screenshot/HTML saved under {debug_dir}."
                ) from exc

            page.wait_for_timeout(2000)

            cookies = context.cookies()
            if not any(c["name"] == "cf_clearance" for c in cookies):
                debug_dir = _write_debug_artifacts(page, "mengantar_missing_cf_clearance")
                raise RuntimeError(
                    f"Mengantar login appeared to succeed (now on {page.url}) but no cf_clearance "
                    f"cookie was found. Cookies present: {sorted(c['name'] for c in cookies)}. "
                    f"Debug screenshot/HTML saved under {debug_dir}."
                )

            cookie_header = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
            Path(cookie_file).parent.mkdir(parents=True, exist_ok=True)
            Path(cookie_file).write_text(cookie_header, encoding="utf-8")
        finally:
            browser.close()


if __name__ == "__main__":
    login_and_refresh_mengantar_cookie()
