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


def _page_contains_any_text(page: Any, texts: List[str]) -> bool:
    try:
        content = page.content()
    except Exception:  # noqa: BLE001
        return False
    return any(text in content for text in texts)


def _is_login_url(url: str) -> bool:
    return "authenticate/login" in url or "account.spx.co.id" in url


def _write_debug_artifacts(page: Any, prefix: str) -> str:
    debug_dir = Path(os.getenv("SPX_WEB_DEBUG_DIR", "/opt/airflow/data/spx_login_debug"))
    try:
        debug_dir.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(debug_dir / f"{prefix}.png"), full_page=True)
        (debug_dir / f"{prefix}.html").write_text(page.content(), encoding="utf-8")
        return str(debug_dir)
    except Exception as exc:  # noqa: BLE001
        return f"(failed to write debug artifacts: {exc})"


def login_and_refresh_spx_cookies() -> None:
    """Log into SPX with a real browser (username/password) and persist the
    resulting spx_token/spx_sid session cookies to the secret files that
    spx_api_source.py reads on every fetch, so the API-based pipeline keeps
    working without anyone manually copying cookies from devtools."""
    login_url = os.getenv("SPX_WEB_LOGIN_URL", "https://account.spx.co.id/staff/pass/login")
    tracking_url = os.getenv("SPX_WEB_TRACKING_URL", "https://spx.co.id/spx-admin/order/trackings")
    username = env("SPX_WEB_USERNAME")
    password = env("SPX_WEB_PASSWORD")

    token_file = os.getenv("SPX_API_SPX_TOKEN_FILE", "/opt/airflow/secrets/spx_token")
    sid_file = os.getenv("SPX_API_SPX_SID_FILE", "/opt/airflow/secrets/spx_sid")

    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Playwright is not installed. Install with 'pip install playwright' and 'playwright install chromium'."
        ) from exc

    headless = os.getenv("SPX_WEB_HEADLESS", "true").lower() == "true"
    timeout_ms = int(os.getenv("SPX_WEB_TIMEOUT_MS", "120000"))

    staff_login_switch_selectors = _selector_candidates(
        "SPX_WEB_STAFF_LOGIN_SWITCH_SELECTOR",
        ["a:has-text('Masuk di sini')", "text=Untuk login Staf"],
    )
    username_selectors = _selector_candidates(
        "SPX_WEB_USERNAME_SELECTOR",
        [
            "input[name='phone']",
            "input[name='loginId']",
            "input[name='username']",
            "input[name='email']",
            "input[type='text']",
        ],
    )
    password_selectors = _selector_candidates(
        "SPX_WEB_PASSWORD_SELECTOR",
        ["input[name='password']", "input[type='password']"],
    )
    submit_selectors = _selector_candidates(
        "SPX_WEB_SUBMIT_SELECTOR",
        ["button[type='submit']", "button:has-text('Masuk')", "button:has-text('Login')"],
    )

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()
        page.set_default_timeout(timeout_ms)
        try:
            # Navigate to the tracking page first and let SPX's own auth redirect
            # take us to whatever the current login URL actually is, rather than
            # hardcoding a login URL that can go stale when SPX changes its routes.
            page.goto(tracking_url, wait_until="networkidle", timeout=timeout_ms)
            # The redirect to the login page is client-side JS that can fire a
            # moment after the network goes idle; give it a beat before checking.
            page.wait_for_timeout(3000)

            if _page_contains_any_text(page, ["Untuk login Staf"]):
                _wait_for_first_visible(page, staff_login_switch_selectors, timeout_ms).click()
                page.wait_for_load_state("networkidle", timeout=timeout_ms)

            try:
                username_locator = _wait_for_first_visible(page, username_selectors, 15000)
                password_locator = _wait_for_first_visible(page, password_selectors, timeout_ms)
                submit_locator = _wait_for_first_visible(page, submit_selectors, timeout_ms)
            except Exception:
                # The natural redirect didn't land on a recognizable login form —
                # fall back to the explicitly configured login URL.
                page.goto(login_url, wait_until="networkidle", timeout=timeout_ms)
                page.wait_for_timeout(2000)
                try:
                    username_locator = _wait_for_first_visible(page, username_selectors, timeout_ms)
                    password_locator = _wait_for_first_visible(page, password_selectors, timeout_ms)
                    submit_locator = _wait_for_first_visible(page, submit_selectors, timeout_ms)
                except Exception as exc:
                    debug_dir = _write_debug_artifacts(page, "spx_login_form_not_found")
                    raise RuntimeError(
                        f"Could not find the SPX login form. Current URL: {page.url}. "
                        f"Debug screenshot/HTML saved under {debug_dir}."
                    ) from exc

            username_locator.fill(username)
            password_locator.fill(password)
            submit_locator.click()

            page.wait_for_url(lambda url: not _is_login_url(url), timeout=timeout_ms)
            page.goto(tracking_url, wait_until="networkidle", timeout=timeout_ms)

            cookies = {cookie["name"]: cookie["value"] for cookie in context.cookies()}
            spx_token = cookies.get("spx_token")
            spx_sid = cookies.get("spx_sid")
            if not spx_token or not spx_sid:
                raise RuntimeError(
                    f"SPX login appeared to succeed (now on {page.url}) but the spx_token/spx_sid "
                    f"cookies were not found. Cookies present: {sorted(cookies)}"
                )

            Path(token_file).parent.mkdir(parents=True, exist_ok=True)
            Path(sid_file).parent.mkdir(parents=True, exist_ok=True)
            Path(token_file).write_text(spx_token, encoding="utf-8")
            Path(sid_file).write_text(spx_sid, encoding="utf-8")
        finally:
            browser.close()


if __name__ == "__main__":
    login_and_refresh_spx_cookies()
