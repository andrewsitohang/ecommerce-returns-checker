from __future__ import annotations

import logging
import os
from typing import Any, Dict

import requests

logger = logging.getLogger(__name__)


def notify_login_failure(context: Dict[str, Any]) -> None:
    """Airflow on_failure_callback for the Playwright-based login refresh tasks.

    These tasks scrape a vendor login page instead of calling an official API,
    so they can silently break whenever the vendor changes their UI. Without
    this callback, the first sign of trouble would be stale/empty data in the
    dashboard days later.
    """
    webhook_url = os.getenv("ALERT_SLACK_WEBHOOK_URL", "").strip()
    if not webhook_url:
        logger.warning(
            "ALERT_SLACK_WEBHOOK_URL is not set; skipping failure notification for %s",
            context["task_instance"].task_id,
        )
        return

    task_instance = context["task_instance"]
    message = (
        f":rotating_light: *{task_instance.dag_id}* task `{task_instance.task_id}` failed.\n"
        f"This task refreshes login credentials via browser automation — if the "
        f"vendor's login page changed, the automated login likely needs a manual fix. "
        f'See the README\'s "Manual Credential Fallback" section.\n'
        f"Logs: {task_instance.log_url}"
    )
    try:
        requests.post(webhook_url, json={"text": message}, timeout=10)
    except requests.RequestException:
        logger.exception("Failed to send login-failure alert to Slack webhook")
