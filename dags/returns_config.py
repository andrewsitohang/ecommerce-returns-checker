from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


def env(name: str, default: Optional[str] = None) -> str:
    value = _resolve(name, default)
    if value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def env_bool(name: str, default: bool = False) -> bool:
    raw_value = _resolve(name, None)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _resolve(name: str, default: Optional[str] = None) -> Optional[str]:
    file_path = os.getenv(f"{name}_FILE", "").strip()
    if file_path:
        return Path(file_path).read_text(encoding="utf-8").strip()
    direct_value = os.getenv(name)
    if direct_value is not None:
        return direct_value
    return default
