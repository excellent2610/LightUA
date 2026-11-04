from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

ADMINS: list[int] = [8608243726]  # замініть на реальні Telegram user_id


@dataclass(slots=True)
class Settings:
    bot_token: str
    db_path: Path
    poll_interval_seconds: int
    request_timeout_seconds: int
    log_level: str
    shadow_mode_enabled: bool = False
    shadow_alerts_energy_enabled: bool = False
    shadow_kolysvitlo_enabled: bool = False
    shadow_alerts_org_enabled: bool = False
    shadow_request_timeout_seconds: int = 15


def _read_bool(name: str, *, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value")


def load_settings() -> Settings:
    load_dotenv()

    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if not bot_token:
        raise ValueError("BOT_TOKEN is required")

    raw_db_path = os.getenv("DB_PATH", "lightua.db").strip().strip('"').strip("'")
    db_path = Path(raw_db_path).expanduser()
    if not db_path.is_absolute():
        project_root = Path(__file__).resolve().parents[1]
        db_path = project_root / db_path
    db_path = db_path.resolve()
    poll_interval_seconds = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))
    request_timeout_seconds = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))
    shadow_request_timeout_seconds = int(
        os.getenv("SHADOW_REQUEST_TIMEOUT_SECONDS", "15")
    )
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    shadow_mode_enabled = _read_bool("SHADOW_MODE_ENABLED", default=False)
    shadow_alerts_energy_enabled = _read_bool(
        "SHADOW_ALERTS_ENERGY_ENABLED",
        default=False,
    )
    shadow_kolysvitlo_enabled = _read_bool(
        "SHADOW_KOLYSVITLO_ENABLED",
        default=False,
    )
    shadow_alerts_org_enabled = _read_bool(
        "SHADOW_ALERTS_ORG_ENABLED",
        default=False,
    )

    if poll_interval_seconds < 1:
        raise ValueError("POLL_INTERVAL_SECONDS must be >= 1")
    if shadow_request_timeout_seconds < 1:
        raise ValueError("SHADOW_REQUEST_TIMEOUT_SECONDS must be >= 1")

    return Settings(
        bot_token=bot_token,
        db_path=db_path,
        poll_interval_seconds=poll_interval_seconds,
        request_timeout_seconds=request_timeout_seconds,
        log_level=log_level,
        shadow_mode_enabled=shadow_mode_enabled,
        shadow_alerts_energy_enabled=shadow_alerts_energy_enabled,
        shadow_kolysvitlo_enabled=shadow_kolysvitlo_enabled,
        shadow_alerts_org_enabled=shadow_alerts_org_enabled,
        shadow_request_timeout_seconds=shadow_request_timeout_seconds,
    )
