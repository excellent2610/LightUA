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
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    if poll_interval_seconds < 1:
        raise ValueError("POLL_INTERVAL_SECONDS must be >= 1")

    return Settings(
        bot_token=bot_token,
        db_path=db_path,
        poll_interval_seconds=poll_interval_seconds,
        request_timeout_seconds=request_timeout_seconds,
        log_level=log_level,
    )
