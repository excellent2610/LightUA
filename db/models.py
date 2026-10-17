from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional


USERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS users (
    telegram_id INTEGER PRIMARY KEY,
    region_slug TEXT NOT NULL,
    region_name TEXT NOT NULL,
    queue INTEGER NOT NULL,
    subqueue INTEGER NOT NULL,
    notifications_enabled INTEGER NOT NULL DEFAULT 0,
    quiet_start TEXT NULL,
    quiet_end TEXT NULL
);
"""


LAST_STATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS last_state (
    telegram_id INTEGER PRIMARY KEY,
    last_light_state TEXT NOT NULL CHECK (last_light_state IN ('on', 'off')),
    last_checked_at TEXT NOT NULL,
    FOREIGN KEY (telegram_id) REFERENCES users (telegram_id) ON DELETE CASCADE
);
"""


@dataclass(slots=True)
class User:
    telegram_id: int
    region_slug: str
    region_name: str
    queue: int
    subqueue: int
    notifications_enabled: bool
    quiet_start: Optional[str]
    quiet_end: Optional[str]


@dataclass(slots=True)
class LastState:
    telegram_id: int
    last_light_state: str
    last_checked_at: datetime | str


@dataclass(slots=True)
class NoOutageNotificationEvent:
    event_key: str
    telegram_id: int
    region_slug: str
    queue: int
    subqueue: int
    schedule_date: date
    schedule_fingerprint: str
    event_type: str
    status: str
    attempt_count: int
    last_attempt_at: datetime
