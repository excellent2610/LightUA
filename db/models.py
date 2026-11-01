from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional
from uuid import UUID


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

AD_CAMPAIGNS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ad_campaigns (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    button_text TEXT NOT NULL,
    destination_url TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

AD_IMPRESSIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ad_impressions (
    id BIGSERIAL PRIMARY KEY,
    reservation_id UUID NOT NULL UNIQUE,
    campaign_id TEXT NOT NULL REFERENCES ad_campaigns(id) ON UPDATE CASCADE,
    telegram_id BIGINT NOT NULL,
    shown_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

AD_CLICKS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ad_clicks (
    id BIGSERIAL PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES ad_campaigns(id) ON UPDATE CASCADE,
    telegram_id BIGINT NOT NULL,
    clicked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

AD_USER_STATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ad_user_state (
    telegram_id BIGINT PRIMARY KEY,
    call_count BIGINT NOT NULL DEFAULT 0,
    last_ad_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

AD_ROTATION_STATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ad_rotation_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    last_ad_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

AD_IMPRESSION_RESERVATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ad_impression_reservations (
    reservation_id UUID PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES ad_campaigns(id) ON UPDATE CASCADE,
    telegram_id BIGINT NOT NULL,
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'reserved',
    delivered_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    lease_until TIMESTAMPTZ,
    last_error TEXT
);
"""

AD_CLICK_SIGNING_KEY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ad_click_signing_key (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    secret TEXT NOT NULL
);
"""

AD_STATISTICS_INDEXES_SQL = (
    "CREATE INDEX IF NOT EXISTS ad_impressions_campaign_time_idx "
    "ON ad_impressions (campaign_id, shown_at)",
    "CREATE INDEX IF NOT EXISTS ad_impressions_user_time_idx "
    "ON ad_impressions (telegram_id, shown_at)",
    "CREATE INDEX IF NOT EXISTS ad_clicks_campaign_time_idx "
    "ON ad_clicks (campaign_id, clicked_at)",
    "CREATE INDEX IF NOT EXISTS ad_clicks_user_time_idx "
    "ON ad_clicks (telegram_id, clicked_at)",
    "CREATE INDEX IF NOT EXISTS ad_reservations_user_expiry_idx "
    "ON ad_impression_reservations (telegram_id, expires_at)",
)


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


@dataclass(slots=True)
class ScheduleChangeDelivery:
    event_id: int
    recipient_id: int
    previous_json: str
    current_json: str
    queue: int
    subqueue: int
    attempt_count: int


@dataclass(frozen=True, slots=True)
class AdCampaign:
    id: str
    name: str
    enabled: bool
    button_text: str
    destination_url: str


@dataclass(frozen=True, slots=True)
class AdImpression:
    id: int
    reservation_id: UUID
    campaign_id: str
    telegram_id: int
    shown_at: datetime


@dataclass(frozen=True, slots=True)
class AdClick:
    id: int
    campaign_id: str
    telegram_id: int
    clicked_at: datetime


@dataclass(frozen=True, slots=True)
class AdStatistics:
    campaign_id: str | None
    name: str | None
    enabled: bool | None
    impressions: int
    unique_users: int
    clicks: int
    unique_clicks: int
    first_impression: datetime | None
    last_impression: datetime | None

    @property
    def ctr(self) -> float:
        if self.impressions == 0:
            return 0.0
        return self.clicks / self.impressions * 100
