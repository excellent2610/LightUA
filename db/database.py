from __future__ import annotations

import asyncio
import hmac
import logging
import os
import random
import secrets
import uuid
from datetime import datetime, time
from pathlib import Path
from typing import Optional

import asyncpg

from db.models import (
    AD_CAMPAIGNS_TABLE_SQL,
    AD_CLICK_SIGNING_KEY_TABLE_SQL,
    AD_CLICKS_TABLE_SQL,
    AD_IMPRESSIONS_TABLE_SQL,
    AD_IMPRESSION_RESERVATIONS_TABLE_SQL,
    AD_ROTATION_STATE_TABLE_SQL,
    AD_STATISTICS_INDEXES_SQL,
    AD_USER_STATE_TABLE_SQL,
    AdCampaign,
    AdStatistics,
    LastState,
    ScheduleChangeDelivery,
    User,
)
from utils.time_utils import KYIV_TZ

logger = logging.getLogger(__name__)

AD_IMPRESSION_MAX_ATTEMPTS = 5
AD_IMPRESSION_RETRY_CONCURRENCY = 3
AD_IMPRESSION_RETRY_POLL_SECONDS = 1.0
AD_IMPRESSION_RETRY_LEASE_SECONDS = 120


def _snapshot_fingerprint(snapshot_json: str) -> str:
    import json
    try:
        value = json.loads(snapshot_json)
        intervals = value.get("intervals", value) if isinstance(value, dict) else value
        from parsing.alerts_parser import ScheduleInterval, _canonical_day_intervals
        converted = []
        for start, end, state in intervals:
            start_time = time.fromisoformat(start)
            midnight = end in {"24:00", "00:00"} and start_time != time(0)
            end_time = time(0) if end == "24:00" else time.fromisoformat(end)
            converted.append(ScheduleInterval(start_time, end_time, state, ends_at_midnight=midnight))
        runs = _canonical_day_intervals(converted)
        canonical = [(f"{start // 60:02d}:{start % 60:02d}",
                      "00:00" if end == 1440 else f"{end // 60:02d}:{end % 60:02d}", state)
                     for start, end, state in runs]
        return json.dumps(canonical, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError):
        return snapshot_json

USERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS users (
    telegram_id BIGINT PRIMARY KEY,
    region_slug TEXT,
    region_name TEXT,
    queue INTEGER,
    subqueue INTEGER,
    notifications_enabled BOOLEAN DEFAULT FALSE,
    quiet_start TEXT,
    quiet_end TEXT,
    first_seen TIMESTAMP,
    last_active TIMESTAMP
)
"""

LAST_STATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS last_state (
    telegram_id BIGINT PRIMARY KEY,
    last_light_state TEXT,
    last_checked_at TIMESTAMP,
    last_no_outages_event_key TEXT
)
"""

SCHEDULE_SNAPSHOT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS schedule_snapshots (
    region_slug TEXT NOT NULL,
    queue INTEGER NOT NULL,
    subqueue INTEGER NOT NULL,
    schedule_json TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (region_slug, queue, subqueue)
)
"""

NO_OUTAGE_EVENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS no_outage_notification_events (
    event_key TEXT PRIMARY KEY,
    telegram_id BIGINT NOT NULL,
    region_slug TEXT NOT NULL,
    queue INTEGER NOT NULL,
    subqueue INTEGER NOT NULL,
    schedule_date DATE NOT NULL,
    schedule_fingerprint TEXT NOT NULL,
    event_type TEXT NOT NULL DEFAULT 'no_outages',
    status TEXT NOT NULL CHECK (status IN ('pending', 'sent', 'failed', 'unknown')),
    attempt_count INTEGER NOT NULL DEFAULT 1,
    last_attempt_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_error TEXT,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
)
"""

AD_CAMPAIGN_STATISTICS_QUERY = """
WITH impression_stats AS (
    SELECT campaign_id, COUNT(*) AS impressions,
           COUNT(DISTINCT telegram_id) AS unique_users,
           MIN(shown_at) AS first_impression, MAX(shown_at) AS last_impression
    FROM ad_impressions
    WHERE ($1::INTEGER IS NULL OR shown_at >=
        CASE WHEN $1 = 1
             THEN date_trunc('day', NOW() AT TIME ZONE 'Europe/Kyiv') AT TIME ZONE 'Europe/Kyiv'
             ELSE NOW() - ($1 * INTERVAL '1 day') END)
    GROUP BY campaign_id
),
click_stats AS (
    SELECT campaign_id, COUNT(*) AS clicks,
           COUNT(DISTINCT telegram_id) AS unique_clicks
    FROM ad_clicks
    WHERE ($1::INTEGER IS NULL OR clicked_at >=
        CASE WHEN $1 = 1
             THEN date_trunc('day', NOW() AT TIME ZONE 'Europe/Kyiv') AT TIME ZONE 'Europe/Kyiv'
             ELSE NOW() - ($1 * INTERVAL '1 day') END)
    GROUP BY campaign_id
)
SELECT c.id AS campaign_id, c.name, c.enabled,
       COALESCE(i.impressions, 0) AS impressions,
       COALESCE(i.unique_users, 0) AS unique_users,
       COALESCE(k.clicks, 0) AS clicks,
       COALESCE(k.unique_clicks, 0) AS unique_clicks,
       i.first_impression, i.last_impression
FROM ad_campaigns c
LEFT JOIN impression_stats i ON i.campaign_id = c.id
LEFT JOIN click_stats k ON k.campaign_id = c.id
WHERE ($2::TEXT IS NULL OR c.id = $2)
ORDER BY c.name, c.id
"""

AD_OVERALL_STATISTICS_QUERY = """
WITH impression_stats AS (
    SELECT COUNT(*) AS impressions,
           COUNT(DISTINCT telegram_id) AS unique_users,
           MIN(shown_at) AS first_impression,
           MAX(shown_at) AS last_impression
    FROM ad_impressions
    WHERE ($1::INTEGER IS NULL OR shown_at >=
        CASE WHEN $1 = 1
             THEN date_trunc('day', NOW() AT TIME ZONE 'Europe/Kyiv') AT TIME ZONE 'Europe/Kyiv'
             ELSE NOW() - ($1 * INTERVAL '1 day') END)
),
click_stats AS (
    SELECT COUNT(*) AS clicks,
           COUNT(DISTINCT telegram_id) AS unique_clicks
    FROM ad_clicks
    WHERE ($1::INTEGER IS NULL OR clicked_at >=
        CASE WHEN $1 = 1
             THEN date_trunc('day', NOW() AT TIME ZONE 'Europe/Kyiv') AT TIME ZONE 'Europe/Kyiv'
             ELSE NOW() - ($1 * INTERVAL '1 day') END)
)
SELECT i.impressions, i.unique_users, k.clicks, k.unique_clicks,
       i.first_impression, i.last_impression
FROM impression_stats i CROSS JOIN click_stats k
"""


class Database:
    def __init__(self, db_path: str | Path) -> None:
        self._database_url = os.getenv("DATABASE_URL", "").strip()
        self._db_path = "lightua_postgres"
        self._pool: asyncpg.Pool | None = None
        self._write_lock = asyncio.Lock()
        self._ad_click_secret: bytes | None = None
        self._ad_impression_worker: AdImpressionPersistenceWorker | None = None

    @property
    def db_path(self) -> str:
        return "lightua_postgres"

    async def init(self) -> None:
        if not self._database_url:
            raise RuntimeError("DATABASE_URL is not set")
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self._database_url,
                min_size=1,
                max_size=5,
            )
        async with self._pool.acquire() as connection:
            await connection.execute(USERS_TABLE_SQL)
            await connection.execute(LAST_STATE_TABLE_SQL)
            # Upgrade existing installations without dropping or rewriting state.
            await connection.execute(
                "ALTER TABLE last_state ADD COLUMN IF NOT EXISTS last_no_outages_event_key TEXT"
            )
            await connection.execute(SCHEDULE_SNAPSHOT_TABLE_SQL)
            for statement in (
                "ALTER TABLE schedule_snapshots ADD COLUMN IF NOT EXISTS logical_date DATE",
                "ALTER TABLE schedule_snapshots ADD COLUMN IF NOT EXISTS observed_date DATE",
                "ALTER TABLE schedule_snapshots ADD COLUMN IF NOT EXISTS source_schedule_date DATE",
                "ALTER TABLE schedule_snapshots ADD COLUMN IF NOT EXISTS candidate_json TEXT",
                "ALTER TABLE schedule_snapshots ADD COLUMN IF NOT EXISTS candidate_logical_date DATE",
                "ALTER TABLE schedule_snapshots ADD COLUMN IF NOT EXISTS candidate_observed_date DATE",
                "ALTER TABLE schedule_snapshots ADD COLUMN IF NOT EXISTS candidate_source_schedule_date DATE",
                "ALTER TABLE schedule_snapshots ADD COLUMN IF NOT EXISTS candidate_count INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE schedule_snapshots ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT NOW()",
            ):
                await connection.execute(statement)
            await connection.execute("""CREATE TABLE IF NOT EXISTS schedule_change_deliveries (
                id BIGSERIAL PRIMARY KEY, event_key TEXT NOT NULL, recipient_id BIGINT NOT NULL,
                previous_json TEXT NOT NULL, current_json TEXT NOT NULL, queue INTEGER NOT NULL,
                subqueue INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','processing','sent','dead')),
                attempt_count INTEGER NOT NULL DEFAULT 0, available_at TIMESTAMP NOT NULL DEFAULT NOW(),
                lease_until TIMESTAMP, last_error TEXT, UNIQUE (event_key, recipient_id))""")
            await connection.execute("CREATE INDEX IF NOT EXISTS schedule_change_delivery_pending_idx ON schedule_change_deliveries(status, available_at, id)")
            await connection.execute(NO_OUTAGE_EVENTS_TABLE_SQL)
            await connection.execute(
                "ALTER TABLE no_outage_notification_events "
                "ADD COLUMN IF NOT EXISTS event_type TEXT NOT NULL DEFAULT 'no_outages'"
            )
            ad_schema_ready = False
            try:
                for statement in (
                    AD_CAMPAIGNS_TABLE_SQL,
                    AD_IMPRESSIONS_TABLE_SQL,
                    AD_CLICKS_TABLE_SQL,
                    AD_USER_STATE_TABLE_SQL,
                    AD_ROTATION_STATE_TABLE_SQL,
                    AD_IMPRESSION_RESERVATIONS_TABLE_SQL,
                    AD_CLICK_SIGNING_KEY_TABLE_SQL,
                    *AD_STATISTICS_INDEXES_SQL,
                ):
                    await connection.execute(statement)
                for statement in (
                    "ALTER TABLE ad_impression_reservations "
                    "ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'reserved'",
                    "ALTER TABLE ad_impression_reservations "
                    "ADD COLUMN IF NOT EXISTS delivered_at TIMESTAMPTZ",
                    "ALTER TABLE ad_impression_reservations "
                    "ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0",
                    "ALTER TABLE ad_impression_reservations "
                    "ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ",
                    "ALTER TABLE ad_impression_reservations "
                    "ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ",
                    "ALTER TABLE ad_impression_reservations "
                    "ADD COLUMN IF NOT EXISTS last_error TEXT",
                ):
                    await connection.execute(statement)
                await connection.execute(
                    "CREATE INDEX IF NOT EXISTS ad_impression_pending_idx "
                    "ON ad_impression_reservations (next_attempt_at, lease_until) "
                    "WHERE status = 'delivered_pending_record'"
                )
                await connection.execute(
                    "INSERT INTO ad_rotation_state (singleton) VALUES (TRUE) "
                    "ON CONFLICT (singleton) DO NOTHING"
                )
                await connection.execute(
                    "INSERT INTO ad_click_signing_key (singleton, secret) VALUES (TRUE, $1) "
                    "ON CONFLICT (singleton) DO NOTHING",
                    secrets.token_urlsafe(32),
                )
                click_secret = await connection.fetchval(
                    "SELECT secret FROM ad_click_signing_key WHERE singleton = TRUE"
                )
                if not isinstance(click_secret, str):
                    raise RuntimeError("Could not initialize ad click signing key")
                self._ad_click_secret = click_secret.encode("utf-8")
                ad_schema_ready = True
            except Exception:
                logger.exception(
                    "Advertising schema initialization failed; the bot will run without ads"
                )
            if ad_schema_ready:
                await self.start_ad_impression_worker()

    async def sync_ad_campaigns(self, campaigns: list[AdCampaign]) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            async with connection.transaction():
                if campaigns:
                    await connection.executemany(
                        """INSERT INTO ad_campaigns (id, name, enabled, button_text, destination_url)
                           VALUES ($1, $2, $3, $4, $5)
                           ON CONFLICT (id) DO UPDATE SET
                             name = EXCLUDED.name, enabled = EXCLUDED.enabled,
                             button_text = EXCLUDED.button_text,
                             destination_url = EXCLUDED.destination_url, updated_at = NOW()""",
                        [
                            (
                                item.id,
                                item.name,
                                item.enabled,
                                item.button_text,
                                item.destination_url,
                            )
                            for item in campaigns
                        ],
                    )
                await connection.execute(
                    """UPDATE ad_campaigns SET enabled = FALSE, updated_at = NOW()
                       WHERE enabled = TRUE AND NOT (id = ANY($1::TEXT[]))""",
                    [item.id for item in campaigns],
                )

    async def reserve_ad_impression(
        self,
        telegram_id: int,
        candidate_ids: list[str],
        *,
        show_every: int,
        max_per_user_per_day: int,
        min_interval_hours: float,
    ) -> tuple[str, uuid.UUID] | None:
        """Atomically apply per-user cadence and reserve one ad delivery."""
        pool = self._require_pool()
        async with pool.acquire() as connection:
            async with connection.transaction():
                # The upsert takes a row lock held to transaction end, serializing
                # cadence and quota decisions for this user across bot processes.
                state = await connection.fetchrow(
                    """INSERT INTO ad_user_state (telegram_id, call_count)
                       VALUES ($1, 1)
                       ON CONFLICT (telegram_id) DO UPDATE SET
                         call_count = ad_user_state.call_count + 1, updated_at = NOW()
                       RETURNING call_count""",
                    telegram_id,
                )
                call_count = int(state["call_count"])
                if (
                    not candidate_ids
                    or show_every <= 0
                    or max_per_user_per_day <= 0
                    or call_count % show_every != 0
                ):
                    return None

                rotation = await connection.fetchrow(
                    "SELECT last_ad_id FROM ad_rotation_state "
                    "WHERE singleton = TRUE FOR UPDATE"
                )
                day_start = (
                    "date_trunc('day', NOW() AT TIME ZONE 'Europe/Kyiv') "
                    "AT TIME ZONE 'Europe/Kyiv'"
                )
                limits = await connection.fetchrow(
                    f"""SELECT
                        (SELECT COUNT(*) FROM ad_impressions
                         WHERE telegram_id = $1 AND shown_at >= {day_start}) +
                        (SELECT COUNT(*) FROM ad_impression_reservations
                         WHERE telegram_id = $1 AND reserved_at >= {day_start}
                           AND (status = 'delivered_pending_record'
                                OR expires_at > NOW())) AS daily_count,
                        EXISTS (
                            SELECT 1 FROM ad_impressions
                            WHERE telegram_id = $1 AND $3 > 0
                              AND shown_at > NOW() - ($3 * INTERVAL '1 hour')
                        ) OR EXISTS (
                            SELECT 1 FROM ad_impression_reservations
                            WHERE telegram_id = $1 AND $3 > 0
                              AND (status = 'delivered_pending_record'
                                   OR expires_at > NOW())
                              AND reserved_at > NOW() - ($3 * INTERVAL '1 hour')
                        ) AS recent_delivery,
                        ARRAY(SELECT id FROM ad_campaigns
                              WHERE enabled = TRUE AND id = ANY($2::TEXT[])) AS enabled_ids""",
                    telegram_id,
                    candidate_ids,
                    min_interval_hours,
                )
                if int(limits["daily_count"] or 0) >= max_per_user_per_day:
                    return None
                if limits["recent_delivery"]:
                    return None

                available_ids = [str(item) for item in limits["enabled_ids"]]
                if not available_ids:
                    return None
                previous_id = rotation["last_ad_id"] if rotation is not None else None
                alternatives = [item for item in available_ids if item != previous_id]
                campaign_id = random.choice(alternatives or available_ids)
                reservation_id = uuid.uuid4()
                row = await connection.fetchrow(
                    """WITH reservation AS (
                           INSERT INTO ad_impression_reservations
                               (reservation_id, campaign_id, telegram_id, expires_at)
                           VALUES ($1, $2, $3,
                                   NOW() + GREATEST(INTERVAL '5 minutes',
                                                    $4 * INTERVAL '1 hour'))
                           RETURNING campaign_id
                       )
                       UPDATE ad_rotation_state
                       SET last_ad_id = reservation.campaign_id, updated_at = NOW()
                       FROM reservation
                       WHERE singleton = TRUE
                       RETURNING reservation.campaign_id""",
                    reservation_id,
                    campaign_id,
                    telegram_id,
                    min_interval_hours,
                )
                return str(row["campaign_id"]), reservation_id

    async def mark_ad_impression_delivery_started(
        self, reservation_id: uuid.UUID
    ) -> None:
        """Persist a non-delivery-confirming marker before contacting Telegram."""
        pool = self._require_pool()
        async with pool.acquire() as connection:
            status = await connection.execute(
                """UPDATE ad_impression_reservations
                   SET status = 'delivery_outcome_unknown'
                   WHERE reservation_id = $1 AND status = 'reserved'""",
                reservation_id,
            )
            if self._extract_affected_rows(status) != 1:
                exists = await connection.fetchval(
                    """SELECT EXISTS (
                           SELECT 1 FROM ad_impression_reservations
                           WHERE reservation_id = $1
                             AND status = 'delivery_outcome_unknown'
                       )""",
                    reservation_id,
                )
                if not exists:
                    raise RuntimeError(
                        f"Could not prepare ad delivery ({reservation_id})"
                    )

    async def mark_ad_impression_delivered(
        self, reservation_id: uuid.UUID, delivered_at: datetime
    ) -> None:
        """Move a reservation to durable pending state only after Telegram accepts it."""
        pool = self._require_pool()
        async with pool.acquire() as connection:
            async with connection.transaction():
                status = await connection.execute(
                    """UPDATE ad_impression_reservations
                       SET status = 'delivered_pending_record',
                           delivered_at = $2,
                           attempt_count = 0, next_attempt_at = NOW(),
                           lease_until = NULL, last_error = NULL
                       WHERE reservation_id = $1
                         AND status = 'delivery_outcome_unknown'""",
                    reservation_id,
                    delivered_at,
                )
                if self._extract_affected_rows(status) != 1:
                    exists = await connection.fetchval(
                        """SELECT EXISTS (
                               SELECT 1 FROM ad_impression_reservations
                               WHERE reservation_id = $1
                                 AND status = 'delivered_pending_record'
                                 AND delivered_at IS NOT NULL
                           ) OR EXISTS (
                               SELECT 1 FROM ad_impressions
                               WHERE reservation_id = $1
                           )""",
                        reservation_id,
                    )
                    if not exists:
                        raise RuntimeError(
                            "Could not mark delivered ad impression pending "
                            f"({reservation_id})"
                        )

    async def claim_due_ad_impressions(
        self, limit: int = AD_IMPRESSION_RETRY_CONCURRENCY
    ) -> list[tuple[uuid.UUID, int]]:
        """Claim a bounded batch; row locks and leases coordinate DB workers."""
        pool = self._require_pool()
        async with pool.acquire() as connection:
            async with connection.transaction():
                rows = await connection.fetch(
                    """WITH due AS (
                           SELECT reservation_id
                           FROM ad_impression_reservations
                           WHERE status = 'delivered_pending_record'
                             AND attempt_count < $1
                             AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
                             AND (lease_until IS NULL OR lease_until <= NOW())
                           ORDER BY next_attempt_at NULLS FIRST, reserved_at
                           LIMIT $2
                           FOR UPDATE SKIP LOCKED
                       )
                       UPDATE ad_impression_reservations AS pending
                       SET attempt_count = pending.attempt_count + 1,
                           lease_until = NOW() + ($3 * INTERVAL '1 second')
                       FROM due
                       WHERE pending.reservation_id = due.reservation_id
                       RETURNING pending.reservation_id, pending.attempt_count""",
                    AD_IMPRESSION_MAX_ATTEMPTS,
                    limit,
                    AD_IMPRESSION_RETRY_LEASE_SECONDS,
                )
        return [
            (row["reservation_id"], int(row["attempt_count"]))
            for row in rows
        ]

    async def record_ad_impression(self, reservation_id: uuid.UUID) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    """INSERT INTO ad_impressions
                       (reservation_id, campaign_id, telegram_id, shown_at)
                       SELECT reservation_id, campaign_id, telegram_id, delivered_at
                       FROM ad_impression_reservations
                       WHERE reservation_id = $1
                         AND status = 'delivered_pending_record'
                         AND delivered_at IS NOT NULL
                       ON CONFLICT (reservation_id) DO NOTHING""",
                    reservation_id,
                )
                await connection.execute(
                    """DELETE FROM ad_impression_reservations AS pending
                       WHERE pending.reservation_id = $1
                         AND pending.status = 'delivered_pending_record'
                         AND EXISTS (
                             SELECT 1 FROM ad_impressions
                             WHERE reservation_id = pending.reservation_id
                         )""",
                    reservation_id,
                )

    async def schedule_ad_impression_retry(
        self, reservation_id: uuid.UUID, attempt: int, error: str
    ) -> None:
        delay_seconds = min(2 ** max(attempt - 1, 0), 60)
        await self._execute_write(
            """UPDATE ad_impression_reservations
               SET next_attempt_at = CASE
                       WHEN attempt_count >= $3 THEN NULL
                       ELSE NOW() + ($4 * INTERVAL '1 second')
                   END,
                   lease_until = NULL, last_error = $5
               WHERE reservation_id = $1
                 AND status = 'delivered_pending_record'
                 AND attempt_count = $2""",
            (
                reservation_id,
                attempt,
                AD_IMPRESSION_MAX_ATTEMPTS,
                delay_seconds,
                error[:2000],
            ),
        )

    async def cancel_ad_impression(self, reservation_id: uuid.UUID) -> None:
        await self._execute_write(
            """DELETE FROM ad_impression_reservations
               WHERE reservation_id = $1
                 AND status IN ('reserved', 'delivery_outcome_unknown')""",
            (reservation_id,),
        )

    def sign_ad_click(self, campaign_id: str, telegram_id: int) -> str:
        if self._ad_click_secret is None:
            raise RuntimeError("Database ad click signing key is not initialized")
        payload = f"{campaign_id}:{telegram_id}".encode("utf-8")
        return hmac.digest(self._ad_click_secret, payload, "sha256").hex()

    async def record_ad_click(
        self,
        campaign_id: str,
        telegram_id: int,
        signature: str,
    ) -> str | None:
        """Validate a signed click and persist it before returning its fixed destination."""
        expected = self.sign_ad_click(campaign_id, telegram_id)
        if not hmac.compare_digest(expected, signature):
            return None

        pool = self._require_pool()
        async with pool.acquire() as connection:
            async with connection.transaction():
                destination = await connection.fetchval(
                    "SELECT destination_url FROM ad_campaigns WHERE id = $1",
                    campaign_id,
                )
                if not isinstance(destination, str):
                    return None
                await connection.execute(
                    """INSERT INTO ad_clicks (campaign_id, telegram_id, clicked_at)
                       VALUES ($1, $2, NOW())""",
                    campaign_id,
                    telegram_id,
                )
                return destination

    async def get_ad_campaign_statistics(
        self,
        *,
        days: int | None = None,
        campaign_id: str | None = None,
    ) -> list[AdStatistics]:
        if days is not None and days <= 0:
            raise ValueError("days must be positive")
        rows = await self._fetchall(
            AD_CAMPAIGN_STATISTICS_QUERY,
            (days, campaign_id),
        )
        return [
            AdStatistics(
                campaign_id=row["campaign_id"],
                name=row["name"],
                enabled=bool(row["enabled"]),
                impressions=int(row["impressions"]),
                unique_users=int(row["unique_users"]),
                clicks=int(row["clicks"]),
                unique_clicks=int(row["unique_clicks"]),
                first_impression=row["first_impression"],
                last_impression=row["last_impression"],
            )
            for row in rows
        ]

    async def get_ad_overall_statistics(self, *, days: int | None = None) -> AdStatistics:
        if days is not None and days <= 0:
            raise ValueError("days must be positive")
        row = await self._fetchone(AD_OVERALL_STATISTICS_QUERY, (days,))
        if row is None:
            return AdStatistics(None, None, None, 0, 0, 0, 0, None, None)
        return AdStatistics(
            campaign_id=None,
            name=None,
            enabled=None,
            impressions=int(row["impressions"]),
            unique_users=int(row["unique_users"]),
            clicks=int(row["clicks"]),
            unique_clicks=int(row["unique_clicks"]),
            first_impression=row["first_impression"],
            last_impression=row["last_impression"],
        )

    async def update_schedule_snapshot(
        self, region_slug: str, queue: int, subqueue: int, schedule_json: str
    ) -> str | None:
        """Persist a profile's latest schedule and return its previous value, if any."""
        pool = self._require_pool()
        async with self._write_lock:
            async with pool.acquire() as connection:
                async with connection.transaction():
                    # Serialize updates for this key, including the initial insert.
                    await connection.execute(
                        "SELECT pg_advisory_xact_lock(hashtext($1))",
                        f"{region_slug}:{queue}.{subqueue}",
                    )
                    previous = await connection.fetchval(
                        "SELECT schedule_json FROM schedule_snapshots "
                        "WHERE region_slug = $1 AND queue = $2 AND subqueue = $3",
                        region_slug,
                        queue,
                        subqueue,
                    )
                    await connection.execute(
                        """INSERT INTO schedule_snapshots
                           (region_slug, queue, subqueue, schedule_json, updated_at)
                           VALUES ($1, $2, $3, $4, NOW())
                           ON CONFLICT (region_slug, queue, subqueue) DO UPDATE SET
                             schedule_json = EXCLUDED.schedule_json,
                             updated_at = EXCLUDED.updated_at""",
                        region_slug,
                        queue,
                        subqueue,
                        schedule_json,
                    )
                    return previous

    async def observe_schedule(
        self, region_slug: str, queue: int, subqueue: int, *, snapshot_json: str,
        fingerprint: str, logical_date, observed_date, source_schedule_date, recipients: list[int],
    ) -> tuple[str | None, bool, bool]:
        """Persist a candidate; confirm a change on the second successful same-day observation."""
        pool = self._require_pool()
        async with self._write_lock:
            async with pool.acquire() as connection:
                async with connection.transaction():
                    key = f"{region_slug}:{queue}.{subqueue}"
                    await connection.execute("SELECT pg_advisory_xact_lock(hashtext($1))", key)
                    row = await connection.fetchrow(
                        "SELECT schedule_json, logical_date, candidate_json, candidate_logical_date, candidate_count "
                        "FROM schedule_snapshots WHERE region_slug=$1 AND queue=$2 AND subqueue=$3 FOR UPDATE",
                        region_slug, queue, subqueue,
                    )
                    if row is None:
                        await connection.execute("""INSERT INTO schedule_snapshots
                            (region_slug,queue,subqueue,schedule_json,logical_date,observed_date,source_schedule_date)
                            VALUES($1,$2,$3,$4,$5,$6,$7)""", region_slug, queue, subqueue,
                            snapshot_json, logical_date, observed_date, source_schedule_date)
                        return None, False, False
                    previous = row["schedule_json"]
                    old_date = row["logical_date"]
                    # Legacy snapshots without an unambiguous date establish a fresh baseline.
                    if old_date is None or old_date != logical_date:
                        await connection.execute("""UPDATE schedule_snapshots SET schedule_json=$4,
                            logical_date=$5,observed_date=$6,source_schedule_date=$7,candidate_json=NULL,
                            candidate_logical_date=NULL,candidate_observed_date=NULL,
                            candidate_source_schedule_date=NULL,candidate_count=0,
                            updated_at=NOW() WHERE region_slug=$1 AND queue=$2 AND subqueue=$3""",
                            region_slug, queue, subqueue, snapshot_json, logical_date, observed_date, source_schedule_date)
                        return None, False, False
                    old_fingerprint = _snapshot_fingerprint(previous)
                    if fingerprint == old_fingerprint:
                        await connection.execute("""UPDATE schedule_snapshots SET candidate_json=NULL,
                            candidate_logical_date=NULL,candidate_observed_date=NULL,
                            candidate_source_schedule_date=NULL,candidate_count=0,
                            updated_at=NOW() WHERE region_slug=$1 AND queue=$2 AND subqueue=$3""",
                            region_slug, queue, subqueue)
                        return None, False, False
                    candidate_matches = (
                        row["candidate_json"] is not None
                        and row["candidate_logical_date"] == logical_date
                        and _snapshot_fingerprint(row["candidate_json"]) == fingerprint
                    )
                    if not candidate_matches:
                        await connection.execute("""UPDATE schedule_snapshots SET candidate_json=$4,
                            candidate_logical_date=$5,candidate_observed_date=$6,
                            candidate_source_schedule_date=$7,candidate_count=1,
                            updated_at=NOW() WHERE region_slug=$1 AND queue=$2 AND subqueue=$3""",
                            region_slug, queue, subqueue, snapshot_json, logical_date, observed_date, source_schedule_date)
                        return None, False, True
                    # A new confirmed transition gets a fresh durable event key. The
                    # snapshot row lock ensures concurrent confirmations create one.
                    event_key = uuid.uuid4().hex
                    await connection.execute("""UPDATE schedule_snapshots SET schedule_json=$4,
                        logical_date=$5,observed_date=$6,source_schedule_date=$7,candidate_json=NULL,
                        candidate_logical_date=NULL,candidate_observed_date=NULL,
                        candidate_source_schedule_date=NULL,candidate_count=0,
                        updated_at=NOW() WHERE region_slug=$1 AND queue=$2 AND subqueue=$3""",
                        region_slug, queue, subqueue, snapshot_json, logical_date, observed_date, source_schedule_date)
                    for recipient_id in recipients:
                        await connection.execute("""INSERT INTO schedule_change_deliveries
                            (event_key,recipient_id,previous_json,current_json,queue,subqueue)
                            VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(event_key,recipient_id) DO NOTHING""",
                            event_key, recipient_id, previous, snapshot_json, queue, subqueue)
                    return previous, True, False

    async def claim_schedule_change_delivery(self) -> ScheduleChangeDelivery | None:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute("""UPDATE schedule_change_deliveries SET status='dead',
                    lease_until=NULL,last_error=COALESCE(last_error,'delivery attempt limit reached')
                    WHERE attempt_count>=8 AND ((status='pending' AND available_at<=NOW())
                        OR (status='processing' AND lease_until<NOW()))""")
                row = await connection.fetchrow("""WITH next AS (
                    SELECT id FROM schedule_change_deliveries
                    WHERE ((status='pending' AND available_at<=NOW())
                       OR (status='processing' AND lease_until<NOW()))
                      AND attempt_count<8
                    ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1)
                    UPDATE schedule_change_deliveries d SET status='processing',
                    attempt_count=d.attempt_count+1,lease_until=NOW()+INTERVAL '2 minutes'
                    FROM next WHERE d.id=next.id
                    RETURNING d.id,d.recipient_id,d.previous_json,d.current_json,d.queue,d.subqueue,d.attempt_count""")
                if row is None:
                    return None
                return ScheduleChangeDelivery(row["id"], row["recipient_id"], row["previous_json"],
                    row["current_json"], row["queue"], row["subqueue"], row["attempt_count"])

    async def finish_schedule_change_delivery(self, delivery_id: int, *, error: str | None = None,
                                              retry_seconds: int = 0) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            await connection.execute("""UPDATE schedule_change_deliveries SET
                status=CASE WHEN $2::TEXT IS NULL THEN 'sent' WHEN attempt_count>=8 THEN 'dead' ELSE 'pending' END,
                available_at=NOW()+($3 * INTERVAL '1 second'), lease_until=NULL,last_error=$2
                WHERE id=$1""", delivery_id, error[:1000] if error else None, retry_seconds)

    async def upsert_user_registration(
        self,
        telegram_id: int,
        region_slug: str,
        region_name: str,
        queue: int,
        subqueue: int,
    ) -> None:
        query = """
        INSERT INTO users (
            telegram_id,
            region_slug,
            region_name,
            queue,
            subqueue,
            first_seen,
            last_active
        )
        VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
        ON CONFLICT (telegram_id) DO UPDATE SET
            region_slug = EXCLUDED.region_slug,
            region_name = EXCLUDED.region_name,
            queue = EXCLUDED.queue,
            subqueue = EXCLUDED.subqueue,
            first_seen = COALESCE(users.first_seen, EXCLUDED.first_seen),
            last_active = EXCLUDED.last_active
        """
        await self._execute_write(
            query,
            (telegram_id, region_slug, region_name, queue, subqueue),
        )

    async def delete_user(self, telegram_id: int) -> None:
        await self._execute_write("DELETE FROM users WHERE telegram_id = $1", (telegram_id,))

    async def get_user(self, telegram_id: int) -> Optional[User]:
        row = await self._fetchone(
            "SELECT * FROM users WHERE telegram_id = $1",
            (telegram_id,),
        )
        if row is None:
            return None
        return User(
            telegram_id=row["telegram_id"],
            region_slug=row["region_slug"],
            region_name=row["region_name"],
            queue=row["queue"],
            subqueue=row["subqueue"],
            notifications_enabled=bool(row["notifications_enabled"]),
            quiet_start=row["quiet_start"],
            quiet_end=row["quiet_end"],
        )

    async def set_notifications(self, telegram_id: int, enabled: bool) -> bool:
        updated_rows = await self._execute_write(
            "UPDATE users SET notifications_enabled = $1 WHERE telegram_id = $2",
            (enabled, telegram_id),
        )
        return updated_rows > 0

    async def set_quiet_hours(self, telegram_id: int, quiet_start: str, quiet_end: str) -> bool:
        updated_rows = await self._execute_write(
            "UPDATE users SET quiet_start = $1, quiet_end = $2 WHERE telegram_id = $3",
            (quiet_start, quiet_end, telegram_id),
        )
        return updated_rows > 0

    async def clear_quiet_hours(self, telegram_id: int) -> bool:
        updated_rows = await self._execute_write(
            "UPDATE users SET quiet_start = NULL, quiet_end = NULL WHERE telegram_id = $1",
            (telegram_id,),
        )
        return updated_rows > 0

    async def get_users_with_notifications(self) -> list[User]:
        rows = await self._fetchall(
            "SELECT * FROM users WHERE notifications_enabled = TRUE",
            (),
        )
        result: list[User] = []
        for row in rows:
            result.append(
                User(
                    telegram_id=row["telegram_id"],
                    region_slug=row["region_slug"],
                    region_name=row["region_name"],
                    queue=row["queue"],
                    subqueue=row["subqueue"],
                    notifications_enabled=True,
                    quiet_start=row["quiet_start"],
                    quiet_end=row["quiet_end"],
                )
            )
        return result

    async def get_last_state(self, telegram_id: int) -> Optional[LastState]:
        row = await self._fetchone(
            "SELECT * FROM last_state WHERE telegram_id = $1",
            (telegram_id,),
        )
        if row is None:
            return None
        return LastState(
            telegram_id=row["telegram_id"],
            last_light_state=row["last_light_state"],
            last_checked_at=row["last_checked_at"],
        )

    async def upsert_last_state(self, telegram_id: int, state: str, checked_at_iso: str) -> None:
        query = """
        INSERT INTO last_state (telegram_id, last_light_state, last_checked_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (telegram_id) DO UPDATE SET
            last_light_state = EXCLUDED.last_light_state,
            last_checked_at = EXCLUDED.last_checked_at
        """
        checked_at = datetime.fromisoformat(checked_at_iso)
        if checked_at.tzinfo is not None:
            # The existing column is TIMESTAMP WITHOUT TIME ZONE; store Kyiv wall time.
            checked_at = checked_at.astimezone(KYIV_TZ).replace(tzinfo=None)
        await self._execute_write(query, (telegram_id, state, checked_at))

    async def claim_no_more_outages_notification(
        self, *, event_key: str, telegram_id: int, region_slug: str,
        queue: int, subqueue: int, schedule_date, schedule_fingerprint: str,
        retry_after_seconds: int = 300, event_type: str = "no_outages",
    ) -> bool:
        """Atomically reserve one event; only confirmed failures may be retried."""
        pool = self._require_pool()
        async with self._write_lock:
            async with pool.acquire() as connection:
                claimed = await connection.fetchval(
                    """INSERT INTO no_outage_notification_events
                       (event_key, telegram_id, region_slug, queue, subqueue, schedule_date,
                        schedule_fingerprint, event_type, status, attempt_count, last_attempt_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', 1, NOW())
                       ON CONFLICT (event_key) DO UPDATE SET
                         status = 'pending', attempt_count = no_outage_notification_events.attempt_count + 1,
                         region_slug = EXCLUDED.region_slug, queue = EXCLUDED.queue,
                         subqueue = EXCLUDED.subqueue, schedule_date = EXCLUDED.schedule_date,
                         schedule_fingerprint = EXCLUDED.schedule_fingerprint,
                         event_type = EXCLUDED.event_type,
                         last_attempt_at = NOW(), last_error = NULL, updated_at = NOW()
                       WHERE no_outage_notification_events.status = 'failed'
                         AND no_outage_notification_events.last_attempt_at <=
                             NOW() - ($9 * INTERVAL '1 second')
                       RETURNING event_key""",
                    event_key, telegram_id, region_slug, queue, subqueue, schedule_date,
                    schedule_fingerprint, event_type, retry_after_seconds,
                )
                return claimed is not None

    async def finish_no_more_outages_notification(
        self, event_key: str, status: str, error: str | None = None
    ) -> None:
        if status not in {"sent", "failed", "unknown"}:
            raise ValueError(f"Invalid notification status: {status}")
        await self._execute_write(
            """UPDATE no_outage_notification_events
               SET status = $2, last_error = $3, updated_at = NOW()
               WHERE event_key = $1 AND status = 'pending'""",
            (event_key, status, error[:1000] if error else None),
        )

    async def delete_last_state(self, telegram_id: int) -> None:
        await self._execute_write("DELETE FROM last_state WHERE telegram_id = $1", (telegram_id,))

    async def touch_user_activity(self, telegram_id: int) -> None:
        query = """
        UPDATE users
        SET last_active = NOW(),
            first_seen = COALESCE(first_seen, NOW())
        WHERE telegram_id = $1
        """
        await self._execute_write(query, (telegram_id,))

    async def _fetch_scalar_int(self, query: str, params: tuple = ()) -> int:
        row = await self._fetchone(query, params)
        if row is None:
            return 0
        return int(list(row.values())[0] or 0)

    async def get_bot_stats(self) -> dict[str, int]:
        total = await self._fetch_scalar_int("SELECT COUNT(*) FROM users")
        new_today = await self._fetch_scalar_int(
            "SELECT COUNT(*) FROM users WHERE DATE(first_seen) = CURRENT_DATE"
        )
        active_24h = await self._fetch_scalar_int(
            "SELECT COUNT(*) FROM users WHERE last_active >= NOW() - INTERVAL '1 day'"
        )
        return {
            "total": total,
            "new_today": new_today,
            "active_24h": active_24h,
        }

    async def _fetchone(self, query: str, params: tuple) -> Optional[asyncpg.Record]:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            return await connection.fetchrow(query, *params)

    async def _fetchall(self, query: str, params: tuple) -> list[asyncpg.Record]:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            return await connection.fetch(query, *params)

    async def _execute_write(self, query: str, params: tuple) -> int:
        pool = self._require_pool()
        async with self._write_lock:
            async with pool.acquire() as connection:
                status = await connection.execute(query, *params)
        return self._extract_affected_rows(status)

    async def start_ad_impression_worker(self) -> None:
        if self._ad_impression_worker is not None:
            return
        await self.prepare_ad_impression_retries()
        self._ad_impression_worker = AdImpressionPersistenceWorker(self)
        await self._ad_impression_worker.start()

    async def prepare_ad_impression_retries(self) -> None:
        """Make persisted pending deliveries recoverable after process startup."""
        pool = self._require_pool()
        async with pool.acquire() as connection:
            # A new process gets a fresh bounded retry cycle for parked deliveries.
            await connection.execute(
                """UPDATE ad_impression_reservations
                   SET attempt_count = 0, next_attempt_at = NOW(),
                       lease_until = NULL, last_error = NULL
                   WHERE status = 'delivered_pending_record'
                     AND attempt_count >= $1""",
                AD_IMPRESSION_MAX_ATTEMPTS,
            )
            # Leases belong to workers in this process lifetime. DB-level
            # idempotency makes reclaiming them safe after a restart.
            await connection.execute(
                """UPDATE ad_impression_reservations
                   SET lease_until = NULL
                   WHERE status = 'delivered_pending_record'"""
            )

    async def close(self):
        worker_stopped = True
        worker = self._ad_impression_worker
        if worker is not None:
            worker_stopped = await worker.stop()
        if self._pool:
            if not worker_stopped:
                self._pool.terminate()
                worker_stopped = await worker.stop()
            else:
                try:
                    await asyncio.wait_for(self._pool.close(), timeout=5.0)
                except TimeoutError:
                    logger.error("Database pool did not close in time; terminating it")
                    self._pool.terminate()
            self._pool = None
        if worker_stopped:
            self._ad_impression_worker = None

    def _require_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("Database pool is not initialized. Call init() first.")
        return self._pool

    @staticmethod
    def _extract_affected_rows(status: str) -> int:
        parts = status.split()
        if not parts:
            return 0
        try:
            return int(parts[-1])
        except ValueError:
            return 0


class AdImpressionPersistenceWorker:
    """Persist delivered impressions with bounded concurrency and retry cycles."""

    def __init__(
        self,
        database: Database,
        *,
        concurrency: int = AD_IMPRESSION_RETRY_CONCURRENCY,
        poll_seconds: float = AD_IMPRESSION_RETRY_POLL_SECONDS,
        shutdown_timeout: float = 5.0,
    ) -> None:
        self.database = database
        self.concurrency = max(1, concurrency)
        self.poll_seconds = poll_seconds
        self.shutdown_timeout = shutdown_timeout
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(
            self._run(), name="ad-impression-persistence-worker"
        )

    async def run_once(self) -> int:
        claimed = await self.database.claim_due_ad_impressions(self.concurrency)
        if claimed:
            await asyncio.gather(
                *(
                    self._persist(reservation_id, attempt)
                    for reservation_id, attempt in claimed
                )
            )
        return len(claimed)

    async def _persist(self, reservation_id: uuid.UUID, attempt: int) -> None:
        try:
            await self.database.record_ad_impression(reservation_id)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.exception(
                "Could not persist delivered ad impression "
                "(reservation_id=%s, attempt=%s/%s)",
                reservation_id,
                attempt,
                AD_IMPRESSION_MAX_ATTEMPTS,
            )
            try:
                await self.database.schedule_ad_impression_retry(
                    reservation_id, attempt, str(error)
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Could not schedule delivered ad impression retry "
                    "(reservation_id=%s)",
                    reservation_id,
                )
            if attempt >= AD_IMPRESSION_MAX_ATTEMPTS:
                logger.error(
                    "Delivered ad impression remains pending after retry limit "
                    "(reservation_id=%s)",
                    reservation_id,
                )

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                claimed_count = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Ad impression retry worker failed to claim records")
                claimed_count = 0

            if claimed_count == 0:
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=self.poll_seconds
                    )
                except TimeoutError:
                    pass

    async def stop(self) -> bool:
        self._stop_event.set()
        task = self._task
        if task is None:
            return True

        done, _ = await asyncio.wait({task}, timeout=self.shutdown_timeout)
        if not done:
            task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=1.0)
            except asyncio.CancelledError:
                pass
            except TimeoutError:
                logger.error(
                    "Ad impression worker task did not finish after cancellation"
                )
                return False
        if not done:
            done = {task} if task.done() else set()
        if not done:
            logger.error("Ad impression worker did not stop within its deadline")
            return False
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Ad impression worker stopped with an error")
        self._task = None
        return True
