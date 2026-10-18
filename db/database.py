from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import datetime, time
from pathlib import Path
from typing import Optional

import asyncpg

from db.models import LastState, ScheduleChangeDelivery, User
from utils.time_utils import KYIV_TZ

logger = logging.getLogger(__name__)


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


class Database:
    def __init__(self, db_path: str | Path) -> None:
        self._database_url = os.getenv("DATABASE_URL", "").strip()
        self._db_path = "lightua_postgres"
        self._pool: asyncpg.Pool | None = None
        self._write_lock = asyncio.Lock()

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

    async def close(self):
        if self._pool:
            await self._pool.close()

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
