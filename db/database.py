from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Optional

import asyncpg

from db.models import LastState, User

logger = logging.getLogger(__name__)

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
    last_checked_at TIMESTAMP
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
        await self._execute_write(query, (telegram_id, state, checked_at_iso))

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
