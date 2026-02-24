from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import aiosqlite

from db.models import LAST_STATE_TABLE_SQL, USERS_TABLE_SQL, LastState, User

logger = logging.getLogger(__name__)

_USERS_COLUMN_ALTERS: dict[str, str] = {
    "region_slug": "ALTER TABLE users ADD COLUMN region_slug TEXT NOT NULL DEFAULT ''",
    "region_name": "ALTER TABLE users ADD COLUMN region_name TEXT NOT NULL DEFAULT ''",
    "queue": "ALTER TABLE users ADD COLUMN queue INTEGER NOT NULL DEFAULT 1",
    "subqueue": "ALTER TABLE users ADD COLUMN subqueue INTEGER NOT NULL DEFAULT 1",
    "notifications_enabled": "ALTER TABLE users ADD COLUMN notifications_enabled INTEGER NOT NULL DEFAULT 0",
    "quiet_start": "ALTER TABLE users ADD COLUMN quiet_start TEXT NULL",
    "quiet_end": "ALTER TABLE users ADD COLUMN quiet_end TEXT NULL",
}

_LAST_STATE_COLUMN_ALTERS: dict[str, str] = {
    "last_light_state": "ALTER TABLE last_state ADD COLUMN last_light_state TEXT NOT NULL DEFAULT 'on'",
    "last_checked_at": "ALTER TABLE last_state ADD COLUMN last_checked_at TEXT NOT NULL DEFAULT ''",
}

_USERS_REQUIRED_COLUMNS = {
    "telegram_id",
    "region_slug",
    "region_name",
    "queue",
    "subqueue",
    "notifications_enabled",
    "quiet_start",
    "quiet_end",
}

_LAST_STATE_REQUIRED_COLUMNS = {
    "telegram_id",
    "last_light_state",
    "last_checked_at",
}


class Database:
    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(Path(db_path))
        self._fallback_db_path = self._build_fallback_path()
        self._write_lock = asyncio.Lock()

    @property
    def db_path(self) -> str:
        return self._db_path

    async def init(self) -> None:
        async with self._connect() as connection:
            await connection.execute(USERS_TABLE_SQL)
            await connection.execute(LAST_STATE_TABLE_SQL)
            await self._migrate_schema(connection)
            await connection.commit()

    async def upsert_user_registration(
        self,
        telegram_id: int,
        region_slug: str,
        region_name: str,
        queue: int,
        subqueue: int,
    ) -> None:
        query = """
        INSERT INTO users (telegram_id, region_slug, region_name, queue, subqueue)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(telegram_id) DO UPDATE SET
            region_slug = excluded.region_slug,
            region_name = excluded.region_name,
            queue = excluded.queue,
            subqueue = excluded.subqueue
        """
        await self._execute_write(
            query,
            (telegram_id, region_slug, region_name, queue, subqueue),
        )

    async def delete_user(self, telegram_id: int) -> None:
        await self._execute_write("DELETE FROM users WHERE telegram_id = ?", (telegram_id,))

    async def get_user(self, telegram_id: int) -> Optional[User]:
        row = await self._fetchone(
            "SELECT * FROM users WHERE telegram_id = ?",
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
            "UPDATE users SET notifications_enabled = ? WHERE telegram_id = ?",
            (1 if enabled else 0, telegram_id),
        )
        return updated_rows > 0

    async def set_quiet_hours(self, telegram_id: int, quiet_start: str, quiet_end: str) -> bool:
        updated_rows = await self._execute_write(
            "UPDATE users SET quiet_start = ?, quiet_end = ? WHERE telegram_id = ?",
            (quiet_start, quiet_end, telegram_id),
        )
        return updated_rows > 0

    async def clear_quiet_hours(self, telegram_id: int) -> bool:
        updated_rows = await self._execute_write(
            "UPDATE users SET quiet_start = NULL, quiet_end = NULL WHERE telegram_id = ?",
            (telegram_id,),
        )
        return updated_rows > 0

    async def get_users_with_notifications(self) -> list[User]:
        rows = await self._fetchall(
            "SELECT * FROM users WHERE notifications_enabled = 1",
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
            "SELECT * FROM last_state WHERE telegram_id = ?",
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
        VALUES (?, ?, ?)
        ON CONFLICT(telegram_id) DO UPDATE SET
            last_light_state = excluded.last_light_state,
            last_checked_at = excluded.last_checked_at
        """
        await self._execute_write(query, (telegram_id, state, checked_at_iso))

    async def delete_last_state(self, telegram_id: int) -> None:
        await self._execute_write("DELETE FROM last_state WHERE telegram_id = ?", (telegram_id,))

    async def _fetchone(self, query: str, params: tuple) -> Optional[aiosqlite.Row]:
        async with self._connect() as connection:
            cursor = await connection.execute(query, params)
            row = await cursor.fetchone()
            await cursor.close()
            return row

    async def _fetchall(self, query: str, params: tuple) -> list[aiosqlite.Row]:
        async with self._connect() as connection:
            cursor = await connection.execute(query, params)
            rows = await cursor.fetchall()
            await cursor.close()
            return rows

    async def _execute_write(self, query: str, params: tuple) -> int:
        async with self._write_lock:
            async with self._connect() as connection:
                cursor = await connection.execute(query, params)
                await connection.commit()
                rowcount = cursor.rowcount
                await cursor.close()
                return rowcount

    @asynccontextmanager
    async def _connect(self):
        connection = await self._connect_with_fallback()
        try:
            await connection.execute("PRAGMA foreign_keys = ON")
            connection.row_factory = aiosqlite.Row
            yield connection
        finally:
            await connection.close()

    async def _connect_with_fallback(self) -> aiosqlite.Connection:
        primary = Path(self._db_path)
        fallback = self._fallback_db_path

        candidates: list[Path] = [primary]
        if fallback.resolve() != primary.resolve():
            candidates.append(fallback)

        last_error: Exception | None = None
        for index, candidate in enumerate(candidates):
            try:
                return await self._connect_single_path(candidate)
            except (sqlite3.OperationalError, PermissionError, OSError) as exc:
                last_error = exc
                is_primary = index == 0
                has_fallback = len(candidates) > 1
                if is_primary and has_fallback:
                    logger.warning(
                        "Primary DB path is unavailable for SQLite (%s): %s. Fallback to %s",
                        candidate,
                        exc,
                        fallback,
                    )
                    continue
                raise

        if last_error is not None:
            raise last_error
        raise RuntimeError("No SQLite path candidates available")

    async def _connect_single_path(self, db_file: Path) -> aiosqlite.Connection:
        if db_file.exists() and db_file.is_dir():
            raise RuntimeError(f"DB_PATH points to a directory, not a file: {db_file}")

        db_file.parent.mkdir(parents=True, exist_ok=True)
        connection = await aiosqlite.connect(str(db_file))

        if str(db_file) != self._db_path:
            logger.warning("Using fallback DB path: %s", db_file)
            self._db_path = str(db_file)
        return connection

    async def _migrate_schema(self, connection: aiosqlite.Connection) -> None:
        await self._migrate_table_columns(connection, "users", _USERS_COLUMN_ALTERS)
        await self._migrate_table_columns(connection, "last_state", _LAST_STATE_COLUMN_ALTERS)
        await self._ensure_table_shape(
            connection=connection,
            table_name="users",
            create_sql=USERS_TABLE_SQL,
            required_columns=_USERS_REQUIRED_COLUMNS,
            required_pk="telegram_id",
        )
        await self._ensure_table_shape(
            connection=connection,
            table_name="last_state",
            create_sql=LAST_STATE_TABLE_SQL,
            required_columns=_LAST_STATE_REQUIRED_COLUMNS,
            required_pk="telegram_id",
            expected_fk_target="users",
        )

    async def _migrate_table_columns(
        self,
        connection: aiosqlite.Connection,
        table_name: str,
        column_alters: dict[str, str],
    ) -> None:
        existing_columns = await self._get_table_columns(connection, table_name)
        for column_name, alter_sql in column_alters.items():
            if column_name in existing_columns:
                continue
            logger.warning("Applying schema migration: %s.%s", table_name, column_name)
            await connection.execute(alter_sql)

    async def _get_table_columns(
        self,
        connection: aiosqlite.Connection,
        table_name: str,
    ) -> set[str]:
        cursor = await connection.execute(f"PRAGMA table_info({table_name})")
        rows = await cursor.fetchall()
        await cursor.close()
        return {str(row["name"]) for row in rows}

    async def _ensure_table_shape(
        self,
        connection: aiosqlite.Connection,
        table_name: str,
        create_sql: str,
        required_columns: set[str],
        required_pk: str,
        expected_fk_target: str | None = None,
    ) -> None:
        info = await self._get_table_info(connection, table_name)
        existing_columns = {str(row["name"]) for row in info}
        pk_columns = {str(row["name"]) for row in info if int(row["pk"]) > 0}
        fk_ok = True
        fk_targets: set[str] = set()
        if expected_fk_target is not None:
            fk_targets = await self._get_foreign_key_targets(connection, table_name)
            fk_ok = fk_targets == {expected_fk_target}

        has_columns = required_columns.issubset(existing_columns)
        has_pk = required_pk in pk_columns
        if has_columns and has_pk and fk_ok:
            return

        logger.warning(
            "Table %s has incompatible schema (columns_ok=%s, pk_ok=%s, fk_ok=%s, fk_targets=%s). Rebuilding table.",
            table_name,
            has_columns,
            has_pk,
            fk_ok,
            sorted(fk_targets),
        )
        await self._rebuild_table(
            connection=connection,
            table_name=table_name,
            create_sql=create_sql,
            copy_columns=existing_columns.intersection(required_columns),
        )

    async def _rebuild_table(
        self,
        connection: aiosqlite.Connection,
        table_name: str,
        create_sql: str,
        copy_columns: set[str],
    ) -> None:
        legacy_name = f"{table_name}__legacy_{int(time.time())}"

        await connection.execute("PRAGMA foreign_keys = OFF")
        try:
            await connection.execute(f"ALTER TABLE {table_name} RENAME TO {legacy_name}")
            await connection.execute(create_sql)

            required_for_copy = self._required_columns_for_safe_copy(table_name)
            if required_for_copy.issubset(copy_columns):
                ordered_columns = sorted(copy_columns)
                csv = ", ".join(ordered_columns)
                await connection.execute(
                    f"INSERT OR IGNORE INTO {table_name} ({csv}) "
                    f"SELECT {csv} FROM {legacy_name}"
                )
            else:
                logger.warning(
                    "Skipping data copy for %s during rebuild, legacy columns are insufficient: %s",
                    table_name,
                    sorted(copy_columns),
                )

            await connection.execute(f"DROP TABLE IF EXISTS {legacy_name}")
        finally:
            await connection.execute("PRAGMA foreign_keys = ON")

    async def _get_table_info(
        self,
        connection: aiosqlite.Connection,
        table_name: str,
    ) -> list[aiosqlite.Row]:
        cursor = await connection.execute(f"PRAGMA table_info({table_name})")
        rows = await cursor.fetchall()
        await cursor.close()
        return rows

    async def _get_foreign_key_targets(
        self,
        connection: aiosqlite.Connection,
        table_name: str,
    ) -> set[str]:
        cursor = await connection.execute(f"PRAGMA foreign_key_list({table_name})")
        rows = await cursor.fetchall()
        await cursor.close()
        return {str(row["table"]) for row in rows if row["table"]}

    def _required_columns_for_safe_copy(self, table_name: str) -> set[str]:
        if table_name == "users":
            return {"telegram_id", "region_slug", "region_name", "queue", "subqueue"}
        if table_name == "last_state":
            return {"telegram_id", "last_light_state", "last_checked_at"}
        return set()

    def _build_fallback_path(self) -> Path:
        local_appdata = os.getenv("LOCALAPPDATA", "").strip()
        if local_appdata:
            return Path(local_appdata) / "LightUA" / "lightua.db"
        return Path.home() / ".lightua" / "lightua.db"
