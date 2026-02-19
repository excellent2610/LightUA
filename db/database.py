import os
import sqlite3
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from config import DB_PATH


@dataclass(slots=True)
class UserSettings:
    user_id: int
    oblast: str
    queue: str
    subqueue: str
    notify_enabled: bool
    quiet_hours_from: Optional[str]
    quiet_hours_to: Optional[str]


class Database:
    def __init__(self, path: str) -> None:
        self.path = self._resolve_writable_db_path(path)
        self._ensure_db_path_writable()
        self._init_schema()

    def _resolve_writable_db_path(self, requested_path: str) -> str:
        requested = Path(requested_path)
        candidates = [requested]

        local_app_data = os.getenv("LOCALAPPDATA")
        if local_app_data:
            candidates.append(Path(local_app_data) / "LightUA" / "lightua.sqlite3")
        candidates.append(Path.home() / ".lightua" / "lightua.sqlite3")

        for candidate in candidates:
            try:
                candidate.parent.mkdir(parents=True, exist_ok=True)
                with open(candidate, "a", encoding="utf-8"):
                    pass
                return str(candidate)
            except OSError:
                continue

        raise PermissionError(
            "Не вдалося знайти директорію для запису SQLite. "
            "Задайте DB_PATH у доступну папку."
        )

    def _ensure_db_path_writable(self) -> None:
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        if os.path.exists(self.path):
            try:
                current_mode = os.stat(self.path).st_mode
                os.chmod(self.path, current_mode | stat.S_IWRITE)
            except OSError:
                pass

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    oblast TEXT NOT NULL DEFAULT '',
                    queue TEXT NOT NULL DEFAULT '',
                    subqueue TEXT NOT NULL DEFAULT '',
                    notify_enabled INTEGER NOT NULL DEFAULT 0,
                    quiet_hours_from TEXT,
                    quiet_hours_to TEXT
                )
                """
            )
            self._migrate_users_table(conn)

    def _migrate_users_table(self, conn: sqlite3.Connection) -> None:
        table_info = conn.execute("PRAGMA table_info(users)").fetchall()
        columns = {row["name"] for row in table_info}

        has_user_id_pk = any(row["name"] == "user_id" and int(row["pk"]) == 1 for row in table_info)
        has_blocking_extra_not_null = any(
            row["name"] not in {"user_id", "oblast", "queue", "subqueue", "notify_enabled", "quiet_hours_from", "quiet_hours_to"}
            and int(row["notnull"]) == 1
            and row["dflt_value"] is None
            for row in table_info
        )

        if not has_user_id_pk or has_blocking_extra_not_null:
            self._rebuild_users_table(conn, columns)
            table_info = conn.execute("PRAGMA table_info(users)").fetchall()
            columns = {row["name"] for row in table_info}

        migration_steps: list[tuple[str, str]] = [
            ("oblast", "ALTER TABLE users ADD COLUMN oblast TEXT NOT NULL DEFAULT ''"),
            ("queue", "ALTER TABLE users ADD COLUMN queue TEXT NOT NULL DEFAULT ''"),
            ("subqueue", "ALTER TABLE users ADD COLUMN subqueue TEXT NOT NULL DEFAULT ''"),
            ("notify_enabled", "ALTER TABLE users ADD COLUMN notify_enabled INTEGER NOT NULL DEFAULT 0"),
            ("quiet_hours_from", "ALTER TABLE users ADD COLUMN quiet_hours_from TEXT"),
            ("quiet_hours_to", "ALTER TABLE users ADD COLUMN quiet_hours_to TEXT"),
        ]

        for column_name, statement in migration_steps:
            if column_name not in columns:
                conn.execute(statement)

    def _rebuild_users_table(self, conn: sqlite3.Connection, old_columns: set[str]) -> None:
        conn.execute("ALTER TABLE users RENAME TO users_old")
        conn.execute(
            """
            CREATE TABLE users (
                user_id INTEGER PRIMARY KEY,
                oblast TEXT NOT NULL DEFAULT '',
                queue TEXT NOT NULL DEFAULT '',
                subqueue TEXT NOT NULL DEFAULT '',
                notify_enabled INTEGER NOT NULL DEFAULT 0,
                quiet_hours_from TEXT,
                quiet_hours_to TEXT
            )
            """
        )

        select_parts = [
            "user_id" if "user_id" in old_columns else "rowid AS user_id",
            "oblast" if "oblast" in old_columns else "'' AS oblast",
            "queue" if "queue" in old_columns else "'' AS queue",
            "subqueue" if "subqueue" in old_columns else "'' AS subqueue",
            "notify_enabled" if "notify_enabled" in old_columns else "0 AS notify_enabled",
            "quiet_hours_from" if "quiet_hours_from" in old_columns else "NULL AS quiet_hours_from",
            "quiet_hours_to" if "quiet_hours_to" in old_columns else "NULL AS quiet_hours_to",
        ]
        conn.execute(
            f"""
            INSERT OR IGNORE INTO users (
                user_id, oblast, queue, subqueue, notify_enabled, quiet_hours_from, quiet_hours_to
            )
            SELECT {", ".join(select_parts)}
            FROM users_old
            """
        )
        conn.execute("DROP TABLE users_old")

    def upsert_user(self, user_id: int, oblast: str, queue: str, subqueue: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users (user_id, oblast, queue, subqueue)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    oblast=excluded.oblast,
                    queue=excluded.queue,
                    subqueue=excluded.subqueue
                """,
                (user_id, oblast, queue, subqueue),
            )

    def get_user(self, user_id: int) -> Optional[UserSettings]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
        if not row:
            return None
        return UserSettings(
            user_id=row["user_id"],
            oblast=row["oblast"],
            queue=row["queue"],
            subqueue=row["subqueue"],
            notify_enabled=bool(row["notify_enabled"]),
            quiet_hours_from=row["quiet_hours_from"],
            quiet_hours_to=row["quiet_hours_to"],
        )

    def get_all_users_with_notifications(self) -> list[UserSettings]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM users WHERE notify_enabled = 1").fetchall()
        return [
            UserSettings(
                user_id=row["user_id"],
                oblast=row["oblast"],
                queue=row["queue"],
                subqueue=row["subqueue"],
                notify_enabled=bool(row["notify_enabled"]),
                quiet_hours_from=row["quiet_hours_from"],
                quiet_hours_to=row["quiet_hours_to"],
            )
            for row in rows
        ]

    def set_notify_enabled(self, user_id: int, enabled: bool) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE users SET notify_enabled = ? WHERE user_id = ?",
                (1 if enabled else 0, user_id),
            )

    def set_quiet_hours(self, user_id: int, time_from: Optional[str], time_to: Optional[str]) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE users SET quiet_hours_from = ?, quiet_hours_to = ? WHERE user_id = ?",
                (time_from, time_to, user_id),
            )


db = Database(DB_PATH)
