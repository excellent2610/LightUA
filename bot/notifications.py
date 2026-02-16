from __future__ import annotations

import asyncio
import importlib
import logging
import os
import sqlite3
import string
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from aiogram import Bot, Router
from aiogram.filters import Command
from aiogram.types import Message

from bot.config import get_schedule_url_for_region
from bot.regions import DEFAULT_REGION_KEY, get_region_name
from bot.user_settings import can_send_notification, is_quiet_time
from core.region_schedule import load_region_schedule

TZ = ZoneInfo("Europe/Kyiv")
DEFAULT_NOTIFY_BEFORE_MINUTES = 5
ALLOWED_NOTIFY_MINUTES = (5, 10, 15, 30)
log = logging.getLogger(__name__)

notifications_router = Router(name="notifications")
_notify_test_tasks: dict[int, asyncio.Task[None]] = {}


def _pick_writable_db_path() -> Path:
    env_path = os.getenv("LIGHTUA_NOTIFICATIONS_DB", "").strip()
    candidates: list[Path] = []
    if env_path:
        candidates.append(Path(env_path))

    # preferred project-local path
    candidates.append(Path(__file__).resolve().parents[1] / "data" / "notifications.db")

    # python executable drive (except C:)
    exe_drive = Path(sys.executable).drive.upper()
    if exe_drive and exe_drive != "C:":
        candidates.append(Path(f"{exe_drive}\\LightUA\\notifications.db"))

    # probe other drives except C:
    for letter in string.ascii_uppercase:
        drive = f"{letter}:"
        if drive == "C:" or drive == exe_drive:
            continue
        root = Path(f"{drive}\\")
        if root.exists():
            candidates.append(root / "LightUA" / "notifications.db")

    for candidate in candidates:
        try:
            candidate.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(candidate) as conn:
                conn.execute("SELECT 1")
            return candidate
        except Exception:
            continue
    raise RuntimeError("No writable path for notifications database.")


DB_PATH = _pick_writable_db_path()


@dataclass(slots=True)
class UserSettings:
    user_id: int
    region: str | None
    queue: str | None
    subqueue: str | None
    notifications_enabled: bool
    notify_before_minutes: int
    notify_off_enabled: bool
    notify_on_enabled: bool
    quiet_hours_enabled: bool
    quiet_from: str | None
    quiet_to: str | None


@dataclass(slots=True)
class Event:
    event_time: datetime
    event_type: str  # "on" | "off"
    interval: str
    start: str
    end: str


class NotificationStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._init_lock = asyncio.Lock()
        self._inited = False

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    async def init(self) -> None:
        async with self._init_lock:
            if self._inited:
                return
            await asyncio.to_thread(self._init_sync)
            self._inited = True

    def _init_sync(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_settings(
                  user_id INTEGER PRIMARY KEY,
                  region TEXT,
                  queue TEXT,
                  subqueue TEXT,
                  notifications_enabled INTEGER NOT NULL DEFAULT 1,
                  notify_before_minutes INTEGER NOT NULL DEFAULT 5,
                  notify_off_enabled INTEGER NOT NULL DEFAULT 1,
                  notify_on_enabled INTEGER NOT NULL DEFAULT 1,
                  quiet_hours_enabled INTEGER NOT NULL DEFAULT 0,
                  quiet_from TEXT,
                  quiet_to TEXT,
                  updated_at TEXT NOT NULL
                )
                """
            )
            self._ensure_user_settings_columns(conn)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sent_notifications(
                  user_id INTEGER NOT NULL,
                  event_key TEXT NOT NULL,
                  sent_at TEXT NOT NULL,
                  PRIMARY KEY(user_id, event_key)
                )
                """
            )
            conn.commit()

    def _ensure_user_settings_columns(self, conn: sqlite3.Connection) -> None:
        # Lightweight online migration for existing databases.
        rows = conn.execute("PRAGMA table_info(user_settings)").fetchall()
        existing = {str(row[1]) for row in rows}
        migrations: list[tuple[str, str]] = [
            ("region", "ALTER TABLE user_settings ADD COLUMN region TEXT"),
            ("notify_off_enabled", "ALTER TABLE user_settings ADD COLUMN notify_off_enabled INTEGER NOT NULL DEFAULT 1"),
            ("notify_on_enabled", "ALTER TABLE user_settings ADD COLUMN notify_on_enabled INTEGER NOT NULL DEFAULT 1"),
            ("quiet_hours_enabled", "ALTER TABLE user_settings ADD COLUMN quiet_hours_enabled INTEGER NOT NULL DEFAULT 0"),
            ("quiet_from", "ALTER TABLE user_settings ADD COLUMN quiet_from TEXT"),
            ("quiet_to", "ALTER TABLE user_settings ADD COLUMN quiet_to TEXT"),
        ]
        for col, ddl in migrations:
            if col not in existing:
                conn.execute(ddl)

    async def ensure_user(self, user_id: int) -> None:
        await self.init()
        await asyncio.to_thread(self._ensure_user_sync, user_id)

    def _ensure_user_sync(self, user_id: int) -> None:
        now = datetime.now(TZ).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO user_settings(user_id, updated_at)
                VALUES(?, ?)
                ON CONFLICT(user_id) DO NOTHING
                """,
                (user_id, now),
            )
            conn.commit()

    async def update_settings(
        self,
        user_id: int,
        *,
        region: str | None = None,
        queue: str | None = None,
        subqueue: str | None = None,
        notifications_enabled: bool | None = None,
        notify_before_minutes: int | None = None,
        notify_off_enabled: bool | None = None,
        notify_on_enabled: bool | None = None,
        quiet_hours_enabled: bool | None = None,
        quiet_from: str | None = None,
        quiet_to: str | None = None,
    ) -> None:
        await self.ensure_user(user_id)
        await asyncio.to_thread(
            self._update_settings_sync,
            user_id,
            region,
            queue,
            subqueue,
            notifications_enabled,
            notify_before_minutes,
            notify_off_enabled,
            notify_on_enabled,
            quiet_hours_enabled,
            quiet_from,
            quiet_to,
        )

    def _update_settings_sync(
        self,
        user_id: int,
        region: str | None,
        queue: str | None,
        subqueue: str | None,
        notifications_enabled: bool | None,
        notify_before_minutes: int | None,
        notify_off_enabled: bool | None,
        notify_on_enabled: bool | None,
        quiet_hours_enabled: bool | None,
        quiet_from: str | None,
        quiet_to: str | None,
    ) -> None:
        now = datetime.now(TZ).isoformat()
        with self._connect() as conn:
            if queue is not None:
                conn.execute(
                    "UPDATE user_settings SET queue=?, updated_at=? WHERE user_id=?",
                    (queue, now, user_id),
                )
            if region is not None:
                conn.execute(
                    "UPDATE user_settings SET region=?, updated_at=? WHERE user_id=?",
                    (region, now, user_id),
                )
            if subqueue is not None:
                conn.execute(
                    "UPDATE user_settings SET subqueue=?, updated_at=? WHERE user_id=?",
                    (subqueue, now, user_id),
                )
            if notifications_enabled is not None:
                conn.execute(
                    "UPDATE user_settings SET notifications_enabled=?, updated_at=? WHERE user_id=?",
                    (1 if notifications_enabled else 0, now, user_id),
                )
            if notify_before_minutes is not None:
                conn.execute(
                    "UPDATE user_settings SET notify_before_minutes=?, updated_at=? WHERE user_id=?",
                    (notify_before_minutes, now, user_id),
                )
            if notify_off_enabled is not None:
                conn.execute(
                    "UPDATE user_settings SET notify_off_enabled=?, updated_at=? WHERE user_id=?",
                    (1 if notify_off_enabled else 0, now, user_id),
                )
            if notify_on_enabled is not None:
                conn.execute(
                    "UPDATE user_settings SET notify_on_enabled=?, updated_at=? WHERE user_id=?",
                    (1 if notify_on_enabled else 0, now, user_id),
                )
            if quiet_hours_enabled is not None:
                conn.execute(
                    "UPDATE user_settings SET quiet_hours_enabled=?, updated_at=? WHERE user_id=?",
                    (1 if quiet_hours_enabled else 0, now, user_id),
                )
            if quiet_from is not None:
                conn.execute(
                    "UPDATE user_settings SET quiet_from=?, updated_at=? WHERE user_id=?",
                    (quiet_from, now, user_id),
                )
            if quiet_to is not None:
                conn.execute(
                    "UPDATE user_settings SET quiet_to=?, updated_at=? WHERE user_id=?",
                    (quiet_to, now, user_id),
                )
            conn.commit()

    async def get_user(self, user_id: int) -> UserSettings:
        await self.ensure_user(user_id)
        row = await asyncio.to_thread(self._get_user_sync, user_id)
        assert row is not None
        return UserSettings(
            user_id=row["user_id"],
            region=row["region"],
            queue=row["queue"],
            subqueue=row["subqueue"],
            notifications_enabled=bool(row["notifications_enabled"]),
            notify_before_minutes=int(row["notify_before_minutes"]),
            notify_off_enabled=bool(row["notify_off_enabled"]),
            notify_on_enabled=bool(row["notify_on_enabled"]),
            quiet_hours_enabled=bool(row["quiet_hours_enabled"]),
            quiet_from=row["quiet_from"],
            quiet_to=row["quiet_to"],
        )

    def _get_user_sync(self, user_id: int) -> sqlite3.Row | None:
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM user_settings WHERE user_id=?",
                (user_id,),
            ).fetchone()

    async def get_enabled_users(self) -> list[UserSettings]:
        await self.init()
        rows = await asyncio.to_thread(self._get_enabled_users_sync)
        return [
            UserSettings(
                user_id=r["user_id"],
                region=r["region"],
                queue=r["queue"],
                subqueue=r["subqueue"],
                notifications_enabled=bool(r["notifications_enabled"]),
                notify_before_minutes=int(r["notify_before_minutes"]),
                notify_off_enabled=bool(r["notify_off_enabled"]),
                notify_on_enabled=bool(r["notify_on_enabled"]),
                quiet_hours_enabled=bool(r["quiet_hours_enabled"]),
                quiet_from=r["quiet_from"],
                quiet_to=r["quiet_to"],
            )
            for r in rows
        ]

    def _get_enabled_users_sync(self) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return list(conn.execute("SELECT * FROM user_settings WHERE notifications_enabled=1").fetchall())

    async def was_sent(self, user_id: int, event_key: str) -> bool:
        await self.init()
        return await asyncio.to_thread(self._was_sent_sync, user_id, event_key)

    def _was_sent_sync(self, user_id: int, event_key: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM sent_notifications WHERE user_id=? AND event_key=?",
                (user_id, event_key),
            ).fetchone()
        return row is not None

    async def mark_sent(self, user_id: int, event_key: str) -> None:
        await self.init()
        await asyncio.to_thread(self._mark_sent_sync, user_id, event_key)

    def _mark_sent_sync(self, user_id: int, event_key: str) -> None:
        now = datetime.now(TZ).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO sent_notifications(user_id, event_key, sent_at)
                VALUES(?, ?, ?)
                """,
                (user_id, event_key, now),
            )
            conn.commit()


store = NotificationStore(DB_PATH)


def _schedule_module() -> Any:
    for name in ("core.parser", "utils.parser", "bot.parser", "parser"):
        try:
            return importlib.import_module(name)
        except ModuleNotFoundError:
            continue
    raise RuntimeError("Schedule parser module not found.")


def _load_schedule(region: str | None = None) -> list[dict[str, str]]:
    parser_mod = _schedule_module()
    region_key = (region or DEFAULT_REGION_KEY).strip().lower()
    region_url = get_schedule_url_for_region(region_key)
    try:
        direct = load_region_schedule(region_key)
        if isinstance(direct, list) and direct:
            return direct
    except Exception:
        pass

    os.environ["SCHEDULE_URL"] = region_url

    for fn_name in ("get_off_schedule", "get_data", "get_schedule", "fetch_and_parse_schedule", "get_blackout_periods"):
        fn = getattr(parser_mod, fn_name, None)
        if callable(fn):
            try:
                data = fn(url=region_url, timeout=35)
            except TypeError:
                try:
                    data = fn(url=region_url)
                except TypeError:
                    try:
                        data = fn(region=region_key, url=region_url)
                    except TypeError:
                        try:
                            data = fn(region=region_key)
                        except TypeError:
                            try:
                                data = fn(region_url)
                            except TypeError:
                                try:
                                    data = fn(region_key)
                                except TypeError:
                                    data = fn()
            if isinstance(data, list):
                return data
    raise RuntimeError("No compatible schedule getter.")


def _parse_hh_mm(value: str, ref: datetime) -> datetime:
    hh, mm = value.split(":", 1)
    h = int(hh)
    m = int(mm)
    if h == 24 and m == 0:
        return ref.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    return ref.replace(hour=h, minute=m, second=0, microsecond=0)


def _get_intervals(
    region_or_queue: str | None,
    queue_or_subqueue: str,
    subqueue: str | None = None,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    # Backward-compatible call forms:
    # _get_intervals(queue, subqueue)
    # _get_intervals(region, queue, subqueue)
    if subqueue is None:
        region = None
        queue = str(region_or_queue or "")
        subqueue = queue_or_subqueue
    else:
        region = region_or_queue
        queue = queue_or_subqueue

    raw = _load_schedule(region)
    from bot import handlers as handlers_mod

    intervals = handlers_mod._extract_for_queue_subqueue(raw, queue, subqueue)
    off_intervals, on_intervals = handlers_mod._split_off_on_intervals(intervals)
    return list(off_intervals), list(on_intervals)


def _build_events(now: datetime, off_intervals: list[dict[str, str]], on_intervals: list[dict[str, str]]) -> list[Event]:
    events: list[Event] = []
    seen: set[tuple[str, str, str]] = set()

    for it in off_intervals:
        start = str(it.get("start", "")).strip()
        end = str(it.get("end", "")).strip()
        if not start or not end:
            continue
        key = ("off", start, end)
        if key in seen:
            continue
        seen.add(key)
        events.append(
            Event(
                event_time=_parse_hh_mm(start, now),
                event_type="off",
                interval=f"{start} – {end}",
                start=start,
                end=end,
            )
        )

    for it in on_intervals:
        start = str(it.get("start", "")).strip()
        end = str(it.get("end", "")).strip()
        if not start or not end:
            continue
        key = ("on", start, end)
        if key in seen:
            continue
        seen.add(key)
        events.append(
            Event(
                event_time=_parse_hh_mm(start, now),
                event_type="on",
                interval=f"{start} – {end}",
                start=start,
                end=end,
            )
        )
    return events


def get_next_event(now: datetime, schedule: list[Event], notify_before_minutes: int) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    before = timedelta(minutes=notify_before_minutes)

    for ev in schedule:
        notify_time = ev.event_time - before
        if notify_time > now:
            candidates.append({"time": notify_time, "type": ev.event_type, "interval": ev.interval, "start": ev.start, "end": ev.end})
            continue

        notify_time_next = (ev.event_time + timedelta(days=1)) - before
        if notify_time_next > now:
            candidates.append(
                {"time": notify_time_next, "type": ev.event_type, "interval": ev.interval, "start": ev.start, "end": ev.end}
            )

    if not candidates:
        return None

    # earliest notify time; for exact tie prefer ON
    candidates.sort(key=lambda x: (x["time"], 0 if x["type"] == "on" else 1))
    return candidates[0]


def _preview_next_event(
    region_or_queue: str | None,
    queue_or_subqueue: str,
    subqueue_or_minutes: str | int,
    notify_before_minutes: int | None = None,
) -> str | None:
    # Backward-compatible call forms:
    # _preview_next_event(queue, subqueue, minutes)
    # _preview_next_event(region, queue, subqueue, minutes)
    if notify_before_minutes is None:
        region = None
        queue = str(region_or_queue or "")
        subqueue = str(queue_or_subqueue)
        notify_before_minutes = int(subqueue_or_minutes)
    else:
        region = region_or_queue
        queue = str(queue_or_subqueue)
        subqueue = str(subqueue_or_minutes)

    now = datetime.now(TZ)
    try:
        off_intervals, on_intervals = _get_intervals(region, queue, subqueue)
        schedule = _build_events(now, off_intervals, on_intervals)
        nxt = get_next_event(now, schedule, notify_before_minutes)
    except Exception:
        return None

    if nxt is None:
        return None
    kind = "ВВІМКНЕННЯ" if nxt["type"] == "off" else "ВІДКЛЮЧЕННЯ"
    return (
        "Найближче попередження:\n"
        f"{nxt['time'].strftime('%H:%M')} · {kind}\n"
        f"Інтервал: {nxt['interval']}"
    )


def _event_key(user_id: int, event_type: str, event_start_dt: datetime, start: str, end: str, before: int) -> str:
    day = event_start_dt.astimezone(TZ).strftime("%Y-%m-%d")
    return f"{user_id}|{day}|{event_type}|{start}|{end}|{before}"


async def send_notification(
    bot: Bot,
    user_id: int,
    ev_type: str,
    start: str,
    end: str,
    queue: str,
    subqueue: str,
    before: int,
    region: str | None = None,
) -> None:
    region_name = get_region_name(region or DEFAULT_REGION_KEY)
    queue_label = f"{queue}.{subqueue}" if subqueue else queue
    if ev_type == "off":
        text = (
            "⚠️ Увага!\n"
            f"Через {before} хвилин буде ВІДКЛЮЧЕННЯ світла ⚡\n\n"
            f"🕒 {start} – {end}\n"
            f"{region_name} · Черга {queue_label}"
        )
    else:
        text = (
            "✅ Гарні новини!\n"
            f"Через {before} хвилин СВІТЛО З'ЯВИТЬСЯ 💡\n\n"
            f"🕒 {start} – {end}\n"
            f"{region_name} · Черга {queue_label}"
        )
    await bot.send_message(user_id, text)


async def check_and_notify_users(bot: Bot) -> None:
    now = datetime.now(TZ)
    users = await store.get_enabled_users()
    for user in users:
        if not user.queue:
            continue
        try:
            off_intervals, on_intervals = _get_intervals(user.region, user.queue, user.subqueue)
            events = _build_events(now, off_intervals, on_intervals)
        except Exception:
            log.exception("Failed to build events for user=%s", user.user_id)
            continue

        due_candidates: list[tuple[datetime, Event]] = []
        before_delta = timedelta(minutes=user.notify_before_minutes)
        for ev in events:
            # current day
            notify_at = ev.event_time - before_delta
            # Keep event eligible after planned notify time until start time.
            # This allows delivery after quiet hours end without marking it lost.
            if notify_at <= now < ev.event_time:
                due_candidates.append((ev.event_time, ev))
            # next day
            ev_next = ev.event_time + timedelta(days=1)
            notify_at_next = ev_next - before_delta
            if notify_at_next <= now < ev_next:
                clone = Event(ev_next, ev.event_type, ev.interval, ev.start, ev.end)
                due_candidates.append((ev_next, clone))

        # same start minute -> prefer ON
        due_candidates.sort(key=lambda x: (x[0], 0 if x[1].event_type == "on" else 1))
        selected: dict[datetime, Event] = {}
        for start_dt, ev in due_candidates:
            if start_dt not in selected:
                selected[start_dt] = ev

        for start_dt, ev in selected.items():
            if not can_send_notification(ev, user):
                continue
            key = _event_key(user.user_id, ev.event_type, start_dt, ev.start, ev.end, user.notify_before_minutes)
            if await store.was_sent(user.user_id, key):
                continue
            try:
                await send_notification(
                    bot,
                    user.user_id,
                    ev.event_type,
                    ev.start,
                    ev.end,
                    user.queue,
                    user.subqueue,
                    user.notify_before_minutes,
                    user.region,
                )
                await store.mark_sent(user.user_id, key)
            except Exception:
                log.exception("Failed to send notification to user=%s", user.user_id)


class NotificationService:
    def __init__(self) -> None:
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    async def start(self, bot: Bot) -> None:
        await store.init()
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(bot), name="notifications-loop")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self, bot: Bot) -> None:
        while not self._stop.is_set():
            try:
                await check_and_notify_users(bot)
            except Exception:
                log.exception("Notification loop error")
            wait = max(1, 60 - datetime.now(TZ).second)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=wait)
            except asyncio.TimeoutError:
                continue


notification_service = NotificationService()


async def sync_user_selection(user_id: int, queue: str, subqueue: str, region: str | None = None) -> None:
    await store.update_settings(user_id, region=region, queue=queue, subqueue=subqueue)


def _parse_queue_subqueue(raw: str) -> tuple[str, str] | None:
    import re

    m = re.search(r"\b(\d+)\s*[\.\-]\s*(\d+)\b", raw)
    if not m:
        return None
    return m.group(1), m.group(2)


def _parse_hhmm(value: str) -> str | None:
    try:
        hh, mm = value.split(":", 1)
        h = int(hh)
        m = int(mm)
    except Exception:
        return None
    if 0 <= h <= 23 and 0 <= m <= 59:
        return f"{h:02d}:{m:02d}"
    return None


def _parse_quiet_hours_arg(raw: str) -> tuple[str, str] | None:
    text = raw.strip().replace("–", "-")
    if "-" in text:
        left, right = text.split("-", 1)
    else:
        parts = text.split()
        if len(parts) != 2:
            return None
        left, right = parts[0], parts[1]
    start = _parse_hhmm(left.strip())
    end = _parse_hhmm(right.strip())
    if start is None or end is None:
        return None
    return start, end


def _enabled_label(value: bool) -> str:
    return "увімкнено" if value else "вимкнено"


def _quiet_label(settings: UserSettings) -> str:
    if not settings.quiet_hours_enabled:
        return "вимкнено"
    if settings.quiet_from and settings.quiet_to:
        return f"{settings.quiet_from} – {settings.quiet_to}"
    return "увімкнено"


def _settings_text(settings: UserSettings) -> str:
    status = _enabled_label(settings.notifications_enabled)
    off_label = _enabled_label(settings.notify_off_enabled)
    on_label = _enabled_label(settings.notify_on_enabled)
    quiet = _quiet_label(settings)
    return (
        "⚙️ Налаштування сповіщень\n\n"
        f"🔔 Статус: {status}\n"
        f"⏱ Попередження: за {settings.notify_before_minutes} хв\n"
        f"🔴 Відключення: {off_label}\n"
        f"🟢 Ввімкнення: {on_label}\n"
        f"🌙 Тиха година: {quiet}"
    )


@notifications_router.message(Command("notify_on"))
async def notify_on_handler(message: Message) -> None:
    if message.from_user is None:
        return
    user_id = message.from_user.id
    await store.ensure_user(user_id)

    parsed = _parse_queue_subqueue((message.text or "").strip())
    if parsed is not None:
        await store.update_settings(user_id, queue=parsed[0], subqueue=parsed[1])

    await store.update_settings(user_id, notifications_enabled=True)
    settings = await store.get_user(user_id)
    if settings.queue and settings.subqueue:
        next_preview = _preview_next_event(settings.region, settings.queue, settings.subqueue, settings.notify_before_minutes)
        extra = f"\n\n{next_preview}" if next_preview else "\n\nНайближчих подій на сьогодні не знайдено."
        await message.answer(
            "🔔 Сповіщення увімкнено.\n"
            f"Черкаси · Черга {settings.queue}.{settings.subqueue}\n"
            f"Попередження за {settings.notify_before_minutes} хв."
            f"{extra}"
        )
    else:
        await message.answer(
            "🔔 Сповіщення увімкнено.\n"
            "Не бачу обрану чергу. Надішліть `/notify_on 4.1` або оберіть чергу через /start.",
            parse_mode="Markdown",
        )


@notifications_router.message(Command("notify_off"))
async def notify_off_handler(message: Message) -> None:
    if message.from_user is None:
        return
    await store.update_settings(message.from_user.id, notifications_enabled=False)
    await message.answer("🔕 Сповіщення вимкнено.")


@notifications_router.message(Command("notify_time"))
async def notify_time_handler(message: Message) -> None:
    if message.from_user is None:
        return
    user_id = message.from_user.id
    parts = (message.text or "").strip().split(maxsplit=1)
    if len(parts) < 2:
        await message.answer(
            "Оберіть час попередження: 5, 10, 15 або 30 хв.\n"
            "Приклад: `/notify_time 10`",
            parse_mode="Markdown",
        )
        return
    try:
        minutes = int(parts[1].strip())
    except ValueError:
        await message.answer("Вкажіть одне з значень: 5, 10, 15, 30.")
        return
    if minutes not in ALLOWED_NOTIFY_MINUTES:
        await message.answer("Доступні значення: 5, 10, 15, 30 хв.")
        return
    await store.update_settings(user_id, notify_before_minutes=minutes)
    await message.answer(f"⏱ Час попередження змінено: {minutes} хв.")


@notifications_router.message(Command("notify_types"))
async def notify_types_handler(message: Message) -> None:
    if message.from_user is None:
        return
    user_id = message.from_user.id
    await store.ensure_user(user_id)
    parts = (message.text or "").strip().split(maxsplit=1)
    if len(parts) < 2:
        settings = await store.get_user(user_id)
        off_mark = "✔️" if settings.notify_off_enabled else "✖️"
        on_mark = "✔️" if settings.notify_on_enabled else "✖️"
        await message.answer(
            "Типи сповіщень:\n"
            f"[{off_mark}] Відключення\n"
            f"[{on_mark}] Ввімкнення\n\n"
            "Керування: `/notify_types 3`\n"
            "0 = вимкнути обидва, 1 = лише відключення,\n"
            "2 = лише ввімкнення, 3 = обидва",
            parse_mode="Markdown",
        )
        return

    arg = parts[1].strip().lower()
    mapping: dict[str, tuple[bool, bool]] = {
        "0": (False, False),
        "none": (False, False),
        "1": (True, False),
        "off": (True, False),
        "2": (False, True),
        "on": (False, True),
        "3": (True, True),
        "both": (True, True),
        "all": (True, True),
    }
    if arg not in mapping:
        await message.answer("Невірне значення. Використайте `/notify_types 0|1|2|3`.", parse_mode="Markdown")
        return

    off_enabled, on_enabled = mapping[arg]
    await store.update_settings(
        user_id,
        notify_off_enabled=off_enabled,
        notify_on_enabled=on_enabled,
    )
    await message.answer(
        "Оновлено типи сповіщень:\n"
        f"🔴 Відключення: {_enabled_label(off_enabled)}\n"
        f"🟢 Ввімкнення: {_enabled_label(on_enabled)}"
    )


@notifications_router.message(Command("quiet_hours"))
async def quiet_hours_handler(message: Message) -> None:
    if message.from_user is None:
        return
    user_id = message.from_user.id
    await store.ensure_user(user_id)
    parts = (message.text or "").strip().split(maxsplit=1)
    if len(parts) < 2:
        settings = await store.get_user(user_id)
        await message.answer(
            "Тиха година:\n"
            f"Поточний стан: {_quiet_label(settings)}\n\n"
            "Команди:\n"
            "`/quiet_hours off` - вимкнути\n"
            "`/quiet_hours on` - увімкнути з поточним інтервалом\n"
            "`/quiet_hours 23:00-07:00` - задати інтервал",
            parse_mode="Markdown",
        )
        return

    arg = parts[1].strip()
    lowered = arg.lower()
    if lowered in {"off", "disable", "0"}:
        await store.update_settings(user_id, quiet_hours_enabled=False)
        await message.answer("🌙 Тиху годину вимкнено.")
        return

    if lowered in {"on", "enable", "1"}:
        settings = await store.get_user(user_id)
        if not settings.quiet_from or not settings.quiet_to:
            await message.answer("Спочатку задайте інтервал: `/quiet_hours 23:00-07:00`", parse_mode="Markdown")
            return
        await store.update_settings(user_id, quiet_hours_enabled=True)
        await message.answer(f"🌙 Тиху годину увімкнено: {settings.quiet_from} – {settings.quiet_to}")
        return

    parsed = _parse_quiet_hours_arg(arg)
    if parsed is None:
        await message.answer("Невірний формат. Приклад: `/quiet_hours 23:00-07:00`", parse_mode="Markdown")
        return
    quiet_from, quiet_to = parsed
    await store.update_settings(
        user_id,
        quiet_hours_enabled=True,
        quiet_from=quiet_from,
        quiet_to=quiet_to,
    )
    await message.answer(f"🌙 Тиху годину увімкнено: {quiet_from} – {quiet_to}")


@notifications_router.message(Command("settings"))
@notifications_router.message(Command("status"))
async def status_handler(message: Message) -> None:
    if message.from_user is None:
        return
    settings = await store.get_user(message.from_user.id)
    queue_label = f"{settings.queue}.{settings.subqueue}" if settings.queue and settings.subqueue else "не обрано"

    next_block = ""
    if (
        settings.notifications_enabled
        and settings.queue
        and settings.subqueue
        and not is_quiet_time(datetime.now(TZ), settings)
    ):
        next_preview = _preview_next_event(settings.queue, settings.subqueue, settings.notify_before_minutes)
        next_block = f"\n{next_preview}" if next_preview else "\nНайближчих подій на сьогодні не знайдено."

    await message.answer(
        f"{_settings_text(settings)}\n"
        f"📍 Черга: {queue_label}"
        f"{next_block}"
    )


@notifications_router.message(Command("notify_test"))
async def notify_test_handler(message: Message) -> None:
    if message.from_user is None:
        return
    user_id = message.from_user.id
    task = _notify_test_tasks.get(user_id)
    if task and not task.done():
        task.cancel()

    async def _send_later() -> None:
        try:
            await asyncio.sleep(5 * 60)
            settings = await store.get_user(user_id)
            q = settings.queue or "4"
            sq = settings.subqueue or "1"
            await message.bot.send_message(
                user_id,
                "🧪 ТЕСТ СПОВІЩЕННЯ\n"
                "Це перевірка доставки push-повідомлень.\n\n"
                f"Черкаси · Черга {q}.{sq}\n"
                "Затримка: 5 хв",
            )
        except asyncio.CancelledError:
            return
        finally:
            if _notify_test_tasks.get(user_id) is asyncio.current_task():
                _notify_test_tasks.pop(user_id, None)

    _notify_test_tasks[user_id] = asyncio.create_task(_send_later(), name=f"notify-test-{user_id}")
    await message.answer("🧪 Тест заплановано. Тестове повідомлення прийде через 5 хвилин.")
