from __future__ import annotations

import asyncio
import logging
import re
from contextlib import suppress
from datetime import datetime, timedelta

from aiogram import Bot

from db.database import Database
from db.models import User
from parsing.alerts_parser import AlertsParser, ParserError
from utils.formatting import format_light_change_message
from utils.time_utils import is_in_quiet_hours, now_kyiv

logger = logging.getLogger(__name__)
_RANGE_RE = re.compile(r"^([🟢🔴])\s+(\d{2}:\d{2})\s*[-–]\s*(\d{2}:\d{2})")


class LightStateScheduler:
    def __init__(
        self,
        bot: Bot,
        db: Database,
        parser: AlertsParser,
        interval_seconds: int = 60,
        max_parallel_checks: int = 5,
    ) -> None:
        self._bot = bot
        self._db = db
        self._parser = parser
        self._interval_seconds = interval_seconds
        self._max_parallel_checks = max_parallel_checks
        self._task: asyncio.Task | None = None
        self._last_warning_event_key_by_user: dict[int, str] = {}

    @staticmethod
    def _time_on_day(base: datetime, hhmm: str) -> datetime:
        hour_str, minute_str = hhmm.split(":")
        return base.replace(hour=int(hour_str), minute=int(minute_str), second=0, microsecond=0)

    def _next_change_from_graph(
        self,
        *,
        full_graph_text: str,
        now: datetime,
    ) -> tuple[datetime, str, str] | None:
        ranges: list[tuple[str, str, str]] = []
        for raw_line in full_graph_text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            match = _RANGE_RE.match(line)
            if not match:
                continue
            emoji, start, end = match.groups()
            ranges.append((emoji, start, end))

        candidates: list[tuple[datetime, str, str]] = []
        for emoji, start, end in ranges:
            start_dt = self._time_on_day(now, start)
            if start_dt <= now:
                continue
            event_type = "ВІДКЛЮЧЕННЯ" if emoji == "🔴" else "СВІТЛО Є"
            candidates.append((start_dt, event_type, f"{start} – {end}"))

        if not candidates:
            return None
        return min(candidates, key=lambda item: item[0])

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._run(), name="light-state-scheduler")
        logger.info("Scheduler started with interval=%ss", self._interval_seconds)

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        logger.info("Scheduler stopped")

    async def _run(self) -> None:
        while True:
            started = asyncio.get_running_loop().time()
            try:
                await self._check_once()
            except Exception:
                logger.exception("Unexpected scheduler failure")

            elapsed = asyncio.get_running_loop().time() - started
            sleep_for = max(0.0, self._interval_seconds - elapsed)
            await asyncio.sleep(sleep_for)

    async def _check_once(self) -> None:
        users = await self._db.get_users_with_notifications()
        if not users:
            return

        semaphore = asyncio.Semaphore(self._max_parallel_checks)
        tasks = [self._check_user(user=user, semaphore=semaphore) for user in users]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _check_user(self, user: User, semaphore: asyncio.Semaphore) -> None:
        async with semaphore:
            now = now_kyiv()
            try:
                state, _schedule = await asyncio.to_thread(
                    self._parser.get_current_state,
                    user.region_slug,
                    user.queue,
                    user.subqueue,
                )
            except ParserError:
                logger.warning(
                    "Parser error for user=%s (%s, %s.%s)",
                    user.telegram_id,
                    user.region_slug,
                    user.queue,
                    user.subqueue,
                )
                return
            except Exception:
                logger.exception("Unexpected check failure for user=%s", user.telegram_id)
                return

            # Send pre-warning 5 minutes before the next scheduled state change.
            try:
                from utils.formatting import format_graph_message

                full_graph_text = format_graph_message(schedule=_schedule, current_state=state)
                next_change = self._next_change_from_graph(full_graph_text=full_graph_text, now=now)
                if next_change is not None and not is_in_quiet_hours(now, user.quiet_start, user.quiet_end):
                    start_dt, event_type, interval_text = next_change
                    warning_dt = start_dt - timedelta(minutes=5)
                    warning_lag_seconds = (now - warning_dt).total_seconds()
                    # Send only near the planned warning timestamp, not any time later
                    # in the whole 5-minute window (e.g. right after bot restart).
                    if 0 <= warning_lag_seconds < self._interval_seconds:
                        event_key = f"{start_dt.isoformat()}|{event_type}|{interval_text}"
                        last_event_key = self._last_warning_event_key_by_user.get(user.telegram_id)
                        if last_event_key != event_key:
                            warning_text = (
                                "⚠️ Попередження за 5 хв\n"
                                f"{event_type} ({user.region_name} · {user.queue}.{user.subqueue})\n\n"
                                f"Інтервал: {interval_text}"
                            )
                            await self._bot.send_message(chat_id=user.telegram_id, text=warning_text)
                            self._last_warning_event_key_by_user[user.telegram_id] = event_key
            except Exception:
                logger.exception("Failed to process pre-warning for user=%s", user.telegram_id)

            previous = await self._db.get_last_state(user.telegram_id)
            await self._db.upsert_last_state(
                telegram_id=user.telegram_id,
                state=state,
                checked_at_iso=now.isoformat(),
            )

            if previous is None:
                return
            if previous.last_light_state == state:
                return
            if is_in_quiet_hours(now, user.quiet_start, user.quiet_end):
                return

            text = format_light_change_message(
                state=state,
                region_name=user.region_name,
                queue=user.queue,
                subqueue=user.subqueue,
            )
            try:
                await self._bot.send_message(chat_id=user.telegram_id, text=text)
            except Exception:
                logger.exception("Failed to send notification to user=%s", user.telegram_id)
