from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta

from aiogram import Router
from aiogram.filters import Command, StateFilter
from aiogram.types import Message

from bot.config import load_settings
from db.database import Database
from parsing.alerts_parser import (
    AlertsParser,
    EmergencyOutageError,
    RegionUnavailableError,
    ScheduleNotFoundError,
)
from utils.ads import get_ad_manager
from utils.formatting import format_graph_message
from utils.time_utils import now_kyiv

logger = logging.getLogger(__name__)

_RANGE_RE = re.compile(r"^([🟢🔴])\s+(\d{2}:\d{2})\s*[-–]\s*(\d{2}:\d{2})")


def _time_on_day(base: datetime, hhmm: str) -> datetime:
    hour, minute = hhmm.split(":")
    return base.replace(hour=int(hour), minute=int(minute), second=0, microsecond=0)


def _next_warning_from_graph(full_graph_text: str, now_dt: datetime) -> tuple[str, str, str] | None:
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

    candidates: list[tuple[datetime, str, str, str]] = []
    for emoji, start, end in ranges:
        start_dt = _time_on_day(now_dt, start)
        if start_dt <= now_dt:
            continue
        candidates.append((start_dt, emoji, start, end))

    if not candidates:
        return None

    start_dt, emoji, start, end = min(candidates, key=lambda item: item[0])
    event_type = "ВІДКЛЮЧЕННЯ" if emoji == "🔴" else "СВІТЛО Є"
    warning_time = (start_dt - timedelta(minutes=5)).strftime("%H:%M")
    return warning_time, event_type, f"{start} – {end}"


def _build_notify_on_text(
    *,
    region_name: str,
    queue: int,
    subqueue: int,
    full_graph_text: str,
) -> str:
    now_dt = now_kyiv()
    next_warning = _next_warning_from_graph(full_graph_text=full_graph_text, now_dt=now_dt)
    header = (
        "🔔 Сповіщення увімкнено.\n"
        f"{region_name} · Черга {queue}.{subqueue}\n"
        "Попередження за 5 хв."
    )

    if next_warning is None:
        return (
            f"{header}\n\n"
            "Найближче попередження:\n"
            "На сьогодні змін стану більше немає"
        )

    warning_time, event_type, interval = next_warning
    return (
        f"{header}\n\n"
        "Найближче попередження:\n"
        f"{warning_time} · {event_type}\n"
        f"Інтервал: {interval}"
    )


def get_router(db: Database) -> Router:
    router = Router(name="notify")
    ad_manager = get_ad_manager()

    @router.message(Command("help"), StateFilter("*"))
    async def help_command(message: Message) -> None:
        await ad_manager.send_with_ad(
            message.bot,
            message.chat.id,
            "Команди:\n"
            "/start - почати / змінити профіль\n"
            "/graph - графік на сьогодні\n"
            "/notify_on - увімкнути сповіщення\n"
            "/notify_off - вимкнути сповіщення\n"
            "/notify_test - тест сповіщення через 5 хв\n"
            "/quiet - налаштувати тиху годину\n"
            "/status - переглянути профіль\n"
            "/help - показати цю довідку"
        )

    @router.message(Command("notify_on"), StateFilter("*"))
    async def notify_on(message: Message) -> None:
        if message.from_user is None:
            return

        user = await db.get_user(message.from_user.id)
        if user is None:
            await ad_manager.send_with_ad(
                message.bot,
                message.chat.id,
                "Спочатку завершіть реєстрацію: /s",
            )
            return

        await db.set_notifications(message.from_user.id, True)

        try:
            settings = load_settings()
            parser = AlertsParser(timeout_seconds=settings.request_timeout_seconds)
            schedule = await asyncio.to_thread(
                parser.get_schedule,
                user.region_slug,
                user.queue,
                user.subqueue,
            )
            schedule.region_name = user.region_name
            current_state = schedule.state_at(now_kyiv())
            full_graph_text = format_graph_message(schedule=schedule, current_state=current_state)
            await ad_manager.send_with_ad(
                message.bot,
                message.chat.id,
                _build_notify_on_text(
                    region_name=user.region_name,
                    queue=user.queue,
                    subqueue=user.subqueue,
                    full_graph_text=full_graph_text,
                )
            )
        except EmergencyOutageError:
            await ad_manager.send_with_ad(
                message.bot,
                message.chat.id,
                "🔔 Сповіщення увімкнено.\n"
                f"{user.region_name} · Черга {user.queue}.{user.subqueue}\n"
                "Попередження за 5 хв.\n\n"
                "Найближче попередження:\n"
                "Недоступно через аварійні відключення"
            )
        except (ScheduleNotFoundError, RegionUnavailableError):
            await ad_manager.send_with_ad(
                message.bot,
                message.chat.id,
                "🔔 Сповіщення увімкнено.\n"
                f"{user.region_name} · Черга {user.queue}.{user.subqueue}\n"
                "Попередження за 5 хв.\n\n"
                "Найближче попередження:\n"
                "Недоступно, спробуйте /graph пізніше"
            )
        except Exception:
            logger.exception("Failed to build next warning in /notify_on")
            await ad_manager.send_with_ad(
                message.bot,
                message.chat.id,
                "🔔 Сповіщення увімкнено.\n"
                f"{user.region_name} · Черга {user.queue}.{user.subqueue}\n"
                "Попередження за 5 хв."
            )

    @router.message(Command("notify_test"), StateFilter("*"))
    async def notify_test(message: Message) -> None:
        if message.from_user is None:
            return

        user = await db.get_user(message.from_user.id)
        if user is None:
            await ad_manager.send_with_ad(
                message.bot,
                message.chat.id,
                "Спочатку завершіть реєстрацію: /start",
            )
            return

        await ad_manager.send_with_ad(
            message.bot,
            message.chat.id,
            "🧪 Тест заплановано. Тестове повідомлення прийде через 5 хвилин.",
        )

        async def send_delayed_test() -> None:
            await asyncio.sleep(5 * 60)
            await ad_manager.send_with_ad(
                message.bot,
                message.chat.id,
                (
                    "🧪 ТЕСТ СПОВІЩЕННЯ\n"
                    "Це перевірка доставки push-повідомлень.\n\n"
                    "Черкаська область · Черга 4.1\n"
                    "Затримка: 5 хв"
                ),
            )

        asyncio.create_task(send_delayed_test())

    @router.message(Command("notify_off"), StateFilter("*"))
    async def notify_off(message: Message) -> None:
        if message.from_user is None:
            return

        user = await db.get_user(message.from_user.id)
        if user is None:
            await ad_manager.send_with_ad(
                message.bot,
                message.chat.id,
                "Спочатку завершіть реєстрацію: /start",
            )
            return

        await db.set_notifications(message.from_user.id, False)
        await ad_manager.send_with_ad(
            message.bot,
            message.chat.id,
            "Сповіщення вимкнено.",
        )

    return router
