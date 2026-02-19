from datetime import datetime, timedelta

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from config import OBLAST_TITLES
from db.database import db
from services.models import TimeInterval
from services.parser import build_user_graph

router = Router()


def _parse_hhmm(value: str) -> tuple[int, int]:
    hour, minute = value.split(":")
    return int(hour), int(minute)


def _interval_start_datetime(now: datetime, interval: TimeInterval) -> datetime:
    hour, minute = _parse_hhmm(interval.start)
    if hour == 24:
        hour = 0
        day = now.date() + timedelta(days=1)
    else:
        day = now.date()
    return datetime.combine(day, datetime.min.time()).replace(hour=hour, minute=minute)


def _find_nearest_event(
    outage_intervals: list[TimeInterval],
    power_on_intervals: list[TimeInterval],
) -> tuple[str, TimeInterval, datetime] | None:
    now = datetime.now()
    candidates: list[tuple[str, TimeInterval, datetime]] = []
    for interval in outage_intervals:
        start_at = _interval_start_datetime(now, interval)
        if start_at > now:
            candidates.append(("ВІДКЛЮЧЕННЯ", interval, start_at))
    for interval in power_on_intervals:
        start_at = _interval_start_datetime(now, interval)
        if start_at > now:
            candidates.append(("ВВІМКНЕННЯ", interval, start_at))
    if not candidates:
        return None
    return min(candidates, key=lambda item: item[2])


@router.message(Command("notify_on"))
async def cmd_notify_on(message: Message) -> None:
    user = db.get_user(message.from_user.id)
    if not user:
        await message.answer("Спочатку налаштуйте область/чергу через /start.")
        return

    db.set_notify_enabled(message.from_user.id, True)

    graph, error = build_user_graph(user.oblast, user.queue, user.subqueue)
    if error or not graph:
        await message.answer("🔔 Сповіщення увімкнено.\nПопередження за 5 хв.")
        return

    nearest = _find_nearest_event(graph.outage_intervals, graph.power_on_intervals)
    oblast_title = OBLAST_TITLES.get(user.oblast, user.oblast)
    queue_subqueue = f"{int(user.queue)}.{int(user.subqueue)}"

    if not nearest:
        await message.answer(
            "🔔 Сповіщення увімкнено.\n"
            f"{oblast_title} · Черга {queue_subqueue}\n"
            "Попередження за 5 хв.\n\n"
            "Найближче попередження:\n"
            "Сьогодні подій більше немає"
        )
        return

    event_type, interval, starts_at = nearest
    warn_at = starts_at - timedelta(minutes=5)
    await message.answer(
        "🔔 Сповіщення увімкнено.\n"
        f"{oblast_title} · Черга {queue_subqueue}\n"
        "Попередження за 5 хв.\n\n"
        "Найближче попередження:\n"
        f"{warn_at.strftime('%H:%M')} · {event_type}\n"
        f"Інтервал: {interval.start} – {interval.end}"
    )


@router.message(Command("notify_off"))
async def cmd_notify_off(message: Message) -> None:
    user = db.get_user(message.from_user.id)
    if not user:
        await message.answer("Спочатку налаштуйте область/чергу через /start.")
        return
    db.set_notify_enabled(message.from_user.id, False)
    await message.answer("Сповіщення вимкнено.")
