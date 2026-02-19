import asyncio
import logging
from datetime import datetime

from aiogram import Bot

from config import CHECK_INTERVAL_SECONDS, NOTIFY_BEFORE_MINUTES
from config import OBLAST_TITLES
from db.database import Database, UserSettings
from services.models import NextEvent, TimeInterval
from services.parser import build_user_graph


def _parse_hhmm(value: str) -> tuple[int, int]:
    hour, minute = value.split(":")
    return int(hour), int(minute)


def _interval_start_on_date(now: datetime, interval: TimeInterval) -> datetime:
    hour, minute = _parse_hhmm(interval.start)
    if hour == 24:
        hour = 0
        target_day = now.date() + timedelta(days=1)
    else:
        target_day = now.date()
    return datetime.combine(target_day, datetime.min.time()).replace(hour=hour, minute=minute)


def _find_next_event(now: datetime, intervals: list[TimeInterval], event_type: str) -> NextEvent | None:
    candidates: list[NextEvent] = []
    for interval in intervals:
        start_at = _interval_start_on_date(now, interval)
        delta_minutes = int((start_at - now).total_seconds() // 60)
        if delta_minutes == NOTIFY_BEFORE_MINUTES:
            candidates.append(NextEvent(event_type=event_type, starts_at=start_at, interval=interval))
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: item.starts_at)[0]


def is_quiet_hours_now(now: datetime, quiet_from: str | None, quiet_to: str | None) -> bool:
    if not quiet_from or not quiet_to:
        return False
    from_h, from_m = _parse_hhmm(quiet_from)
    to_h, to_m = _parse_hhmm(quiet_to)
    start = now.replace(hour=from_h, minute=from_m, second=0, microsecond=0)
    end = now.replace(hour=to_h, minute=to_m, second=0, microsecond=0)

    # Інтервал тиші через північ, наприклад 22:00-07:00.
    if start <= end:
        return start <= now < end
    return now >= start or now < end


def _notify_message(user: UserSettings, event: NextEvent) -> str:
    title = "🔴 ВІДКЛЮЧЕННЯ СВІТЛА" if event.event_type == "outage" else "🟢 СВІТЛО Є"
    queue_subqueue = f"{int(user.queue)}.{int(user.subqueue)}"
    oblast_title = OBLAST_TITLES.get(user.oblast, user.oblast)
    return (
        f"{title} ({oblast_title} · {queue_subqueue})\n\n"
        f"{event.interval.start} – {event.interval.end}\n\n"
        "Джерело: alerts.org.ua"
    )


async def notification_loop(bot: Bot, db: Database) -> None:
    logging.info("Notification loop started")
    sent_events: set[str] = set()
    current_day = datetime.now().date()

    while True:
        now = datetime.now().replace(second=0, microsecond=0)
        if now.date() != current_day:
            sent_events.clear()
            current_day = now.date()

        users = db.get_all_users_with_notifications()
        for user in users:
            if is_quiet_hours_now(now, user.quiet_hours_from, user.quiet_hours_to):
                continue

            graph, error = build_user_graph(user.oblast, user.queue, user.subqueue)
            if error or not graph:
                logging.warning("Graph parse error for user %s: %s", user.user_id, error)
                continue

            outage_event = _find_next_event(now, graph.outage_intervals, "outage")
            power_on_event = _find_next_event(now, graph.power_on_intervals, "power_on")

            for event in [outage_event, power_on_event]:
                if not event:
                    continue
                event_key = f"{user.user_id}:{event.event_type}:{event.starts_at.isoformat()}"
                if event_key in sent_events:
                    continue
                sent_events.add(event_key)
                try:
                    await bot.send_message(chat_id=user.user_id, text=_notify_message(user, event))
                except Exception:
                    logging.exception("Failed to send notification to user %s", user.user_id)

        await asyncio.sleep(CHECK_INTERVAL_SECONDS)
