from __future__ import annotations

from datetime import datetime
from typing import Any

from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Kyiv")


async def get_user_settings(user_id: int) -> Any:
    from bot.notifications import store

    return await store.get_user(user_id)


async def update_user_settings(user_id: int, **kwargs: Any) -> None:
    from bot.notifications import store

    await store.update_settings(user_id, **kwargs)


def _parse_hhmm(value: str | None) -> tuple[int, int] | None:
    if not value:
        return None
    try:
        hh, mm = value.split(":", 1)
        h = int(hh)
        m = int(mm)
    except Exception:
        return None
    if not (0 <= h <= 23 and 0 <= m <= 59):
        return None
    return h, m


def is_quiet_time(now: datetime, settings: Any) -> bool:
    if not bool(getattr(settings, "quiet_hours_enabled", False)):
        return False

    start = _parse_hhmm(getattr(settings, "quiet_from", None))
    end = _parse_hhmm(getattr(settings, "quiet_to", None))
    if start is None or end is None:
        return False

    now_local = now.astimezone(TZ)
    now_minutes = now_local.hour * 60 + now_local.minute
    start_minutes = start[0] * 60 + start[1]
    end_minutes = end[0] * 60 + end[1]

    if start_minutes == end_minutes:
        return True
    if start_minutes < end_minutes:
        return start_minutes <= now_minutes < end_minutes
    return now_minutes >= start_minutes or now_minutes < end_minutes


def can_send_notification(event: Any, settings: Any) -> bool:
    if not bool(getattr(settings, "notifications_enabled", False)):
        return False

    ev_type = str(getattr(event, "event_type", "")).lower()
    if ev_type == "off" and not bool(getattr(settings, "notify_off_enabled", True)):
        return False
    if ev_type == "on" and not bool(getattr(settings, "notify_on_enabled", True)):
        return False

    now = datetime.now(TZ)
    if is_quiet_time(now, settings):
        return False

    return True
