from __future__ import annotations

from datetime import datetime, time
from typing import Optional
from zoneinfo import ZoneInfo


KYIV_TZ = ZoneInfo("Europe/Kyiv")


def now_kyiv() -> datetime:
    return datetime.now(tz=KYIV_TZ)


def parse_hhmm(value: str) -> Optional[time]:
    raw = value.strip()
    try:
        parsed = datetime.strptime(raw, "%H:%M")
        return parsed.time()
    except ValueError:
        return None


def is_in_quiet_hours(now: datetime, quiet_start: Optional[str], quiet_end: Optional[str]) -> bool:
    if not quiet_start or not quiet_end:
        return False

    start_t = parse_hhmm(quiet_start)
    end_t = parse_hhmm(quiet_end)
    if start_t is None or end_t is None:
        return False

    current_t = now.astimezone(KYIV_TZ).time()
    if start_t == end_t:
        return True

    if start_t < end_t:
        return start_t <= current_t < end_t

    return current_t >= start_t or current_t < end_t


def format_time(value: Optional[str]) -> str:
    return value if value else "не налаштовано"
