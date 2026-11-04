from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

from parsing.models import ScheduleInterval


KYIV_TZ = ZoneInfo("Europe/Kyiv")
_TIME_RE = re.compile(r"^(?P<hour>\d{1,2}):(?P<minute>[0-5]\d)$")
_RANGE_RE = re.compile(
    r"(?P<start>\d+:\d+)\s*[-–—]\s*(?P<end>\d+:\d+)"
)


def parse_clock_time(value: str, *, allow_24: bool = False) -> int:
    match = _TIME_RE.fullmatch(value.strip())
    if match is None:
        raise ValueError(f"Invalid schedule time: {value!r}")
    hour = int(match.group("hour"))
    minute = int(match.group("minute"))
    if hour == 24 and minute == 0 and allow_24:
        return 1440
    if hour > 23:
        raise ValueError(f"Invalid schedule time: {value!r}")
    return hour * 60 + minute


def interval_from_times(
    start_text: str,
    end_text: str,
    state: str,
) -> tuple[ScheduleInterval, ...]:
    start = parse_clock_time(start_text)
    end = parse_clock_time(end_text, allow_24=True)
    if end == start:
        raise ValueError("Zero-length schedule interval")
    if end > start:
        return (ScheduleInterval(start, end, state),)
    if end == 0:
        return (ScheduleInterval(start, 1440, state),)
    return (
        ScheduleInterval(start, 1440, state),
        ScheduleInterval(0, end, state),
    )


def parse_time_ranges(text: str, state: str) -> tuple[ScheduleInterval, ...]:
    intervals: list[ScheduleInterval] = []
    for match in _RANGE_RE.finditer(text):
        intervals.extend(
            interval_from_times(
                match.group("start"),
                match.group("end"),
                state,
            )
        )
    return normalize_intervals(intervals)


def normalize_intervals(
    intervals: Iterable[ScheduleInterval],
) -> tuple[ScheduleInterval, ...]:
    unique = sorted(
        set(intervals),
        key=lambda item: (item.start_minute, item.end_minute, item.state),
    )
    normalized: list[ScheduleInterval] = []
    for interval in unique:
        if normalized and interval.start_minute < normalized[-1].end_minute:
            raise ValueError("Overlapping schedule intervals")
        if (
            normalized
            and interval.start_minute == normalized[-1].end_minute
            and interval.state == normalized[-1].state
        ):
            previous = normalized.pop()
            normalized.append(
                ScheduleInterval(
                    previous.start_minute,
                    interval.end_minute,
                    previous.state,
                )
            )
        else:
            normalized.append(interval)
    return tuple(normalized)


def complete_from_outages(
    outages: Iterable[ScheduleInterval],
) -> tuple[ScheduleInterval, ...]:
    normalized_outages = normalize_intervals(outages)
    if any(interval.state != "off" for interval in normalized_outages):
        raise ValueError("Outage-only input may contain only off intervals")

    completed: list[ScheduleInterval] = []
    cursor = 0
    for interval in normalized_outages:
        if cursor < interval.start_minute:
            completed.append(ScheduleInterval(cursor, interval.start_minute, "on"))
        completed.append(interval)
        cursor = interval.end_minute
    if cursor < 1440:
        completed.append(ScheduleInterval(cursor, 1440, "on"))
    return normalize_intervals(completed)


def covers_full_day(intervals: Iterable[ScheduleInterval]) -> bool:
    normalized = normalize_intervals(intervals)
    if not normalized or normalized[0].start_minute != 0:
        return False
    return (
        normalized[-1].end_minute == 1440
        and all(
            previous.end_minute == current.start_minute
            for previous, current in zip(normalized, normalized[1:])
        )
    )


def local_date(value: datetime) -> date:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Datetime must be timezone-aware")
    return value.astimezone(KYIV_TZ).date()


def kyiv_timestamp_to_utc(
    schedule_day: date,
    hour: int,
    minute: int,
) -> datetime | None:
    """Convert a source-reported Kyiv wall time, rejecting DST ambiguity."""
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        return None
    naive = datetime.combine(schedule_day, time(hour, minute))
    first = naive.replace(tzinfo=KYIV_TZ, fold=0)
    second = naive.replace(tzinfo=KYIV_TZ, fold=1)
    first_utc = first.astimezone(timezone.utc)
    second_utc = second.astimezone(timezone.utc)
    if first_utc != second_utc:
        return None
    if first_utc.astimezone(KYIV_TZ).replace(tzinfo=None) != naive:
        return None
    return first_utc
