from __future__ import annotations

import re
from typing import Dict, List

from errors.data import ScheduleNotFoundError

TIME_RE = re.compile(r"^\d{2}:\d{2}$")


def _is_valid_time(value: str) -> bool:
    if not TIME_RE.fullmatch(value):
        return False
    hour = int(value[:2])
    minute = int(value[3:])
    if hour < 0 or hour > 24:
        return False
    if minute < 0 or minute > 59:
        return False
    if hour == 24 and minute != 0:
        return False
    return True


def validate_schedule_queues(queues: Dict[str, List[Dict[str, str]]]) -> None:
    if not queues:
        raise ScheduleNotFoundError("No queues found in schedule")

    has_intervals = False
    for queue, intervals in queues.items():
        if not intervals:
            continue
        for interval in intervals:
            start = interval.get("from", "")
            end = interval.get("to", "")
            if not _is_valid_time(start) or not _is_valid_time(end):
                raise ScheduleNotFoundError(f"Invalid interval in queue {queue}")
            has_intervals = True
    if not has_intervals:
        raise ScheduleNotFoundError("Queues found, but no valid intervals parsed")
