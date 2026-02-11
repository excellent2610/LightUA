from __future__ import annotations

import re
from datetime import datetime
from typing import List, Optional, Set, Tuple

from errors.data import InvalidTimeFormatError

QUEUE_RE = re.compile(r"\b([1-6])\s*[.\-]\s*([1-2])\b")
INTERVAL_RE = re.compile(
    r"(?P<start>\d{1,2}[:.]\d{2})\s*(?:-|–|—|до|to)\s*(?P<end>\d{1,2}[:.]\d{2})",
    re.IGNORECASE,
)


def clean_text(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def normalize_queue(value: str) -> Optional[str]:
    match = QUEUE_RE.search(clean_text(value))
    if not match:
        return None
    return f"{int(match.group(1))}.{int(match.group(2))}"


def normalize_time(value: str) -> str:
    candidate = clean_text(value).replace(".", ":")
    if candidate == "24:00":
        return "24:00"
    try:
        parsed = datetime.strptime(candidate, "%H:%M")
    except ValueError as exc:
        raise InvalidTimeFormatError(f"Invalid time format: {value}") from exc
    return parsed.strftime("%H:%M")


def split_intervals(value: str) -> List[Tuple[str, str]]:
    text = clean_text(value)
    intervals: List[Tuple[str, str]] = []
    for match in INTERVAL_RE.finditer(text):
        start = normalize_time(match.group("start"))
        end = normalize_time(match.group("end"))
        intervals.append((start, end))
    seen: Set[Tuple[str, str]] = set()
    unique: List[Tuple[str, str]] = []
    for interval in intervals:
        if interval in seen:
            continue
        seen.add(interval)
        unique.append(interval)
    return unique
