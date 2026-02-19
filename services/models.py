from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(slots=True)
class TimeInterval:
    start: str
    end: str


@dataclass(slots=True)
class UserGraph:
    oblast_title: str
    queue_subqueue: str
    outage_intervals: list[TimeInterval]
    power_on_intervals: list[TimeInterval]


@dataclass(slots=True)
class NextEvent:
    event_type: str
    starts_at: datetime
    interval: TimeInterval


@dataclass(slots=True)
class ParsedOblastData:
    outage: dict[str, list[TimeInterval]]
    power_on: dict[str, list[TimeInterval]]


@dataclass(slots=True)
class ParseResult:
    data: ParsedOblastData
    fetched_at: datetime
    source_url: str
    error: Optional[str] = None
