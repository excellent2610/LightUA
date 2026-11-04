from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
import hashlib
import json


class FetchStatus(str, Enum):
    SUCCESS = "success"
    INCOMPLETE = "incomplete"
    UNAVAILABLE = "unavailable"
    ERROR = "error"


class ComparisonStatus(str, Enum):
    MATCH = "match"
    MISMATCH = "mismatch"
    NOT_COMPARABLE = "not_comparable"
    STALE = "stale"
    INCOMPLETE = "incomplete"


@dataclass(frozen=True, slots=True, order=True)
class ScheduleInterval:
    """A local-clock interval in minutes since midnight; 1440 means 24:00."""

    start_minute: int
    end_minute: int
    state: str

    def __post_init__(self) -> None:
        if not 0 <= self.start_minute < 1440:
            raise ValueError("Interval start must be within the schedule day")
        if not 0 < self.end_minute <= 1440:
            raise ValueError("Interval end must be within the schedule day")
        if self.start_minute >= self.end_minute:
            raise ValueError("Intervals must be split at midnight")
        if self.state not in {"on", "off"}:
            raise ValueError("Interval state must be 'on' or 'off'")


@dataclass(frozen=True, slots=True)
class SourceTarget:
    source_url: str
    region_slug: str
    queue: int
    subqueue: int

    def __post_init__(self) -> None:
        if not self.source_url.startswith("https://"):
            raise ValueError("Source URLs must use HTTPS")
        if not self.region_slug:
            raise ValueError("Region slug is required")
        if self.queue < 1 or self.subqueue < 1:
            raise ValueError("Queue and subqueue must be positive")


@dataclass(frozen=True, slots=True)
class SourceScheduleObservation:
    source_id: str
    region_slug: str
    operator_id: str | None
    queue: int
    subqueue: int
    schedule_date: date | None
    observed_at: datetime
    source_updated_at: datetime | None
    fetch_status: FetchStatus
    intervals: tuple[ScheduleInterval, ...] = ()
    source_url: str | None = None
    error_code: str | None = None
    content_hash: str | None = None
    retry_after_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.source_id or not self.region_slug:
            raise ValueError("Source and region identifiers are required")
        if self.queue < 1 or self.subqueue < 1:
            raise ValueError("Queue and subqueue must be positive")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        if self.source_updated_at is not None and (
            self.source_updated_at.tzinfo is None
            or self.source_updated_at.utcoffset() is None
        ):
            raise ValueError("source_updated_at must be timezone-aware")
        if self.retry_after_at is not None and (
            self.retry_after_at.tzinfo is None
            or self.retry_after_at.utcoffset() is None
        ):
            raise ValueError("retry_after_at must be timezone-aware")
        object.__setattr__(
            self, "observed_at", self.observed_at.astimezone(timezone.utc)
        )
        if self.source_updated_at is not None:
            object.__setattr__(
                self,
                "source_updated_at",
                self.source_updated_at.astimezone(timezone.utc),
            )
        if self.retry_after_at is not None:
            object.__setattr__(
                self,
                "retry_after_at",
                self.retry_after_at.astimezone(timezone.utc),
            )
        if self.fetch_status is FetchStatus.SUCCESS and self.retry_after_at is not None:
            raise ValueError("Successful observations cannot have a retry deadline")
        if self.fetch_status is not FetchStatus.SUCCESS and self.intervals:
            raise ValueError("Only successful fetches may contain schedule intervals")
        if self.fetch_status is FetchStatus.SUCCESS:
            canonical_intervals = sorted(
                {
                    (interval.start_minute, interval.end_minute, interval.state)
                    for interval in self.intervals
                }
            )
            canonical_content = json.dumps(
                canonical_intervals,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
            calculated_hash = hashlib.sha256(canonical_content).hexdigest()
            if self.content_hash is not None and self.content_hash != calculated_hash:
                raise ValueError("content_hash does not match normalized schedule intervals")
            object.__setattr__(self, "content_hash", calculated_hash)
        else:
            object.__setattr__(self, "content_hash", None)


@dataclass(frozen=True, slots=True)
class ComparisonResult:
    comparison_status: ComparisonStatus
    reason: str
    differing_intervals: tuple[ScheduleInterval, ...] = ()
