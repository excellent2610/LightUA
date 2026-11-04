from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from parsing.models import (
    ComparisonResult,
    ComparisonStatus,
    FetchStatus,
    SourceScheduleObservation,
)
from parsing.normalization import KYIV_TZ, covers_full_day


@dataclass(frozen=True, slots=True)
class SourceFreshnessPolicy:
    poll_interval_seconds: int = 900
    cache_ttl_seconds: int = 600
    minimum_request_interval_seconds: int = 10
    maximum_source_age: timedelta = timedelta(hours=36)
    maximum_observation_age: timedelta = timedelta(minutes=45)

    def __post_init__(self) -> None:
        if self.poll_interval_seconds < 1:
            raise ValueError("poll_interval_seconds must be positive")
        if self.cache_ttl_seconds < 0:
            raise ValueError("cache_ttl_seconds cannot be negative")
        if self.minimum_request_interval_seconds < 0:
            raise ValueError("minimum_request_interval_seconds cannot be negative")
        if self.maximum_source_age <= timedelta(0):
            raise ValueError("maximum_source_age must be positive")
        if self.maximum_observation_age <= timedelta(0):
            raise ValueError("maximum_observation_age must be positive")


def observation_is_stale(
    observation: SourceScheduleObservation,
    *,
    now: datetime,
    policy: SourceFreshnessPolicy,
) -> bool:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    now_utc = now.astimezone(timezone.utc)
    observed_age = now_utc - observation.observed_at
    if observed_age < timedelta(0) or observed_age > policy.maximum_observation_age:
        return True

    if observation.source_updated_at is not None:
        source_age = now_utc - observation.source_updated_at
        if source_age < timedelta(0) or source_age > policy.maximum_source_age:
            return True

    if observation.schedule_date is not None:
        if observation.schedule_date < now_utc.astimezone(KYIV_TZ).date():
            return True
    return False


def compare_observations(
    left: SourceScheduleObservation,
    right: SourceScheduleObservation,
    *,
    now: datetime,
    left_policy: SourceFreshnessPolicy,
    right_policy: SourceFreshnessPolicy,
) -> ComparisonResult:
    if left.fetch_status is not FetchStatus.SUCCESS or right.fetch_status is not FetchStatus.SUCCESS:
        raise ValueError("Only successful source observations can be compared")

    if left.operator_id is None or right.operator_id is None:
        return ComparisonResult(
            ComparisonStatus.NOT_COMPARABLE,
            "Operator identity is required for comparison",
        )
    if left.schedule_date is None or right.schedule_date is None:
        return ComparisonResult(
            ComparisonStatus.NOT_COMPARABLE,
            "Schedule date is required for comparison",
        )

    left_context = (
        left.region_slug,
        left.operator_id,
        left.queue,
        left.subqueue,
        left.schedule_date,
    )
    right_context = (
        right.region_slug,
        right.operator_id,
        right.queue,
        right.subqueue,
        right.schedule_date,
    )
    if left_context != right_context:
        return ComparisonResult(
            ComparisonStatus.NOT_COMPARABLE,
            "Region, operator, queue, subqueue, and schedule date must match",
        )

    if observation_is_stale(left, now=now, policy=left_policy) or observation_is_stale(
        right, now=now, policy=right_policy
    ):
        return ComparisonResult(ComparisonStatus.STALE, "At least one observation is stale")

    if not left.intervals or not right.intervals:
        return ComparisonResult(
            ComparisonStatus.INCOMPLETE,
            "A successful observation has no normalized schedule intervals",
        )
    if not covers_full_day(left.intervals) or not covers_full_day(right.intervals):
        return ComparisonResult(
            ComparisonStatus.INCOMPLETE,
            "At least one observation does not cover the full local day",
        )

    left_intervals = tuple(sorted(left.intervals))
    right_intervals = tuple(sorted(right.intervals))
    if left_intervals == right_intervals:
        return ComparisonResult(ComparisonStatus.MATCH, "Normalized schedules match")
    differing = tuple(
        sorted(
            set(left_intervals).symmetric_difference(right_intervals),
            key=lambda item: (item.start_minute, item.end_minute, item.state),
        )
    )
    return ComparisonResult(
        ComparisonStatus.MISMATCH,
        "Normalized schedules differ",
        differing_intervals=differing,
    )
