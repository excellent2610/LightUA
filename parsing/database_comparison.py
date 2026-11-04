from __future__ import annotations

from datetime import datetime

from db.database import Database
from parsing.comparison import SourceFreshnessPolicy, compare_observations
from parsing.models import ComparisonResult, FetchStatus, SourceScheduleObservation


async def compare_with_latest_database_observation(
    database: Database,
    observation: SourceScheduleObservation,
    *,
    now: datetime,
    policy: SourceFreshnessPolicy,
) -> ComparisonResult | None:
    """Compare only with prior source observations, never user schedule snapshots."""
    if (
        observation.fetch_status is not FetchStatus.SUCCESS
        or observation.operator_id is None
        or observation.schedule_date is None
    ):
        return None

    previous = await database.get_latest_source_schedule_observation(
        source_id=observation.source_id,
        region_slug=observation.region_slug,
        operator_id=observation.operator_id,
        queue=observation.queue,
        subqueue=observation.subqueue,
        schedule_date=observation.schedule_date,
    )
    if previous is None:
        return None
    return compare_observations(
        previous,
        observation,
        now=now,
        left_policy=policy,
        right_policy=policy,
    )
