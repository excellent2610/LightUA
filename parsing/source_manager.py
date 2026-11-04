from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Callable, Mapping

import requests

from db.database import Database
from parsing.alerts_energy import AlertsEnergyParser
from parsing.alerts_org import AlertsOrgParser
from parsing.base import SourceAdapter
from parsing.comparison import (
    SourceFreshnessPolicy,
    compare_observations,
)
from parsing.database_comparison import compare_with_latest_database_observation
from parsing.kolysvitlo import KolySvitloParser
from parsing.models import (
    ComparisonResult,
    FetchStatus,
    SourceScheduleObservation,
    SourceTarget,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SourcePollReport:
    observations: tuple[SourceScheduleObservation, ...]
    pairwise_comparisons: Mapping[tuple[str, str], ComparisonResult]
    database_comparisons: Mapping[str, ComparisonResult]


DEFAULT_SOURCE_TARGETS: dict[str, tuple[SourceTarget, ...]] = {
    "alerts_energy": (
        SourceTarget(
            "https://alerts.energy/cherkaska-oblast/schedule/cherkaska_oblast_uz/4.2",
            "cherkaska-oblast",
            4,
            2,
        ),
    ),
    "kolysvitlo": (
        SourceTarget(
            "https://kolysvitlo.com/schedule/cherkasy_oblenergo",
            "cherkaska-oblast",
            4,
            2,
        ),
    ),
    "alerts_org": (
        SourceTarget(
            "https://alerts.org.ua/cherkaska-oblast/",
            "cherkaska-oblast",
            4,
            2,
        ),
    ),
}

DEFAULT_SOURCE_POLICIES: dict[str, SourceFreshnessPolicy] = {
    "alerts_energy": SourceFreshnessPolicy(
        poll_interval_seconds=900,
        cache_ttl_seconds=600,
        minimum_request_interval_seconds=10,
        maximum_source_age=timedelta(hours=36),
        maximum_observation_age=timedelta(minutes=45),
    ),
    "kolysvitlo": SourceFreshnessPolicy(
        poll_interval_seconds=900,
        cache_ttl_seconds=600,
        minimum_request_interval_seconds=10,
        maximum_source_age=timedelta(hours=30),
        maximum_observation_age=timedelta(minutes=45),
    ),
    "alerts_org": SourceFreshnessPolicy(
        poll_interval_seconds=900,
        cache_ttl_seconds=600,
        minimum_request_interval_seconds=10,
        maximum_source_age=timedelta(hours=30),
        maximum_observation_age=timedelta(minutes=45),
    ),
}

DEFAULT_ADAPTERS: dict[str, SourceAdapter] = {
    "alerts_energy": AlertsEnergyParser(),
    "kolysvitlo": KolySvitloParser(),
    "alerts_org": AlertsOrgParser(),
}

_MAX_DATETIME = datetime.max.replace(tzinfo=timezone.utc)


class SourceScheduleManager:
    def __init__(
        self,
        database: Database,
        *,
        enabled_sources: Mapping[str, bool],
        policies: Mapping[str, SourceFreshnessPolicy] | None = None,
        targets: Mapping[str, tuple[SourceTarget, ...]] | None = None,
        adapters: Mapping[str, SourceAdapter] | None = None,
        timeout_seconds: int = 15,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._enabled_sources = dict(enabled_sources)
        self._policies = dict(policies or DEFAULT_SOURCE_POLICIES)
        self._targets = dict(targets or DEFAULT_SOURCE_TARGETS)
        self._adapters = dict(adapters or DEFAULT_ADAPTERS)
        self._timeout_seconds = timeout_seconds
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._last_attempt_at: dict[str, datetime] = {}
        self._next_request_at: dict[str, datetime] = {}
        self._retry_after_restored: set[str] = set()
        self._cache: dict[
            tuple[str, str, str, int, int],
            tuple[datetime, SourceScheduleObservation],
        ] = {}
        self._latest_observations: dict[str, SourceScheduleObservation] = {}
        self._lock = asyncio.Lock()
        self._validate_configuration()

    def _validate_configuration(self) -> None:
        for source_id, enabled in self._enabled_sources.items():
            if not isinstance(enabled, bool):
                raise TypeError(f"Enabled flag for {source_id} must be bool")
            if source_id not in self._adapters or source_id not in self._policies:
                raise ValueError(f"Missing adapter or policy for {source_id}")
            if enabled and not self._targets.get(source_id):
                raise ValueError(f"Enabled source {source_id} must have at least one target")
        for source_id, targets in self._targets.items():
            if source_id not in self._adapters or source_id not in self._policies:
                raise ValueError(f"Missing adapter or policy for {source_id}")
            if not targets:
                raise ValueError(f"Source {source_id} must have at least one target")
            if len(targets) != 1:
                raise ValueError(
                    f"Source {source_id} currently supports exactly one target"
                )

    async def run_due_sources(
        self,
        *,
        now: datetime | None = None,
    ) -> SourcePollReport:
        now_utc = now or self._clock()
        if now_utc.tzinfo is None or now_utc.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        now_utc = now_utc.astimezone(timezone.utc)
        new_observations: list[SourceScheduleObservation] = []
        database_comparisons: dict[str, ComparisonResult] = {}

        async with self._lock:
            for source_id, enabled in self._enabled_sources.items():
                if not enabled:
                    continue
                policy = self._policies[source_id]
                if not await self._restore_retry_after(source_id):
                    continue
                target = self._targets[source_id][0]
                cached_observation = self._get_cached_observation(
                    source_id,
                    target,
                    now_utc,
                )
                if cached_observation is not None:
                    logger.debug(
                        "Using cached source observation source=%s region=%s queue=%s.%s observed_at=%s",
                        source_id,
                        target.region_slug,
                        target.queue,
                        target.subqueue,
                        cached_observation.observed_at.isoformat(),
                    )
                    self._latest_observations[source_id] = cached_observation
                    continue

                last_attempt = self._last_attempt_at.get(source_id)
                if (
                    last_attempt is not None
                    and (now_utc - last_attempt).total_seconds()
                    < policy.poll_interval_seconds
                ):
                    continue

                self._last_attempt_at[source_id] = now_utc
                for target in self._targets[source_id]:
                    next_request_at = self._next_request_at.get(source_id)
                    if next_request_at is not None and now_utc < next_request_at:
                        logger.debug(
                            "Skipping deferred source request source=%s region=%s queue=%s.%s until=%s",
                            source_id,
                            target.region_slug,
                            target.queue,
                            target.subqueue,
                            next_request_at.isoformat(),
                        )
                        continue

                    try:
                        observation = await asyncio.to_thread(
                            self._fetch_and_parse,
                            self._adapters[source_id],
                            target,
                            now_utc,
                            policy,
                        )
                    except Exception:
                        logger.exception(
                            "Unexpected source polling failure source=%s region=%s queue=%s.%s",
                            source_id,
                            target.region_slug,
                            target.queue,
                            target.subqueue,
                        )
                        observation = self._failed_observation(
                            self._adapters[source_id],
                            target,
                            now_utc,
                            FetchStatus.ERROR,
                            "poll_error",
                        )
                    try:
                        previous_comparison = await compare_with_latest_database_observation(
                            self._database,
                            observation,
                            now=now_utc,
                            policy=policy,
                        )
                    except Exception:
                        logger.exception(
                            "Could not compare source observation with history source=%s",
                            source_id,
                        )
                        previous_comparison = None
                    try:
                        await self._database.record_source_schedule_observation(observation)
                    except Exception:
                        logger.exception(
                            "Could not persist source observation source=%s region=%s queue=%s.%s",
                            source_id,
                            target.region_slug,
                            target.queue,
                            target.subqueue,
                        )
                        continue

                    if previous_comparison is not None:
                        database_comparisons[source_id] = previous_comparison
                    self._latest_observations[source_id] = observation
                    new_observations.append(observation)

        pairwise: dict[tuple[str, str], ComparisonResult] = {}
        source_ids = sorted(self._latest_observations)
        for index, left_id in enumerate(source_ids):
            left = self._latest_observations[left_id]
            for right_id in source_ids[index + 1 :]:
                right = self._latest_observations[right_id]
                if (
                    left.fetch_status is not FetchStatus.SUCCESS
                    or right.fetch_status is not FetchStatus.SUCCESS
                ):
                    continue
                pairwise[(left_id, right_id)] = compare_observations(
                    left,
                    right,
                    now=now_utc,
                    left_policy=self._policies[left_id],
                    right_policy=self._policies[right_id],
                )

        return SourcePollReport(
            observations=tuple(new_observations),
            pairwise_comparisons=pairwise,
            database_comparisons=database_comparisons,
        )

    async def _restore_retry_after(self, source_id: str) -> bool:
        if source_id in self._retry_after_restored:
            return True
        try:
            deadline = await self._database.get_latest_source_retry_after(
                source_id=source_id,
            )
        except Exception:
            logger.exception(
                "Could not restore source rate-limit state source=%s",
                source_id,
            )
            return False
        if deadline is not None:
            if deadline.tzinfo is None or deadline.utcoffset() is None:
                logger.error(
                    "Ignoring timezone-naive persisted Retry-After source=%s",
                    source_id,
                )
                return False
            self._next_request_at[source_id] = deadline.astimezone(timezone.utc)
        self._retry_after_restored.add(source_id)
        return True

    def _get_cached_observation(
        self,
        source_id: str,
        target: SourceTarget,
        now: datetime,
    ) -> SourceScheduleObservation | None:
        cache_key = self._cache_key(source_id, target)
        cached = self._cache.get(cache_key)
        if cached is None:
            return None
        expires_at, observation = cached
        if now >= expires_at:
            del self._cache[cache_key]
            return None
        return observation

    @staticmethod
    def _cache_key(
        source_id: str,
        target: SourceTarget,
    ) -> tuple[str, str, str, int, int]:
        return (
            source_id,
            target.source_url,
            target.region_slug,
            target.queue,
            target.subqueue,
        )

    def _fetch_and_parse(
        self,
        adapter: SourceAdapter,
        target: SourceTarget,
        observed_at: datetime,
        policy: SourceFreshnessPolicy,
    ) -> SourceScheduleObservation:
        source_id = adapter.source_id
        cache_key = self._cache_key(source_id, target)
        self._next_request_at[source_id] = self._add_seconds_safely(
            observed_at,
            policy.minimum_request_interval_seconds,
        )
        try:
            response = requests.get(
                target.source_url,
                headers={"User-Agent": "LightUA-shadow-schedule-check/1.0"},
                timeout=self._timeout_seconds,
                allow_redirects=False,
            )
        except requests.RequestException as exc:
            request_completed_at = self._request_completion_time(observed_at)
            self._extend_minimum_request_deadline(
                source_id,
                request_completed_at,
                policy,
            )
            observation = self._failed_observation(
                adapter,
                target,
                observed_at,
                FetchStatus.UNAVAILABLE,
                f"http_{type(exc).__name__.lower()}",
            )
            logger.warning(
                "Source request unavailable source=%s status=%s",
                source_id,
                type(exc).__name__,
            )
            return observation

        request_completed_at = self._request_completion_time(observed_at)
        self._extend_minimum_request_deadline(
            source_id,
            request_completed_at,
            policy,
        )
        if response.status_code != 200:
            observation = self._failed_observation(
                adapter,
                target,
                observed_at,
                FetchStatus.UNAVAILABLE,
                f"http_{response.status_code}",
            )
            if response.status_code in {429, 503}:
                retry_after_header = response.headers.get("Retry-After")
                retry_after = self._retry_after_deadline(
                    retry_after_header,
                    request_completed_at,
                )
                if retry_after is not None:
                    self._next_request_at[source_id] = max(
                        self._next_request_at[source_id],
                        retry_after,
                    )
                    observation = replace(
                        observation,
                        retry_after_at=self._next_request_at[source_id],
                    )
                elif retry_after_header:
                    logger.warning(
                        "Ignoring invalid Retry-After header source=%s region=%s queue=%s.%s status=%s",
                        source_id,
                        target.region_slug,
                        target.queue,
                        target.subqueue,
                        response.status_code,
                    )
            logger.warning(
                "Source returned HTTP %s source=%s region=%s queue=%s.%s",
                response.status_code,
                source_id,
                target.region_slug,
                target.queue,
                target.subqueue,
            )
            return observation

        content_type = response.headers.get("Content-Type", "").lower()
        if "html" not in content_type and "text/plain" not in content_type:
            return self._failed_observation(
                adapter,
                target,
                observed_at,
                FetchStatus.ERROR,
                "unexpected_content_type",
            )

        try:
            observation = adapter.parse(response.text, target, observed_at)
        except Exception:
            logger.exception(
                "Source parser failed source=%s region=%s queue=%s.%s",
                source_id,
                target.region_slug,
                target.queue,
                target.subqueue,
            )
            return self._failed_observation(
                adapter,
                target,
                observed_at,
                FetchStatus.ERROR,
                "parser_error",
            )
        if (
            observation.fetch_status is FetchStatus.SUCCESS
            and policy.cache_ttl_seconds > 0
        ):
            self._cache[cache_key] = (
                self._add_seconds_safely(observed_at, policy.cache_ttl_seconds),
                observation,
            )
        return observation

    def _request_completion_time(self, request_started_at: datetime) -> datetime:
        completed_at = self._clock()
        if completed_at.tzinfo is None or completed_at.utcoffset() is None:
            raise ValueError("Source manager clock must return a timezone-aware datetime")
        return max(
            request_started_at.astimezone(timezone.utc),
            completed_at.astimezone(timezone.utc),
        )

    def _extend_minimum_request_deadline(
        self,
        source_id: str,
        request_completed_at: datetime,
        policy: SourceFreshnessPolicy,
    ) -> None:
        minimum_deadline = self._add_seconds_safely(
            request_completed_at,
            policy.minimum_request_interval_seconds,
        )
        self._next_request_at[source_id] = max(
            self._next_request_at.get(source_id, minimum_deadline),
            minimum_deadline,
        )

    @classmethod
    def _retry_after_deadline(
        cls,
        header_value: str | None,
        received_at: datetime,
    ) -> datetime | None:
        if not header_value:
            return None
        value = header_value.strip()
        if not value:
            return None
        if value.isdecimal():
            try:
                delay_seconds = int(value)
            except ValueError:
                return None
            return cls._add_seconds_safely(received_at, delay_seconds)
        try:
            deadline = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if deadline.tzinfo is None or deadline.utcoffset() is None:
            return None
        try:
            return deadline.astimezone(timezone.utc)
        except (OverflowError, ValueError):
            return _MAX_DATETIME

    @staticmethod
    def _add_seconds_safely(value: datetime, seconds: int) -> datetime:
        try:
            return value + timedelta(seconds=seconds)
        except OverflowError:
            return _MAX_DATETIME

    @staticmethod
    def _failed_observation(
        adapter: SourceAdapter,
        target: SourceTarget,
        observed_at: datetime,
        fetch_status: FetchStatus,
        error_code: str,
    ) -> SourceScheduleObservation:
        return SourceScheduleObservation(
            source_id=adapter.source_id,
            region_slug=target.region_slug,
            operator_id=None,
            queue=target.queue,
            subqueue=target.subqueue,
            schedule_date=None,
            observed_at=observed_at,
            source_updated_at=None,
            fetch_status=fetch_status,
            source_url=target.source_url,
            error_code=error_code,
        )
