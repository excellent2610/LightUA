from __future__ import annotations

import json
import asyncio
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import requests

from db.database import (
    SOURCE_SCHEDULE_OBSERVATIONS_CONTENT_HASH_MIGRATION_SQL,
    SOURCE_SCHEDULE_OBSERVATIONS_RETRY_AFTER_MIGRATION_SQL,
    SOURCE_SCHEDULE_OBSERVATIONS_TABLE_SQL,
    Database,
)
from bot.shadow_scheduler import ShadowScheduleScheduler
from parsing.alerts_energy import AlertsEnergyParser
from parsing.alerts_org import AlertsOrgParser
from parsing.comparison import (
    SourceFreshnessPolicy,
    compare_observations,
    observation_is_stale,
)
from parsing.database_comparison import compare_with_latest_database_observation
from parsing.kolysvitlo import KolySvitloParser
from parsing.models import (
    ComparisonStatus,
    FetchStatus,
    ScheduleInterval,
    SourceScheduleObservation,
    SourceTarget,
)
from parsing.normalization import (
    complete_from_outages,
    covers_full_day,
    interval_from_times,
    kyiv_timestamp_to_utc,
    local_date,
    normalize_intervals,
    parse_clock_time,
)
from parsing.source_manager import SourceScheduleManager


FIXTURES = Path(__file__).parent / "fixtures"
NOW = datetime(2026, 10, 17, 12, 0, tzinfo=timezone.utc)
TARGET = SourceTarget(
    "https://alerts.energy/cherkaska-oblast/schedule/cherkaska_oblast_uz/4.2",
    "cherkaska-oblast",
    4,
    2,
)
EXPECTED_INTERVALS = (
    ScheduleInterval(0, 360, "on"),
    ScheduleInterval(360, 480, "off"),
    ScheduleInterval(480, 1140, "on"),
    ScheduleInterval(1140, 1260, "off"),
    ScheduleInterval(1260, 1440, "on"),
)


def fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def observation(
    *,
    source_id: str = "source-a",
    region: str = "cherkaska-oblast",
    operator: str | None = "operator-a",
    queue: int = 4,
    subqueue: int = 2,
    schedule_day: date | None = date(2026, 10, 17),
    intervals: tuple[ScheduleInterval, ...] = EXPECTED_INTERVALS,
    fetched_at: datetime = NOW,
    status: FetchStatus = FetchStatus.SUCCESS,
) -> SourceScheduleObservation:
    return SourceScheduleObservation(
        source_id=source_id,
        region_slug=region,
        operator_id=operator,
        queue=queue,
        subqueue=subqueue,
        schedule_date=schedule_day,
        observed_at=fetched_at,
        source_updated_at=None,
        fetch_status=status,
        intervals=intervals if status is FetchStatus.SUCCESS else (),
    )


class EchoAdapter:
    def __init__(self, source_id: str) -> None:
        self.source_id = source_id

    def parse(self, html, target, observed_at):
        if not html:
            raise ValueError("Expected a non-empty response body")
        return replace(
            observation(
                source_id=self.source_id,
                region=target.region_slug,
                queue=target.queue,
                subqueue=target.subqueue,
                fetched_at=observed_at,
            ),
            source_url=target.source_url,
        )


class FakeDatabase:
    def __init__(self) -> None:
        self.observations: list[SourceScheduleObservation] = []
        self.user_snapshots = {"unchanged": "schedule"}
        self.notifications: list[object] = []
        self.latest_queries: list[dict[str, object]] = []
        self.latest_observation: SourceScheduleObservation | None = None
        self.retry_after_by_source: dict[str, datetime | None] = {}

    async def get_latest_source_schedule_observation(self, **kwargs):
        self.latest_queries.append(kwargs)
        return self.latest_observation

    async def record_source_schedule_observation(self, value):
        self.observations.append(value)
        self.retry_after_by_source[value.source_id] = value.retry_after_at
        return len(self.observations)

    async def get_latest_source_retry_after(self, *, source_id: str):
        return self.retry_after_by_source.get(source_id)


class NormalizationTests(unittest.TestCase):
    def test_midnight_and_24_hour_boundaries(self) -> None:
        self.assertEqual(parse_clock_time("00:00"), 0)
        self.assertEqual(parse_clock_time("24:00", allow_24=True), 1440)
        with self.assertRaises(ValueError):
            parse_clock_time("24:00")
        self.assertEqual(
            interval_from_times("22:00", "02:00", "off"),
            (
                ScheduleInterval(1320, 1440, "off"),
                ScheduleInterval(0, 120, "off"),
            ),
        )
        self.assertEqual(
            interval_from_times("22:00", "00:00", "off"),
            (ScheduleInterval(1320, 1440, "off"),),
        )

    def test_duplicate_intervals_are_removed_and_overlaps_rejected(self) -> None:
        interval = ScheduleInterval(60, 120, "off")
        self.assertEqual(normalize_intervals((interval, interval)), (interval,))
        with self.assertRaises(ValueError):
            normalize_intervals(
                (ScheduleInterval(60, 120, "off"), ScheduleInterval(90, 150, "off"))
            )

    def test_invalid_time_ranges_are_not_silently_discarded(self) -> None:
        from parsing.normalization import parse_time_ranges

        with self.assertRaises(ValueError):
            parse_time_ranges("06:00 - 08:00, 09:99 - 11:00", "off")

    def test_no_outages_means_a_complete_day_on(self) -> None:
        intervals = complete_from_outages(())
        self.assertEqual(intervals, (ScheduleInterval(0, 1440, "on"),))
        self.assertTrue(covers_full_day(intervals))

    def test_kyiv_dst_gap_and_ambiguous_time_are_rejected(self) -> None:
        self.assertIsNone(kyiv_timestamp_to_utc(date(2026, 3, 29), 3, 30))
        self.assertIsNone(kyiv_timestamp_to_utc(date(2026, 10, 25), 3, 30))
        self.assertEqual(
            kyiv_timestamp_to_utc(date(2026, 10, 24), 3, 30),
            datetime(2026, 10, 24, 0, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(
            local_date(datetime(2026, 10, 16, 21, 30, tzinfo=timezone.utc)),
            date(2026, 10, 17),
        )

    def test_content_hash_is_stable_across_interval_order_and_null_for_failures(self) -> None:
        ordered = observation()
        reversed_order = observation(intervals=tuple(reversed(EXPECTED_INTERVALS)))
        self.assertEqual(ordered.content_hash, reversed_order.content_hash)
        self.assertEqual(len(ordered.content_hash), 64)
        self.assertIsNone(observation(status=FetchStatus.UNAVAILABLE).content_hash)


class SourceParserTests(unittest.TestCase):
    def test_alerts_energy_parses_schedule_and_source_update(self) -> None:
        result = AlertsEnergyParser().parse(fixture("alerts-energy-4-2.html"), TARGET, NOW)
        self.assertEqual(result.fetch_status, FetchStatus.SUCCESS)
        self.assertEqual(result.operator_id, "cherkaska_oblast_uz")
        self.assertEqual(result.schedule_date, date(2026, 10, 17))
        self.assertEqual(result.intervals, EXPECTED_INTERVALS)
        self.assertEqual(
            result.source_updated_at,
            datetime(2026, 10, 17, 7, 0, tzinfo=timezone.utc),
        )

    def test_alerts_energy_accepts_explicitly_empty_outages(self) -> None:
        html = """
        <h1>Графік 4.2</h1><p>17 жовтня 2026</p>
        <p>Інтервали відключень</p><p>Усього за день: 0</p>
        <p>Відключень не заплановано</p>
        """
        result = AlertsEnergyParser().parse(html, TARGET, NOW)
        self.assertEqual(result.fetch_status, FetchStatus.SUCCESS)
        self.assertEqual(result.intervals, (ScheduleInterval(0, 1440, "on"),))

    def test_alerts_energy_missing_metadata_or_invalid_intervals_is_incomplete(self) -> None:
        missing = AlertsEnergyParser().parse("<html><p>Антибот перевірка</p></html>", TARGET, NOW)
        malformed = AlertsEnergyParser().parse(
            """
            <h1>Графік 4.2</h1><p>17 жовтня 2026</p>
            <p>Інтервали відключень 06:00 - 06:00</p>
            """,
            TARGET,
            NOW,
        )
        self.assertEqual(missing.fetch_status, FetchStatus.INCOMPLETE)
        self.assertEqual(malformed.fetch_status, FetchStatus.INCOMPLETE)

    def test_alerts_energy_does_not_infer_empty_schedule_from_unrelated_text(self) -> None:
        html = """
        <h1>Графік 4.2</h1><p>17 жовтня 2026</p>
        <p>Відключень не заплановано</p>
        """
        result = AlertsEnergyParser().parse(html, TARGET, NOW)
        self.assertEqual(result.fetch_status, FetchStatus.INCOMPLETE)

    def test_adapters_reject_a_region_that_does_not_match_the_target_url(self) -> None:
        alerts_target = replace(TARGET, region_slug="kyiv")
        energy = AlertsEnergyParser().parse(
            fixture("alerts-energy-4-2.html"),
            alerts_target,
            NOW,
        )
        org_target = SourceTarget(
            "https://alerts.org.ua/cherkaska-oblast/",
            "kyiv",
            4,
            2,
        )
        alerts_org = AlertsOrgParser().parse(
            fixture("alerts-org-4-2.html"),
            org_target,
            NOW,
        )
        self.assertEqual(energy.fetch_status, FetchStatus.INCOMPLETE)
        self.assertEqual(energy.error_code, "target_region_not_in_source_url")
        self.assertEqual(alerts_org.fetch_status, FetchStatus.INCOMPLETE)
        self.assertEqual(alerts_org.error_code, "target_region_not_in_source_url")

    def test_kolysvitlo_parses_day_rows_and_update(self) -> None:
        target = SourceTarget(
            "https://kolysvitlo.com/schedule/cherkasy_oblenergo",
            "cherkaska-oblast",
            4,
            2,
        )
        result = KolySvitloParser().parse(fixture("kolysvitlo-4-2.html"), target, NOW)
        self.assertEqual(result.fetch_status, FetchStatus.SUCCESS)
        self.assertEqual(result.operator_id, "cherkasy_oblenergo")
        self.assertEqual(result.schedule_date, date(2026, 10, 17))
        self.assertEqual(result.intervals, EXPECTED_INTERVALS)
        self.assertEqual(
            result.source_updated_at,
            datetime(2026, 10, 17, 7, 0, tzinfo=timezone.utc),
        )

    def test_kolysvitlo_missing_row_and_invalid_markup_are_incomplete(self) -> None:
        target = SourceTarget(
            "https://kolysvitlo.com/schedule/cherkasy_oblenergo",
            "cherkaska-oblast",
            4,
            2,
        )
        no_row = KolySvitloParser().parse("<html><h1>Графік</h1></html>", target, NOW)
        invalid = KolySvitloParser().parse(
            """
            <h1>17 жовтня 2026</h1>
            <div data-panel="today"><div data-group="4.2">
              <span class="ks-bar" title="24:00 - 25:00 немає світла"></span>
            </div></div>
            """,
            target,
            NOW,
        )
        self.assertEqual(no_row.fetch_status, FetchStatus.INCOMPLETE)
        self.assertEqual(invalid.fetch_status, FetchStatus.INCOMPLETE)

    def test_kolysvitlo_does_not_infer_empty_schedule_from_unparsed_bits(self) -> None:
        target = SourceTarget(
            "https://kolysvitlo.com/schedule/cherkasy_oblenergo",
            "cherkaska-oblast",
            4,
            2,
        )
        result = KolySvitloParser().parse(
            """
            <h1>17 жовтня 2026</h1>
            <div data-panel="today"><div data-group="4.2" data-h="000000"></div></div>
            """,
            target,
            NOW,
        )
        self.assertEqual(result.fetch_status, FetchStatus.INCOMPLETE)

    def test_kolysvitlo_explicit_no_outages_is_a_full_on_day(self) -> None:
        target = SourceTarget(
            "https://kolysvitlo.com/schedule/cherkasy_oblenergo",
            "cherkaska-oblast",
            4,
            2,
        )
        result = KolySvitloParser().parse(
            """
            <h1>17 жовтня 2026</h1>
            <div data-panel="today"><div data-group="4.2">
              <span class="ks-bar" title="00:00 - 24:00 відключень не заплановано"></span>
            </div></div>
            """,
            target,
            NOW,
        )
        self.assertEqual(result.fetch_status, FetchStatus.SUCCESS)
        self.assertEqual(result.intervals, (ScheduleInterval(0, 1440, "on"),))

    def test_alerts_org_parses_full_day_but_does_not_invent_update_time(self) -> None:
        target = SourceTarget(
            "https://alerts.org.ua/cherkaska-oblast/",
            "cherkaska-oblast",
            4,
            2,
        )
        result = AlertsOrgParser().parse(fixture("alerts-org-4-2.html"), target, NOW)
        self.assertEqual(result.fetch_status, FetchStatus.SUCCESS)
        self.assertEqual(result.operator_id, "operator.example")
        self.assertEqual(result.schedule_date, date(2026, 10, 17))
        self.assertEqual(result.intervals, EXPECTED_INTERVALS)
        self.assertIsNone(result.source_updated_at)

    def test_alerts_org_ignores_unlabeled_external_links_as_operator_identity(self) -> None:
        html = fixture("alerts-org-4-2.html").replace("Джерело:", "Додаткова сторінка:")
        result = AlertsOrgParser().parse(html, TARGET, NOW)
        self.assertEqual(result.fetch_status, FetchStatus.SUCCESS)
        self.assertIsNone(result.operator_id)

    def test_alerts_org_missing_group_or_bad_period_is_incomplete(self) -> None:
        target = SourceTarget(
            "https://alerts.org.ua/cherkaska-oblast/",
            "cherkaska-oblast",
            4,
            2,
        )
        missing = AlertsOrgParser().parse("<h1>Графік 17 жовтня 2026</h1>", target, NOW)
        bad_period = AlertsOrgParser().parse(
            """
            <h1>Графік 17 жовтня 2026</h1>
            <div class="group"><div class="group-name">Група 4.2</div>
             <div class="period"><div data-start="00:00" data-end="24:00"></div></div>
            </div>
            """,
            target,
            NOW,
        )
        self.assertEqual(missing.fetch_status, FetchStatus.INCOMPLETE)
        self.assertEqual(bad_period.fetch_status, FetchStatus.INCOMPLETE)


class ComparisonTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = SourceFreshnessPolicy(
            maximum_observation_age=timedelta(hours=1),
            maximum_source_age=timedelta(hours=2),
        )

    def compare(self, left, right):
        return compare_observations(
            left,
            right,
            now=NOW,
            left_policy=self.policy,
            right_policy=self.policy,
        )

    def test_match_and_mismatch(self) -> None:
        self.assertEqual(
            self.compare(observation(), observation(source_id="source-b")).comparison_status,
            ComparisonStatus.MATCH,
        )
        different = (
            ScheduleInterval(0, 300, "on"),
            ScheduleInterval(300, 480, "off"),
            *EXPECTED_INTERVALS[2:],
        )
        self.assertEqual(
            self.compare(
                observation(),
                observation(source_id="source-b", intervals=different),
            ).comparison_status,
            ComparisonStatus.MISMATCH,
        )

    def test_unknown_operator_context_or_date_is_not_comparable(self) -> None:
        for other in (
            observation(source_id="source-b", operator=None),
            observation(source_id="source-b", region="kyiv"),
            observation(source_id="source-b", queue=5),
            observation(source_id="source-b", schedule_day=None),
            observation(source_id="source-b", schedule_day=date(2026, 10, 18)),
        ):
            with self.subTest(other=other):
                self.assertEqual(
                    self.compare(observation(), other).comparison_status,
                    ComparisonStatus.NOT_COMPARABLE,
                )

    def test_incomplete_and_stale_are_distinct(self) -> None:
        incomplete = observation(
            source_id="source-b",
            intervals=(ScheduleInterval(0, 720, "on"),),
        )
        self.assertEqual(
            self.compare(observation(), incomplete).comparison_status,
            ComparisonStatus.INCOMPLETE,
        )
        stale = observation(source_id="source-b", fetched_at=NOW - timedelta(hours=2))
        self.assertEqual(
            self.compare(observation(), stale).comparison_status,
            ComparisonStatus.STALE,
        )

    def test_freshness_uses_kyiv_calendar_date_and_source_update_if_present(self) -> None:
        policy = SourceFreshnessPolicy(
            maximum_observation_age=timedelta(hours=2),
            maximum_source_age=timedelta(hours=2),
        )
        late_utc = datetime(2026, 10, 17, 22, 30, tzinfo=timezone.utc)
        old_local_date = observation(
            fetched_at=late_utc,
        )
        self.assertTrue(
            observation_is_stale(
                old_local_date,
                now=late_utc,
                policy=policy,
            )
        )
        updated = replace(
            observation(),
            source_updated_at=NOW - timedelta(hours=3),
        )
        self.assertTrue(observation_is_stale(updated, now=NOW, policy=policy))


class SourceManagerTests(unittest.IsolatedAsyncioTestCase):
    def manager(self, database, *, targets=None, policy=None):
        return SourceScheduleManager(
            database,
            enabled_sources={"alerts_energy": True},
            policies={
                "alerts_energy": policy
                or SourceFreshnessPolicy(
                    poll_interval_seconds=1,
                    cache_ttl_seconds=0,
                    minimum_request_interval_seconds=0,
                )
            },
            targets=targets
            or {"alerts_energy": (TARGET,)},
            timeout_seconds=1,
            clock=lambda: NOW,
        )

    @staticmethod
    def response(
        status: int,
        *,
        text: str = "",
        content_type: str = "text/html",
        headers: dict[str, str] | None = None,
    ):
        response_headers = {"Content-Type": content_type}
        if headers:
            response_headers.update(headers)
        return SimpleNamespace(
            status_code=status,
            text=text,
            headers=response_headers,
        )

    async def test_http_statuses_and_transport_errors_are_persisted(self) -> None:
        for status in (403, 404, 429, 500):
            database = FakeDatabase()
            manager = self.manager(database)
            with patch(
                "parsing.source_manager.requests.get",
                return_value=self.response(status),
            ):
                report = await manager.run_due_sources(now=NOW)
            self.assertEqual(report.observations[0].fetch_status, FetchStatus.UNAVAILABLE)
            self.assertEqual(len(database.observations), 1)

        for exception in (requests.Timeout("timeout"), requests.ConnectionError("offline")):
            database = FakeDatabase()
            manager = self.manager(database)
            with patch(
                "parsing.source_manager.requests.get",
                side_effect=exception,
            ):
                report = await manager.run_due_sources(now=NOW)
            self.assertEqual(report.observations[0].fetch_status, FetchStatus.UNAVAILABLE)
            self.assertEqual(len(database.observations), 1)

    async def test_invalid_content_type_antibot_and_bad_json_are_not_success(self) -> None:
        cases = (
            self.response(200, text="{malformed-json", content_type="application/json"),
            self.response(200, text="<html><title>Robot check</title></html>"),
        )
        expected = (FetchStatus.ERROR, FetchStatus.INCOMPLETE)
        for response, status in zip(cases, expected):
            database = FakeDatabase()
            manager = self.manager(database)
            with patch("parsing.source_manager.requests.get", return_value=response):
                report = await manager.run_due_sources(now=NOW)
            self.assertEqual(report.observations[0].fetch_status, status)
            self.assertEqual(len(database.observations), 1)

    async def test_database_comparison_uses_only_same_source_success_observations(self) -> None:
        database = FakeDatabase()
        database.latest_observation = observation(source_id="alerts_energy")
        current = observation(source_id="alerts_energy")
        policy = SourceFreshnessPolicy()
        result = await compare_with_latest_database_observation(
            database,
            current,
            now=NOW,
            policy=policy,
        )
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.comparison_status, ComparisonStatus.MATCH)
        self.assertEqual(database.latest_queries[0]["source_id"], "alerts_energy")

        unknown_operator = observation(source_id="alerts_energy", operator=None)
        self.assertIsNone(
            await compare_with_latest_database_observation(
                database,
                unknown_operator,
                now=NOW,
                policy=policy,
            )
        )
        self.assertEqual(len(database.latest_queries), 1)

    async def test_cache_hit_does_not_append_observation_and_keeps_original_time(self) -> None:
        database = FakeDatabase()
        policy = SourceFreshnessPolicy(
            poll_interval_seconds=1,
            cache_ttl_seconds=60,
            minimum_request_interval_seconds=0,
        )
        manager = self.manager(
            database,
            targets={"alerts_energy": (TARGET,)},
            policy=policy,
        )
        with patch(
            "parsing.source_manager.requests.get",
            return_value=self.response(200, text=fixture("alerts-energy-4-2.html")),
        ) as request:
            first = await manager.run_due_sources(now=NOW)
            second = await manager.run_due_sources(now=NOW + timedelta(seconds=2))
            expired = await manager.run_due_sources(now=NOW + timedelta(seconds=61))
        self.assertEqual(len(first.observations), 1)
        self.assertEqual(second.observations, ())
        self.assertEqual(len(expired.observations), 1)
        self.assertEqual(len(database.observations), 2)
        self.assertEqual(request.call_count, 2)
        self.assertEqual(database.observations[0].observed_at, NOW)
        self.assertEqual(
            manager._latest_observations["alerts_energy"].observed_at,
            NOW + timedelta(seconds=61),
        )
        self.assertEqual(database.user_snapshots, {"unchanged": "schedule"})
        self.assertEqual(database.notifications, [])

    async def test_default_poll_policy_can_reuse_cache_between_polls(self) -> None:
        database = FakeDatabase()
        policy = SourceFreshnessPolicy(
            poll_interval_seconds=900,
            cache_ttl_seconds=600,
            minimum_request_interval_seconds=10,
        )
        manager = self.manager(database, policy=policy)
        with patch(
            "parsing.source_manager.requests.get",
            return_value=self.response(200, text=fixture("alerts-energy-4-2.html")),
        ) as request:
            first = await manager.run_due_sources(now=NOW)
            cached = await manager.run_due_sources(now=NOW + timedelta(seconds=30))
        self.assertEqual(len(first.observations), 1)
        self.assertEqual(cached.observations, ())
        self.assertEqual(request.call_count, 1)
        self.assertEqual(len(database.observations), 1)
        self.assertEqual(
            manager._latest_observations["alerts_energy"].observed_at,
            NOW,
        )

    async def test_multiple_targets_for_one_source_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "exactly one target"):
            self.manager(
                FakeDatabase(),
                targets={"alerts_energy": (TARGET, TARGET)},
            )

    async def test_cache_keys_are_independent_for_each_source(self) -> None:
        database = FakeDatabase()
        policy = SourceFreshnessPolicy(
            poll_interval_seconds=1,
            cache_ttl_seconds=60,
            minimum_request_interval_seconds=0,
        )
        manager = SourceScheduleManager(
            database,
            enabled_sources={"source-a": True, "source-b": True},
            policies={"source-a": policy, "source-b": policy},
            targets={"source-a": (TARGET,), "source-b": (TARGET,)},
            adapters={
                "source-a": EchoAdapter("source-a"),
                "source-b": EchoAdapter("source-b"),
            },
            clock=lambda: NOW,
        )
        with patch(
            "parsing.source_manager.requests.get",
            return_value=self.response(200, text="valid"),
        ) as request:
            report = await manager.run_due_sources(now=NOW)
        self.assertEqual(request.call_count, 2)
        self.assertEqual(
            {item.source_id for item in report.observations},
            {"source-a", "source-b"},
        )
        self.assertEqual(len(database.observations), 2)

    async def test_retry_after_seconds_and_http_date_defer_requests_until_deadline(self) -> None:
        cases = (
            (429, "30"),
            (503, format_datetime(NOW + timedelta(seconds=40), usegmt=True)),
        )
        for status, retry_after in cases:
            with self.subTest(status=status, retry_after=retry_after):
                database = FakeDatabase()
                policy = SourceFreshnessPolicy(
                    poll_interval_seconds=1,
                    cache_ttl_seconds=0,
                    minimum_request_interval_seconds=0,
                )
                manager = self.manager(database, policy=policy)
                with patch(
                    "parsing.source_manager.requests.get",
                    side_effect=(
                        self.response(
                            status,
                            headers={"Retry-After": retry_after},
                        ),
                        self.response(
                            200,
                            text=fixture("alerts-energy-4-2.html"),
                        ),
                    ),
                ) as request:
                    first = await manager.run_due_sources(now=NOW)
                    blocked = await manager.run_due_sources(
                        now=NOW + timedelta(seconds=29 if status == 429 else 39)
                    )
                    self.assertEqual(request.call_count, 1)
                    self.assertEqual(blocked.observations, ())
                    deadline = 30 if status == 429 else 40
                    allowed = await manager.run_due_sources(
                        now=NOW + timedelta(seconds=deadline)
                    )
                self.assertEqual(first.observations[0].fetch_status, FetchStatus.UNAVAILABLE)
                self.assertEqual(request.call_count, 2)
                self.assertEqual(allowed.observations[0].fetch_status, FetchStatus.SUCCESS)
                self.assertEqual(len(database.observations), 2)

    async def test_invalid_retry_after_does_not_create_an_unbounded_wait(self) -> None:
        database = FakeDatabase()
        manager = self.manager(
            database,
            policy=SourceFreshnessPolicy(
                poll_interval_seconds=1,
                cache_ttl_seconds=0,
                minimum_request_interval_seconds=0,
            ),
        )
        with patch(
            "parsing.source_manager.requests.get",
            side_effect=(
                self.response(429, headers={"Retry-After": "not-a-date"}),
                self.response(200, text=fixture("alerts-energy-4-2.html")),
            ),
        ) as request:
            first = await manager.run_due_sources(now=NOW)
            second = await manager.run_due_sources(now=NOW + timedelta(seconds=1))
        self.assertEqual(first.observations[0].fetch_status, FetchStatus.UNAVAILABLE)
        self.assertEqual(second.observations[0].fetch_status, FetchStatus.SUCCESS)
        self.assertEqual(request.call_count, 2)

    async def test_retry_delay_for_one_source_does_not_block_another_source(self) -> None:
        database = FakeDatabase()
        policy = SourceFreshnessPolicy(
            poll_interval_seconds=1,
            cache_ttl_seconds=0,
            minimum_request_interval_seconds=0,
        )
        manager = SourceScheduleManager(
            database,
            enabled_sources={"source-a": True, "source-b": True},
            policies={"source-a": policy, "source-b": policy},
            targets={"source-a": (TARGET,), "source-b": (TARGET,)},
            adapters={
                "source-a": EchoAdapter("source-a"),
                "source-b": EchoAdapter("source-b"),
            },
            clock=lambda: NOW,
        )
        with patch(
            "parsing.source_manager.requests.get",
            side_effect=(
                self.response(429, headers={"Retry-After": "3600"}),
                self.response(200, text="valid"),
            ),
        ) as request:
            report = await manager.run_due_sources(now=NOW)
        self.assertEqual(request.call_count, 2)
        self.assertEqual(len(report.observations), 2)
        self.assertEqual(report.observations[0].fetch_status, FetchStatus.UNAVAILABLE)
        self.assertEqual(report.observations[1].fetch_status, FetchStatus.SUCCESS)
        self.assertEqual(
            {item.source_id for item in database.observations},
            {"source-a", "source-b"},
        )

    async def test_retry_after_is_restored_from_history_after_manager_restart(self) -> None:
        database = FakeDatabase()
        policy = SourceFreshnessPolicy(
            poll_interval_seconds=1,
            cache_ttl_seconds=0,
            minimum_request_interval_seconds=0,
        )
        first_manager = self.manager(database, policy=policy)
        retry_deadline = NOW + timedelta(seconds=60)
        with patch(
            "parsing.source_manager.requests.get",
            return_value=self.response(
                429,
                headers={"Retry-After": "60"},
            ),
        ) as first_request:
            first = await first_manager.run_due_sources(now=NOW)
        self.assertEqual(first_request.call_count, 1)
        self.assertEqual(first.observations[0].retry_after_at, retry_deadline)
        self.assertEqual(database.retry_after_by_source["alerts_energy"], retry_deadline)

        restarted_manager = self.manager(database, policy=policy)
        with patch("parsing.source_manager.requests.get") as restarted_request:
            blocked = await restarted_manager.run_due_sources(
                now=NOW + timedelta(seconds=30)
            )
        restarted_request.assert_not_called()
        self.assertEqual(blocked.observations, ())
        self.assertEqual(len(database.observations), 1)

        with patch(
            "parsing.source_manager.requests.get",
            return_value=self.response(
                200,
                text=fixture("alerts-energy-4-2.html"),
            ),
        ) as allowed_request:
            allowed = await restarted_manager.run_due_sources(now=retry_deadline)
        self.assertEqual(allowed_request.call_count, 1)
        self.assertEqual(allowed.observations[0].fetch_status, FetchStatus.SUCCESS)

    async def test_disabled_source_is_not_requested(self) -> None:
        database = FakeDatabase()
        manager = SourceScheduleManager(
            database,
            enabled_sources={"alerts_energy": False},
            targets={"alerts_energy": (TARGET,)},
        )
        with patch("parsing.source_manager.requests.get") as request:
            report = await manager.run_due_sources(now=NOW)
        request.assert_not_called()
        self.assertEqual(report.observations, ())

    async def test_unexpected_worker_failure_still_creates_an_observation(self) -> None:
        database = FakeDatabase()
        manager = self.manager(database)
        with patch.object(manager, "_fetch_and_parse", side_effect=RuntimeError("boom")):
            report = await manager.run_due_sources(now=NOW)
        self.assertEqual(report.observations[0].fetch_status, FetchStatus.ERROR)
        self.assertEqual(report.observations[0].error_code, "poll_error")
        self.assertEqual(len(database.observations), 1)


class ObservationDatabaseTests(unittest.IsolatedAsyncioTestCase):
    class Connection:
        def __init__(self) -> None:
            self.inserted: list[tuple[str, tuple[object, ...]]] = []

        async def fetchval(self, query: str, *params: object) -> int:
            self.inserted.append((query, params))
            return len(self.inserted)

    class Acquire:
        def __init__(self, connection) -> None:
            self.connection = connection

        async def __aenter__(self):
            return self.connection

        async def __aexit__(self, *_args):
            del _args
            return None

    class Pool:
        def __init__(self, connection) -> None:
            self.connection = connection

        def acquire(self):
            return ObservationDatabaseTests.Acquire(self.connection)

    async def test_every_observation_is_appended_without_a_unique_profile_key(self) -> None:
        connection = self.Connection()
        database = Database(":unused:")
        database._pool = self.Pool(connection)
        successful = observation()
        retry_deadline = NOW + timedelta(minutes=1)
        failed = replace(
            observation(status=FetchStatus.UNAVAILABLE),
            retry_after_at=retry_deadline,
        )
        self.assertEqual(
            await database.record_source_schedule_observation(successful),
            1,
        )
        self.assertEqual(
            await database.record_source_schedule_observation(failed),
            2,
        )
        self.assertEqual(len(connection.inserted), 2)
        self.assertIn("RETURNING id", connection.inserted[0][0])
        self.assertEqual(
            json.loads(connection.inserted[0][1][9]),
            [
                {
                    "start_minute": item.start_minute,
                    "end_minute": item.end_minute,
                    "state": item.state,
                }
                for item in EXPECTED_INTERVALS
            ],
        )
        self.assertIsNone(connection.inserted[1][1][9])
        self.assertEqual(connection.inserted[0][1][10], successful.content_hash)
        self.assertIsNone(connection.inserted[1][1][10])
        self.assertEqual(connection.inserted[1][1][13], retry_deadline)
        self.assertNotIn("UNIQUE", SOURCE_SCHEDULE_OBSERVATIONS_TABLE_SQL.upper())

    async def test_latest_observation_reader_decodes_saved_intervals(self) -> None:
        database = Database(":unused:")
        record = {
            "source_id": "source-a",
            "region_slug": "cherkaska-oblast",
            "operator_id": "operator-a",
            "queue": 4,
            "subqueue": 2,
            "schedule_date": date(2026, 10, 17),
            "observed_at": NOW,
            "source_updated_at": None,
            "fetch_status": "success",
            "intervals_json": '[{"start_minute":0,"end_minute":1440,"state":"on"}]',
            "content_hash": observation(
                intervals=(ScheduleInterval(0, 1440, "on"),)
            ).content_hash,
            "source_url": None,
            "error_code": None,
            "retry_after_at": None,
        }

        fetch_arguments: list[tuple[object, ...]] = []

        async def fetchone(*args):
            fetch_arguments.append(args)
            return record

        database._fetchone = fetchone
        result = await database.get_latest_source_schedule_observation(
            source_id="source-a",
            region_slug="cherkaska-oblast",
            operator_id="operator-a",
            queue=4,
            subqueue=2,
            schedule_date=date(2026, 10, 17),
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.intervals, (ScheduleInterval(0, 1440, "on"),))
        self.assertEqual(
            result.content_hash,
            observation(intervals=(ScheduleInterval(0, 1440, "on"),)).content_hash,
        )
        self.assertIn("content_hash", fetch_arguments[0][0])
        self.assertIn(
            "ADD COLUMN IF NOT EXISTS content_hash TEXT",
            SOURCE_SCHEDULE_OBSERVATIONS_CONTENT_HASH_MIGRATION_SQL,
        )
        self.assertIn(
            "ADD COLUMN IF NOT EXISTS retry_after_at TIMESTAMPTZ",
            SOURCE_SCHEDULE_OBSERVATIONS_RETRY_AFTER_MIGRATION_SQL,
        )
        self.assertEqual(len(fetch_arguments), 1)

    async def test_retry_after_reader_uses_latest_attempt_not_latest_success(self) -> None:
        deadline = NOW + timedelta(minutes=5)
        database = Database(":unused:")
        fetch_arguments: list[tuple[object, ...]] = []

        async def fetchone(*args):
            fetch_arguments.append(args)
            return {"retry_after_at": deadline}

        database._fetchone = fetchone
        self.assertEqual(
            await database.get_latest_source_retry_after(source_id="source-a"),
            deadline,
        )
        query = fetch_arguments[0][0]
        self.assertIn("ORDER BY id DESC", query)
        self.assertNotIn("fetch_status='success'", query)

    async def test_shadow_schema_is_initialized_only_when_enabled(self) -> None:
        class InitConnection:
            def __init__(self, *, fail_shadow: bool = False) -> None:
                self.statements: list[str] = []
                self.fail_shadow = fail_shadow

            async def execute(self, query: str, *_args: object) -> str:
                self.statements.append(query)
                if self.fail_shadow and query == SOURCE_SCHEDULE_OBSERVATIONS_TABLE_SQL:
                    raise RuntimeError("shadow DDL failed")
                return "OK"

            async def fetchval(self, query: str, *_args: object) -> str:
                self.statements.append(query)
                return "test-signing-secret"

        class InitPool:
            def __init__(self, connection) -> None:
                self.connection = connection

            def acquire(self):
                return ObservationDatabaseTests.Acquire(self.connection)

        for enabled in (False, True):
            connection = InitConnection()
            pool = InitPool(connection)
            with (
                patch.dict("os.environ", {"DATABASE_URL": "postgresql://test"}),
                patch("db.database.asyncpg.create_pool", new=AsyncMock(return_value=pool)),
            ):
                database = Database(":unused:", shadow_enabled=enabled)
                with patch.object(
                    database,
                    "start_ad_impression_worker",
                    new=AsyncMock(),
                ):
                    await database.init()
            shadow_statements = [
                statement
                for statement in connection.statements
                if "source_schedule_observations" in statement.lower()
            ]
            self.assertEqual(bool(shadow_statements), enabled)
            self.assertEqual(database.shadow_schema_ready, enabled)
            if enabled:
                self.assertIn(
                    SOURCE_SCHEDULE_OBSERVATIONS_TABLE_SQL,
                    shadow_statements,
                )

        connection = InitConnection(fail_shadow=True)
        pool = InitPool(connection)
        with (
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://test"}),
            patch("db.database.asyncpg.create_pool", new=AsyncMock(return_value=pool)),
        ):
            database = Database(":unused:", shadow_enabled=True)
            with patch.object(
                database,
                "start_ad_impression_worker",
                new=AsyncMock(),
            ):
                await database.init()
        self.assertFalse(database.shadow_schema_ready)
        self.assertTrue(
            any("schedule_change_deliveries" in sql for sql in connection.statements)
        )


class ShadowSchedulerLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_is_idempotent_and_stop_cleans_up_task(self) -> None:
        manager = SimpleNamespace(
            run_due_sources=AsyncMock(return_value=SimpleNamespace(
                observations=(),
                pairwise_comparisons={},
                database_comparisons={},
            ))
        )
        scheduler = ShadowScheduleScheduler(manager, tick_interval_seconds=30)
        await scheduler.start()
        task = scheduler._task
        await scheduler.start()
        self.assertIs(scheduler._task, task)
        await asyncio.sleep(0)
        await scheduler.stop()
        self.assertIsNone(scheduler._task)
        self.assertGreaterEqual(manager.run_due_sources.await_count, 1)


if __name__ == "__main__":
    unittest.main()
