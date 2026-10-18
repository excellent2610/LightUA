from __future__ import annotations

import asyncio
import unittest
from datetime import date, datetime, time, timedelta
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from bot.scheduler import LightStateScheduler
from db.database import Database
from db.models import LastState, User
from parsing.alerts_parser import (DaySchedule, ParserError, ScheduleInterval,
    _extract_group_intervals, _extract_schedule_date, _extract_source_updated_at)
from bs4 import BeautifulSoup


KYIV = ZoneInfo("Europe/Kyiv")
NOW = datetime(2026, 10, 17, 12, 0, tzinfo=KYIV)


def make_user(user_id: int, region: str = "region-a", queue: int = 2, subqueue: int = 1) -> User:
    return User(user_id, region, region, queue, subqueue, True, None, None)


def make_schedule(region: str, queue: int, subqueue: int, start: int = 0) -> DaySchedule:
    return DaySchedule(
        region_slug=region,
        region_name=region,
        queue=queue,
        subqueue=subqueue,
        intervals=[ScheduleInterval(time(start), time(23, 59), "off")],
        schedule_date=NOW.date(),
        fetched_at=NOW - timedelta(minutes=10),
        source_updated_at=NOW - timedelta(minutes=10),
    )


def make_morning_schedule(user: User, now: datetime, *, intervals=None,
                          schedule_date_value: date | None = None,
                          confirmed_empty: bool = False) -> DaySchedule:
    return DaySchedule(
        user.region_slug, user.region_name, user.queue, user.subqueue,
        intervals if intervals is not None else [ScheduleInterval(time(10), time(11), "off")],
        schedule_date=schedule_date_value or now.astimezone(KYIV).date(),
        fetched_at=now,
        confirmed_empty=confirmed_empty,
    )


class FakeDatabase:
    """In-memory stand-in; snapshots are shared across scheduler instances."""

    def __init__(self, users: list[User]) -> None:
        self.users = users
        self.snapshots: dict[tuple[str, int, int], str] = {}
        self.snapshot_lock = asyncio.Lock()
        self.states = {user.telegram_id: "off" for user in users}
        self.checked_at = {user.telegram_id: NOW.isoformat() for user in users}
        self.final_events: dict[str, dict[str, object]] = {}
        self.notice_lock = asyncio.Lock()
        self.monotonic = 0.0

    async def get_users_with_notifications(self) -> list[User]:
        return self.users

    async def update_schedule_snapshot(
        self, region: str, queue: int, subqueue: int, value: str
    ) -> str | None:
        async with self.snapshot_lock:
            key = (region, queue, subqueue)
            previous = self.snapshots.get(key)
            self.snapshots[key] = value
            return previous

    async def get_last_state(self, user_id: int) -> LastState:
        return LastState(user_id, self.states[user_id], self.checked_at[user_id])

    async def upsert_last_state(self, telegram_id: int, state: str, checked_at_iso: str) -> None:
        self.states[telegram_id] = state
        self.checked_at[telegram_id] = checked_at_iso

    async def claim_no_more_outages_notification(self, **event: object) -> bool:
        async with self.notice_lock:
            key = str(event["event_key"])
            old = self.final_events.get(key)
            if old:
                if old["status"] != "failed":
                    return False
                if self.monotonic - float(old["last_attempt"]) < int(event.get("retry_after_seconds", 300)):
                    return False
            self.final_events[key] = {
                **event, "status": "pending",
                "attempt_count": int(old["attempt_count"]) + 1 if old else 1,
                "last_attempt": self.monotonic,
            }
            return True

    async def finish_no_more_outages_notification(self, event_key: str, status: str, error=None) -> None:
        async with self.notice_lock:
            if self.final_events[event_key]["status"] == "pending":
                self.final_events[event_key]["status"] = status
                self.final_events[event_key]["error"] = error


class FakeParser:
    def __init__(self) -> None:
        self.schedules: dict[tuple[str, int, int], DaySchedule] = {}
        self.calls: list[tuple[str, int, int]] = []

    def get_schedule(self, region: str, queue: int, subqueue: int) -> DaySchedule:
        key = (region, queue, subqueue)
        self.calls.append(key)
        schedule = self.schedules[key]
        if isinstance(schedule, Exception):
            raise schedule
        return schedule


class FakeBot:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []
        self.failure: Exception | None = None

    async def send_message(self, **kwargs: object) -> None:
        message = str(kwargs.get("text", ""))
        if self.failure is not None and (
            "більше немає запланованих" in message or "☀️ Графік відключень" in message
        ):
            failure, self.failure = self.failure, None
            raise failure
        self.messages.append(kwargs)


class FakeConnection:
    def __init__(self, storage: dict[tuple[str, int, int], str]) -> None:
        self.storage = storage

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def transaction(self):
        return self

    async def execute(self, query: str, *args: object) -> str:
        if query.lstrip().startswith("INSERT INTO schedule_snapshots"):
            region, queue, subqueue, value = args
            self.storage[(region, queue, subqueue)] = value
        elif query.lstrip().startswith("INSERT INTO last_state"):
            user_id, state, checked_at = args
            self.storage[("last_state", user_id)] = (state, checked_at)
            return "INSERT 0 1"
        elif query.lstrip().startswith("UPDATE last_state"):
            user_id, event_key = args
            if self.storage.get(("last_state", user_id)) == event_key:
                return "UPDATE 0"
            self.storage[("last_state", user_id)] = event_key
            return "UPDATE 1"
        elif "UPDATE no_outage_notification_events" in query:
            key, status, error = args
            event = self.storage.get(("event", key))
            if event and event["status"] == "pending":
                event["status"] = status
                event["error"] = error
                return "UPDATE 1"
        return "OK"

    async def fetchval(self, query: str, *args: object):
        if query.lstrip().startswith("SELECT schedule_json"):
            region, queue, subqueue = args
            return self.storage.get((region, queue, subqueue))
        if "INSERT INTO no_outage_notification_events" in query:
            async with self.pool.lock:
                key, user_id, region, queue, subqueue, schedule_date, fingerprint, event_type, _retry = args
                if ("event", key) in self.storage:
                    return None
                self.storage[key and ("event", key)] = {
                    "telegram_id": user_id, "region": region, "queue": queue,
                    "subqueue": subqueue, "date": schedule_date,
                    "fingerprint": fingerprint, "event_type": event_type,
                    "status": "pending", "attempt_count": 1,
                }
                return key
        return None


class FakePool:
    def __init__(self) -> None:
        self.storage: dict[tuple[str, int, int], str] = {}
        self.lock = asyncio.Lock()

    def acquire(self) -> FakeConnection:
        connection = FakeConnection(self.storage)
        connection.pool = self
        return connection


class ScheduleNotificationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.users = [
            make_user(11),
            make_user(12),
            make_user(21, subqueue=2),
            make_user(31, region="region-b"),
        ]
        self.db = FakeDatabase(self.users)
        self.parser = FakeParser()
        self.bot = FakeBot()
        self._set_schedule_starts({})
        self.scheduler = LightStateScheduler(self.bot, self.db, self.parser)

    def _set_schedule_starts(self, starts: dict[tuple[str, int, int], int]) -> None:
        for user in self.users:
            key = (user.region_slug, user.queue, user.subqueue)
            self.parser.schedules[key] = make_schedule(*key, start=starts.get(key, 0))

    async def _check(self, *schedulers: LightStateScheduler) -> None:
        # Run the parser synchronously so tests do not create worker threads.
        async def direct_to_thread(function, *args):
            return function(*args)

        with patch("bot.scheduler.asyncio.to_thread", new=direct_to_thread), patch(
            "bot.scheduler.now_kyiv", return_value=NOW
        ):
            await asyncio.gather(*(scheduler._check_once() for scheduler in schedulers))

    async def test_first_schedule_is_saved_without_notification(self) -> None:
        await self._check(self.scheduler)
        self.assertEqual(self.bot.messages, [])
        self.assertEqual(len(self.db.snapshots), 3)

    async def test_unchanged_schedule_does_not_notify(self) -> None:
        await self._check(self.scheduler)
        self.bot.messages.clear()
        self.parser.calls.clear()

        await self._check(self.scheduler)

        self.assertEqual(self.bot.messages, [])
        self.assertEqual(len(self.parser.calls), 3)  # One fetch per distinct profile.

    async def test_changed_schedule_notifies_only_matching_profile(self) -> None:
        await self._check(self.scheduler)
        self.bot.messages.clear()
        target = ("region-a", 2, 1)
        self.parser.schedules[target] = make_schedule(*target, start=1)

        await self._check(self.scheduler)

        self.assertEqual([item["chat_id"] for item in self.bot.messages], [11, 12])
        self.assertIn("01:00–23:59", str(self.bot.messages[0]["text"]))
        self.assertEqual({item["chat_id"] for item in self.bot.messages}, {11, 12})

    async def test_snapshot_survives_scheduler_restart(self) -> None:
        await self._check(self.scheduler)
        restarted = LightStateScheduler(self.bot, self.db, self.parser)

        await self._check(restarted)

        self.assertEqual(self.bot.messages, [])
        self.assertEqual(len(self.db.snapshots), 3)

    async def test_database_snapshot_round_trips_across_database_instances(self) -> None:
        pool = FakePool()
        first_process = Database(":unused:")
        first_process._pool = pool
        second_process = Database(":unused:")
        second_process._pool = pool

        self.assertIsNone(await first_process.update_schedule_snapshot("region-a", 2, 1, "old"))
        self.assertEqual(
            await second_process.update_schedule_snapshot("region-a", 2, 1, "new"), "old"
        )
        self.assertEqual(pool.storage[("region-a", 2, 1)], "new")

    async def test_database_notification_claim_is_persistent_and_atomic(self) -> None:
        pool = FakePool()
        first_process = Database(":unused:")
        first_process._pool = pool
        restarted_process = Database(":unused:")
        restarted_process._pool = pool

        event = dict(event_key="event", telegram_id=11, region_slug="r", queue=2,
                     subqueue=1, schedule_date=NOW.date(), schedule_fingerprint="[]")
        self.assertTrue(await first_process.claim_no_more_outages_notification(**event))
        self.assertFalse(await restarted_process.claim_no_more_outages_notification(**event))

    async def test_last_state_timestamp_is_encoded_as_postgres_timestamp(self) -> None:
        pool = FakePool()
        database = Database(":unused:")
        database._pool = pool

        await database.upsert_last_state(11, "off", NOW.isoformat())

        _state, checked_at = pool.storage[("last_state", 11)]
        self.assertIsInstance(checked_at, datetime)
        self.assertIsNone(checked_at.tzinfo)
        self.assertEqual(checked_at, NOW.replace(tzinfo=None))

    async def test_concurrent_reprocessing_sends_one_update(self) -> None:
        await self._check(self.scheduler)
        self.bot.messages.clear()
        target = ("region-a", 2, 1)
        self.parser.schedules[target] = make_schedule(*target, start=1)
        second_scheduler = LightStateScheduler(self.bot, self.db, self.parser)

        await self._check(self.scheduler, second_scheduler)

        self.assertCountEqual([item["chat_id"] for item in self.bot.messages], [11, 12])
        # Both concurrent runs read the same profiles; the update is claimed only once.
        self.assertEqual(len(self.bot.messages), 2)

    async def test_existing_light_state_notification_still_works(self) -> None:
        self.db.states[11] = "on"
        schedule = make_schedule("region-a", 2, 1)

        with patch("bot.scheduler.now_kyiv", return_value=NOW):
            await self.scheduler._check_user(self.users[0], schedule, "off", NOW)

        self.assertEqual(len(self.bot.messages), 1)
        self.assertIn("ВІДКЛЮЧЕННЯ СВІТЛА", str(self.bot.messages[0]["text"]))

    async def test_existing_five_minute_warning_still_works(self) -> None:
        schedule = DaySchedule(
            "region-a", "region-a", 2, 1,
            [ScheduleInterval(time(12, 5), time(13), "off")],
            schedule_date=NOW.date(), fetched_at=NOW - timedelta(minutes=10), source_updated_at=NOW - timedelta(minutes=10),
        )
        self.db.states[11] = "on"

        with patch("bot.scheduler.now_kyiv", return_value=NOW):
            await self.scheduler._check_user(self.users[0], schedule, "on", NOW)

        self.assertEqual(len(self.bot.messages), 1)
        self.assertIn("Попередження за 5 хв", str(self.bot.messages[0]["text"]))

    async def test_quiet_hours_suppress_final_and_state_notices(self) -> None:
        user = make_user(11)
        user.quiet_start = "11:00"
        user.quiet_end = "13:00"
        schedule = DaySchedule(
            "region-a", "region-a", 2, 1,
            [ScheduleInterval(time(8), time(10), "off")],
            schedule_date=NOW.date(), fetched_at=NOW - timedelta(minutes=10), source_updated_at=NOW - timedelta(minutes=10),
        )

        with patch("bot.scheduler.now_kyiv", return_value=NOW):
            await self.scheduler._check_user(user, schedule, "on", NOW)

        self.assertEqual(self.bot.messages, [])

    async def _run_end_of_day(self, schedule: DaySchedule, *, state: str = "on") -> None:
        user = self.users[0]
        with patch("bot.scheduler.now_kyiv", return_value=NOW):
            await self.scheduler._check_user(user, schedule, state, NOW)

    async def test_last_outage_ended_sends_final_notice(self) -> None:
        schedule = DaySchedule(
            "region-a", "region-a", 2, 1,
            [ScheduleInterval(time(8), time(10), "off")],
            schedule_date=NOW.date(), fetched_at=NOW - timedelta(minutes=10), source_updated_at=NOW - timedelta(minutes=10),
        )

        await self._run_end_of_day(schedule)

        finals = [m for m in self.bot.messages if "більше немає запланованих" in str(m["text"])]
        self.assertEqual(len(finals), 1)

    async def test_future_outage_prevents_final_notice(self) -> None:
        schedule = DaySchedule(
            "region-a", "region-a", 2, 1,
            [ScheduleInterval(time(8), time(10), "off"), ScheduleInterval(time(14), time(15), "off")],
            schedule_date=NOW.date(), fetched_at=NOW - timedelta(minutes=10), source_updated_at=NOW - timedelta(minutes=10),
        )

        await self._run_end_of_day(schedule)

        self.assertFalse(any("більше немає запланованих" in str(m["text"]) for m in self.bot.messages))

    async def test_schedule_without_outages_does_not_create_false_final_notice(self) -> None:
        schedule = DaySchedule(
            "region-a", "region-a", 2, 1,
            [ScheduleInterval(time(0), time(23, 59), "on")],
            schedule_date=NOW.date(), fetched_at=NOW - timedelta(minutes=10), source_updated_at=NOW - timedelta(minutes=10),
        )

        await self._run_end_of_day(schedule)

        self.assertFalse(any("більше немає запланованих" in str(m["text"]) for m in self.bot.messages))

    async def test_stale_or_undated_source_does_not_send_final_notice(self) -> None:
        stale = DaySchedule(
            "region-a", "region-a", 2, 1,
            [ScheduleInterval(time(8), time(10), "off")],
            schedule_date=NOW.date(), fetched_at=NOW - timedelta(hours=1), source_updated_at=NOW - timedelta(days=2),
        )
        undated = DaySchedule(
            "region-a", "region-a", 2, 1,
            [ScheduleInterval(time(8), time(10), "off")],
            schedule_date=None, fetched_at=NOW - timedelta(minutes=10),
        )

        await self._run_end_of_day(stale)
        await self._run_end_of_day(undated)

        self.assertFalse(any("більше немає запланованих" in str(m["text"]) for m in self.bot.messages))

    async def test_parser_error_does_not_send_final_notice(self) -> None:
        self.parser.schedules[("region-a", 2, 1)] = ParserError("source unavailable")

        await self._check(self.scheduler)

        self.assertEqual(self.bot.messages, [])

    async def test_same_cycle_restart_and_reprocessing_are_deduplicated(self) -> None:
        user = self.users[0]
        schedule = DaySchedule(
            "region-a", "region-a", 2, 1,
            [ScheduleInterval(time(8), time(10), "off")],
            schedule_date=NOW.date(), fetched_at=NOW - timedelta(minutes=10), source_updated_at=NOW - timedelta(minutes=10),
        )
        await self._run_end_of_day(schedule)
        # Simulate a repeated state transition after a scheduler restart.
        self.db.states[user.telegram_id] = "off"
        self.db.checked_at[user.telegram_id] = NOW.isoformat()
        restarted = LightStateScheduler(self.bot, self.db, self.parser)
        with patch("bot.scheduler.now_kyiv", return_value=NOW):
            await restarted._check_user(user, schedule, "on", NOW)

        finals = [m for m in self.bot.messages if "більше немає запланованих" in str(m["text"])]
        self.assertEqual(len(finals), 1)

    async def test_parallel_claims_send_at_most_one_final_notice(self) -> None:
        user = self.users[0]
        schedule = DaySchedule(
            "region-a", "region-a", 2, 1,
            [ScheduleInterval(time(8), time(10), "off")],
            schedule_date=NOW.date(), fetched_at=NOW - timedelta(minutes=10), source_updated_at=NOW - timedelta(minutes=10),
        )
        previous = LastState(user.telegram_id, "off", NOW.isoformat())

        second_scheduler = LightStateScheduler(self.bot, self.db, self.parser)
        with patch("bot.scheduler.now_kyiv", return_value=NOW):
            await asyncio.gather(*[
                scheduler._notify_no_more_outages(user, schedule, previous, NOW)
                for scheduler in (self.scheduler, second_scheduler, self.scheduler,
                                  second_scheduler, self.scheduler)
            ])

        finals = [m for m in self.bot.messages if "більше немає запланованих" in str(m["text"])]
        self.assertEqual(len(finals), 1)

    async def test_midnight_interval_and_new_date_fail_closed(self) -> None:
        crossing = DaySchedule(
            "region-a", "region-a", 2, 1,
            [ScheduleInterval(time(22), time(2), "off")],
            schedule_date=NOW.date(), fetched_at=NOW - timedelta(minutes=10), source_updated_at=NOW - timedelta(minutes=10),
        )
        self.assertFalse(self.scheduler._has_confirmed_no_more_outages(crossing, NOW))

        next_day_now = NOW + timedelta(days=1)
        previous_day_graph = DaySchedule(
            "region-a", "region-a", 2, 1,
            [ScheduleInterval(time(8), time(10), "off")],
            schedule_date=NOW.date(), fetched_at=next_day_now,
        )
        self.assertFalse(self.scheduler._has_confirmed_no_more_outages(previous_day_graph, next_day_now))

        current_day_graph = DaySchedule(
            "region-a", "region-a", 2, 1,
            [ScheduleInterval(time(0), time(2), "off")],
            schedule_date=next_day_now.date(), fetched_at=next_day_now,
            source_updated_at=next_day_now.replace(hour=1, minute=50),
        )
        after_overnight_outage = next_day_now.replace(hour=2, minute=1)
        current_day_graph.fetched_at = after_overnight_outage
        self.assertTrue(
            self.scheduler._has_confirmed_no_more_outages(
                current_day_graph, after_overnight_outage
            )
        )

    async def test_parser_uses_explicit_schedule_dates_only(self) -> None:
        dated_title = BeautifulSoup("<title>Графік 17.10.2026</title>", "html.parser")
        undated_title = BeautifulSoup("<title>Графік відключень</title>", "html.parser")

        self.assertEqual(
            _extract_schedule_date(dated_title, "https://alerts.org.ua/region-a/"), date(2026, 10, 17)
        )
        self.assertEqual(
            _extract_schedule_date(undated_title, "https://alerts.org.ua/region-a/2026-10-17.html"),
            date(2026, 10, 17),
        )
        self.assertIsNone(_extract_schedule_date(undated_title, "https://alerts.org.ua/region-a/"))

    async def test_end_of_day_notice_is_profile_scoped(self) -> None:
        self.parser.schedules[("region-a", 2, 1)] = DaySchedule(
            "region-a", "region-a", 2, 1,
            [ScheduleInterval(time(8), time(10), "off")],
            schedule_date=NOW.date(), fetched_at=NOW - timedelta(minutes=10), source_updated_at=NOW - timedelta(minutes=10),
        )

        await self._check(self.scheduler)

        finals = [m["chat_id"] for m in self.bot.messages if "більше немає запланованих" in str(m["text"])]
        self.assertEqual(finals, [11, 12])

    async def test_confirmed_telegram_error_can_retry_and_success_marks_sent(self) -> None:
        class TelegramBadRequest(Exception):
            pass

        user = self.users[0]
        schedule = DaySchedule(user.region_slug, user.region_name, user.queue, user.subqueue,
            [ScheduleInterval(time(8), time(10), "off")], schedule_date=NOW.date(),
            fetched_at=NOW - timedelta(minutes=10), source_updated_at=NOW - timedelta(minutes=10))
        self.bot.failure = TelegramBadRequest("rejected")
        await self._run_end_of_day(schedule)
        key, event = next(iter(self.db.final_events.items()))
        self.assertEqual(event["status"], "failed")
        self.db.monotonic += 300
        await self._run_end_of_day(schedule)
        self.assertEqual(self.db.final_events[key]["status"], "sent")
        self.assertEqual(sum("більше немає запланованих" in str(m["text"]) for m in self.bot.messages), 1)

    async def test_ambiguous_timeout_is_not_retried(self) -> None:
        user = self.users[0]
        schedule = DaySchedule(user.region_slug, user.region_name, user.queue, user.subqueue,
            [ScheduleInterval(time(8), time(10), "off")], schedule_date=NOW.date(),
            fetched_at=NOW - timedelta(minutes=10), source_updated_at=NOW - timedelta(minutes=10))
        self.bot.failure = TimeoutError("unknown delivery result")
        await self._run_end_of_day(schedule)
        event = next(iter(self.db.final_events.values()))
        self.assertEqual(event["status"], "unknown")
        await self._run_end_of_day(schedule)
        self.assertEqual(sum("більше немає запланованих" in str(m["text"]) for m in self.bot.messages), 0)

    async def test_a_b_a_event_sequence_is_deduplicated_by_history(self) -> None:
        user = self.users[0]
        previous = LastState(user.telegram_id, "off", NOW.isoformat())
        for start in (8, 9, 8):
            schedule = DaySchedule(user.region_slug, user.region_name, user.queue, user.subqueue,
                [ScheduleInterval(time(start), time(start + 2), "off")], schedule_date=NOW.date(),
                fetched_at=NOW - timedelta(minutes=10), source_updated_at=NOW - timedelta(minutes=10))
            await self.scheduler._notify_no_more_outages(user, schedule, previous, NOW)
        self.assertEqual(len(self.bot.messages), 2)

    async def test_midnight_end_24_is_a_completed_interval(self) -> None:
        schedule = DaySchedule(
            "region-a", "region-a", 2, 1,
            [ScheduleInterval(time(22), time(0), "off", ends_at_midnight=True)],
            schedule_date=NOW.date(), fetched_at=NOW - timedelta(minutes=10),
            source_updated_at=NOW - timedelta(minutes=10),
        )
        after_midnight = datetime(2026, 10, 18, 0, 1, tzinfo=KYIV)
        self.assertFalse(self.scheduler._has_confirmed_no_more_outages(schedule, after_midnight))

    async def test_midnight_marker_does_not_break_existing_snapshot_fingerprint(self) -> None:
        schedule = DaySchedule("region-a", "region-a", 2, 1,
            [ScheduleInterval(time(22), time(0), "off", ends_at_midnight=True)])
        self.assertEqual(self.scheduler._schedule_fingerprint(schedule), '[["22:00","00:00","off"]]')
        self.assertEqual(
            self.scheduler._schedule_fingerprint(schedule, include_midnight_marker=True),
            '[["22:00","24:00","off"]]',
        )

    async def test_schedule_fingerprint_is_stable_across_day_transitions(self) -> None:
        user = self.users[0]
        intervals = [
            ScheduleInterval(time(0), time(2), "on"),
            ScheduleInterval(time(2), time(3), "off"),
            ScheduleInterval(time(3), time(10), "on"),
            ScheduleInterval(time(10), time(12), "off"),
            ScheduleInterval(time(12), time(14), "on"),
            ScheduleInterval(time(14), time(16), "off"),
            ScheduleInterval(time(16), time(20), "on"),
            ScheduleInterval(time(20), time(22), "off"),
            ScheduleInterval(time(22), time(0), "on", ends_at_midnight=True),
        ]
        schedule = DaySchedule(user.region_slug, user.region_name, user.queue,
                               user.subqueue, intervals, schedule_date=NOW.date())
        profile = (user.region_slug, user.queue, user.subqueue)
        semaphore = asyncio.Semaphore(1)
        times = ((19, 55), (20, 0), (20, 1), (21, 55), (22, 0), (22, 1))

        async def direct_to_thread(function, *args):
            return function(*args)

        for hour, minute in times:
            moment = NOW.replace(hour=hour, minute=minute)
            with patch("bot.scheduler.asyncio.to_thread", new=direct_to_thread), patch(
                "bot.scheduler.now_kyiv", return_value=moment
            ):
                await self.scheduler._check_profile(profile, [user], semaphore)

        changes = [m for m in self.bot.messages if "🔄 Графік змінився!" in str(m["text"])]
        self.assertEqual(changes, [])

    async def test_fingerprint_detects_real_future_interval_changes(self) -> None:
        original = DaySchedule("region-a", "region-a", 2, 1,
            [ScheduleInterval(time(20), time(22), "off")])
        shifted = DaySchedule("region-a", "region-a", 2, 1,
            [ScheduleInterval(time(21), time(23), "off")])
        added = DaySchedule("region-a", "region-a", 2, 1,
            [ScheduleInterval(time(20), time(22), "off"),
             ScheduleInterval(time(23), time(0), "off", ends_at_midnight=True)])
        removed = DaySchedule("region-a", "region-a", 2, 1,
            [ScheduleInterval(time(20), time(22), "off")])

        original_fingerprint = self.scheduler._schedule_fingerprint(original)
        self.assertNotEqual(original_fingerprint, self.scheduler._schedule_fingerprint(shifted))
        self.assertNotEqual(original_fingerprint, self.scheduler._schedule_fingerprint(added))
        self.assertNotEqual(self.scheduler._schedule_fingerprint(added),
                            self.scheduler._schedule_fingerprint(removed))

    async def test_fingerprint_ignores_interval_order_and_duplicate_rows(self) -> None:
        first = DaySchedule("region-a", "region-a", 2, 1,
            [ScheduleInterval(time(20), time(22), "off"),
             ScheduleInterval(time(22), time(0), "on", ends_at_midnight=True)])
        reordered = DaySchedule("region-a", "region-a", 2, 1,
            [ScheduleInterval(time(22), time(0), "on", ends_at_midnight=True),
             ScheduleInterval(time(20), time(22), "off"),
             ScheduleInterval(time(20), time(22), "off")])
        self.assertEqual(self.scheduler._schedule_fingerprint(first),
                         self.scheduler._schedule_fingerprint(reordered))

    async def test_repeated_cycle_does_not_repeat_no_outages_notice(self) -> None:
        schedule = DaySchedule(
            "region-a", "region-a", 2, 1,
            [ScheduleInterval(time(8), time(10), "off")],
            schedule_date=NOW.date(), fetched_at=NOW - timedelta(minutes=10),
            source_updated_at=NOW - timedelta(minutes=10),
        )

        await self._run_end_of_day(schedule)
        await self._run_end_of_day(schedule)

        finals = [m for m in self.bot.messages if "більше немає запланованих" in str(m["text"])]
        self.assertEqual(len(finals), 1)

    async def test_morning_schedule_is_sent_at_0700(self) -> None:
        now = datetime(2026, 10, 17, 7, 0, tzinfo=KYIV)
        user = self.users[0]
        await self.scheduler._send_morning_schedule_if_due(
            user, make_morning_schedule(user, now), "on", now
        )
        message = next(m["text"] for m in self.bot.messages if "☀️ Графік відключень" in str(m["text"]))
        self.assertIn("Область: region-a", message)
        self.assertIn("Черга: 2, підчерга 1", message)

    async def test_restart_after_0700_sends_missed_schedule_for_today(self) -> None:
        user = self.users[0]
        now = datetime(2026, 10, 17, 9, 15, tzinfo=KYIV)
        await self.scheduler._send_morning_schedule_if_due(
            user, make_morning_schedule(user, now), "on", now
        )
        self.assertEqual(sum("☀️ Графік відключень" in str(m["text"]) for m in self.bot.messages), 1)

    async def test_morning_schedule_waits_until_quiet_hours_end_at_0800(self) -> None:
        user = self.users[0]
        user.quiet_start, user.quiet_end = "23:00", "08:00"
        before = datetime(2026, 10, 17, 7, 59, tzinfo=KYIV)
        await self.scheduler._send_morning_schedule_if_due(
            user, make_morning_schedule(user, before), "on", before
        )
        self.assertFalse(self.bot.messages)
        at_end = before.replace(hour=8, minute=0)
        await self.scheduler._send_morning_schedule_if_due(
            user, make_morning_schedule(user, at_end), "on", at_end
        )
        self.assertEqual(sum("☀️ Графік відключень" in str(m["text"]) for m in self.bot.messages), 1)

    async def test_quiet_hours_ending_before_0700_do_not_delay_morning_schedule(self) -> None:
        user = self.users[0]
        user.quiet_start, user.quiet_end = "22:00", "06:30"
        now = datetime(2026, 10, 17, 7, 0, tzinfo=KYIV)
        await self.scheduler._send_morning_schedule_if_due(
            user, make_morning_schedule(user, now), "on", now
        )
        self.assertTrue(any("☀️ Графік відключень" in str(m["text"]) for m in self.bot.messages))

    async def test_cross_midnight_quiet_hours_delay_until_the_same_mornings_end(self) -> None:
        user = self.users[0]
        user.quiet_start, user.quiet_end = "22:00", "08:00"
        now = datetime(2026, 10, 17, 7, 30, tzinfo=KYIV)
        await self.scheduler._send_morning_schedule_if_due(
            user, make_morning_schedule(user, now), "on", now
        )
        self.assertFalse(self.bot.messages)
        at_end = now.replace(hour=8, minute=0)
        await self.scheduler._send_morning_schedule_if_due(
            user, make_morning_schedule(user, at_end), "on", at_end
        )
        self.assertEqual(sum("☀️ Графік відключень" in str(m["text"]) for m in self.bot.messages), 1)

    async def test_morning_schedule_uses_new_calendar_date_only(self) -> None:
        user = self.users[0]
        now = datetime(2026, 10, 18, 7, 0, tzinfo=KYIV)
        yesterday = make_morning_schedule(user, now, schedule_date_value=now.date() - timedelta(days=1))
        await self.scheduler._send_morning_schedule_if_due(user, yesterday, "on", now)
        self.assertFalse(self.bot.messages)
        await self.scheduler._send_morning_schedule_if_due(
            user, make_morning_schedule(user, now), "on", now
        )
        self.assertEqual(sum("☀️ Графік відключень" in str(m["text"]) for m in self.bot.messages), 1)

    async def test_morning_schedule_is_deduplicated_across_cycles_and_restart(self) -> None:
        user = self.users[0]
        now = datetime(2026, 10, 17, 7, 0, tzinfo=KYIV)
        schedule = make_morning_schedule(user, now)
        await self.scheduler._send_morning_schedule_if_due(user, schedule, "on", now)
        restarted = LightStateScheduler(self.bot, self.db, self.parser)
        await restarted._send_morning_schedule_if_due(user, schedule, "on", now)
        await self.scheduler._send_morning_schedule_if_due(user, schedule, "on", now)
        self.assertEqual(sum("☀️ Графік відключень" in str(m["text"]) for m in self.bot.messages), 1)

    async def test_parallel_scheduler_instances_claim_one_morning_message(self) -> None:
        user = self.users[0]
        now = datetime(2026, 10, 17, 7, 0, tzinfo=KYIV)
        schedule = make_morning_schedule(user, now)
        other = LightStateScheduler(self.bot, self.db, self.parser)
        await asyncio.gather(
            self.scheduler._send_morning_schedule_if_due(user, schedule, "on", now),
            other._send_morning_schedule_if_due(user, schedule, "on", now),
        )
        self.assertEqual(sum("☀️ Графік відключень" in str(m["text"]) for m in self.bot.messages), 1)

    async def test_morning_telegram_error_can_retry_after_throttle(self) -> None:
        class TelegramBadRequest(Exception):
            pass

        user = self.users[0]
        now = datetime(2026, 10, 17, 7, 0, tzinfo=KYIV)
        schedule = make_morning_schedule(user, now)
        self.bot.failure = TelegramBadRequest("rejected")
        await self.scheduler._send_morning_schedule_if_due(user, schedule, "on", now)
        event = next(iter(self.db.final_events.values()))
        self.assertEqual(event["status"], "failed")
        await self.scheduler._send_morning_schedule_if_due(user, schedule, "on", now)
        self.assertEqual(event["attempt_count"], 1)
        self.db.monotonic += 300
        await self.scheduler._send_morning_schedule_if_due(user, schedule, "on", now)
        self.assertEqual(self.db.final_events[event["event_key"]]["status"], "sent")
        self.assertEqual(self.db.final_events[event["event_key"]]["attempt_count"], 2)

    async def test_morning_schedule_requires_confirmed_nonempty_or_explicit_empty_graph(self) -> None:
        user = self.users[0]
        now = datetime(2026, 10, 17, 7, 0, tzinfo=KYIV)
        unconfirmed_empty = make_morning_schedule(user, now, intervals=[])
        await self.scheduler._send_morning_schedule_if_due(user, unconfirmed_empty, "on", now)
        self.assertFalse(self.bot.messages)
        confirmed_empty = make_morning_schedule(user, now, intervals=[], confirmed_empty=True)
        await self.scheduler._send_morning_schedule_if_due(user, confirmed_empty, "on", now)
        self.assertTrue(any("відключень не заплановано" in str(m["text"]) for m in self.bot.messages))

    async def test_morning_schedule_suppresses_disabled_users_and_current_quiet_hours(self) -> None:
        user = self.users[0]
        now = datetime(2026, 10, 17, 7, 0, tzinfo=KYIV)
        schedule = make_morning_schedule(user, now)
        user.notifications_enabled = False
        await self.scheduler._send_morning_schedule_if_due(user, schedule, "on", now)
        user.notifications_enabled = True
        user.quiet_start, user.quiet_end = "06:00", "08:00"
        await self.scheduler._send_morning_schedule_if_due(user, schedule, "on", now)
        self.assertFalse(self.bot.messages)

    async def test_morning_parse_failure_does_not_send_a_graph(self) -> None:
        user = self.users[0]
        self.db.users = [user]
        self.parser.schedules[(user.region_slug, user.queue, user.subqueue)] = ParserError("offline")

        async def direct_to_thread(function, *args):
            return function(*args)

        morning = datetime(2026, 10, 17, 7, 0, tzinfo=KYIV)
        with patch("bot.scheduler.asyncio.to_thread", new=direct_to_thread), patch(
            "bot.scheduler.now_kyiv", return_value=morning
        ):
            await self.scheduler._check_once()
        self.assertFalse(self.bot.messages)

    async def test_morning_graphs_are_isolated_by_region_queue_and_subqueue(self) -> None:
        morning = datetime(2026, 10, 17, 7, 0, tzinfo=KYIV)
        profile_intervals = {
            ("region-a", 2, 1): [ScheduleInterval(time(10), time(11), "off")],
            ("region-a", 2, 2): [ScheduleInterval(time(11), time(12), "off")],
            ("region-b", 2, 1): [ScheduleInterval(time(12), time(13), "off")],
        }
        for user in self.users:
            key = (user.region_slug, user.queue, user.subqueue)
            self.parser.schedules[key] = DaySchedule(
                user.region_slug, user.region_name, user.queue, user.subqueue,
                profile_intervals[key],
                schedule_date=morning.date(), fetched_at=morning,
            )

        async def direct_to_thread(function, *args):
            return function(*args)

        with patch("bot.scheduler.asyncio.to_thread", new=direct_to_thread), patch(
            "bot.scheduler.now_kyiv", return_value=morning
        ):
            await self.scheduler._check_once()

        morning_messages = {
            int(message["chat_id"]): str(message["text"])
            for message in self.bot.messages
            if "☀️ Графік відключень" in str(message["text"])
        }
        self.assertEqual(set(morning_messages), {11, 12, 21, 31})
        self.assertIn("Черга: 2, підчерга 1", morning_messages[11])
        self.assertIn("10:00 - 11:00", morning_messages[11])
        self.assertIn("11:00 - 12:00", morning_messages[21])
        self.assertIn("12:00 - 13:00", morning_messages[31])


if __name__ == "__main__":
    unittest.main()
