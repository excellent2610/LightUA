from __future__ import annotations

import asyncio
import csv
import hmac
import json
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit
from unittest.mock import AsyncMock, patch

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from db.database import (
    AD_CAMPAIGN_STATISTICS_QUERY,
    AD_IMPRESSION_MAX_ATTEMPTS,
    AdImpressionPersistenceWorker,
    Database,
)
from db.models import AdCampaign, AdStatistics
from bot.main import DATABASE_APP_KEY, handle_ad_click
from handlers.admin import _authorized, get_router
from lightua_statistics.code.advertising import (
    CSV_FIELDS,
    _serialize_row,
    write_advertising_csv,
)
from utils.ads import AdContent, AdManager


class FakeAdDatabase:
    """Persistent-in-test stand-in with the same per-user reservation boundaries."""

    def __init__(self) -> None:
        self.campaigns: dict[str, AdCampaign] = {}
        self.call_counts: dict[int, int] = {}
        self.last_ads: dict[int, str] = {}
        self.reservations: dict[uuid.UUID, tuple[str, int, float]] = {}
        self.impressions: list[tuple[str, int, float]] = []
        self.recorded_reservations: set[uuid.UUID] = set()
        self.impression_states: dict[uuid.UUID, str] = {}
        self.impression_attempts: dict[uuid.UUID, int] = {}
        self.impression_next_attempt: dict[uuid.UUID, float | None] = {}
        self.impression_delivered_at: dict[uuid.UUID, object] = {}
        self.impression_shown_at: dict[uuid.UUID, object] = {}
        self.impression_leases: set[uuid.UUID] = set()
        self.impression_errors: dict[uuid.UUID, str] = {}
        self.retry_delays: list[int] = []
        self.clicks: list[tuple[str, int]] = []
        self.now = 1_000.0
        self.lock = asyncio.Lock()
        self.fail_reserve = False
        self.fail_impression_writes = 0
        self.fail_delivery_state_updates = 0
        self.fail_cancel_attempts = 0
        self.cancel_calls = 0
        self.delivery_state_update_calls = 0
        self.active_impression_writes = 0
        self.max_active_impression_writes = 0

    async def sync_ad_campaigns(self, campaigns: list[AdCampaign]) -> None:
        self.campaigns.update({campaign.id: campaign for campaign in campaigns})

    async def reserve_ad_impression(
        self,
        telegram_id: int,
        candidate_ids: list[str],
        *,
        show_every: int,
        max_per_user_per_day: int,
        min_interval_hours: float,
    ) -> tuple[str, uuid.UUID] | None:
        async with self.lock:
            if self.fail_reserve:
                raise RuntimeError("database unavailable")
            count = self.call_counts.get(telegram_id, 0) + 1
            self.call_counts[telegram_id] = count
            if (
                count % show_every
                or max_per_user_per_day <= 0
                or not candidate_ids
            ):
                return None

            delivered = [row for row in self.impressions if row[1] == telegram_id]
            pending = [
                row for row in self.reservations.values() if row[1] == telegram_id
            ]
            if len(delivered) + len(pending) >= max_per_user_per_day:
                return None
            last_times = [row[2] for row in delivered + pending]
            if last_times and self.now - max(last_times) < min_interval_hours * 3600:
                return None

            enabled = [
                campaign_id
                for campaign_id in candidate_ids
                if self.campaigns.get(campaign_id) is not None
                and self.campaigns[campaign_id].enabled
            ]
            if not enabled:
                return None
            alternatives = [
                item for item in enabled if item != self.last_ads.get(telegram_id)
            ]
            campaign_id = alternatives[0] if alternatives else enabled[0]
            reservation_id = uuid.uuid4()
            self.reservations[reservation_id] = (campaign_id, telegram_id, self.now)
            self.impression_states[reservation_id] = "reserved"
            self.impression_attempts[reservation_id] = 0
            self.impression_next_attempt[reservation_id] = None
            self.last_ads[telegram_id] = campaign_id
            return campaign_id, reservation_id

    async def mark_ad_impression_delivery_started(
        self, reservation_id: uuid.UUID
    ) -> None:
        if self.impression_states.get(reservation_id) == "reserved":
            self.impression_states[reservation_id] = "delivery_outcome_unknown"

    async def mark_ad_impression_delivered(
        self, reservation_id: uuid.UUID, delivered_at: object
    ) -> None:
        self.delivery_state_update_calls += 1
        if self.fail_delivery_state_updates:
            self.fail_delivery_state_updates -= 1
            raise RuntimeError("temporary database failure")
        if self.impression_states.get(reservation_id) == "delivery_outcome_unknown":
            self.impression_states[reservation_id] = "delivered_pending_record"
            self.impression_attempts[reservation_id] = 0
            self.impression_next_attempt[reservation_id] = self.now
            self.impression_delivered_at[reservation_id] = delivered_at

    async def claim_due_ad_impressions(
        self, limit: int
    ) -> list[tuple[uuid.UUID, int]]:
        async with self.lock:
            due_ids = [
                reservation_id
                for reservation_id in self.reservations
                if self.impression_states.get(reservation_id)
                == "delivered_pending_record"
                and self.impression_attempts[reservation_id] < AD_IMPRESSION_MAX_ATTEMPTS
                and self.impression_next_attempt[reservation_id] is not None
                and self.impression_next_attempt[reservation_id] <= self.now
                and reservation_id not in self.impression_leases
            ][:limit]
            claimed = []
            for reservation_id in due_ids:
                self.impression_attempts[reservation_id] += 1
                self.impression_leases.add(reservation_id)
                claimed.append(
                    (reservation_id, self.impression_attempts[reservation_id])
                )
            return claimed

    async def record_ad_impression(self, reservation_id: uuid.UUID) -> None:
        if reservation_id in self.recorded_reservations:
            return
        if self.impression_states.get(reservation_id) != "delivered_pending_record":
            return
        self.active_impression_writes += 1
        self.max_active_impression_writes = max(
            self.max_active_impression_writes, self.active_impression_writes
        )
        try:
            await asyncio.sleep(0.01)
            if self.fail_impression_writes:
                self.fail_impression_writes -= 1
                raise RuntimeError("temporary database failure")
            campaign_id, user_id, timestamp = self.reservations.pop(reservation_id)
            self.impressions.append((campaign_id, user_id, timestamp))
            self.impression_shown_at[reservation_id] = self.impression_delivered_at[
                reservation_id
            ]
            self.recorded_reservations.add(reservation_id)
            self.impression_states.pop(reservation_id, None)
            self.impression_attempts.pop(reservation_id, None)
            self.impression_next_attempt.pop(reservation_id, None)
            self.impression_delivered_at.pop(reservation_id, None)
            self.impression_errors.pop(reservation_id, None)
        finally:
            self.active_impression_writes -= 1

    async def schedule_ad_impression_retry(
        self, reservation_id: uuid.UUID, attempt: int, error: str
    ) -> None:
        if self.impression_states.get(reservation_id) != "delivered_pending_record":
            return
        delay = min(2 ** max(attempt - 1, 0), 60)
        self.retry_delays.append(delay)
        self.impression_errors[reservation_id] = error
        self.impression_leases.discard(reservation_id)
        self.impression_next_attempt[reservation_id] = (
            None
            if attempt >= AD_IMPRESSION_MAX_ATTEMPTS
            else self.now + delay
        )

    async def prepare_ad_impression_retries(self) -> None:
        for reservation_id, state in self.impression_states.items():
            if state == "delivered_pending_record":
                self.impression_leases.discard(reservation_id)
                if self.impression_attempts[reservation_id] >= AD_IMPRESSION_MAX_ATTEMPTS:
                    self.impression_attempts[reservation_id] = 0
                    self.impression_next_attempt[reservation_id] = self.now

    def make_impression_retries_due(self) -> None:
        for reservation_id in self.reservations:
            if self.impression_states.get(reservation_id) == "delivered_pending_record":
                self.impression_next_attempt[reservation_id] = self.now

    async def cancel_ad_impression(self, reservation_id: uuid.UUID) -> None:
        self.cancel_calls += 1
        if self.fail_cancel_attempts:
            self.fail_cancel_attempts -= 1
            raise RuntimeError("temporary database failure")
        if self.impression_states.get(reservation_id) in {
            "reserved",
            "delivery_outcome_unknown",
        }:
            self.reservations.pop(reservation_id, None)
            self.impression_states.pop(reservation_id, None)
            self.impression_attempts.pop(reservation_id, None)
            self.impression_next_attempt.pop(reservation_id, None)
            self.impression_delivered_at.pop(reservation_id, None)

    def sign_ad_click(self, campaign_id: str, telegram_id: int) -> str:
        payload = f"{campaign_id}:{telegram_id}".encode()
        return hmac.digest(b"unit-test-key", payload, "sha256").hex()

    async def record_ad_click(
        self, campaign_id: str, telegram_id: int, signature: str
    ) -> str | None:
        if not hmac.compare_digest(
            self.sign_ad_click(campaign_id, telegram_id), signature
        ):
            return None
        campaign = self.campaigns.get(campaign_id)
        if campaign is None:
            return None
        self.clicks.append((campaign_id, telegram_id))
        return campaign.destination_url


class FakeAcquire:
    def __init__(self, connection: object) -> None:
        self.connection = connection

    async def __aenter__(self) -> object:
        return self.connection

    async def __aexit__(self, *_: object) -> bool:
        return False


class FakeStatsConnection:
    def __init__(self) -> None:
        self.row = {
            "campaign_id": "spring",
            "name": "Spring",
            "enabled": True,
            "impressions": 3,
            "unique_users": 2,
            "clicks": 2,
            "unique_clicks": 1,
            "first_impression": None,
            "last_impression": None,
        }
        self.query = ""

    async def fetch(self, query: str, *_: object) -> list[dict[str, object]]:
        self.query = query
        return [self.row]


class FakeStatsPool:
    def __init__(self, connection: FakeStatsConnection) -> None:
        self.connection = connection

    def acquire(self) -> FakeAcquire:
        return FakeAcquire(self.connection)


class FakeClickConnection:
    def __init__(self) -> None:
        self.clicks: list[tuple[str, int]] = []
        self.queries: list[str] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchval(self, _query: str, campaign_id: str) -> str | None:
        self.queries.append(_query)
        return "https://advertiser.example/" if campaign_id == "spring" else None

    async def execute(self, _query: str, campaign_id: str, user_id: int) -> None:
        self.queries.append(_query)
        self.clicks.append((campaign_id, user_id))


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_: object) -> bool:
        return False


class FakeClickPool:
    def __init__(self, connection: FakeClickConnection) -> None:
        self.connection = connection

    def acquire(self) -> FakeAcquire:
        return FakeAcquire(self.connection)


class FakeImpressionConnection:
    def __init__(self) -> None:
        self.executions: list[tuple[str, tuple[object, ...]]] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def execute(self, query: str, *args: object) -> str:
        self.executions.append((query, args))
        return "INSERT 0 1" if query.lstrip().startswith("INSERT") else "DELETE 1"


class FakeImpressionPool:
    def __init__(self, connection: FakeImpressionConnection) -> None:
        self.connection = connection

    def acquire(self) -> FakeAcquire:
        return FakeAcquire(self.connection)


class FakeDeliveryStateConnection:
    def __init__(self) -> None:
        self.statuses = ["UPDATE 1", "UPDATE 0"]
        self.executions: list[str] = []
        self.fetches: list[str] = []

    async def execute(self, query: str, *_: object) -> str:
        self.executions.append(query)
        return self.statuses.pop(0)

    async def fetchval(self, query: str, *_: object) -> bool:
        self.fetches.append(query)
        return True


class FakeDeliveryStatePool:
    def __init__(self, connection: FakeDeliveryStateConnection) -> None:
        self.connection = connection

    def acquire(self) -> FakeAcquire:
        return FakeAcquire(self.connection)


class AdvertisingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.config_path = self.root / "config.json"
        self.ad_path = self.root / "spring.json"
        self.write_config()
        self.write_ad()
        self.db = FakeAdDatabase()
        self.bot = SimpleNamespace(send_message=AsyncMock())
        self.manager = AdManager(self.config_path, db=self.db)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def write_config(
        self,
        *,
        ads_enabled: bool = True,
        show_every: int = 1,
        max_per_user_per_day: int = 3,
        min_interval_hours: float = 0,
        enabled: bool = True,
    ) -> None:
        self.config_path.write_text(
            json.dumps(
                {
                    "ads_enabled": ads_enabled,
                    "show_every": show_every,
                    "max_per_user_per_day": max_per_user_per_day,
                    "min_interval_hours": min_interval_hours,
                    "ads": [{"file": "spring.json", "enabled": enabled}],
                }
            ),
            encoding="utf-8",
        )

    def write_ad(self, *, enabled: bool = True) -> None:
        self.ad_path.write_text(
            json.dumps(
                {
                    "id": "spring",
                    "name": "Spring",
                    "text": "Ad copy",
                    "button_text": "Visit",
                    "url": "https://advertiser.example/",
                    "enabled": enabled,
                }
            ),
            encoding="utf-8",
        )

    async def send(self, user_id: int = 42, *, persist: bool = True) -> None:
        await self.manager.send_with_ad(
            self.bot,
            user_id,
            "Main content",
            telegram_id=user_id,
        )
        if persist:
            await AdImpressionPersistenceWorker(self.db).run_once()

    async def test_successful_send_moves_pending_impression_to_recorded(self) -> None:
        await self.send(persist=False)
        self.assertEqual(
            set(self.db.impression_states.values()),
            {"delivered_pending_record"},
        )
        reservation_id = next(iter(self.db.reservations))
        delivered_at = self.db.impression_delivered_at[reservation_id]
        self.db.now += 600
        worker = AdImpressionPersistenceWorker(self.db, poll_seconds=0.01)
        self.assertEqual(await worker.run_once(), 1)

        self.assertEqual(len(self.db.impressions), 1)
        self.assertEqual(self.db.impressions[0][:2], ("spring", 42))
        self.assertEqual(self.db.impression_shown_at[reservation_id], delivered_at)
        self.assertIn("Ad copy", self.bot.send_message.await_args.args[1])

    async def test_delivery_state_update_retries_transient_database_error(self) -> None:
        self.db.fail_delivery_state_updates = 2
        with patch("utils.ads.asyncio.sleep", new_callable=AsyncMock) as sleep:
            await self.send(persist=False)

        reservation_id = next(iter(self.db.reservations))
        self.assertEqual(self.db.delivery_state_update_calls, 3)
        self.assertEqual(
            self.db.impression_states[reservation_id],
            "delivered_pending_record",
        )
        self.assertEqual([call.args[0] for call in sleep.await_args_list], [1, 2])

    async def test_failed_delivery_state_transition_stays_unknown_across_restart(
        self,
    ) -> None:
        self.db.fail_delivery_state_updates = 5
        with patch("utils.ads.asyncio.sleep", new_callable=AsyncMock):
            await self.send(persist=False)

        reservation_id = next(iter(self.db.reservations))
        self.assertEqual(self.db.delivery_state_update_calls, 5)
        self.assertEqual(
            self.db.impression_states[reservation_id],
            "delivery_outcome_unknown",
        )
        self.assertEqual(self.db.impressions, [])

        await self.db.prepare_ad_impression_retries()
        restarted_worker = AdImpressionPersistenceWorker(self.db)
        self.assertEqual(await restarted_worker.run_once(), 0)
        self.assertEqual(
            self.db.impression_states[reservation_id],
            "delivery_outcome_unknown",
        )

    async def test_delivered_impression_is_retried_after_database_error(self) -> None:
        self.db.fail_impression_writes = 1

        await self.send(persist=False)
        self.assertIn("Ad copy", self.bot.send_message.await_args.args[1])
        worker = AdImpressionPersistenceWorker(self.db, poll_seconds=0.01)
        await worker.run_once()
        self.assertEqual(len(self.db.impressions), 0)
        self.assertIn("delivered_pending_record", self.db.impression_states.values())
        self.db.make_impression_retries_due()
        await worker.run_once()

        self.assertEqual(len(self.db.impressions), 1)
        self.assertEqual(self.db.impressions[0][:2], ("spring", 42))
        self.assertEqual(self.db.reservations, {})

    async def test_exhausted_retries_leave_impression_pending_in_database(self) -> None:
        self.db.fail_impression_writes = AD_IMPRESSION_MAX_ATTEMPTS + 1
        await self.send(persist=False)
        worker = AdImpressionPersistenceWorker(self.db)

        for _ in range(AD_IMPRESSION_MAX_ATTEMPTS):
            self.db.make_impression_retries_due()
            await worker.run_once()

        reservation_id = next(iter(self.db.reservations))
        self.assertEqual(
            self.db.impression_attempts[reservation_id],
            AD_IMPRESSION_MAX_ATTEMPTS,
        )
        self.assertIsNone(self.db.impression_next_attempt[reservation_id])
        self.assertEqual(
            self.db.impression_states[reservation_id], "delivered_pending_record"
        )
        self.assertEqual(self.db.retry_delays, [1, 2, 4, 8, 16])
        self.assertEqual(len(self.db.impressions), 0)

    async def test_restart_requeues_pending_impression(self) -> None:
        self.db.fail_impression_writes = AD_IMPRESSION_MAX_ATTEMPTS + 1
        await self.send(persist=False)
        first_worker = AdImpressionPersistenceWorker(self.db)
        for _ in range(AD_IMPRESSION_MAX_ATTEMPTS):
            self.db.make_impression_retries_due()
            await first_worker.run_once()

        self.db.fail_impression_writes = 0
        await self.db.prepare_ad_impression_retries()
        restarted_worker = AdImpressionPersistenceWorker(self.db)

        self.assertEqual(await restarted_worker.run_once(), 1)
        self.assertEqual(len(self.db.impressions), 1)
        self.assertEqual(self.db.reservations, {})

    async def test_two_workers_do_not_duplicate_an_impression(self) -> None:
        await self.send(persist=False)
        worker_a = AdImpressionPersistenceWorker(self.db)
        worker_b = AdImpressionPersistenceWorker(self.db)

        await asyncio.gather(worker_a.run_once(), worker_b.run_once())

        self.assertEqual(len(self.db.impressions), 1)
        self.assertEqual(len(self.db.recorded_reservations), 1)

    async def test_worker_shutdown_leaves_no_background_task(self) -> None:
        worker = AdImpressionPersistenceWorker(self.db, poll_seconds=10)
        claim_started = asyncio.Event()

        async def blocked_claim(_limit: int) -> list[tuple[uuid.UUID, int]]:
            claim_started.set()
            await asyncio.Event().wait()
            return []

        self.db.claim_due_ad_impressions = blocked_claim
        await worker.start()
        await claim_started.wait()
        task = worker._task
        self.assertIsNotNone(task)

        self.assertTrue(await worker.stop())
        self.assertTrue(task.done())
        self.assertFalse(
            any(
                task.get_name() == "ad-impression-persistence-worker"
                for task in asyncio.all_tasks()
            )
        )

    async def test_retry_worker_concurrency_is_bounded(self) -> None:
        await self.manager.sync_campaigns_from_config()
        reservations = []
        for user_id in range(100, 108):
            reservation = await self.db.reserve_ad_impression(
                user_id,
                ["spring"],
                show_every=1,
                max_per_user_per_day=3,
                min_interval_hours=0,
            )
            self.assertIsNotNone(reservation)
            _, reservation_id = reservation
            reservations.append(reservation_id)
            await self.db.mark_ad_impression_delivery_started(reservation_id)
            await self.db.mark_ad_impression_delivered(
                reservation_id, datetime.now(timezone.utc)
            )
        worker = AdImpressionPersistenceWorker(self.db, concurrency=2)

        claimed = await worker.run_once()

        self.assertEqual(claimed, 2)
        self.assertLessEqual(self.db.max_active_impression_writes, 2)
        self.assertEqual(len(self.db.impressions), 2)

    async def test_repeated_recording_of_same_reservation_is_idempotent(self) -> None:
        await self.manager.sync_campaigns_from_config()
        reservation = await self.db.reserve_ad_impression(
            42,
            ["spring"],
            show_every=1,
            max_per_user_per_day=3,
            min_interval_hours=0,
        )
        self.assertIsNotNone(reservation)
        _, reservation_id = reservation
        await self.db.mark_ad_impression_delivery_started(reservation_id)
        delivered_at = datetime.now(timezone.utc)
        await self.db.mark_ad_impression_delivered(reservation_id, delivered_at)

        await self.db.record_ad_impression(reservation_id)
        await self.db.record_ad_impression(reservation_id)

        self.assertEqual(len(self.db.impressions), 1)
        self.assertEqual(self.db.recorded_reservations, {reservation_id})

    async def test_failed_ad_delivery_is_not_an_impression_and_main_retries(self) -> None:
        self.bot.send_message.side_effect = [RuntimeError("telegram failed"), None]

        await self.send()

        self.assertEqual(self.db.impressions, [])
        self.assertEqual(self.db.reservations, {})
        self.assertEqual(self.bot.send_message.await_count, 2)
        self.assertEqual(self.bot.send_message.await_args.args[1], "Main content")

    async def test_telegram_failure_retries_reservation_cancellation(self) -> None:
        self.db.fail_cancel_attempts = 2
        self.bot.send_message.side_effect = [RuntimeError("telegram failed"), None]

        with patch("utils.ads.asyncio.sleep", new_callable=AsyncMock) as sleep:
            await self.send()

        self.assertEqual(self.db.cancel_calls, 3)
        self.assertEqual([call.args[0] for call in sleep.await_args_list], [1, 2])
        self.assertEqual(self.db.reservations, {})
        self.assertEqual(self.db.impressions, [])

    async def test_exhausted_cancellation_retries_leave_no_impression(self) -> None:
        self.db.fail_cancel_attempts = 5
        self.bot.send_message.side_effect = [RuntimeError("telegram failed"), None]

        with patch("utils.ads.asyncio.sleep", new_callable=AsyncMock) as sleep:
            await self.send()

        reservation_id = next(iter(self.db.reservations))
        self.assertEqual(self.db.cancel_calls, 5)
        self.assertEqual(sleep.await_count, 4)
        self.assertEqual(
            self.db.impression_states[reservation_id],
            "delivery_outcome_unknown",
        )
        self.assertEqual(self.db.impressions, [])
        self.assertEqual(
            await AdImpressionPersistenceWorker(self.db).run_once(),
            0,
        )

    async def test_cancelled_telegram_send_leaves_unknown_uncounted(self) -> None:
        self.bot.send_message.side_effect = asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await self.send(persist=False)

        reservation_id = next(iter(self.db.reservations))
        self.assertEqual(
            self.db.impression_states[reservation_id],
            "delivery_outcome_unknown",
        )
        self.assertEqual(self.db.impressions, [])
        self.assertEqual(
            await AdImpressionPersistenceWorker(self.db).run_once(),
            0,
        )

    async def test_delivery_started_transition_is_idempotent(self) -> None:
        connection = FakeDeliveryStateConnection()
        database = Database(":unused:")
        database._pool = FakeDeliveryStatePool(connection)  # type: ignore[assignment]
        reservation_id = uuid.uuid4()

        await database.mark_ad_impression_delivery_started(reservation_id)
        await database.mark_ad_impression_delivery_started(reservation_id)

        self.assertEqual(len(connection.executions), 2)
        self.assertEqual(len(connection.fetches), 1)
        self.assertIn("status = 'reserved'", connection.executions[0])
        self.assertIn(
            "status = 'delivery_outcome_unknown'",
            connection.fetches[0],
        )

    async def test_database_failure_does_not_block_main_message(self) -> None:
        self.db.fail_reserve = True

        await self.send()

        self.assertEqual(self.bot.send_message.await_args.args[1], "Main content")
        self.assertEqual(self.db.impressions, [])

    async def test_unique_users_clicks_and_ctr_are_separate(self) -> None:
        connection = FakeStatsConnection()
        database = Database(":unused:")
        database._pool = FakeStatsPool(connection)  # type: ignore[assignment]

        stats = (await database.get_ad_campaign_statistics())[0]

        self.assertEqual(stats.impressions, 3)
        self.assertEqual(stats.unique_users, 2)
        self.assertEqual(stats.clicks, 2)
        self.assertEqual(stats.unique_clicks, 1)
        self.assertEqual(stats.ctr, 66.66666666666666)
        self.assertIn("COUNT(DISTINCT telegram_id)", connection.query)

    async def test_database_records_delivery_timestamp_as_shown_at(self) -> None:
        connection = FakeImpressionConnection()
        database = Database(":unused:")
        database._pool = FakeImpressionPool(connection)  # type: ignore[assignment]
        reservation_id = uuid.uuid4()

        await database.record_ad_impression(reservation_id)

        insert_sql, insert_args = connection.executions[0]
        self.assertIn(
            "SELECT reservation_id, campaign_id, telegram_id, delivered_at",
            insert_sql,
        )
        self.assertNotIn("NOW()", insert_sql)
        self.assertEqual(insert_args, (reservation_id,))

    async def test_ctr_is_zero_without_impressions(self) -> None:
        stats = AdStatistics("empty", "Empty", True, 0, 0, 4, 2, None, None)

        self.assertEqual(stats.ctr, 0)

    async def test_click_signature_records_each_click_for_signed_user(self) -> None:
        await self.manager._sync_campaigns(self.manager._load_campaigns(self.manager.load_config()["ads"]))
        signature = self.db.sign_ad_click("spring", 42)

        first = await self.db.record_ad_click("spring", 42, signature)
        second = await self.db.record_ad_click("spring", 42, signature)
        forged = await self.db.record_ad_click(
            "another-campaign", 42, signature
        )

        self.assertEqual(first, "https://advertiser.example/")
        self.assertEqual(second, first)
        self.assertIsNone(forged)
        self.assertEqual(len(self.db.clicks), 2)
        self.assertEqual(len({user_id for _, user_id in self.db.clicks}), 1)

    async def test_database_click_validation_uses_signed_campaign_and_user(self) -> None:
        connection = FakeClickConnection()
        database = Database(":unused:")
        database._pool = FakeClickPool(connection)  # type: ignore[assignment]
        database._ad_click_secret = b"unit-test-key"
        signature = database.sign_ad_click("spring", 42)

        destination = await database.record_ad_click("spring", 42, signature)
        forged = await database.record_ad_click("other", 42, signature)

        self.assertEqual(destination, "https://advertiser.example/")
        self.assertIsNone(forged)
        self.assertEqual(connection.clicks, [("spring", 42)])
        self.assertIn("FROM ad_campaigns", connection.queries[0])
        self.assertIn("INSERT INTO ad_clicks", connection.queries[1])

    async def test_click_endpoint_records_then_redirects_to_database_destination(self) -> None:
        campaigns = self.manager._load_campaigns(self.manager.load_config()["ads"])
        await self.manager._sync_campaigns(campaigns)
        with patch.dict("os.environ", {"PUBLIC_BASE_URL": "https://lightua.example"}):
            tracked_manager = AdManager(self.config_path, db=self.db)
        tracked_url = tracked_manager._click_url(campaigns[0][1], 42)
        self.assertTrue(tracked_url.startswith("https://lightua.example/ads/click/spring?"))
        parsed_url = urlsplit(tracked_url)
        app = web.Application()
        app[DATABASE_APP_KEY] = self.db
        app.router.add_get("/ads/click/{campaign_id}", handle_ad_click)

        async with TestClient(TestServer(app)) as client:
            response = await client.get(
                f"{parsed_url.path}?{parsed_url.query}",
                allow_redirects=False,
            )

        self.assertEqual(response.status, 302)
        self.assertEqual(response.headers["Location"], "https://advertiser.example/")
        self.assertEqual(self.db.clicks, [("spring", 42)])

    async def test_show_every_is_per_user_and_survives_manager_restart(self) -> None:
        self.write_config(show_every=2)
        self.manager = AdManager(self.config_path, db=self.db)
        await self.send()
        restarted = AdManager(self.config_path, db=self.db)
        await restarted.send_with_ad(
            self.bot, 42, "Main content", telegram_id=42
        )
        await AdImpressionPersistenceWorker(self.db).run_once()

        self.assertEqual(len(self.db.impressions), 1)
        self.assertEqual(self.db.call_counts[42], 2)

    async def test_show_every_counter_is_independent_for_each_user(self) -> None:
        self.write_config(show_every=2)
        self.manager = AdManager(self.config_path, db=self.db)
        await self.send(42)
        await self.send(43)
        await self.send(42)

        self.assertEqual(self.db.call_counts, {42: 2, 43: 1})
        self.assertEqual([row[1] for row in self.db.impressions], [42])

    async def test_daily_limit_prevents_additional_impressions(self) -> None:
        self.write_config(max_per_user_per_day=1)
        self.manager = AdManager(self.config_path, db=self.db)
        await self.send()
        await self.send()

        self.assertEqual(len(self.db.impressions), 1)

    async def test_minimum_interval_prevents_additional_impressions(self) -> None:
        self.write_config(min_interval_hours=1)
        self.manager = AdManager(self.config_path, db=self.db)
        await self.send()
        await self.send()

        self.assertEqual(len(self.db.impressions), 1)

    async def test_parallel_requests_cannot_exceed_user_daily_limit(self) -> None:
        self.write_config(max_per_user_per_day=1)
        self.manager = AdManager(self.config_path, db=self.db)
        await asyncio.gather(self.send(), self.send())

        self.assertEqual(len(self.db.impressions), 1)
        self.assertEqual(self.db.reservations, {})

    async def test_disabled_campaign_is_not_sent(self) -> None:
        self.write_config(enabled=False)
        self.manager = AdManager(self.config_path, db=self.db)
        await self.send()

        self.assertEqual(self.db.impressions, [])
        self.assertEqual(self.bot.send_message.await_args.args[1], "Main content")

    async def test_no_active_campaigns_sends_main_message(self) -> None:
        self.write_config(ads_enabled=False)
        self.manager = AdManager(self.config_path, db=self.db)
        await self.send()

        self.assertEqual(self.db.impressions, [])
        self.assertEqual(self.bot.send_message.await_args.args[1], "Main content")

    async def test_random_pick_avoids_repeating_previous_file(self) -> None:
        first = AdContent("text", "open", "https://a.example", "first", "First", True)
        second = AdContent("text", "open", "https://b.example", "second", "Second", True)
        self.manager._last_ad_file = "one.json"

        with patch("utils.ads.random.choice", side_effect=lambda values: values[0]) as choose:
            picked = self.manager._pick_ad([("one.json", first), ("two.json", second)])

        self.assertEqual(picked, ("two.json", second))
        self.assertEqual(choose.call_args.args[0], [("two.json", second)])

    async def test_tracking_link_is_not_built_without_public_base_url(self) -> None:
        campaign = AdContent(
            "Ad", "Visit", "https://advertiser.example/", "spring", "Spring", True
        )

        with patch.dict("os.environ", {"PUBLIC_BASE_URL": ""}):
            manager = AdManager(self.config_path, db=self.db)

        self.assertFalse(manager.click_tracking_enabled)
        self.assertEqual(manager._click_url(campaign, 42), campaign.url)

    async def test_http_public_base_url_disables_tracking(self) -> None:
        campaign = AdContent(
            "Ad", "Visit", "https://advertiser.example/", "spring", "Spring", True
        )

        with patch.dict("os.environ", {"PUBLIC_BASE_URL": "http://lightua.example"}):
            with self.assertLogs("utils.ads", level="ERROR"):
                manager = AdManager(self.config_path, db=self.db)

        self.assertFalse(manager.click_tracking_enabled)
        self.assertEqual(manager._click_url(campaign, 42), campaign.url)

    async def test_admin_authorization_checks_telegram_user_id(self) -> None:
        authorized_user_id = 8608243726

        self.assertTrue(
            _authorized(SimpleNamespace(from_user=SimpleNamespace(id=authorized_user_id)))
        )
        self.assertFalse(
            _authorized(SimpleNamespace(from_user=SimpleNamespace(id=1)))
        )

    async def test_non_admin_ad_statistics_handler_does_not_query_database(self) -> None:
        database = SimpleNamespace(
            get_ad_overall_statistics=AsyncMock(),
            get_ad_campaign_statistics=AsyncMock(),
        )
        router = get_router(database)  # type: ignore[arg-type]
        handler = next(
            item.callback
            for item in router.message.handlers
            if item.callback.__name__ == "advertising_stats"
        )
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=1),
            text="/ads",
            answer=AsyncMock(),
        )

        await handler(message)

        database.get_ad_overall_statistics.assert_not_awaited()
        database.get_ad_campaign_statistics.assert_not_awaited()
        message.answer.assert_not_awaited()

    def test_stats_query_has_unique_counts_and_time_indexes(self) -> None:
        self.assertGreaterEqual(
            AD_CAMPAIGN_STATISTICS_QUERY.count("COUNT(DISTINCT telegram_id)"),
            2,
        )

    def test_advertising_csv_reports_ctr_and_click_tracking_state(self) -> None:
        row = _serialize_row(
            {
                "campaign_id": "spring",
                "name": "Spring",
                "enabled": True,
                "impressions": 3,
                "unique_users": 2,
                "clicks": 2,
                "unique_clicks": 1,
                "first_impression": None,
                "last_impression": None,
            },
            click_tracking="disabled",
        )
        destination = self.root / "advertising.csv"
        write_advertising_csv([row], destination)

        with destination.open(encoding="utf-8", newline="") as source:
            reader = csv.DictReader(source)
            saved = next(reader)

        self.assertEqual(tuple(reader.fieldnames or ()), CSV_FIELDS)
        self.assertEqual(saved["ctr_percent"], "66.67")
        self.assertEqual(saved["click_tracking"], "disabled")
