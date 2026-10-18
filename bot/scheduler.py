from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from contextlib import suppress
from datetime import date, datetime, timedelta

from aiogram import Bot

from db.database import Database
from db.models import User
from parsing.alerts_parser import AlertsParser, DaySchedule, ParserError, _canonical_day_intervals
from utils.formatting import format_light_change_message
from utils.time_utils import KYIV_TZ, is_in_quiet_hours, now_kyiv, parse_hhmm

logger = logging.getLogger(__name__)
_RANGE_RE = re.compile(r"^([🟢🔴])\s+(\d{2}:\d{2})\s*[-–]\s*(\d{2}:\d{2})")
END_OF_DAY_RETRY_SECONDS = 300
MAX_SOURCE_SCHEDULE_AGE = timedelta(hours=36)


class LightStateScheduler:
    def __init__(
        self,
        bot: Bot,
        db: Database,
        parser: AlertsParser,
        interval_seconds: int = 60,
        max_parallel_checks: int = 5,
    ) -> None:
        self._bot = bot
        self._db = db
        self._parser = parser
        self._interval_seconds = interval_seconds
        self._max_parallel_checks = max_parallel_checks
        self._task: asyncio.Task | None = None
        self._last_warning_event_key_by_user: dict[int, str] = {}

    @staticmethod
    def _time_on_day(base: datetime, hhmm: str) -> datetime:
        hour_str, minute_str = hhmm.split(":")
        return base.replace(hour=int(hour_str), minute=int(minute_str), second=0, microsecond=0)

    def _next_change_from_graph(
        self,
        *,
        full_graph_text: str,
        now: datetime,
    ) -> tuple[datetime, str, str] | None:
        ranges: list[tuple[str, str, str]] = []
        for raw_line in full_graph_text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            match = _RANGE_RE.match(line)
            if not match:
                continue
            emoji, start, end = match.groups()
            ranges.append((emoji, start, end))

        candidates: list[tuple[datetime, str, str]] = []
        for emoji, start, end in ranges:
            start_dt = self._time_on_day(now, start)
            if start_dt <= now:
                continue
            event_type = "ВІДКЛЮЧЕННЯ" if emoji == "🔴" else "СВІТЛО Є"
            candidates.append((start_dt, event_type, f"{start} – {end}"))

        if not candidates:
            return None
        return min(candidates, key=lambda item: item[0])

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._run(), name="light-state-scheduler")
        logger.info("Scheduler started with interval=%ss", self._interval_seconds)

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        logger.info("Scheduler stopped")

    async def _run(self) -> None:
        while True:
            started = asyncio.get_running_loop().time()
            try:
                await self._check_once()
            except Exception:
                logger.exception("Unexpected scheduler failure")

            elapsed = asyncio.get_running_loop().time() - started
            sleep_for = max(0.0, self._interval_seconds - elapsed)
            await asyncio.sleep(sleep_for)

    async def _check_once(self) -> None:
        await self._drain_schedule_change_outbox()
        users = await self._db.get_users_with_notifications()
        if not users:
            return

        profiles: dict[tuple[str, int, int], list[User]] = {}
        for user in users:
            key = (user.region_slug, user.queue, user.subqueue)
            profiles.setdefault(key, []).append(user)
        semaphore = asyncio.Semaphore(self._max_parallel_checks)
        tasks = [
            self._check_profile(key, profile_users, semaphore)
            for key, profile_users in profiles.items()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                logger.error("Schedule profile check failed", exc_info=(type(result), result, result.__traceback__))
        await self._drain_schedule_change_outbox()

    async def _check_profile(
        self,
        profile: tuple[str, int, int],
        users: list[User],
        semaphore: asyncio.Semaphore,
    ) -> None:
        async with semaphore:
            region_slug, queue, subqueue = profile
            now = now_kyiv()
            try:
                schedule = await asyncio.to_thread(
                    self._parser.get_schedule, region_slug, queue, subqueue
                )
                schedule.region_name = users[0].region_name
                # Use the time of the successful response, not the time before the
                # network request, for state and date-bound decisions.
                now = now_kyiv()
                state = schedule.state_at(now)
            except ParserError:
                logger.warning(
                    "Parser error for profile=(%s, %s.%s)", region_slug, queue, subqueue
                )
                return
            except Exception:
                logger.exception("Unexpected check failure for profile=(%s, %s.%s)", region_slug, queue, subqueue)
                return

            schedule_json = self._schedule_fingerprint(schedule)
            snapshot_json = self._schedule_snapshot(schedule, schedule_json)
            source_date = schedule.schedule_date
            observed_date = now.astimezone(KYIV_TZ).date()
            logical_date = source_date or observed_date
            previous_schedule, confirmed, candidate_pending = await self._db.observe_schedule(
                region_slug, queue, subqueue, snapshot_json=snapshot_json,
                fingerprint=schedule_json, logical_date=logical_date,
                source_schedule_date=source_date, observed_date=observed_date,
                recipients=[u.telegram_id for u in users if not is_in_quiet_hours(
                    now, u.quiet_start, u.quiet_end)],
            )
            if previous_schedule is None:
                logger.info("Schedule baseline/candidate stored for %s %s.%s", region_slug, queue, subqueue)
            elif confirmed:
                logger.info("Schedule change confirmed for %s %s.%s", region_slug, queue, subqueue)

            if candidate_pending:
                logger.info(
                    "Skipping user notifications for unconfirmed schedule candidate %s %s.%s",
                    region_slug, queue, subqueue,
                )
                return

            await asyncio.gather(
                *(self._check_user(user=user, schedule=schedule, state=state, now=now) for user in users),
                return_exceptions=True,
            )

    @staticmethod
    def _schedule_fingerprint(schedule: DaySchedule, *, include_midnight_marker: bool = False) -> str:
        # Fingerprint the complete source graph, independently of wall clock or
        # current state. Normalize ordering and duplicate source rows because
        # neither changes the schedule's meaning.
        runs = _canonical_day_intervals(schedule.intervals, default_state=schedule.default_state)
        end_of_day = "24:00" if include_midnight_marker else "00:00"
        data = [(f"{start // 60:02d}:{start % 60:02d}",
                 end_of_day if end == 1440 else f"{end // 60:02d}:{end % 60:02d}", state)
                for start, end, state in runs]
        return json.dumps(data, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _schedule_snapshot(schedule: DaySchedule, fingerprint: str) -> str:
        """Persist the full canonical graph together with its source date."""
        return json.dumps(
            {"schedule_date": schedule.schedule_date.isoformat() if schedule.schedule_date else None,
             "intervals": json.loads(fingerprint)},
            ensure_ascii=False,
            separators=(",", ":"),
        )

    @staticmethod
    def _snapshot_parts(snapshot_json: str) -> tuple[date | None, str]:
        """Read current snapshots and legacy interval-list snapshots."""
        try:
            value = json.loads(snapshot_json)
        except (TypeError, json.JSONDecodeError):
            return None, snapshot_json

        if isinstance(value, dict) and "intervals" in value:
            raw_date = value.get("schedule_date")
            try:
                schedule_date = date.fromisoformat(raw_date) if raw_date else None
            except (TypeError, ValueError):
                schedule_date = None
            intervals = value["intervals"]
        else:
            # Snapshots created before schedule dates were included were lists.
            schedule_date = None
            intervals = value
        return schedule_date, json.dumps(intervals, ensure_ascii=False, separators=(",", ":"))

    async def _notify_schedule_change(
        self, users: list[User], schedule: DaySchedule, previous_json: str, now: datetime
    ) -> None:
        eligible = [
            user for user in users
            if not is_in_quiet_hours(now, user.quiet_start, user.quiet_end)
        ]
        if not eligible:
            logger.info(
                "Schedule changed for %s %s.%s; 0 users outside quiet hours",
                schedule.region_slug, schedule.queue, schedule.subqueue,
            )
            return
        try:
            previous = set(tuple(item) for item in json.loads(previous_json))
            current = set(tuple(item) for item in json.loads(self._schedule_fingerprint(schedule)))
            added, removed = sorted(current - previous), sorted(previous - current)
            if added or removed:
                lines = ["🔄 Графік змінився!", "", f"⚡ Черга {schedule.queue}.{schedule.subqueue}"]
                label = lambda item: f"{item[0]}–{item[1]} ({'без світла' if item[2] == 'off' else 'світло є'})"
                if added:
                    lines.extend(["", "➕ Додано:"] + [label(item) for item in added])
                if removed:
                    lines.extend(["", "➖ Скасовано:"] + [label(item) for item in removed])
                lines.extend(["", "Перевірте актуальний графік у боті."])
                text = "\n".join(lines)
            else:
                raise ValueError("No interval delta")
        except (ValueError, TypeError, json.JSONDecodeError):
            from utils.formatting import format_graph_message

            text = (
                f"🔄 Графік для вашої черги {schedule.queue}.{schedule.subqueue} було оновлено.\n\n"
                "📅 Новий графік:\n" + format_graph_message(schedule, schedule.state_at(now), now=now)
            )
        errors = 0
        for user in eligible:
            try:
                await self._bot.send_message(chat_id=user.telegram_id, text=text)
            except Exception:
                errors += 1
                logger.exception("Failed to send schedule update to user=%s", user.telegram_id)
        logger.info(
            "Schedule update delivered for %s %s.%s: target=%s sent=%s errors=%s",
            schedule.region_slug, schedule.queue, schedule.subqueue,
            len(eligible), len(eligible) - errors, errors,
        )

    async def _drain_schedule_change_outbox(self) -> None:
        claim = getattr(self._db, "claim_schedule_change_delivery", None)
        if claim is None:
            return
        # Bounded work per scheduler tick; expired leases recover interrupted sends.
        for _ in range(100):
            delivery = await claim()
            if delivery is None:
                break
            try:
                previous = json.loads(delivery.previous_json)
                current = json.loads(delivery.current_json)
                prev_intervals = previous.get("intervals", previous) if isinstance(previous, dict) else previous
                curr_intervals = current.get("intervals", current) if isinstance(current, dict) else current
                before, after = set(map(tuple, prev_intervals)), set(map(tuple, curr_intervals))
                added, removed = sorted(after - before), sorted(before - after)
                lines = ["🔄 Графік змінився!", "", f"⚡ Черга {delivery.queue}.{delivery.subqueue}"]
                label = lambda item: f"{item[0]}–{item[1]} ({'без світла' if item[2] == 'off' else 'світло є'})"
                if added:
                    lines.extend(["", "➕ Додано:"] + [label(item) for item in added])
                if removed:
                    lines.extend(["", "➖ Скасовано:"] + [label(item) for item in removed])
                lines.extend(["", "Перевірте актуальний графік у боті."])
                await self._bot.send_message(chat_id=delivery.recipient_id, text="\n".join(lines))
            except Exception as exc:
                delay = min(3600, 30 * (2 ** min(delivery.attempt_count - 1, 7)))
                try:
                    await self._db.finish_schedule_change_delivery(
                        delivery.event_id, error=f"{type(exc).__name__}: {exc}", retry_seconds=delay
                    )
                except Exception:
                    logger.exception("Could not persist schedule outbox retry id=%s", delivery.event_id)
                logger.exception("Schedule outbox delivery failed id=%s", delivery.event_id)
            else:
                await self._db.finish_schedule_change_delivery(delivery.event_id)

    async def _check_user(self, user: User, schedule: DaySchedule, state: str, now: datetime) -> None:
        await self._send_morning_schedule_if_due(user, schedule, state, now)
        # Send pre-warning 5 minutes before the next scheduled state change.
        try:
            from utils.formatting import format_graph_message

            full_graph_text = format_graph_message(schedule=schedule, current_state=state, now=now)
            next_change = self._next_change_from_graph(full_graph_text=full_graph_text, now=now)
            if next_change is not None and not is_in_quiet_hours(now, user.quiet_start, user.quiet_end):
                start_dt, event_type, interval_text = next_change
                warning_dt = start_dt - timedelta(minutes=5)
                warning_lag_seconds = (now - warning_dt).total_seconds()
                # Send only near the planned warning timestamp, not any time later
                # in the whole 5-minute window (e.g. right after bot restart).
                if 0 <= warning_lag_seconds < self._interval_seconds:
                    event_key = f"{start_dt.isoformat()}|{event_type}|{interval_text}"
                    last_event_key = self._last_warning_event_key_by_user.get(user.telegram_id)
                    if last_event_key != event_key:
                        if event_type == "ВІДКЛЮЧЕННЯ":
                            warning_text = (
                                "⚠️ За 5 хвилин вимкнуть світло\n\n"
                                f"📍 {user.region_name} · Черга {user.queue}.{user.subqueue}\n\n"
                                f"🕒 Початок: {interval_text.split(' – ', 1)[0]}\n"
                                f"🕓 Орієнтовне завершення: {interval_text.split(' – ', 1)[1]}\n\n"
                                "За графіком alerts.org.ua."
                            )
                        else:
                            warning_text = (
                                "💡 За 5 хвилин має з'явитися світло\n\n"
                                f"📍 {user.region_name} · Черга {user.queue}.{user.subqueue}\n\n"
                                f"🕒 Очікуваний час: {interval_text.split(' – ', 1)[0]}\n"
                                f"🕓 Інтервал відключення: {interval_text}\n\n"
                                "За графіком alerts.org.ua."
                            )
                        await self._bot.send_message(chat_id=user.telegram_id, text=warning_text)
                        self._last_warning_event_key_by_user[user.telegram_id] = event_key
        except Exception:
            logger.exception("Failed to process pre-warning for user=%s", user.telegram_id)

        previous = await self._db.get_last_state(user.telegram_id)
        await self._db.upsert_last_state(
            telegram_id=user.telegram_id,
            state=state,
            checked_at_iso=now.isoformat(),
        )

        if previous is None:
            return
        if previous.last_light_state != state and not is_in_quiet_hours(
            now, user.quiet_start, user.quiet_end
        ):
            text = format_light_change_message(
                state=state,
                region_name=user.region_name,
                queue=user.queue,
                subqueue=user.subqueue,
            )
            try:
                await self._bot.send_message(chat_id=user.telegram_id, text=text)
            except Exception:
                logger.exception("Failed to send notification to user=%s", user.telegram_id)

        # Retry a previously confirmed Telegram failure on a later cycle. The
        # event store suppresses sent, in-flight, and ambiguous outcomes.
        if state == "on" and not is_in_quiet_hours(now, user.quiet_start, user.quiet_end):
            await self._notify_no_more_outages(user, schedule, previous, now)

    @staticmethod
    def _quiet_end_after(moment: datetime, user: User) -> datetime | None:
        quiet_start = parse_hhmm(user.quiet_start or "")
        quiet_end = parse_hhmm(user.quiet_end or "")
        if quiet_start is None or quiet_end is None or quiet_start == quiet_end:
            return None
        local = moment.astimezone(KYIV_TZ)
        if not is_in_quiet_hours(local, user.quiet_start, user.quiet_end):
            return None
        end_day = local.date()
        if quiet_start > quiet_end and local.time() >= quiet_start:
            end_day += timedelta(days=1)
        return datetime.combine(end_day, quiet_end).replace(tzinfo=KYIV_TZ)

    def _morning_schedule_send_at(self, user: User, schedule_day: date, now: datetime) -> datetime | None:
        target = datetime.combine(schedule_day, datetime.min.time()).replace(
            hour=7, tzinfo=KYIV_TZ
        )
        quiet_start = parse_hhmm(user.quiet_start or "")
        quiet_end = parse_hhmm(user.quiet_end or "")
        if quiet_start is not None and quiet_end is not None:
            if quiet_start == quiet_end:
                return None  # Existing quiet-hours semantics: quiet all day.
            target_end = self._quiet_end_after(target, user)
            if target_end is not None:
                target = target_end

        local_now = now.astimezone(KYIV_TZ)
        if local_now >= target and is_in_quiet_hours(local_now, user.quiet_start, user.quiet_end):
            current_quiet_end = self._quiet_end_after(local_now, user)
            if current_quiet_end is None:
                return None
            target = max(target, current_quiet_end)
        return target

    async def _send_morning_schedule_if_due(
        self, user: User, schedule: DaySchedule, state: str, now: datetime
    ) -> None:
        if not user.notifications_enabled:
            return
        today = now.astimezone(KYIV_TZ).date()
        if schedule.schedule_date != today:
            logger.warning(
                "Morning schedule skipped: date mismatch for user=%s schedule_date=%s today=%s",
                user.telegram_id, schedule.schedule_date, today,
            )
            return
        send_at = self._morning_schedule_send_at(user, today, now)
        if send_at is None or now.astimezone(KYIV_TZ) < send_at:
            return
        if is_in_quiet_hours(now, user.quiet_start, user.quiet_end):
            return

        fetched_at = schedule.fetched_at
        if fetched_at is None or fetched_at.tzinfo is None:
            logger.warning("Morning schedule skipped: missing fetch timestamp for user=%s", user.telegram_id)
            return
        fetched_age = now.astimezone(KYIV_TZ) - fetched_at.astimezone(KYIV_TZ)
        if fetched_age < timedelta(0) or fetched_age > max(timedelta(minutes=2), timedelta(seconds=2 * self._interval_seconds)):
            logger.warning("Morning schedule skipped: stale fetch for user=%s age=%s", user.telegram_id, fetched_age)
            return
        if schedule.source_updated_at is not None:
            if schedule.source_updated_at.tzinfo is None:
                logger.warning("Morning schedule skipped: naive source update time for user=%s", user.telegram_id)
                return
            source_age = now.astimezone(KYIV_TZ) - schedule.source_updated_at.astimezone(KYIV_TZ)
            if source_age < timedelta(0) or source_age > MAX_SOURCE_SCHEDULE_AGE:
                logger.warning("Morning schedule skipped: stale source schedule for user=%s age=%s", user.telegram_id, source_age)
                return
        if not schedule.intervals and not schedule.confirmed_empty:
            logger.warning("Morning schedule skipped: empty unconfirmed graph for user=%s", user.telegram_id)
            return

        from utils.formatting import format_graph_message

        if schedule.confirmed_empty:
            graph_text = "📊 На сьогодні відключень не заплановано."
        else:
            graph_text = format_graph_message(schedule=schedule, current_state=state, now=now)
        message = (
            "☀️ Графік відключень на сьогодні\n\n"
            f"📍 Область: {user.region_name}\n"
            f"🔌 Черга: {user.queue}, підчерга {user.subqueue}\n\n"
            f"{graph_text}\n\n💡 Гарного дня!"
        )
        fingerprint = self._schedule_fingerprint(schedule, include_midnight_marker=True)
        event_key = hashlib.sha256(
            f"morning_schedule|{user.telegram_id}|{today.isoformat()}".encode("utf-8")
        ).hexdigest()
        try:
            claimed = await self._db.claim_no_more_outages_notification(
                event_key=event_key,
                telegram_id=user.telegram_id,
                region_slug=schedule.region_slug,
                queue=schedule.queue,
                subqueue=schedule.subqueue,
                schedule_date=today,
                schedule_fingerprint=fingerprint,
                event_type="morning_schedule",
                retry_after_seconds=END_OF_DAY_RETRY_SECONDS,
            )
        except Exception:
            logger.exception("Could not reserve morning schedule for user=%s", user.telegram_id)
            return
        if not claimed:
            return
        try:
            await self._bot.send_message(chat_id=user.telegram_id, text=message)
        except Exception as exc:
            status = _notification_failure_status(exc)
            try:
                await self._db.finish_no_more_outages_notification(
                    event_key, status, f"{type(exc).__name__}: {exc}"
                )
            except Exception:
                logger.exception("Could not record morning schedule result for user=%s", user.telegram_id)
            logger.exception("Morning schedule send failed for user=%s outcome=%s", user.telegram_id, status)
            return
        await self._db.finish_no_more_outages_notification(event_key, "sent")
        logger.info("Morning schedule sent to user=%s date=%s", user.telegram_id, today)

    def _has_confirmed_no_more_outages(self, schedule: DaySchedule, now: datetime) -> bool:
        """Require today's graph and an explicit source update timestamp."""
        if schedule.schedule_date != now.astimezone(KYIV_TZ).date():
            return False
        source_updated_at = schedule.source_updated_at
        if source_updated_at is None or source_updated_at.tzinfo is None:
            return False
        source_updated_at = source_updated_at.astimezone(KYIV_TZ)
        update_age = now.astimezone(KYIV_TZ) - source_updated_at
        if update_age < timedelta(0) or update_age > MAX_SOURCE_SCHEDULE_AGE:
            return False

        current = now.astimezone(KYIV_TZ)
        has_completed_outage = False
        for interval in schedule.intervals:
            if interval.state != "off":
                continue
            # A time-only interval crossing midnight cannot be assigned safely to
            # this calendar date, so fail closed for this summary.
            if interval.start >= interval.end and not interval.ends_at_midnight:
                return False
            start = current.replace(
                hour=interval.start.hour,
                minute=interval.start.minute,
                second=0,
                microsecond=0,
            )
            end = current.replace(hour=interval.end.hour, minute=interval.end.minute,
                                  second=0, microsecond=0)
            if interval.ends_at_midnight:
                end = end + timedelta(days=1)
            if start <= current < end:
                return False
            if start > current:
                return False
            if end <= current:
                has_completed_outage = True
        return has_completed_outage

    async def _notify_no_more_outages(
        self, user: User, schedule: DaySchedule, previous, now: datetime
    ) -> None:
        previous_checked = previous.last_checked_at
        if isinstance(previous_checked, str):
            try:
                previous_checked = datetime.fromisoformat(previous_checked)
            except ValueError:
                return
        if not isinstance(previous_checked, datetime):
            return
        if previous_checked.date() != now.astimezone(KYIV_TZ).date():
            return
        if not self._has_confirmed_no_more_outages(schedule, now):
            return

        fingerprint = self._schedule_fingerprint(schedule, include_midnight_marker=True)
        event_payload = (
            f"{user.telegram_id}|{now.astimezone(KYIV_TZ).date().isoformat()}|{schedule.region_slug}|"
            f"{schedule.queue}.{schedule.subqueue}|{fingerprint}"
        )
        event_key = hashlib.sha256(event_payload.encode("utf-8")).hexdigest()
        try:
            claimed = await self._db.claim_no_more_outages_notification(
                event_key=event_key,
                telegram_id=user.telegram_id,
                region_slug=schedule.region_slug,
                queue=schedule.queue,
                subqueue=schedule.subqueue,
                schedule_date=now.astimezone(KYIV_TZ).date(),
                schedule_fingerprint=fingerprint,
                retry_after_seconds=END_OF_DAY_RETRY_SECONDS,
            )
        except Exception:
            logger.exception(
                "Failed to claim end-of-day notice for user=%s", user.telegram_id
            )
            return
        if not claimed:
            return
        try:
            await self._bot.send_message(
                chat_id=user.telegram_id,
                text="✅ На сьогодні більше немає запланованих відключень світла! Гарного вечора 💡",
            )
        except Exception as exc:
            # A timeout/network failure has an ambiguous outcome; do not retry it
            # automatically. A Telegram API rejection is confirmed and retryable.
            status = _notification_failure_status(exc)
            try:
                await self._db.finish_no_more_outages_notification(
                    event_key, status, f"{type(exc).__name__}: {exc}"
                )
            except Exception:
                logger.exception("Failed to record end-of-day outcome for event=%s", event_key)
            logger.exception(
                "End-of-day Telegram send failed for user=%s with outcome=%s",
                user.telegram_id, status,
            )
            return
        await self._db.finish_no_more_outages_notification(event_key, "sent")


def _notification_failure_status(exc: Exception) -> str:
    name = type(exc).__name__
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError)) or name in {
        "TelegramNetworkError", "TelegramServerError", "ClientTimeout", "ConnectionError",
    }:
        return "unknown"
    if name in {
        "TelegramBadRequest", "TelegramForbiddenError", "TelegramNotFound",
        "TelegramUnauthorizedError", "TelegramRetryAfter", "TelegramConflictError",
    }:
        return "failed"
    return "unknown"
