from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from parsing.source_manager import SourcePollReport, SourceScheduleManager


logger = logging.getLogger(__name__)


class ShadowScheduleScheduler:
    def __init__(
        self,
        manager: SourceScheduleManager,
        *,
        tick_interval_seconds: int = 30,
    ) -> None:
        if tick_interval_seconds < 1:
            raise ValueError("tick_interval_seconds must be positive")
        self._manager = manager
        self._tick_interval_seconds = tick_interval_seconds
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(
            self._run(),
            name="shadow-schedule-scheduler",
        )
        logger.info("Shadow schedule scheduler started")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None
        logger.info("Shadow schedule scheduler stopped")

    async def run_once(self) -> SourcePollReport:
        report = await self._manager.run_due_sources()
        for observation in report.observations:
            logger.info(
                "Shadow fetch source=%s status=%s region=%s operator=%s queue=%s.%s date=%s observed_at=%s source_updated_at=%s error=%s",
                observation.source_id,
                observation.fetch_status.value,
                observation.region_slug,
                observation.operator_id or "unknown",
                observation.queue,
                observation.subqueue,
                observation.schedule_date,
                observation.observed_at.isoformat(),
                (
                    observation.source_updated_at.isoformat()
                    if observation.source_updated_at
                    else "unknown"
                ),
                observation.error_code or "none",
            )
        for (left, right), result in report.pairwise_comparisons.items():
            logger.info(
                "Shadow comparison %s vs %s: %s (%s) differing_intervals=%s",
                left,
                right,
                result.comparison_status.value,
                result.reason,
                result.differing_intervals,
            )
        for source_id, result in report.database_comparisons.items():
            logger.info(
                "Shadow database comparison source=%s: %s (%s)",
                source_id,
                result.comparison_status.value,
                result.reason,
            )
        return report

    async def _run(self) -> None:
        while True:
            try:
                await self.run_once()
            except Exception:
                logger.exception("Unexpected shadow scheduler failure")
            await asyncio.sleep(self._tick_interval_seconds)
