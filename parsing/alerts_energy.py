from __future__ import annotations

import re
from datetime import datetime
from urllib.parse import urlparse

from parsing.base import SourceAdapter
from parsing.models import FetchStatus, SourceScheduleObservation, SourceTarget
from parsing.normalization import complete_from_outages, parse_time_ranges
from parsing.source_utils import (
    default_reference_day,
    make_observation,
    parse_page_date,
    parse_updated_marker,
    soup_and_text,
)


_QUEUE_RE = re.compile(r"(?<!\d)(\d+)\.(\d+)(?!\d)")
_SOURCE_UPDATE_RE = r"Оновлення джерела.{0,100}"
_NO_OUTAGES_MARKERS = (
    "без планових відключень",
    "відключень не заплановано",
    "відключення не плануються",
)
_ZERO_OUTAGES_RE = re.compile(r"усього за день\s*:\s*0(?:\D|$)", re.IGNORECASE)


class AlertsEnergyParser(SourceAdapter):
    source_id = "alerts_energy"

    def parse(
        self,
        html: str,
        target: SourceTarget,
        observed_at: datetime,
    ) -> SourceScheduleObservation:
        soup, text = soup_and_text(html)
        title = soup.find("h1") or soup.find("title")
        title_text = title.get_text(" ", strip=True) if title else ""
        reference_day = default_reference_day(observed_at)
        schedule_date = parse_page_date(text, reference_year=reference_day.year)

        parts = [part for part in urlparse(target.source_url).path.split("/") if part]
        operator_id: str | None = None
        page_queue: tuple[int, int] | None = None
        try:
            schedule_index = parts.index("schedule")
            operator_id = parts[schedule_index + 1]
            queue_match = _QUEUE_RE.fullmatch(parts[schedule_index + 2])
            if queue_match:
                page_queue = (int(queue_match.group(1)), int(queue_match.group(2)))
        except (ValueError, IndexError):
            pass

        if not parts or parts[0] != target.region_slug:
            return make_observation(
                source_id=self.source_id,
                target=target,
                observed_at=observed_at,
                operator_id=operator_id,
                schedule_date=None,
                source_updated_at=None,
                fetch_status=FetchStatus.INCOMPLETE,
                error_code="target_region_not_in_source_url",
            )
        if page_queue != (target.queue, target.subqueue):
            return make_observation(
                source_id=self.source_id,
                target=target,
                observed_at=observed_at,
                operator_id=operator_id,
                schedule_date=schedule_date,
                source_updated_at=None,
                fetch_status=FetchStatus.INCOMPLETE,
                error_code="target_queue_not_in_source_url",
            )

        page_matches_queue = bool(_QUEUE_RE.search(title_text)) and (
            tuple(map(int, _QUEUE_RE.search(title_text).groups()))
            == (target.queue, target.subqueue)
        )
        interval_start = text.lower().find("інтервали відключень")
        interval_end = text.lower().find("усього за день", interval_start + 1)
        interval_text = (
            text[interval_start:interval_end if interval_end >= 0 else None]
            if interval_start >= 0
            else ""
        )
        try:
            outages = parse_time_ranges(interval_text, "off")
        except ValueError:
            outages = ()
            malformed_intervals = True
        else:
            malformed_intervals = False
        explicitly_empty = any(
            marker in interval_text.lower() for marker in _NO_OUTAGES_MARKERS
        ) or bool(
            interval_start >= 0
            and _ZERO_OUTAGES_RE.search(text[interval_start:])
        )
        if (
            schedule_date is None
            or not page_matches_queue
            or interval_start < 0
            or malformed_intervals
            or (
                not outages and not explicitly_empty
            )
        ):
            return make_observation(
                source_id=self.source_id,
                target=target,
                observed_at=observed_at,
                operator_id=operator_id,
                schedule_date=schedule_date,
                source_updated_at=parse_updated_marker(
                    text,
                    reference_day=reference_day,
                    marker=_SOURCE_UPDATE_RE,
                ),
                fetch_status=FetchStatus.INCOMPLETE,
                error_code=(
                    "invalid_schedule_intervals"
                    if malformed_intervals
                    else "schedule_metadata_or_intervals_missing"
                ),
            )

        intervals = complete_from_outages(outages)
        updated_at = parse_updated_marker(
            text,
            reference_day=schedule_date,
            marker=_SOURCE_UPDATE_RE,
        )
        return make_observation(
            source_id=self.source_id,
            target=target,
            observed_at=observed_at,
            operator_id=operator_id,
            schedule_date=schedule_date,
            source_updated_at=updated_at,
            fetch_status=FetchStatus.SUCCESS,
            intervals=intervals,
        )
