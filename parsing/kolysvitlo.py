from __future__ import annotations

import re
from datetime import datetime
from urllib.parse import urlparse

from parsing.base import SourceAdapter
from parsing.models import FetchStatus, SourceScheduleObservation, SourceTarget
from parsing.normalization import complete_from_outages, interval_from_times, normalize_intervals
from parsing.source_utils import (
    default_reference_day,
    make_observation,
    parse_page_date,
    parse_updated_at,
    soup_and_text,
)


_TIME_RANGE_RE = re.compile(
    r"(?P<start>\d{1,2}:[0-5]\d)\s*[-–—]\s*(?P<end>\d{1,2}:[0-5]\d)"
)
_FRESH_RE = re.compile(
    r"Дані від\s+(?P<updated>.*?)(?:\s*·|\s*$)",
    re.IGNORECASE,
)
_REGION_BY_OPERATOR_SLUG = {
    "cherkasy_oblenergo": "cherkaska-oblast",
}


class KolySvitloParser(SourceAdapter):
    source_id = "kolysvitlo"

    def parse(
        self,
        html: str,
        target: SourceTarget,
        observed_at: datetime,
    ) -> SourceScheduleObservation:
        soup, text = soup_and_text(html)
        reference_day = default_reference_day(observed_at)
        schedule_date = parse_page_date(text, reference_year=reference_day.year)
        path_parts = [part for part in urlparse(target.source_url).path.split("/") if part]
        operator_id = path_parts[1] if len(path_parts) > 1 and path_parts[0] == "schedule" else None
        target_region_matches = (
            operator_id is not None
            and _REGION_BY_OPERATOR_SLUG.get(operator_id) == target.region_slug
        )

        today_panel = soup.find(attrs={"data-panel": "today"})
        row = (
            today_panel.find(attrs={"data-group": f"{target.queue}.{target.subqueue}"})
            if today_panel
            else None
        )
        if schedule_date is None or row is None or not target_region_matches:
            return make_observation(
                source_id=self.source_id,
                target=target,
                observed_at=observed_at,
                operator_id=operator_id,
                schedule_date=schedule_date,
                source_updated_at=None,
                fetch_status=FetchStatus.INCOMPLETE,
                error_code=(
                    "target_region_not_verified_for_operator"
                    if not target_region_matches
                    else "today_schedule_or_group_missing"
                ),
            )
        outages = []
        bars = row.select(".ks-bar[title]")
        if not bars:
            return make_observation(
                source_id=self.source_id,
                target=target,
                observed_at=observed_at,
                operator_id=operator_id,
                schedule_date=schedule_date,
                source_updated_at=None,
                fetch_status=FetchStatus.INCOMPLETE,
                error_code="schedule_row_has_no_valid_day_data",
            )
        for bar in bars:
            title = str(bar.get("title", ""))
            lowered_title = title.lower()
            time_range = _TIME_RANGE_RE.search(title)
            if time_range is None:
                return make_observation(
                    source_id=self.source_id,
                    target=target,
                    observed_at=observed_at,
                    operator_id=operator_id,
                    schedule_date=schedule_date,
                    source_updated_at=None,
                    fetch_status=FetchStatus.INCOMPLETE,
                    error_code="invalid_outage_bar",
                )
            if any(
                marker in lowered_title
                for marker in (
                    "немає відключень",
                    "відключень не заплановано",
                    "без відключень",
                )
            ):
                continue
            if "немає" not in lowered_title and "відключ" not in lowered_title:
                continue
            try:
                outages.extend(
                    interval_from_times(
                        time_range.group("start"),
                        time_range.group("end"),
                        "off",
                    )
                )
            except ValueError:
                return make_observation(
                    source_id=self.source_id,
                    target=target,
                    observed_at=observed_at,
                    operator_id=operator_id,
                    schedule_date=schedule_date,
                    source_updated_at=None,
                    fetch_status=FetchStatus.INCOMPLETE,
                    error_code="invalid_outage_interval",
                )

        try:
            outages = list(normalize_intervals(outages))
            intervals = complete_from_outages(outages)
        except ValueError:
            return make_observation(
                source_id=self.source_id,
                target=target,
                observed_at=observed_at,
                operator_id=operator_id,
                schedule_date=schedule_date,
                source_updated_at=None,
                fetch_status=FetchStatus.INCOMPLETE,
                error_code="overlapping_or_invalid_intervals",
            )

        freshness = soup.find(class_="ks-fresh")
        update_match = _FRESH_RE.search(freshness.get_text(" ", strip=True)) if freshness else None
        source_updated_at = (
            parse_updated_at(
                update_match.group("updated"),
                reference_day=schedule_date,
            )
            if update_match
            else None
        )
        return make_observation(
            source_id=self.source_id,
            target=target,
            observed_at=observed_at,
            operator_id=operator_id,
            schedule_date=schedule_date,
            source_updated_at=source_updated_at,
            fetch_status=FetchStatus.SUCCESS,
            intervals=intervals,
        )
