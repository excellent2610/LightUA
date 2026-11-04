from __future__ import annotations

import re
from datetime import datetime
from urllib.parse import urljoin, urlparse

from bs4 import Tag

from parsing.base import SourceAdapter
from parsing.models import FetchStatus, ScheduleInterval, SourceScheduleObservation, SourceTarget
from parsing.normalization import covers_full_day, interval_from_times, normalize_intervals
from parsing.source_utils import (
    default_reference_day,
    make_observation,
    parse_page_date,
    parse_updated_marker,
    source_operator_id,
    soup_and_text,
)


_UPDATE_RE = r"Оновлено\s*:?.{0,80}"
_GROUP_RE = re.compile(
    r"^\s*(?:Група|Черга)\s*(\d+)\s*[./-]\s*(\d+)\s*$",
    re.IGNORECASE,
)


class AlertsOrgParser(SourceAdapter):
    source_id = "alerts_org"

    def parse(
        self,
        html: str,
        target: SourceTarget,
        observed_at: datetime,
    ) -> SourceScheduleObservation:
        soup, text = soup_and_text(html)
        reference_day = default_reference_day(observed_at)
        heading = soup.find("h1")
        schedule_date = parse_page_date(
            heading.get_text(" ", strip=True) if heading else text,
            reference_year=reference_day.year,
        )
        source_link = None
        description = soup.find(class_="description")
        if description:
            for anchor in description.find_all("a", href=True):
                preceding_text: list[str] = []
                for item in description.contents:
                    if item is anchor:
                        break
                    preceding_text.append(
                        item.get_text(" ", strip=True) if isinstance(item, Tag) else str(item)
                    )
                if not re.search(
                    r"(?:джерело|оператор)\s*:?\s*$",
                    " ".join(preceding_text),
                    flags=re.IGNORECASE,
                ):
                    continue
                source_link = urljoin(target.source_url, str(anchor["href"]))
                break
        operator_id = source_operator_id(source_link)
        path_parts = [
            part for part in urlparse(target.source_url).path.split("/") if part
        ]
        target_region_matches = bool(path_parts) and path_parts[0] == target.region_slug

        target_group: Tag | None = None
        for group in soup.select(".group"):
            name = group.select_one(".group-name")
            if name is None:
                continue
            match = _GROUP_RE.match(name.get_text(" ", strip=True))
            if match and (int(match.group(1)), int(match.group(2))) == (
                target.queue,
                target.subqueue,
            ):
                target_group = group
                break

        if schedule_date is None or target_group is None or not target_region_matches:
            return make_observation(
                source_id=self.source_id,
                target=target,
                observed_at=observed_at,
                operator_id=operator_id,
                schedule_date=schedule_date,
                source_updated_at=None,
                fetch_status=FetchStatus.INCOMPLETE,
                error_code=(
                    "target_region_not_in_source_url"
                    if not target_region_matches
                    else "schedule_date_or_group_missing"
                ),
            )

        intervals: list[ScheduleInterval] = []
        for period in target_group.select(".period > div[data-start][data-end]"):
            state_node = period.select_one("b.on, b.off")
            if state_node is None:
                return make_observation(
                    source_id=self.source_id,
                    target=target,
                    observed_at=observed_at,
                    operator_id=operator_id,
                    schedule_date=schedule_date,
                    source_updated_at=None,
                    fetch_status=FetchStatus.INCOMPLETE,
                    error_code="period_state_missing",
                )
            state = "off" if "off" in state_node.get("class", []) else "on"
            try:
                intervals.extend(
                    interval_from_times(
                        str(period["data-start"]),
                        str(period["data-end"]),
                        state,
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
                    error_code="invalid_period",
                )

        try:
            normalized = normalize_intervals(intervals)
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
        if not covers_full_day(normalized):
            return make_observation(
                source_id=self.source_id,
                target=target,
                observed_at=observed_at,
                operator_id=operator_id,
                schedule_date=schedule_date,
                source_updated_at=None,
                fetch_status=FetchStatus.INCOMPLETE,
                error_code="schedule_does_not_cover_full_day",
            )

        source_updated_at = parse_updated_marker(
            text,
            reference_day=schedule_date,
            marker=_UPDATE_RE,
        )
        return make_observation(
            source_id=self.source_id,
            target=target,
            observed_at=observed_at,
            operator_id=operator_id,
            schedule_date=schedule_date,
            source_updated_at=source_updated_at,
            fetch_status=FetchStatus.SUCCESS,
            intervals=normalized,
        )
