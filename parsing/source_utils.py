from __future__ import annotations

import re
from datetime import date, datetime, timezone
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from parsing.models import FetchStatus, SourceScheduleObservation, SourceTarget
from parsing.normalization import KYIV_TZ, kyiv_timestamp_to_utc, local_date


MONTHS = {
    "січня": 1,
    "січ": 1,
    "лютого": 2,
    "лют": 2,
    "березня": 3,
    "бер": 3,
    "квітня": 4,
    "квіт": 4,
    "травня": 5,
    "трав": 5,
    "червня": 6,
    "черв": 6,
    "липня": 7,
    "лип": 7,
    "серпня": 8,
    "серп": 8,
    "вересня": 9,
    "вер": 9,
    "жовтня": 10,
    "жовт": 10,
    "листопада": 11,
    "лист": 11,
    "грудня": 12,
    "груд": 12,
}

_NUMERIC_DATE_RE = re.compile(r"(?<!\d)(\d{1,2})[./](\d{1,2})[./](\d{2,4})(?!\d)")
_MONTH_DATE_RE = re.compile(
    r"(?<!\d)(\d{1,2})\s+([а-яіїєґ]+)\.?(?:\s+(\d{4})\s*р?\.?)?",
    re.IGNORECASE,
)


def parse_page_date(
    text: str,
    *,
    reference_year: int,
) -> date | None:
    numeric = _NUMERIC_DATE_RE.search(text)
    if numeric:
        day, month, raw_year = map(int, numeric.groups())
        year = raw_year + 2000 if raw_year < 100 else raw_year
        try:
            return date(year, month, day)
        except ValueError:
            return None

    for month_match in _MONTH_DATE_RE.finditer(text):
        day = int(month_match.group(1))
        month = MONTHS.get(month_match.group(2).lower().rstrip("."))
        if month is None:
            continue
        year = int(month_match.group(3)) if month_match.group(3) else reference_year
        try:
            return date(year, month, day)
        except ValueError:
            continue
    return None


def parse_updated_at(text: str, *, reference_day: date) -> datetime | None:
    numeric = _NUMERIC_DATE_RE.search(text)
    if numeric:
        updated_day = parse_page_date(numeric.group(0), reference_year=reference_day.year)
    else:
        day_match = _MONTH_DATE_RE.search(text)
        updated_day = (
            parse_page_date(day_match.group(0), reference_year=reference_day.year)
            if day_match
            else None
        )
    time_match = re.search(r"\b([01]?\d|2[0-3]):([0-5]\d)\b", text)
    if updated_day is None or time_match is None:
        return None

    if updated_day > reference_day and updated_day.month == 12 and reference_day.month == 1:
        updated_day = updated_day.replace(year=reference_day.year - 1)
    return kyiv_timestamp_to_utc(
        updated_day,
        int(time_match.group(1)),
        int(time_match.group(2)),
    )


def observed_time_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    return value.astimezone(timezone.utc)


def source_operator_id(source_url: str | None) -> str | None:
    if not source_url:
        return None
    host = urlparse(source_url).hostname
    if not host or host.endswith("alerts.org.ua"):
        return None
    return host.lower()


def make_observation(
    *,
    source_id: str,
    target: SourceTarget,
    observed_at: datetime,
    operator_id: str | None,
    schedule_date: date | None,
    source_updated_at: datetime | None,
    fetch_status: FetchStatus,
    intervals=(),
    error_code: str | None = None,
) -> SourceScheduleObservation:
    return SourceScheduleObservation(
        source_id=source_id,
        region_slug=target.region_slug,
        operator_id=operator_id,
        queue=target.queue,
        subqueue=target.subqueue,
        schedule_date=schedule_date,
        observed_at=observed_time_utc(observed_at),
        source_updated_at=source_updated_at,
        fetch_status=fetch_status,
        intervals=tuple(intervals),
        source_url=target.source_url,
        error_code=error_code,
    )


def soup_and_text(html: str) -> tuple[BeautifulSoup, str]:
    soup = BeautifulSoup(html, "html.parser")
    return soup, " ".join(soup.get_text(" ", strip=True).split())


def default_reference_day(observed_at: datetime) -> date:
    return local_date(observed_at)


def parse_updated_marker(
    text: str,
    *,
    reference_day: date,
    marker: str,
) -> datetime | None:
    match = re.search(marker, text, flags=re.IGNORECASE)
    if match is None:
        return None
    return parse_updated_at(match.group(0), reference_day=reference_day)


def explicit_schedule_page(text: str) -> bool:
    lowered = text.lower()
    return any(token in lowered for token in ("графік", "відключ", "черга", "група"))


def local_midnight(day: date) -> datetime:
    return datetime.combine(day, datetime.min.time(), tzinfo=KYIV_TZ)
