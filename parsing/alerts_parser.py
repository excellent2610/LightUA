
from __future__ import annotations

import re
import time as time_module
from dataclasses import dataclass
from datetime import date, datetime, time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from utils.time_utils import KYIV_TZ, now_kyiv


BASE_URL = "https://alerts.org.ua"
TIME_TOKEN_RE = r"(?:(?:[01]?\d|2[0-3]):[0-5]\d|24:00)"
TIME_RANGE_RE = re.compile(
    rf"(?P<start>(?:[01]?\d|2[0-3]):[0-5]\d)\s*[-–—]\s*(?P<end>{TIME_TOKEN_RE})"
)
DATE_PAGE_RE = re.compile(r"/(?P<date>\d{4}-\d{2}-\d{2})\.html?$")
TITLE_DATE_RE = re.compile(r"(?<!\d)(?P<day>\d{2})[.\-/](?P<month>\d{2})[.\-/](?P<year>\d{4})(?!\d)")
UPDATED_RE = re.compile(
    r"Оновлено\s*:\s*(?P<day>\d{2})[.](?P<month>\d{2})[.](?P<year>\d{4})"
    r"\s*(?:о|об)\s*(?P<hour>\d{1,2}):(?P<minute>\d{2})",
    re.IGNORECASE,
)

EMERGENCY_KEYWORDS = ("аварійн", "екстрен", "urgent blackout", "emergency")
NO_SCHEDULE_KEYWORDS = (
    "графік відсутній",
    "графік не опубліковано",
    "дані відсутні",
    "schedule unavailable",
    "schedule not found",
)
NO_OUTAGES_KEYWORDS = (
    "відключень не заплановано",
    "відключення не плануються",
    "сьогодні відключень не буде",
    "графік без відключень",
    "no outages scheduled",
)
ANTI_BOT_KEYWORDS = (
    "please wait while your request is being verified",
    "verifying that you are not a robot",
    "verify you are human",
    "cf-turnstile",
    "attention required",
)


class ParserError(Exception):
    pass


class RegionUnavailableError(ParserError):
    pass


class ScheduleNotFoundError(ParserError):
    pass


class EmergencyOutageError(ParserError):
    pass


@dataclass(slots=True, frozen=True)
class ScheduleInterval:
    start: time
    end: time
    state: str
    ends_at_midnight: bool = False


@dataclass(slots=True)
class DaySchedule:
    region_slug: str
    region_name: str
    queue: int
    subqueue: int
    intervals: list[ScheduleInterval]
    default_state: str = "on"
    source_url: str | None = None
    schedule_date: date | None = None
    fetched_at: datetime | None = None
    source_updated_at: datetime | None = None
    confirmed_empty: bool = False

    def state_at(self, moment: datetime) -> str:
        local_time = moment.astimezone(KYIV_TZ).time()
        for interval in self.intervals:
            if _time_in_interval(
                local_time,
                interval.start,
                interval.end,
                interval.ends_at_midnight,
            ):
                return interval.state
        return self.default_state


class AlertsParser:
    def __init__(self, timeout_seconds: int = 15) -> None:
        self.timeout_seconds = timeout_seconds
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                )
            }
        )

    def get_schedule(self, region_slug: str, queue: int, subqueue: int) -> DaySchedule:
        region_slug = region_slug.strip().strip("/")

        # Backward compatibility with legacy region slugs.
        legacy_slugs = {
            "volynska": "volynska-oblast",
            "vinnytska": "vinnytcka-oblast",
            "dnipropetrovska": "dnipropetrovska-oblast",
            "donetska": "donetcka-oblast",
            "zhytomyrska": "zhytomyrska-oblast",
            "zakarpatska": "zakarpatska-oblast",
            "zaporizka": "zaporizka-oblast",
            "kyivska": "kyivska-oblast",
            "kirovogradska": "kirovogradska-oblast",
            "luganska": "luganska-oblast",
            "lvivska": "lvivska-oblast",
            "mykolaivska": "mykolaivska-oblast",
            "odeska": "odeska-oblast",
            "poltavska": "poltavska-oblast",
            "rivnenska": "rivnenska-oblast",
            "sumska": "sumska-oblast",
            "ternopilska": "ternopilska-oblast",
            "harkivska": "harkivska-oblast",
            "hersonska": "hersonska-oblast",
            "hmelnitska": "hmelnitcka-oblast",
            "cherkaska": "cherkaska-oblast",
            "chernivetska": "chernivetcka-oblast",
            "chernihivska": "chernigivska-oblast",
            "ivano-frankivska": "ivano-frankivska-oblast",
        }
        region_slug = legacy_slugs.get(region_slug, region_slug)
        region_url = f"{BASE_URL}/{region_slug}/"

        html = self._fetch_url(region_url)
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text("\n", strip=True)
        normalized_text = _normalize_text(text)

        if _looks_like_anti_bot_page(normalized_text):
            raise RegionUnavailableError("alerts.org.ua повернув anti-bot сторінку")
        if any(keyword in normalized_text for keyword in EMERGENCY_KEYWORDS):
            raise EmergencyOutageError("Аварійні відключення")

        intervals = _extract_group_intervals(text=text, queue=queue, subqueue=subqueue)
        confirmed_empty = any(keyword in normalized_text for keyword in NO_OUTAGES_KEYWORDS)
        source_url = region_url
        source_soup = soup

        if not intervals and not confirmed_empty:
            latest_url = self._find_latest_schedule_url(soup=soup, region_slug=region_slug)
            if latest_url:
                latest_html = self._fetch_url(latest_url)
                latest_soup = BeautifulSoup(latest_html, "html.parser")
                latest_text = latest_soup.get_text("\n", strip=True)
                latest_normalized = _normalize_text(latest_text)
                if not _looks_like_anti_bot_page(latest_normalized):
                    intervals = _extract_group_intervals(
                        text=latest_text,
                        queue=queue,
                        subqueue=subqueue,
                    )
                    confirmed_empty = any(
                        keyword in latest_normalized
                        for keyword in NO_OUTAGES_KEYWORDS
                    )
                    if intervals or confirmed_empty:
                        source_url = latest_url
                        source_soup = latest_soup

        if not intervals and not confirmed_empty:
            if any(keyword in normalized_text for keyword in NO_SCHEDULE_KEYWORDS):
                raise ScheduleNotFoundError("Графік не опубліковано")
            raise ScheduleNotFoundError("Не знайдено інтервали для вибраної черги")

        if intervals and not _covers_full_day(intervals):
            raise ScheduleNotFoundError("Отримано неповний графік для вибраної черги")

        region_name = self._extract_region_name(source_soup, region_slug)
        schedule_date = _extract_schedule_date(source_soup, source_url)
        return DaySchedule(
            region_slug=region_slug,
            region_name=region_name,
            queue=queue,
            subqueue=subqueue,
            intervals=intervals,
            default_state="on",
            source_url=source_url,
            schedule_date=schedule_date,
            fetched_at=now_kyiv(),
            source_updated_at=_extract_source_updated_at(source_soup),
            confirmed_empty=confirmed_empty,
        )

    def get_current_state(
        self,
        region_slug: str,
        queue: int,
        subqueue: int,
    ) -> tuple[str, DaySchedule]:
        schedule = self.get_schedule(
            region_slug=region_slug,
            queue=queue,
            subqueue=subqueue,
        )
        state = schedule.state_at(now_kyiv())
        return state, schedule

    def _fetch_url(self, url: str) -> str:
        last_error: requests.RequestException | None = None

        for attempt in range(3):
            try:
                response = self._session.get(
                    url,
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                return response.text
            except requests.RequestException as exc:
                last_error = exc
                if attempt < 2:
                    time_module.sleep(1 + attempt)

        raise RegionUnavailableError(
            f"Не вдалося отримати сторінку після 3 спроб: {url}"
        ) from last_error

    def _find_latest_schedule_url(
        self,
        soup: BeautifulSoup,
        region_slug: str,
    ) -> str | None:
        candidates: list[tuple[datetime, str]] = []
        target = f"/{region_slug.lower()}/"

        for anchor in soup.find_all("a", href=True):
            href = str(anchor.get("href", "")).strip()
            if not href:
                continue
            url = urljoin(BASE_URL, href)
            parsed = urlparse(url)
            if parsed.netloc and "alerts.org.ua" not in parsed.netloc:
                continue
            path = parsed.path.lower()
            if target not in path:
                continue

            date_match = DATE_PAGE_RE.search(path)
            if not date_match:
                continue
            try:
                parsed_date = datetime.strptime(date_match.group("date"), "%Y-%m-%d")
            except ValueError:
                continue

            candidates.append((parsed_date, url))

        if not candidates:
            return None
        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    def _extract_region_name(self, soup: BeautifulSoup, region_slug: str) -> str:
        title = soup.find("title")
        if title:
            title_text = " ".join(title.get_text(" ", strip=True).split())
            # Example: "Черкаська область 💡 графік відключення світла 21.02.2026"
            match = re.search(
                r"([А-ЯІЇЄҐA-Z][^|]+?область)\b",
                title_text,
                flags=re.IGNORECASE,
            )
            if match:
                return match.group(1).strip()

        h1 = soup.find("h1")
        if h1:
            h1_text = " ".join(h1.get_text(" ", strip=True).split())
            match = re.search(
                r"([А-ЯІЇЄҐA-Z][^|]+?область)\b",
                h1_text,
                flags=re.IGNORECASE,
            )
            if match:
                return match.group(1).strip()

        return " ".join(piece.capitalize() for piece in region_slug.split("-"))


def _extract_group_intervals(
    text: str,
    queue: int,
    subqueue: int,
) -> list[ScheduleInterval]:
    lines = _prepare_lines(text)
    blocks = _collect_target_blocks(lines=lines, queue=queue, subqueue=subqueue)
    parsed: list[ScheduleInterval] = []
    for block in blocks:
        parsed.extend(_parse_intervals_from_block_lines(block))

    deduped: list[ScheduleInterval] = []
    seen: set[tuple[time, time, str, bool]] = set()
    for item in parsed:
        key = (item.start, item.end, item.state, item.ends_at_midnight)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    deduped.sort(key=lambda x: (x.start.hour, x.start.minute, x.end.hour, x.end.minute))
    return deduped


def _prepare_lines(text: str) -> list[str]:
    normalized = text.replace("\u00A0", " ")
    result: list[str] = []
    for raw in normalized.splitlines():
        cleaned = " ".join(raw.split()).strip()
        if cleaned:
            result.append(cleaned)
    return result


def _collect_target_blocks(
    lines: list[str],
    queue: int,
    subqueue: int,
) -> list[list[str]]:
    header_re = re.compile(
        r"^\s*(?:група|черга)\s*(?P<queue>\d+)(?:\s*[./-]\s*(?P<subqueue>\d+))?\b.*$",
        flags=re.IGNORECASE,
    )
    headers: list[tuple[int, int, int | None]] = []
    for idx, line in enumerate(lines):
        match = header_re.match(line)
        if match:
            headers.append(
                (
                    idx,
                    int(match.group("queue")),
                    int(match.group("subqueue")) if match.group("subqueue") else None,
                )
            )

    has_split_groups = any(
        group == queue and sub is not None
        for _, group, sub in headers
    )
    blocks: list[list[str]] = []
    for header_index, (line_index, group, sub) in enumerate(headers):
        selected = (
            group == queue and sub == subqueue
        ) if has_split_groups else (
            group == queue and sub is None
        )
        if not selected:
            continue
        end_index = (
            headers[header_index + 1][0]
            if header_index + 1 < len(headers)
            else min(len(lines), line_index + 120)
        )
        block = lines[line_index + 1:end_index]
        if block:
            blocks.append(block)
    return blocks


def _parse_intervals_from_block_lines(
    block_lines: list[str],
) -> list[ScheduleInterval]:
    result: list[ScheduleInterval] = []
    for index, line in enumerate(block_lines):
        match = TIME_RANGE_RE.search(line)
        if match is None:
            continue

        start = _parse_time(match.group("start"))
        end = _parse_time(match.group("end"))
        tail = line[match.end():].strip()
        state = _guess_state(tail) or _guess_state(line)

        # Some pages render OFF/ON in a separate next element/line.
        if state is None:
            for offset in (1, 2, 3):
                look_index = index + offset
                if look_index >= len(block_lines):
                    break
                look_line = block_lines[look_index]
                if TIME_RANGE_RE.search(look_line):
                    break
                state = _guess_state(look_line)
                if state:
                    break

        if state is None:
            continue

        result.append(
            ScheduleInterval(
                start=start,
                end=end,
                state=state,
                ends_at_midnight=match.group("end") == "24:00",
            )
        )

    return result


def _parse_time(raw: str) -> time:
    value = raw.strip()
    if value == "24:00":
        return time(hour=0, minute=0)
    return datetime.strptime(value, "%H:%M").time()


def _time_in_interval(
    current: time,
    start: time,
    end: time,
    ends_at_midnight: bool = False,
) -> bool:
    if ends_at_midnight:
        return current >= start
    if start == end:
        return True
    if start < end:
        return start <= current < end
    return current >= start or current < end


def _covers_full_day(intervals: list[ScheduleInterval]) -> bool:
    """Reject gaps, overlaps, and ambiguous boundaries in source schedules."""
    try:
        _canonical_day_intervals(intervals, require_full_day=True)
    except ValueError:
        return False
    return True


def _canonical_day_intervals(
    intervals: list[ScheduleInterval],
    *,
    default_state: str = "on",
    require_full_day: bool = False,
) -> list[tuple[int, int, str]]:
    """Return state runs in minutes since midnight, merging equal adjacent runs."""
    if default_state not in {"on", "off"}:
        raise ValueError("Invalid default schedule state")

    ranges: list[tuple[int, int, str]] = []
    unique_intervals = set()
    if require_full_day and not intervals:
        raise ValueError("Schedule does not cover the full day")
    for interval in intervals:
        if interval.state not in {"on", "off"}:
            raise ValueError("Invalid interval state")
        start = interval.start.hour * 60 + interval.start.minute
        if interval.ends_at_midnight:
            if interval.end != time(0):
                raise ValueError("Midnight marker requires an end time of 00:00")
            end = 24 * 60
        else:
            end = interval.end.hour * 60 + interval.end.minute
        if start >= 24 * 60 or end > 24 * 60 or end == start:
            raise ValueError("Invalid or ambiguous interval bounds")
        if end < start:
            unique_intervals.add((start, 24 * 60, interval.state))
            if end > 0:
                unique_intervals.add((0, end, interval.state))
        else:
            unique_intervals.add((start, end, interval.state))

    ranges.extend(unique_intervals)
    ranges.sort()
    runs: list[tuple[int, int, str]] = []
    cursor = 0

    def append_run(start: int, end: int, state: str) -> None:
        if start == end:
            return
        if runs and runs[-1][1] == start and runs[-1][2] == state:
            old_start, _old_end, old_state = runs[-1]
            runs[-1] = (old_start, end, old_state)
        else:
            runs.append((start, end, state))

    for start, end, state in ranges:
        if start < cursor:
            raise ValueError("Overlapping schedule intervals")
        if start > cursor:
            if require_full_day:
                raise ValueError("Schedule does not cover the full day")
            append_run(cursor, start, default_state)
        append_run(start, end, state)
        cursor = end

    if cursor < 24 * 60:
        if require_full_day:
            raise ValueError("Schedule does not cover the full day")
        append_run(cursor, 24 * 60, default_state)
    return runs


def _normalize_text(text: str) -> str:
    return " ".join(text.lower().split())


def _guess_state(text: str) -> str | None:
    lowered = f" {text.lower()} "

    if re.search(r"\boff\b", lowered):
        return "off"
    if re.search(r"\bon\b", lowered):
        return "on"

    if "без світла" in lowered or "відключ" in lowered or "немає світла" in lowered:
        return "off"
    if "світло є" in lowered or "зі світлом" in lowered or "подача" in lowered:
        return "on"
    return None


def _looks_like_anti_bot_page(normalized_text: str) -> bool:
    return any(keyword in normalized_text for keyword in ANTI_BOT_KEYWORDS)


def _extract_schedule_date(soup: BeautifulSoup, source_url: str) -> date | None:
    """Return a date explicitly tied to the schedule page, never today's inferred date."""
    path_match = DATE_PAGE_RE.search(urlparse(source_url).path.lower())
    if path_match:
        try:
            return datetime.strptime(path_match.group("date"), "%Y-%m-%d").date()
        except ValueError:
            return None

    for element in (soup.find("title"), soup.find("h1")):
        if element is None:
            continue
        match = TITLE_DATE_RE.search(" ".join(element.get_text(" ", strip=True).split()))
        if match:
            try:
                return date(
                    int(match.group("year")),
                    int(match.group("month")),
                    int(match.group("day")),
                )
            except ValueError:
                continue
    return None


def _extract_source_updated_at(soup: BeautifulSoup) -> datetime | None:
    match = UPDATED_RE.search(soup.get_text(" ", strip=True))
    if not match:
        return None
    try:
        return datetime(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
            int(match.group("hour")),
            int(match.group("minute")),
            tzinfo=KYIV_TZ,
        )
    except ValueError:
        return None
