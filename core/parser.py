from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, List, Set, Tuple
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from core.extractor import extract_queue_chunks
from core.normalizer import split_intervals
from core.validator import validate_schedule_queues
from errors.parsing import ParseError
from models.schedule import Interval, Schedule
from utils.http import build_mirror_url, fetch_html

BASE_CITY_PATH = "https://alerts.org.ua/cherkaska-oblast/cherkasy/cherkasy"
DEFAULT_CITY = "Черкаси"
DEFAULT_REGION = "Черкаська область"
DEFAULT_SOURCE = "alerts.org.ua"


def _date_from_url(url: str) -> str:
    path = urlparse(url).path
    match = re.search(r"(\d{4}-\d{2}-\d{2})\.html$", path)
    if not match:
        return datetime.now(ZoneInfo("Europe/Kyiv")).strftime("%Y-%m-%d")
    return match.group(1)


def build_schedule_url(date_str: str | None = None) -> str:
    if date_str is None:
        date_str = datetime.now(ZoneInfo("Europe/Kyiv")).strftime("%Y-%m-%d")
    return f"{BASE_CITY_PATH}/{date_str}.html"


def _parse_queues_from_html(html: str) -> Dict[str, List[Interval]]:
    chunks_map = extract_queue_chunks(html)
    queue_models: Dict[str, List[Interval]] = {}
    for queue, chunks in chunks_map.items():
        intervals: List[Interval] = []
        for chunk in chunks:
            for start, end in split_intervals(chunk):
                intervals.append(Interval(start=start, end=end))
        deduped: List[Interval] = []
        seen: Set[Tuple[str, str]] = set()
        for interval in intervals:
            key = (interval.start, interval.end)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(interval)
        if deduped:
            queue_models[queue] = deduped
    return queue_models


def get_schedule(url: str | None = None) -> Dict[str, object]:
    if not url:
        url = build_schedule_url()

    html = fetch_html(url)
    try:
        primary_queues = _parse_queues_from_html(html)
    except ParseError:
        primary_queues = {}

    mirror_url = build_mirror_url(url)
    mirror_queues: Dict[str, List[Interval]] = {}
    if mirror_url != url:
        try:
            mirror_html = fetch_html(mirror_url)
            mirror_queues = _parse_queues_from_html(mirror_html)
        except Exception:
            mirror_queues = {}

    queue_models = mirror_queues if len(mirror_queues) > len(primary_queues) else primary_queues

    serializable_queues = {
        queue: [interval.to_dict() for interval in intervals]
        for queue, intervals in queue_models.items()
    }
    validate_schedule_queues(serializable_queues)

    schedule = Schedule(
        date=_date_from_url(url),
        city=DEFAULT_CITY,
        region=DEFAULT_REGION,
        source=DEFAULT_SOURCE,
        queues=queue_models,
    )
    return schedule.to_dict()
