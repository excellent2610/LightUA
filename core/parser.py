"""Parser for outage schedule from alerts.org.ua (Cherkasy only)."""

from __future__ import annotations

import re
from typing import Any

import requests

ALERTS_URL = "https://alerts.org.ua/cherkaska-oblast/cherkasy/"

_TIME_BLOCK_RE = re.compile(
    r"(?P<start>\d{1,2}:\d{2})\s*[-–]\s*(?P<end>\d{1,2}:\d{2})(?:\s*(?P<status>OFF|ON))?",
    re.IGNORECASE,
)
_QUEUE_RE = re.compile(
    r"(?:черг[аи]|queue)\s*([1-6])(?:\s*[\.\-]\s*([1-2]))?",
    re.IGNORECASE,
)
_QUEUE_DOTTED_RE = re.compile(r"\b([1-6])\s*[\.\-]\s*([1-2])\b")

_OFF_HINTS = (" off", "red", "danger", "blackout", "вимк", "відключ", "без світла")
_ON_HINTS = (" on", "green", "success", "увімк", "є світло")


def _normalize_time(value: str) -> str:
    h, m = value.split(":", 1)
    return f"{int(h):02d}:{m}"


def build_schedule_url(*args: Any, **kwargs: Any) -> str:
    """Backward-compatible URL builder."""
    if len(args) >= 2:
        region_slug = str(args[0]).strip().strip("/")
        city_slug = str(args[1]).strip().strip("/")
        if region_slug and city_slug:
            return f"https://alerts.org.ua/{region_slug}/{city_slug}/"

    region_slug = str(kwargs.get("region_slug") or kwargs.get("region") or "").strip().strip("/")
    city_slug = str(kwargs.get("city_slug") or kwargs.get("city") or "").strip().strip("/")
    if region_slug and city_slug:
        return f"https://alerts.org.ua/{region_slug}/{city_slug}/"
    return ALERTS_URL


def _status_from_context(explicit_status: str | None, context: str) -> str:
    if explicit_status:
        return explicit_status.upper()

    ctx = f" {context.lower()} "
    if any(h in ctx for h in _OFF_HINTS):
        return "OFF"
    if any(h in ctx for h in _ON_HINTS):
        return "ON"
    # Fallback when status is not explicit in text but only encoded visually.
    return "OFF"


def _queue_from_context(context: str) -> tuple[str | None, str | None]:
    # Prefer explicit "queue/черга" labels closest to the interval.
    matches = list(_QUEUE_RE.finditer(context))
    if matches:
        m = matches[-1]
        return m.group(1), m.group(2)

    # Fallback for plain tokens like "4.1" near the interval block.
    matches2 = list(_QUEUE_DOTTED_RE.finditer(context))
    if matches2:
        m = matches2[-1]
        return m.group(1), m.group(2)

    return None, None


def _extract_blocks(raw_html: str) -> list[dict[str, str]]:
    blocks: list[dict[str, str]] = []
    current_queue: str | None = None
    current_subqueue: str | None = None
    cursor = 0

    for m in _TIME_BLOCK_RE.finditer(raw_html):
        # Carry queue context from previous HTML slice to reduce empty queue/subqueue fields.
        prefix = raw_html[cursor:m.start()]
        q_pref, sq_pref = _queue_from_context(prefix[-1200:])
        if q_pref:
            current_queue = q_pref
        if sq_pref:
            current_subqueue = sq_pref

        start_i = max(m.start() - 450, 0)
        end_i = min(m.end() + 180, len(raw_html))
        context = raw_html[start_i:end_i]
        status = _status_from_context(m.group("status"), context)
        if status != "OFF":
            cursor = m.end()
            continue

        queue, subqueue = _queue_from_context(context)
        if queue:
            current_queue = queue
        if subqueue:
            current_subqueue = subqueue

        blocks.append(
            {
                "start": _normalize_time(m.group("start")),
                "end": _normalize_time(m.group("end")),
                "status": "OFF",
                "queue": current_queue or "",
                "subqueue": current_subqueue or "",
            }
        )
        cursor = m.end()

    seen: set[tuple[str, str, str, str, str]] = set()
    unique: list[dict[str, str]] = []
    for b in blocks:
        key = (b["start"], b["end"], b["status"], b["queue"], b["subqueue"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(b)
    return unique


def fetch_schedule_html(url: str = ALERTS_URL, timeout: int = 25) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    # First attempt: plain requests.
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
        text = r.text
        # If Cloudflare challenge page is returned, fallback to cloudscraper.
        if "cloudflare" not in text.lower():
            return text
    except requests.RequestException:
        pass

    # Fallback: cloudscraper for Cloudflare-protected response.
    try:
        import cloudscraper  # type: ignore

        s = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows"})
        r2 = s.get(url, headers=headers, timeout=timeout)
        r2.raise_for_status()
        return r2.text
    except Exception as exc:
        raise RuntimeError(f"Failed to fetch schedule from {url}") from exc


def parse_off_schedule(raw_html: str) -> list[dict[str, str]]:
    return _extract_blocks(raw_html)


def get_off_schedule(url: str = ALERTS_URL, timeout: int = 25) -> list[dict[str, str]]:
    html = fetch_schedule_html(url=url, timeout=timeout)
    return parse_off_schedule(html)


# Compatibility aliases for current project imports.
def parse_schedule(raw_html: str) -> list[dict[str, str]]:
    return parse_off_schedule(raw_html)


def get_schedule(url: str = ALERTS_URL, timeout: int = 25) -> list[dict[str, str]]:
    return get_off_schedule(url=url, timeout=timeout)


def fetch_and_parse_schedule(url: str = ALERTS_URL, timeout: int = 25) -> list[dict[str, str]]:
    return get_off_schedule(url=url, timeout=timeout)


def get_blackout_periods(url: str = ALERTS_URL, timeout: int = 25) -> list[dict[str, str]]:
    return get_off_schedule(url=url, timeout=timeout)


def fetch_off_periods(url: str = ALERTS_URL, timeout: int = 25) -> list[dict[str, str]]:
    return get_off_schedule(url=url, timeout=timeout)


class ScheduleParser:
    def __init__(self, url: str = ALERTS_URL, timeout: int = 25) -> None:
        self.url = url
        self.timeout = timeout

    def fetch_html(self) -> str:
        return fetch_schedule_html(url=self.url, timeout=self.timeout)

    def parse(self, raw_html: str) -> list[dict[str, str]]:
        return parse_off_schedule(raw_html)

    def get_schedule(self) -> list[dict[str, str]]:
        return get_off_schedule(url=self.url, timeout=self.timeout)


def parse(raw_html: str) -> list[dict[str, str]]:
    return parse_off_schedule(raw_html)


def get_data(*args: Any, **kwargs: Any) -> list[dict[str, str]]:
    return get_off_schedule(
        url=kwargs.get("url", ALERTS_URL),
        timeout=kwargs.get("timeout", 25),
    )


def __getattr__(name: str) -> Any:
    if name.lower().endswith("url"):
        return ALERTS_URL
    if name.startswith("get_") or name.startswith("fetch_"):
        return get_off_schedule
    if name.startswith("parse_") or name == "parse":
        return parse_off_schedule
    if name.lower().endswith("parser"):
        return ScheduleParser
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
