"""Parser for outage schedule from alerts.org.ua (Cherkasy only)."""

from __future__ import annotations

import re
from typing import Any

import requests
from bs4 import BeautifulSoup  # added

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
_GROUP_LINE_RE = re.compile(r"(?:група|group|черга|queue)\s*([1-6])\s*[\.\-]\s*([1-2])", re.IGNORECASE)  # modified

_OFF_HINTS = (  # modified
    " off",
    "red",
    "danger",
    "text-red",
    "bg-red",
    "text-danger",
    "bg-danger",
    "blackout",
    "outage",
    "вимк",
    "відключ",
    "без світла",
    "немає світла",
)
_ON_HINTS = (  # modified
    " on",
    "green",
    "success",
    "text-green",
    "bg-green",
    "text-success",
    "bg-success",
    "увімк",
    "вкл",
    "є світло",
    "зі світлом",
    "наявне світло",
)


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


def _status_from_context(explicit_status: str | None, context: str) -> str:  # modified
    if explicit_status:
        return explicit_status.upper()

    ctx = f" {context.lower()} "
    off_hit = any(h in ctx for h in _OFF_HINTS)
    on_hit = any(h in ctx for h in _ON_HINTS)
    if off_hit and not on_hit:
        return "OFF"
    if on_hit and not off_hit:
        return "ON"
    if off_hit and on_hit:
        center = len(ctx) // 2  # added

        def nearest_distance(hints: tuple[str, ...]) -> int:  # added
            best = 10**9
            for hint in hints:
                pos = ctx.find(hint)
                while pos != -1:
                    dist = abs(pos - center)
                    if dist < best:
                        best = dist
                    pos = ctx.find(hint, pos + 1)
            return best

        off_dist = nearest_distance(_OFF_HINTS)  # added
        on_dist = nearest_distance(_ON_HINTS)  # added
        if off_dist < on_dist:
            return "OFF"
        if on_dist < off_dist:
            return "ON"
    return "UNKNOWN"


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

    for m in _TIME_BLOCK_RE.finditer(raw_html):
        start_i = max(m.start() - 260, 0)  # modified
        end_i = min(m.end() + 220, len(raw_html))  # modified
        context = raw_html[start_i:end_i]
        status = _status_from_context(m.group("status"), context)
        if status not in {"OFF", "ON"}:  # modified
            continue

        # Prefer queue marker closest to interval block; do not carry stale queue across page.  # added
        queue, subqueue = _queue_from_context(context)
        if not queue or not subqueue:
            continue

        blocks.append(
            {
                "start": _normalize_time(m.group("start")),
                "end": _normalize_time(m.group("end")),
                "status": status,  # modified
                "queue": queue,
                "subqueue": subqueue,
            }
        )

    seen: set[tuple[str, str, str, str, str]] = set()
    unique: list[dict[str, str]] = []
    for b in blocks:
        key = (b["start"], b["end"], b["status"], b["queue"], b["subqueue"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(b)
    return unique


def _extract_blocks_from_text(raw_html: str) -> list[dict[str, str]]:  # added
    soup = BeautifulSoup(raw_html, "html.parser")
    text = soup.get_text("\n", strip=True)
    if not text:
        return []

    blocks: list[dict[str, str]] = []
    current_queue: str | None = None
    current_subqueue: str | None = None
    current_status: str | None = None  # added

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        g = _GROUP_LINE_RE.search(line)
        if g:
            current_queue, current_subqueue = g.group(1), g.group(2)
            continue
        # Support headings that may contain only dotted queue format like "4.1".  # added
        g2 = _QUEUE_DOTTED_RE.search(line)
        if g2 and len(line) <= 20:
            current_queue, current_subqueue = g2.group(1), g2.group(2)
            continue
        q3, sq3 = _queue_from_context(line)
        if q3 and sq3 and len(line) <= 40:
            current_queue, current_subqueue = q3, sq3
            continue

        matches = list(_TIME_BLOCK_RE.finditer(line))
        # Track section status for lines like "Без світла" / "Зі світлом".  # added
        inferred_line_status = _status_from_context(None, line)
        if inferred_line_status in {"OFF", "ON"} and not matches:
            current_status = inferred_line_status
            continue

        if not matches:
            continue
        if not current_queue or not current_subqueue:
            continue

        for m in matches:  # added
            status = (m.group("status") or "").upper()
            if status not in {"OFF", "ON"}:
                local_start = max(m.start() - 24, 0)
                local_end = min(m.end() + 24, len(line))
                local_ctx = line[local_start:local_end]
                status = _status_from_context(None, local_ctx)
            if status not in {"OFF", "ON"} and current_status in {"OFF", "ON"}:  # added
                status = current_status
            if status not in {"OFF", "ON"}:
                continue

            blocks.append(
                {
                    "start": _normalize_time(m.group("start")),
                    "end": _normalize_time(m.group("end")),
                    "status": status,
                    "queue": current_queue,
                    "subqueue": current_subqueue,
                }
            )

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
    text_blocks = _extract_blocks_from_text(raw_html)  # added
    if text_blocks:
        return text_blocks
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
