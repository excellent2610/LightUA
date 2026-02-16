from __future__ import annotations

import re
from urllib.request import Request, urlopen

from bot.config import get_schedule_url_for_region
from bs4 import BeautifulSoup
from core import parser as legacy_parser
from bot.regions import region_swaps_on_off


def _fetch_html(url: str, timeout: int = 25) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (LightUA bot)",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return raw.decode("utf-8", errors="replace")


_QUEUE_RE = re.compile(r"\b([1-6])\s*[\.\-]\s*([1-2])\b")
_TIME_RE = re.compile(r"(\d{1,2}[:.]\d{2})\s*[-–]\s*(\d{1,2}[:.]\d{2})")
_OFF_HINTS = ("без світла", "відключ", "вимк")
_ON_HINTS = ("зі світлом", "світло є", "є світло", "увімк")


def _norm_time(value: str) -> str:
    hh, mm = value.replace(".", ":").split(":", 1)
    return f"{int(hh):02d}:{int(mm):02d}"


def load_region_schedule(region: str | None) -> list[dict[str, str]]:
    swap = region_swaps_on_off(region)
    url = get_schedule_url_for_region(region)
    html = _fetch_html(url)

    # Primary parser: reuse existing OFF/ON detection from core.parser on fetched oblast HTML.
    try:
        parsed = legacy_parser.parse_off_schedule(html)
    except Exception:
        parsed = []
    if isinstance(parsed, list) and parsed:
        normalized: list[dict[str, str]] = []
        seen_norm: set[tuple[str, str, str, str, str]] = set()
        for item in parsed:
            if not isinstance(item, dict):
                continue
            queue = str(item.get("queue", "")).strip()
            subqueue = str(item.get("subqueue", "")).strip()
            start = str(item.get("start", "")).strip()
            end = str(item.get("end", "")).strip()
            status = str(item.get("status", "")).upper().strip()
            if swap:
                if status == "OFF":
                    status = "ON"
                elif status == "ON":
                    status = "OFF"
            if not queue or not subqueue or not start or not end or status not in {"OFF", "ON"}:
                continue
            key = (queue, subqueue, start, end, status)
            if key in seen_norm:
                continue
            seen_norm.add(key)
            normalized.append(
                {
                    "queue": queue,
                    "subqueue": subqueue,
                    "start": start,
                    "end": end,
                    "status": status,
                }
            )
        if normalized:
            return normalized

    # Fallback parser: text-only extraction if page structure changed.
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    result: list[dict[str, str]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    current_q: str | None = None
    current_sq: str | None = None
    current_status: str | None = None

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        low = line.lower()

        q_match = _QUEUE_RE.search(line)
        if q_match and len(line) <= 32:
            current_q, current_sq = q_match.group(1), q_match.group(2)
            continue

        if any(h in low for h in _OFF_HINTS):
            current_status = "OFF"
            # continue is intentional: status-only heading line.
            continue
        if any(h in low for h in _ON_HINTS):
            current_status = "ON"
            continue

        if not current_q or not current_sq or current_status not in {"OFF", "ON"}:
            continue

        matches = list(_TIME_RE.finditer(line))
        if not matches:
            continue
        for m in matches:
            start = _norm_time(m.group(1))
            end = _norm_time(m.group(2))
            key = (current_q, current_sq, start, end, current_status)
            if key in seen:
                continue
            seen.add(key)
            status = current_status
            if swap:
                status = "ON" if status == "OFF" else "OFF"
            result.append(
                {
                    "queue": current_q,
                    "subqueue": current_sq,
                    "start": start,
                    "end": end,
                    "status": status,
                }
            )
    return result
