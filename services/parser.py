import re
from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup

from config import ALERTS_CF_CLEARANCE, OBLAST_TITLES, OBLAST_URLS, REQUEST_TIMEOUT
from services.models import ParsedOblastData, ParseResult, TimeInterval, UserGraph

GROUP_RE = re.compile(r"^Група\s+(\d{1,2})\.(\d{1,2})$", re.IGNORECASE)
INTERVAL_STATUS_RE = re.compile(
    r"^((?:[01]\d|2[0-4]):[0-5]\d)\s*[-–]\s*((?:[01]\d|2[0-4]):[0-5]\d)\s*(?:\*\*)?\s*(OFF|ON)\s*(?:\*\*)?$",
    re.IGNORECASE,
)
INTERVAL_ONLY_RE = re.compile(
    r"^((?:[01]\d|2[0-4]):[0-5]\d)\s*[-–]\s*((?:[01]\d|2[0-4]):[0-5]\d)$",
    re.IGNORECASE,
)
STATUS_ONLY_RE = re.compile(r"^(?:\*\*)?\s*(OFF|ON)\s*(?:\*\*)?$", re.IGNORECASE)


def _normalize_line(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def _split_lines(text: str) -> list[str]:
    return [_normalize_line(line) for line in text.splitlines() if _normalize_line(line)]


def _parse_group_lines(lines: list[str]) -> ParsedOblastData:
    outage: dict[str, list[TimeInterval]] = {}
    power_on: dict[str, list[TimeInterval]] = {}
    current_group: Optional[str] = None
    pending_interval: Optional[TimeInterval] = None

    for line in lines:
        group_match = GROUP_RE.match(line)
        if group_match:
            current_group = f"{int(group_match.group(1))}.{int(group_match.group(2))}"
            outage.setdefault(current_group, [])
            power_on.setdefault(current_group, [])
            pending_interval = None
            continue

        interval_match = INTERVAL_STATUS_RE.match(line)
        if interval_match and current_group:
            interval = TimeInterval(start=interval_match.group(1), end=interval_match.group(2))
            status = interval_match.group(3).upper()
            if status == "OFF":
                outage[current_group].append(interval)
            else:
                power_on[current_group].append(interval)
            pending_interval = None
            continue

        interval_only_match = INTERVAL_ONLY_RE.match(line)
        if interval_only_match and current_group:
            pending_interval = TimeInterval(start=interval_only_match.group(1), end=interval_only_match.group(2))
            continue

        status_only_match = STATUS_ONLY_RE.match(line)
        if status_only_match and current_group and pending_interval:
            status = status_only_match.group(1).upper()
            if status == "OFF":
                outage[current_group].append(pending_interval)
            else:
                power_on[current_group].append(pending_interval)
            pending_interval = None

    return ParsedOblastData(outage=outage, power_on=power_on)


def _parse_html_content(html: str) -> ParsedOblastData:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    return _parse_group_lines(_split_lines(text))


def _parse_jina_content(content: str) -> ParsedOblastData:
    return _parse_group_lines(_split_lines(content))


def _is_cloudflare_page(text: str) -> bool:
    lower = text.lower()
    markers = [
        "verifying that you are not a robot",
        "cf-challenge",
        "cf-browser-verification",
        "just a moment",
        "attention required",
        "cloudflare",
    ]
    return any(marker in lower for marker in markers)


def _fetch_alerts_html(url: str) -> tuple[Optional[str], Optional[str]]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept-Language": "uk,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    cookies = {"cf_clearance": ALERTS_CF_CLEARANCE} if ALERTS_CF_CLEARANCE else None
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers, cookies=cookies)
        response.raise_for_status()
        return response.text, None
    except requests.RequestException as exc:
        return None, f"Помилка запиту: {exc}"


def _fetch_jina_mirror(url: str) -> tuple[Optional[str], Optional[str], str]:
    mirror_url = f"https://r.jina.ai/http://{url.removeprefix('https://')}"
    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "uk,en;q=0.9"}
    try:
        response = requests.get(mirror_url, timeout=REQUEST_TIMEOUT, headers=headers)
        response.raise_for_status()
        return response.text, None, mirror_url
    except requests.RequestException as exc:
        return None, f"Помилка fallback-запиту: {exc}", mirror_url


def parse_oblast_page(oblast_key: str) -> ParseResult:
    url = OBLAST_URLS[oblast_key]
    fetched_at = datetime.now()

    html, error = _fetch_alerts_html(url)
    if error:
        return ParseResult(
            data=ParsedOblastData(outage={}, power_on={}),
            fetched_at=fetched_at,
            source_url=url,
            error=error,
        )

    assert html is not None
    parsed = _parse_html_content(html)
    if parsed.outage or parsed.power_on:
        return ParseResult(data=parsed, fetched_at=fetched_at, source_url=url, error=None)

    # Якщо від alerts.org.ua прийшла антибот-сторінка або нестандартна розмітка,
    # пробуємо безпечний текстовий fallback через r.jina.ai для цього ж URL.
    mirror_content, mirror_error, mirror_url = _fetch_jina_mirror(url)
    if mirror_error:
        cloudflare_hint = " alerts.org.ua повертає anti-bot сторінку." if _is_cloudflare_page(html) else ""
        return ParseResult(
            data=ParsedOblastData(outage={}, power_on={}),
            fetched_at=fetched_at,
            source_url=url,
            error=f"Не вдалося розпарсити HTML сторінку.{cloudflare_hint} {mirror_error}".strip(),
        )

    assert mirror_content is not None
    parsed_mirror = _parse_jina_content(mirror_content)
    if parsed_mirror.outage or parsed_mirror.power_on:
        return ParseResult(data=parsed_mirror, fetched_at=fetched_at, source_url=mirror_url, error=None)

    return ParseResult(
        data=ParsedOblastData(outage={}, power_on={}),
        fetched_at=fetched_at,
        source_url=url,
        error="На сторінці не знайдено груп та інтервалів OFF/ON.",
    )


def build_user_graph(oblast_key: str, queue: str, subqueue: str) -> tuple[Optional[UserGraph], Optional[str]]:
    queue_key = f"{int(queue)}.{int(subqueue)}"
    parse_result = parse_oblast_page(oblast_key)
    if parse_result.error:
        return None, parse_result.error

    outage = parse_result.data.outage.get(queue_key, [])
    power_on = parse_result.data.power_on.get(queue_key, [])
    if not outage and not power_on:
        return None, f"Для черги {queue_key} дані на сторінці не знайдено."

    return (
        UserGraph(
            oblast_title=OBLAST_TITLES[oblast_key],
            queue_subqueue=queue_key,
            outage_intervals=outage,
            power_on_intervals=power_on,
        ),
        None,
    )


def format_outage_message(graph: UserGraph) -> str:
    intervals = "\n".join(f"{item.start} – {item.end}" for item in graph.outage_intervals) or "Немає інтервалів."
    return (
        f"🔴 ВІДКЛЮЧЕННЯ СВІТЛА ({graph.oblast_title} · {graph.queue_subqueue})\n\n"
        f"{intervals}\n\n"
        "Джерело: alerts.org.ua"
    )


def format_power_on_message(graph: UserGraph) -> str:
    intervals = "\n".join(f"{item.start} – {item.end}" for item in graph.power_on_intervals) or "Немає інтервалів."
    return (
        f"🟢 СВІТЛО Є ({graph.oblast_title} · {graph.queue_subqueue})\n\n"
        f"{intervals}\n\n"
        "Джерело: alerts.org.ua"
    )
