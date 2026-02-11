from __future__ import annotations

import html
import json
import re
from typing import Dict, List, Tuple

QUEUE_RE = re.compile(r"^[1-6]\.[1-2]$")
MAX_MESSAGE_LEN = 3800


def is_valid_queue(value: str) -> bool:
    return bool(QUEUE_RE.fullmatch(value.strip()))


def format_queue_schedule(queue: str, intervals: List[Dict[str, str]]) -> str:
    outage = extract_outage_intervals(intervals)
    if not outage:
        return f"Черга {queue}: інтервали відсутні."
    lines = [f"Черга {queue}:"]
    for start, end in outage:
        lines.append(f"- {start} – {end}")
    if len(lines) == 1:
        lines.append("- інтервали відсутні")
    return "\n".join(lines)


def format_schedule_brief(data: Dict[str, object]) -> str:
    date = str(data.get("date", ""))
    city = str(data.get("city", ""))
    queues = data.get("queues", {})
    queue_count = len(queues) if isinstance(queues, dict) else 0
    return (
        f"Графік на {date}, {city}.\n"
        f"Знайдено черг: {queue_count}.\n"
        "Використай /schedule 1.1 для конкретної черги або /all для повного JSON."
    )


def format_full_json(data: Dict[str, object]) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)


def split_message(text: str, max_len: int = MAX_MESSAGE_LEN) -> List[str]:
    if len(text) <= max_len:
        return [text]
    chunks: List[str] = []
    current = []
    current_len = 0
    for line in text.splitlines(True):
        if current_len + len(line) > max_len and current:
            chunks.append("".join(current))
            current = [line]
            current_len = len(line)
            continue
        current.append(line)
        current_len += len(line)
    if current:
        chunks.append("".join(current))
    return chunks


def format_full_json_chunks(data: Dict[str, object], max_len: int = 3200) -> List[str]:
    payload = format_full_json(data)
    parts = split_message(payload, max_len=max_len)
    total = len(parts)
    messages: List[str] = []
    for idx, part in enumerate(parts, start=1):
        escaped = html.escape(part)
        messages.append(f"<b>Графік JSON {idx}/{total}</b>\n<pre>{escaped}</pre>")
    return messages


def _queue_sort_key(value: str) -> tuple[int, int]:
    left, right = value.split(".", 1)
    return int(left), int(right)


def _display_time(value: str, is_end: bool) -> str:
    return value


def _to_minutes(value: str) -> int:
    hour = int(value[:2])
    minute = int(value[3:])
    if hour == 24:
        return 24 * 60
    return hour * 60 + minute


def _prepare_intervals(intervals_raw: List[Dict[str, str]]) -> List[Tuple[str, str]]:
    prepared: List[Tuple[str, str]] = []
    for item in intervals_raw:
        if not isinstance(item, dict):
            continue
        start = str(item.get("from", "")).strip()
        end = str(item.get("to", "")).strip()
        if not start or not end:
            continue
        prepared.append((start, end))
    return prepared


def extract_outage_intervals(intervals_raw: List[Dict[str, str]]) -> List[Tuple[str, str]]:
    intervals = _prepare_intervals(intervals_raw)
    if not intervals:
        return []
    # Keep exact parser output order/content.
    return [(_display_time(start, False), _display_time(end, True)) for start, end in intervals]


def format_schedule_text_chunks(data: Dict[str, object], max_len: int = 3500) -> List[str]:
    queues_raw = data.get("queues", {})
    if not isinstance(queues_raw, dict) or not queues_raw:
        return ["Години відсутності електропостачання:\n\nДані відсутні."]

    lines: List[str] = ["Інтервали графіка:", ""]
    for queue, intervals_raw in queues_raw.items():
        if not isinstance(intervals_raw, list):
            continue
        rendered = [
            f"{start} – {end}"
            for start, end in extract_outage_intervals(intervals_raw)
        ]
        if not rendered:
            continue
        lines.append(f"{queue}: {', '.join(rendered)}")
        lines.append("")

    lines.append(
        "Перелік адрес, що знеструмлюються по чергах (підчергах) ГПВ можна переглянути "
        "за посиланням https://www.cherkasyoblenergo.com/off"
    )
    lines.append("")
    lines.append(
        "Телеграм бот для сповіщення про відключення світла можна знайти за цим посиланням "
        "@L1ght_UA_bot"
    )

    full_text = "\n".join(lines).strip()
    return split_message(full_text, max_len=max_len)
