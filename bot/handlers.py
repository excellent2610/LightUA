from __future__ import annotations

import asyncio
import json
import re
from typing import Any

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import Message

from bot.formatters import format_off_message, format_on_message  # modified
from bot.notifications import sync_user_selection
from core import parser as schedule_parser

router = Router()

# Simple dialog state: /start -> queue -> subqueue -> answer.
_USER_STATE: dict[int, dict[str, str]] = {}
_NUM_RE = re.compile(r"^\d+$")


def _is_interval(item: Any) -> bool:
    return isinstance(item, dict) and "start" in item and "end" in item and "status" in item


def _collect_intervals(node: Any) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    if isinstance(node, list):
        for x in node:
            result.extend(_collect_intervals(x))
        return result
    if isinstance(node, dict):
        if _is_interval(node):
            result.append(
                {
                    "start": str(node.get("start", "")),
                    "end": str(node.get("end", "")),
                    "status": str(node.get("status", "")).upper(),
                }
            )
            return result
        for value in node.values():
            result.extend(_collect_intervals(value))
    return result


def _dedupe_intervals(intervals: list[dict[str, str]]) -> list[dict[str, str]]:  # added
    seen: set[tuple[str, str, str]] = set()
    out: list[dict[str, str]] = []
    for it in intervals:
        status = str(it.get("status", "")).upper()
        if status not in {"OFF", "ON"}:
            continue
        key = (str(it.get("start", "")), str(it.get("end", "")), status)
        if key in seen:
            continue
        seen.add(key)
        out.append({"start": key[0], "end": key[1], "status": status})
    return out


def _extract_for_queue_subqueue(data: Any, queue: str, subqueue: str) -> list[dict[str, str]]:
    if isinstance(data, list):
        tagged = [
            x
            for x in data
            if isinstance(x, dict)
            and str(x.get("queue", "")).strip() == queue
            and str(x.get("subqueue", "")).strip() == subqueue
        ]
        if tagged:
            return _dedupe_intervals(_collect_intervals(tagged))  # modified

    # Best-effort matching in nested JSON.
    targets = {f"{queue}.{subqueue}", f"{queue}-{subqueue}", f"{queue}_{subqueue}", f"{queue}{subqueue}"}
    matched_nodes: list[Any] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            q = str(node.get("queue", "")).strip()
            sq = str(node.get("subqueue", "")).strip()
            if q == queue and sq == subqueue:
                matched_nodes.append(node)

            for k, v in node.items():
                ks = str(k).strip()
                if ks in targets:
                    matched_nodes.append(v)
                if ks == queue and isinstance(v, dict):
                    if subqueue in v:
                        matched_nodes.append(v[subqueue])
                    dot = f"{queue}.{subqueue}"
                    if dot in v:
                        matched_nodes.append(v[dot])
                walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(data)
    if matched_nodes:
        found = _dedupe_intervals(_collect_intervals(matched_nodes))  # modified
        if found:
            return found
    return []


def _to_minutes(value: str) -> int | None:  # added
    try:
        hh, mm = value.split(":", 1)
        h = int(hh)
        m = int(mm)
    except Exception:
        return None
    if h == 24 and m == 0:
        return 24 * 60
    if 0 <= h <= 23 and 0 <= m <= 59:
        return h * 60 + m
    return None


def _to_hhmm(total_minutes: int) -> str:  # added
    if total_minutes <= 0:
        return "00:00"
    if total_minutes >= 24 * 60:
        return "24:00"
    return f"{total_minutes // 60:02d}:{total_minutes % 60:02d}"


def _split_off_on_intervals(intervals: list[dict[str, str]]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:  # added
    off_intervals: list[dict[str, str]] = []
    on_intervals: list[dict[str, str]] = []

    for it in intervals:
        status = str(it.get("status", "")).upper()
        if status == "OFF":
            off_intervals.append({"start": str(it.get("start", "")), "end": str(it.get("end", "")), "status": "OFF"})
        elif status == "ON":
            on_intervals.append({"start": str(it.get("start", "")), "end": str(it.get("end", "")), "status": "ON"})

    # If parser returned only OFF, build ON as complement for 00:00-24:00.  # added
    if on_intervals:
        return _dedupe_intervals(off_intervals), _dedupe_intervals(on_intervals)

    off_ranges: list[tuple[int, int]] = []
    for it in off_intervals:
        start_m = _to_minutes(it["start"])
        end_m = _to_minutes(it["end"])
        if start_m is None or end_m is None or start_m >= end_m:
            continue
        off_ranges.append((start_m, end_m))

    if not off_ranges:
        return [], [{"start": "00:00", "end": "24:00", "status": "ON"}]

    off_ranges.sort()
    merged: list[tuple[int, int]] = []
    for start_m, end_m in off_ranges:
        if not merged or start_m > merged[-1][1]:
            merged.append((start_m, end_m))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end_m))

    cursor = 0
    inferred_on: list[dict[str, str]] = []
    for start_m, end_m in merged:
        if cursor < start_m:
            inferred_on.append({"start": _to_hhmm(cursor), "end": _to_hhmm(start_m), "status": "ON"})
        cursor = max(cursor, end_m)
    if cursor < 24 * 60:
        inferred_on.append({"start": _to_hhmm(cursor), "end": "24:00", "status": "ON"})

    return _dedupe_intervals(off_intervals), inferred_on


async def _load_schedule_data() -> Any:
    def _sync() -> Any:
        for fn_name in ("get_data", "get_schedule", "fetch_and_parse_schedule", "get_off_schedule"):
            fn = getattr(schedule_parser, fn_name, None)
            if callable(fn):
                return fn()
        raise RuntimeError("No schedule loader found")

    return await asyncio.to_thread(_sync)


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else 0
    _USER_STATE[user_id] = {"stage": "await_queue"}
    await message.answer("Бот активний.\nВведіть номер черги (наприклад: 1):")


@router.message(Command("all"))
async def cmd_all(message: Message) -> None:
    try:
        data = await asyncio.wait_for(_load_schedule_data(), timeout=35)
    except Exception:
        await message.answer("Не вдалося отримати графік із alerts.org.ua")
        return

    if not isinstance(data, list):
        text = json.dumps(data, ensure_ascii=False, indent=2) if isinstance(data, dict) else str(data)
        if len(text) > 3900:
            text = text[:3900] + "\n..."
        await message.answer(f"Повний графік:\n```json\n{text}\n```", parse_mode="Markdown")
        return

    groups: dict[str, list[dict[str, str]]] = {}
    unknown: list[dict[str, str]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        if str(item.get("status", "")).upper() != "OFF":
            continue
        q = str(item.get("queue", "")).strip()
        sq = str(item.get("subqueue", "")).strip()
        if q and sq:
            key = f"{q}.{sq}"
            groups.setdefault(key, []).append(item)
        else:
            unknown.append(item)

    if not groups and not unknown:
        await message.answer("🟢 Сьогодні відключень не заплановано")
        return

    def render_lines(items: list[dict[str, str]]) -> list[str]:
        lines: list[str] = []
        seen: set[tuple[str, str]] = set()
        for it in items:
            start = str(it.get("start", ""))
            end = str(it.get("end", ""))
            if not start or not end:
                continue
            key = (start, end)
            if key in seen:
                continue
            seen.add(key)
            lines.append(f"{start} – {end}")
        return lines

    sections: list[str] = ["🔴 ПОВНИЙ ГРАФІК ВІДКЛЮЧЕНЬ (Черкаси)"]
    for key in sorted(groups.keys(), key=lambda x: tuple(map(int, x.split(".")))):
        lines = render_lines(groups[key])
        if not lines:
            continue
        sections.append(f"\nЧерга {key}:\n" + "\n".join(lines))

    if unknown:
        lines = render_lines(unknown)
        if lines:
            sections.append("\nБез визначеної черги:\n" + "\n".join(lines))

    sections.append("\nДжерело: alerts.org.ua")
    text = "\n".join(sections)
    if len(text) > 3900:
        text = text[:3900] + "\n..."
    await message.answer(text)


@router.message(F.text & ~F.text.startswith("/"))
async def on_text(message: Message) -> None:
    if not message.text:
        return

    user_id = message.from_user.id if message.from_user else 0
    state = _USER_STATE.get(user_id)
    if not state:
        return

    text = message.text.strip()
    stage = state.get("stage", "")

    try:
        if stage == "await_queue":
            if not _NUM_RE.fullmatch(text):
                await message.answer("Невірний формат черги. Введіть число, наприклад: 4")
                return
            state["queue"] = text
            state["stage"] = "await_subqueue"
            await message.answer(f"Черга {text} збережена.\nВведіть номер підчерги (наприклад: 1):")
            return

        if stage == "await_subqueue":
            if not _NUM_RE.fullmatch(text):
                await message.answer("Невірний формат підчерги. Введіть число, наприклад: 1")
                return

            queue = state.get("queue", "")
            subqueue = text
            await message.answer("Отримую графік, зачекайте...")

            data = await asyncio.wait_for(_load_schedule_data(), timeout=35)
            intervals = _extract_for_queue_subqueue(data, queue, subqueue)
            off_intervals, on_intervals = _split_off_on_intervals(intervals)  # added
            city_label = f"Черкаси · {queue}.{subqueue}"  # added
            await message.answer(format_off_message(off_intervals, city=city_label))  # modified
            await message.answer(format_on_message(on_intervals, city=city_label))  # added
            await sync_user_selection(user_id, queue, subqueue)
            _USER_STATE.pop(user_id, None)
            return

        # Unknown stale stage -> reset safely.
        _USER_STATE.pop(user_id, None)
        await message.answer("Сесію скинуто. Натисніть /start")
    except asyncio.TimeoutError:
        _USER_STATE.pop(user_id, None)
        await message.answer("Сервер довго відповідає. Спробуйте ще раз: /start")
    except Exception:
        _USER_STATE.pop(user_id, None)
        await message.answer("Сталася помилка. Спробуйте ще раз: /start")
