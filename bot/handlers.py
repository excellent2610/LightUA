from __future__ import annotations

import asyncio
import json
import os
import re
from typing import Any

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import Message

from bot.formatters import format_off_message, format_on_message  # modified
from bot.notifications import store, sync_user_selection
from bot.regions import (
    DEFAULT_REGION_KEY,
    get_region_config,
    get_region_name,
    list_regions,
    region_supports_subqueues,
)
from core import parser as schedule_parser
from core.region_schedule import load_region_schedule

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
    if not subqueue:
        by_queue = [
            x
            for x in (data if isinstance(data, list) else [])
            if isinstance(x, dict) and str(x.get("queue", "")).strip() == queue
        ]
        if by_queue:
            return _dedupe_intervals(_collect_intervals(by_queue))

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

    # If both sets are present, trust explicit statuses as-is.
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
    return await _load_schedule_data_for_region(DEFAULT_REGION_KEY)


async def _load_schedule_data_for_region(region: str | None) -> Any:
    region_key = (region or DEFAULT_REGION_KEY).strip().lower()
    from bot.config import get_schedule_url_for_region

    region_url = get_schedule_url_for_region(region_key)

    def _sync() -> Any:
        try:
            direct = load_region_schedule(region_key)
            if isinstance(direct, list) and direct:
                return direct
        except Exception:
            pass

        # Force the exact selected oblast page URL.
        os.environ["SCHEDULE_URL"] = region_url
        for fn_name in ("get_off_schedule", "get_data", "get_schedule", "fetch_and_parse_schedule"):
            fn = getattr(schedule_parser, fn_name, None)
            if callable(fn):
                try:
                    return fn(url=region_url, timeout=35)
                except TypeError:
                    pass
                try:
                    return fn(url=region_url)
                except TypeError:
                    pass
                try:
                    return fn(region=region_key, url=region_url)
                except TypeError:
                    pass
                try:
                    return fn(region=region_key)
                except TypeError:
                    pass
                try:
                    return fn(region_url)
                except TypeError:
                    pass
                return fn()
        raise RuntimeError("No schedule loader found")

    return await asyncio.to_thread(_sync)


async def _finalize_selection(message: Message, user_id: int, region: str, queue: str, subqueue: str) -> None:
    await message.answer("Отримую графік, зачекайте...")
    data = await asyncio.wait_for(_load_schedule_data_for_region(region), timeout=35)
    intervals = _extract_for_queue_subqueue(data, queue, subqueue)
    off_intervals, on_intervals = _split_off_on_intervals(intervals)
    queue_label = f"{queue}.{subqueue}" if subqueue else queue
    city_label = f"{get_region_name(region)} · {queue_label}"
    if off_intervals:
        await message.answer(format_off_message(off_intervals, city=city_label))
    if on_intervals:
        await message.answer(format_on_message(on_intervals, city=city_label))
    if not off_intervals and not on_intervals:
        await message.answer("🟢 СЬОГОДНІ ВІДКЛЮЧЕНЬ НЕМАЄ")
    await sync_user_selection(user_id, queue, subqueue, region=region)
    _USER_STATE.pop(user_id, None)


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else 0
    user = await store.get_user(user_id)
    current_region = (user.region or "").strip().lower()

    _USER_STATE[user_id] = {"stage": "await_region"}
    lines = ["Оберіть область:"]
    if current_region:
        lines.insert(0, f"Поточна область: {get_region_name(current_region)}")
    for idx, (_, cfg) in enumerate(list_regions(), start=1):
        lines.append(f"{idx}. {cfg['name']}")
    await message.answer("\n".join(lines))


@router.message(Command("region"))
async def cmd_region(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else 0
    user = await store.get_user(user_id)
    current = user.region or DEFAULT_REGION_KEY
    lines = [f"Поточна область: {get_region_name(current)}", "", "Оберіть нову область:"]
    for idx, (_, cfg) in enumerate(list_regions(), start=1):
        lines.append(f"{idx}. {cfg['name']}")
    _USER_STATE[user_id] = {"stage": "await_region", "region_change": "1"}
    await message.answer("\n".join(lines))


@router.message(Command("all"))
async def cmd_all(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else 0
    user = await store.get_user(user_id)
    region = user.region or DEFAULT_REGION_KEY
    region_name = get_region_name(region)

    try:
        data = await asyncio.wait_for(_load_schedule_data_for_region(region), timeout=35)
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

    sections: list[str] = [f"🔴 ПОВНИЙ ГРАФІК ВІДКЛЮЧЕНЬ ({region_name})"]
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
            region = state.get("region") or DEFAULT_REGION_KEY
            if not _NUM_RE.fullmatch(text):
                await message.answer("Невірний формат черги. Введіть число, наприклад: 4")
                return
            state["queue"] = text
            if region_supports_subqueues(region):
                state["stage"] = "await_subqueue"
                await message.answer(f"Черга {text} збережена.\nВведіть номер підчерги (наприклад: 1):")
            else:
                await _finalize_selection(message, user_id, region, text, "")
            return

        if stage == "await_region":
            regions = list_regions()
            selected_key: str | None = None
            if _NUM_RE.fullmatch(text):
                idx = int(text)
                if 1 <= idx <= len(regions):
                    selected_key = regions[idx - 1][0]
            else:
                normalized = text.strip().lower()
                if normalized in dict(regions):
                    selected_key = normalized
            if not selected_key:
                await message.answer("Невірний вибір області. Введіть номер зі списку.")
                return

            current_user = await store.get_user(user_id)
            previous_region = (current_user.region or "").strip().lower()
            await store.update_settings(user_id, region=selected_key)
            if previous_region and previous_region != selected_key:
                await store.update_settings(user_id, queue="", subqueue="")
            state["region"] = selected_key
            state["stage"] = "await_queue"
            await message.answer(
                f"Область збережено: {get_region_name(selected_key)}\n"
                "Введіть номер черги (наприклад: 1):"
            )
            return

        if stage == "await_subqueue":
            region = state.get("region") or DEFAULT_REGION_KEY
            cfg = get_region_config(region)
            requires_subqueue = region_supports_subqueues(region)
            if requires_subqueue:
                if not _NUM_RE.fullmatch(text):
                    await message.answer("Невірний формат підчерги. Введіть число, наприклад: 1")
                    return
                subqueue_num = int(text)
                allowed_subqueues = cfg.get("subqueues") or []
                if allowed_subqueues and subqueue_num not in allowed_subqueues:
                    allowed = ", ".join(str(x) for x in allowed_subqueues)
                    await message.answer(f"Для цієї області доступні підчерги: {allowed}")
                    return
                subqueue = text
            else:
                subqueue = ""

            queue = state.get("queue", "")
            await _finalize_selection(message, user_id, region, queue, subqueue)
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
