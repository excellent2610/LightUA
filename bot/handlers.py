from __future__ import annotations

import asyncio
import json
import re
from typing import Any

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from bot.formatters import format_off_message
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


def _dedupe_off(intervals: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str, str]] = set()
    out: list[dict[str, str]] = []
    for it in intervals:
        if str(it.get("status", "")).upper() != "OFF":
            continue
        key = (str(it.get("start", "")), str(it.get("end", "")), "OFF")
        if key in seen:
            continue
        seen.add(key)
        out.append({"start": key[0], "end": key[1], "status": "OFF"})
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
            return _dedupe_off(_collect_intervals(tagged))

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
        found = _dedupe_off(_collect_intervals(matched_nodes))
        if found:
            return found
    return []


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


@router.message()
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
            await message.answer(format_off_message(intervals, city=f"Черкаси · {queue}.{subqueue}"))
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
