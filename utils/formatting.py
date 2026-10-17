from __future__ import annotations

import re
from typing import TYPE_CHECKING

from db.models import User
from utils.time_utils import format_time

if TYPE_CHECKING:
    from parsing.alerts_parser import DaySchedule


def format_light_change_message(state: str, region_name: str, queue: int, subqueue: int) -> str:
    region_name = normalize_region_label(region_name)
    if state == "off":
        return f"🔴 ВІДКЛЮЧЕННЯ СВІТЛА ({region_name} · {queue}.{subqueue})"
    return f"🟢 СВІТЛО Є ({region_name} · {queue}.{subqueue})"


def format_status_message(user: User) -> str:
    region_name = normalize_region_label(user.region_name)
    notifications = "увімкнені" if user.notifications_enabled else "вимкнені"
    if user.quiet_start and user.quiet_end:
        quiet = f"{format_time(user.quiet_start)} - {format_time(user.quiet_end)}"
    else:
        quiet = "не налаштовано"

    return (
        "⚙️ Поточний профіль\n"
        f"Область: {region_name}\n"
        f"Черга: {user.queue}\n"
        f"Підчерга: {user.subqueue}\n"
        f"Сповіщення: {notifications}\n"
        f"Тиха година: {quiet}"
    )


def format_graph_message(schedule: "DaySchedule", current_state: str) -> str:
    region_name = normalize_region_label(schedule.region_name)
    lines = [
        f"📊 Графік на сьогодні ({region_name} · {schedule.queue}.{schedule.subqueue})",
    ]

    if not schedule.intervals:
        lines.append("Дані про інтервали відсутні.")
    else:
        for interval in schedule.intervals:
            emoji = "🔴" if interval.state == "off" else "🟢"
            label = "Без світла" if interval.state == "off" else "Світло є"
            end_time = "24:00" if interval.ends_at_midnight else interval.end.strftime("%H:%M")
            lines.append(f"{emoji} {interval.start.strftime('%H:%M')} - {end_time} ({label})")

    lines.append("")
    lines.append(f"Поточний стан: {format_light_change_message(current_state, region_name, schedule.queue, schedule.subqueue)}")
    return "\n".join(lines)


def normalize_region_label(value: str) -> str:
    cleaned = " ".join((value or "").replace("\u00A0", " ").split())
    cleaned = re.sub(r"[^\w\s\u0400-\u04FF\-']", " ", cleaned, flags=re.UNICODE)
    cleaned = re.sub(r"\b\d+\s*(?:г|год|год\.)\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -|")

    if not cleaned:
        return value or "Невідома область"

    oblast_match = re.search(
        r"([А-ЯІЇЄҐA-Z][а-яіїєґa-z'\-]+(?:\s+[А-ЯІЇЄҐA-Z][а-яіїєґa-z'\-]+)*)\s+область\b",
        cleaned,
        flags=re.IGNORECASE,
    )
    if oblast_match:
        # Keep the exact region phrase, discard title noise around it.
        phrase = f"{oblast_match.group(1)} область"
        return " ".join(part.capitalize() if part.islower() else part for part in phrase.split())

    kyiv_match = re.search(r"\b(київ|м київ|місто київ)\b", cleaned, flags=re.IGNORECASE)
    if kyiv_match:
        return "Київ"

    parts = cleaned.split(" ")
    deduped: list[str] = []
    for part in parts:
        if deduped and deduped[-1].lower() == part.lower():
            continue
        deduped.append(part)
    return " ".join(deduped)
