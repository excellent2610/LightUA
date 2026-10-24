from __future__ import annotations

import re
from datetime import datetime
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
        quiet = f"{format_time(user.quiet_start)}–{format_time(user.quiet_end)}"
    else:
        quiet = "не налаштовано"

    return (
        "⚙️ Твій профіль\n\n"
        f"📍 {region_name}\n"
        f"🔌 Черга: {user.queue}.{user.subqueue}\n"
        f"🔔 Сповіщення: {notifications}\n"
        f"🌙 Тиха година: {quiet}"
    )


def format_graph_message(schedule: "DaySchedule", current_state: str, now: datetime | None = None) -> str:
    region_name = normalize_region_label(schedule.region_name)
    today = now.date() if now else None
    schedule_day = schedule.schedule_date
    day_label = (
        schedule_day.strftime("%d.%m.%Y")
        if schedule_day is not None and today is not None and schedule_day != today
        else "сьогодні"
    )
    lines = [f"☀️ Світло {day_label}", "", f"📍 {region_name} · Черга {schedule.queue}.{schedule.subqueue}"]

    if not schedule.intervals:
        lines.extend(["", "ℹ️ Інтервали графіка відсутні."])
    else:
        lines.extend(["", "🟢 Світло є"])
        for interval in schedule.intervals:
            end_time = "24:00" if interval.ends_at_midnight else interval.end.strftime("%H:%M")
            if interval.state == "on":
                lines.append(f"🟢 {interval.start.strftime('%H:%M')}–{end_time}")
        lines.extend(["", "🔴 Без світла"])
        for interval in schedule.intervals:
            end_time = "24:00" if interval.ends_at_midnight else interval.end.strftime("%H:%M")
            if interval.state == "off":
                lines.append(f"🔴 {interval.start.strftime('%H:%M')}–{end_time}")

    if current_state in {"on", "off"}:
        status = "⚡ За графіком зараз світло є" if current_state == "on" else "⚡ За графіком зараз світла немає"
        lines.extend(["", status])
        if now is not None:
            upcoming = []
            for interval in schedule.intervals:
                if interval.state == current_state:
                    continue
                boundary = interval.start
                candidate = now.replace(hour=boundary.hour, minute=boundary.minute, second=0, microsecond=0)
                if candidate > now:
                    upcoming.append(candidate)
            if upcoming:
                next_event = min(upcoming)
                action = "відключення" if current_state == "on" else "відновлення світла"
                lines.append(f"Найближче {action} — о {next_event:%H:%M}.")
    lines.extend(["", "Джерело: alerts.org.ua"])
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
