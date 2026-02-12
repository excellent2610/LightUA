"""Formatting utilities for Telegram responses."""

from __future__ import annotations

import re
from typing import Any

SOURCE_LABEL = "alerts.org.ua"


def _interval_parts(item: Any) -> tuple[str | None, str | None, str | None]:
    if isinstance(item, dict):
        return item.get("start"), item.get("end"), item.get("status")
    return getattr(item, "start", None), getattr(item, "end", None), getattr(item, "status", None)


def _collect_off_lines(intervals: list[Any]) -> list[str]:
    lines: list[str] = []
    for item in intervals or []:
        start, end, status = _interval_parts(item)
        if str(status).upper() != "OFF":
            continue
        if not start or not end:
            continue
        lines.append(f"{start} – {end}")
    return lines


def is_valid_queue(value: Any) -> bool:
    """Legacy compatibility validator used by handlers."""
    if value is None:
        return False
    text = str(value).strip()
    # Queue format expected by handlers, e.g. "1.1", "4.2".
    return bool(re.fullmatch(r"\d+\.\d+", text))


# Changed formatter to display only OFF blocks and alerts.org.ua source.
def format_off_message(intervals: list[Any], city: str = "Черкаси") -> str:
    lines = _collect_off_lines(intervals)
    if not lines:
        return "🟢 Сьогодні відключень не заплановано"

    body = "\n".join(lines)
    return f"🔴 ВІДКЛЮЧЕННЯ СВІТЛА ({city})\n\n{body}\n\nДжерело: {SOURCE_LABEL}"


# Compatibility aliases for existing handlers.
def format_schedule(intervals: list[Any], city: str = "Черкаси") -> str:
    return format_off_message(intervals=intervals, city=city)


def format_outages(intervals: list[Any], city: str = "Черкаси") -> str:
    return format_off_message(intervals=intervals, city=city)


def build_off_message(intervals: list[Any], city: str = "Черкаси") -> str:
    return format_off_message(intervals=intervals, city=city)


def format_off_schedule(intervals: list[Any], city: str = "Черкаси") -> str:
    return format_off_message(intervals=intervals, city=city)


def format_blackouts(intervals: list[Any], city: str = "Черкаси") -> str:
    return format_off_message(intervals=intervals, city=city)


def __getattr__(name: str) -> Any:
    """Compatibility fallback for legacy formatter API names."""
    if name.startswith("format_") or name.endswith("_message") or name.startswith("build_"):
        return format_off_message
    if name.startswith("is_") or name.startswith("validate_"):
        return is_valid_queue
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
