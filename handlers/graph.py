from __future__ import annotations

import asyncio
import logging
import re

from aiogram import Router
from aiogram.filters import Command, StateFilter
from aiogram.types import Message

from db.database import Database
from parsing.alerts_parser import (
    AlertsParser,
    EmergencyOutageError,
    RegionUnavailableError,
    ScheduleNotFoundError,
)
from utils.ads import AdManager, get_ad_manager
from utils.formatting import format_graph_message
from utils.time_utils import now_kyiv

logger = logging.getLogger(__name__)

_HEADER_RE = re.compile(r"^📊\s+Графік на сьогодні\s+\((.+)\)$")
_STATUS_RE = re.compile(r"^Поточний стан:\s*([🟢🔴])")
_STATUS_FULL_RE = re.compile(r"^Поточний стан:\s*(.+)$")
_RANGE_RE = re.compile(r"^([🟢🔴])\s+(\d{2}:\d{2})\s*[-–]\s*(\d{2}:\d{2})")


def _build_compact_graph_messages(full_graph_text: str, current_state: object) -> list[str]:
    lines = [line.strip() for line in full_graph_text.splitlines()]
    profile_label: str | None = None
    current_emoji: str | None = None
    current_status_line: str | None = None
    green_ranges: list[str] = []
    red_ranges: list[str] = []

    for line in lines:
        if not line:
            continue

        header_match = _HEADER_RE.match(line)
        if header_match:
            profile_label = header_match.group(1)
            continue

        status_match = _STATUS_RE.match(line)
        if status_match:
            current_emoji = status_match.group(1)
            continue

        status_full_match = _STATUS_FULL_RE.match(line)
        if status_full_match:
            current_status_line = status_full_match.group(1).strip()
            continue

        range_match = _RANGE_RE.match(line)
        if range_match:
            emoji, start, end = range_match.groups()
            slot = f"{start} – {end}"
            if emoji == "🟢":
                green_ranges.append(slot)
            else:
                red_ranges.append(slot)

    if not profile_label:
        return []

    if current_emoji not in {"🟢", "🔴"}:
        state_text = str(current_state).lower()
        current_emoji = "🔴" if "off" in state_text or "без" in state_text else "🟢"

    def build_message(is_green: bool, ranges: list[str]) -> str | None:
        if not ranges:
            return None
        title = "🟢 СВІТЛО Є" if is_green else "🔴 ВІДКЛЮЧЕННЯ СВІТЛА"
        return f"{title} ({profile_label})\n\n" + "\n".join(ranges) + "\n\nДжерело: alerts.org.ua"

    result = []
    green_message = build_message(True, green_ranges)
    red_message = build_message(False, red_ranges)
    if green_message is not None:
        result.append(green_message)
    if red_message is not None:
        result.append(red_message)

    if current_status_line is None:
        state_label = "СВІТЛО Є" if current_emoji == "🟢" else "ВІДКЛЮЧЕННЯ СВІТЛА"
        current_status_line = f"{current_emoji} {state_label} ({profile_label})"
    result.append(f"Поточний стан: {current_status_line}\n\nДжерело: alerts.org.ua")

    return result


async def send_graph_for_user(
    *,
    message: Message,
    db: Database,
    parser: AlertsParser,
    telegram_id: int,
    ad_manager: AdManager | None = None,
    compact: bool = False,
) -> None:
    if ad_manager is None:
        ad_manager = get_ad_manager()

    user = await db.get_user(telegram_id)
    if user is None:
        await ad_manager.send_with_ad(
            message.bot,
            message.chat.id,
            "Ви ще не зареєстровані. Використайте /start для початку.",
        )
        return

    try:
        schedule = await asyncio.to_thread(
            parser.get_schedule,
            user.region_slug,
            user.queue,
            user.subqueue,
        )
        schedule.region_name = user.region_name
        current_state = schedule.state_at(now_kyiv())
        full_graph_text = format_graph_message(schedule=schedule, current_state=current_state, now=now_kyiv())
        if compact:
            compact_messages = _build_compact_graph_messages(full_graph_text=full_graph_text, current_state=current_state)
            if compact_messages:
                for text in compact_messages:
                    await ad_manager.send_with_ad(message.bot, message.chat.id, text)
                return
        await ad_manager.send_with_ad(message.bot, message.chat.id, full_graph_text)
    except EmergencyOutageError:
        await ad_manager.send_with_ad(
            message.bot,
            message.chat.id,
            "⚠️ Зафіксовано аварійні відключення. Плановий графік тимчасово може не відповідати фактичному стану.",
        )
    except ScheduleNotFoundError:
        await ad_manager.send_with_ad(
            message.bot,
            message.chat.id,
            "ℹ️ На сьогодні графік для вибраної області/черги не знайдено.\n"
            "Ймовірно, планові відключення не застосовуються або дані ще не опубліковані.",
        )
    except RegionUnavailableError:
        await ad_manager.send_with_ad(
            message.bot,
            message.chat.id,
            "⚠️ Сторінка області тимчасово недоступна. Спробуйте пізніше.",
        )
    except Exception:
        logger.exception("Unexpected parser failure in /graph")
        await ad_manager.send_with_ad(
            message.bot,
            message.chat.id,
            "Сталася помилка під час отримання графіка. Спробуйте пізніше.",
        )


def get_router(db: Database, parser: AlertsParser) -> Router:
    router = Router(name="graph")
    ad_manager = get_ad_manager()

    @router.message(Command("graph"), StateFilter("*"))
    async def graph_command(message: Message) -> None:
        if message.from_user is None:
            return

        await send_graph_for_user(
            message=message,
            db=db,
            parser=parser,
            telegram_id=message.from_user.id,
            ad_manager=ad_manager,
            compact=True,
        )

    return router
