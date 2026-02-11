from __future__ import annotations

import asyncio
from typing import Dict

from aiogram import Router
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import Message

from bot.config import SCHEDULE_URL
from bot.formatters import (
    format_full_json_chunks,
    format_schedule_text_chunks,
    format_queue_schedule,
    format_schedule_brief,
    is_valid_queue,
)
from core.parser import get_schedule
from errors.data import InvalidTimeFormatError, ScheduleNotFoundError
from errors.network import NetworkError
from errors.parsing import ParseError

router = Router()
_user_queue: Dict[int, str] = {}


async def _load_schedule() -> Dict[str, object]:
    return await asyncio.to_thread(get_schedule, SCHEDULE_URL)


@router.message(CommandStart())
async def start_handler(message: Message) -> None:
    await message.answer(
        "Бот активний.\n"
        "Команди:\n"
        "/queue 1.1 - зберегти свою чергу\n"
        "/schedule - графік для збереженої черги\n"
        "/schedule 1.1 - графік для конкретної черги\n"
        "/all - години відключення для всіх черг\n"
        "/json - повний JSON-графік"
    )


@router.message(Command("queue"))
async def queue_handler(message: Message, command: CommandObject) -> None:
    queue = (command.args or "").strip()
    if not is_valid_queue(queue):
        await message.answer("Невірний формат черги. Приклад: /queue 1.1")
        return
    if not message.from_user:
        await message.answer("Не вдалося визначити користувача.")
        return
    _user_queue[message.from_user.id] = queue
    await message.answer(f"Чергу збережено: {queue}")


@router.message(Command("schedule"))
async def schedule_handler(message: Message, command: CommandObject) -> None:
    queue = (command.args or "").strip()
    if not queue and message.from_user:
        queue = _user_queue.get(message.from_user.id, "")
    if queue and not is_valid_queue(queue):
        await message.answer("Невірний формат черги. Приклад: /schedule 1.1")
        return
    try:
        data = await _load_schedule()
    except (NetworkError, ParseError, ScheduleNotFoundError, InvalidTimeFormatError) as exc:
        await message.answer(f"Помилка парсингу: {exc}")
        return
    except Exception as exc:
        await message.answer(f"Неочікувана помилка: {exc}")
        return

    queues = data.get("queues", {})
    if not isinstance(queues, dict):
        await message.answer("Дані графіка пошкоджені.")
        return

    if not queue:
        await message.answer(format_schedule_brief(data))
        return

    intervals = queues.get(queue)
    if not isinstance(intervals, list):
        await message.answer(f"Чергу {queue} не знайдено в поточному графіку.")
        return
    await message.answer(format_queue_schedule(queue, intervals))


@router.message(Command("all"))
async def all_handler(message: Message) -> None:
    try:
        data = await _load_schedule()
    except (NetworkError, ParseError, ScheduleNotFoundError, InvalidTimeFormatError) as exc:
        await message.answer(f"Помилка парсингу: {exc}")
        return
    except Exception as exc:
        await message.answer(f"Неочікувана помилка: {exc}")
        return

    for chunk in format_schedule_text_chunks(data):
        await message.answer(chunk)


@router.message(Command("json"))
async def json_handler(message: Message) -> None:
    try:
        data = await _load_schedule()
    except (NetworkError, ParseError, ScheduleNotFoundError, InvalidTimeFormatError) as exc:
        await message.answer(f"Помилка парсингу: {exc}")
        return
    except Exception as exc:
        await message.answer(f"Неочікувана помилка: {exc}")
        return

    for chunk in format_full_json_chunks(data):
        await message.answer(chunk, parse_mode="HTML")
