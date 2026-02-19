import re

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from db.database import db

router = Router()
QUIET_RANGE_RE = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)-([01]\d|2[0-3]):([0-5]\d)$")


@router.message(Command("quiet_hours"))
async def cmd_quiet_hours(message: Message) -> None:
    user = db.get_user(message.from_user.id)
    if not user:
        await message.answer("Спочатку налаштуйте область/чергу через /start.")
        return

    command_text = (message.text or "").strip()
    parts = command_text.split(maxsplit=1)
    if len(parts) == 1:
        await message.answer("Формат: /quiet_hours 22:00-07:00 | /quiet_hours off | /quiet_hours on")
        return

    arg = parts[1].strip().lower()
    if arg == "off":
        db.set_quiet_hours(message.from_user.id, None, None)
        await message.answer("Тиха година вимкнена.")
        return

    if arg == "on":
        if user.quiet_hours_from and user.quiet_hours_to:
            await message.answer(f"Тиха година активна: {user.quiet_hours_from}-{user.quiet_hours_to}")
        else:
            db.set_quiet_hours(message.from_user.id, "22:00", "07:00")
            await message.answer("Тиха година увімкнена з дефолтним інтервалом 22:00-07:00.")
        return

    match = QUIET_RANGE_RE.fullmatch(arg)
    if not match:
        await message.answer("Некоректний формат. Приклад: /quiet_hours 22:00-07:00")
        return

    time_from = f"{match.group(1)}:{match.group(2)}"
    time_to = f"{match.group(3)}:{match.group(4)}"
    db.set_quiet_hours(message.from_user.id, time_from, time_to)
    await message.answer(f"Тиха година збережена: {time_from}-{time_to}")


@router.message(Command("quiet_hours_on"))
async def cmd_quiet_hours_on(message: Message) -> None:
    user = db.get_user(message.from_user.id)
    if not user:
        await message.answer("Спочатку налаштуйте область/чергу через /start.")
        return
    if user.quiet_hours_from and user.quiet_hours_to:
        await message.answer(f"Тиха година активна: {user.quiet_hours_from}-{user.quiet_hours_to}")
    else:
        db.set_quiet_hours(message.from_user.id, "22:00", "07:00")
        await message.answer("Тиха година увімкнена з дефолтним інтервалом 22:00-07:00.")


@router.message(Command("quiet_hours_off"))
async def cmd_quiet_hours_off(message: Message) -> None:
    user = db.get_user(message.from_user.id)
    if not user:
        await message.answer("Спочатку налаштуйте область/чергу через /start.")
        return
    db.set_quiet_hours(message.from_user.id, None, None)
    await message.answer("Тиха година вимкнена.")
