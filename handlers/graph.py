from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from db.database import db
from services.parser import build_user_graph, format_outage_message, format_power_on_message

router = Router()


@router.message(Command("graph"))
async def cmd_graph(message: Message) -> None:
    user = db.get_user(message.from_user.id)
    if not user:
        await message.answer("Спочатку налаштуйте параметри через /start.")
        return

    graph, error = build_user_graph(user.oblast, user.queue, user.subqueue)
    if error or not graph:
        await message.answer(f"Не вдалося отримати графік: {error or 'невідома помилка'}")
        return

    await message.answer(format_outage_message(graph))
    await message.answer(format_power_on_message(graph))
