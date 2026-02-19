import asyncio

from aiogram import Bot
from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from config import OBLAST_TITLES
from db.database import db

router = Router()
_pending_tests: dict[int, asyncio.Task] = {}


async def _send_test_notification(bot: Bot, chat_id: int) -> None:
    try:
        await asyncio.sleep(5 * 60)
        user = db.get_user(chat_id)
        if not user:
            return
        queue_subqueue = f"{int(user.queue)}.{int(user.subqueue)}"
        location = OBLAST_TITLES.get(user.oblast, user.oblast)
        text = (
            "🧪 ТЕСТ СПОВІЩЕННЯ\n"
            "Це перевірка доставки push-повідомлень.\n\n"
            f"{location} · Черга {queue_subqueue}\n"
            "Затримка: 5 хв"
        )
        await bot.send_message(chat_id=chat_id, text=text)
    finally:
        _pending_tests.pop(chat_id, None)


@router.message(Command("notify_test"))
async def cmd_notify_test(message: Message) -> None:
    user = db.get_user(message.from_user.id)
    if not user:
        await message.answer("Спочатку налаштуйте область/чергу через /start.")
        return

    old_task = _pending_tests.get(message.from_user.id)
    if old_task and not old_task.done():
        old_task.cancel()

    _pending_tests[message.from_user.id] = asyncio.create_task(
        _send_test_notification(message.bot, message.from_user.id)
    )
    await message.answer("🧪 Тест заплановано. Тестове повідомлення прийде через 5 хвилин.")
