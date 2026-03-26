from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from bot.config import ADMINS
from db.database import Database


def get_router(db: Database) -> Router:
    router = Router()

    @router.message(Command("admin"))
    async def admin_stats(message: Message) -> None:
        user = message.from_user
        if user is None or user.id not in ADMINS:
            return  # тихо ігноруємо не-адміністрів

        stats = await db.get_bot_stats()
        text = (
            "📊 Bot statistics:\n\n"
            f"👥 Total users: {stats['total']}\n"
            f"🆕 New today: {stats['new_today']}\n"
            f"🔥 Active (24h): {stats['active_24h']}"
        )
        await message.answer(text)

    return router
