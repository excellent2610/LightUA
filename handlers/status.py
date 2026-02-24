from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from db.database import Database
from utils.formatting import format_status_message


def get_router(db: Database) -> Router:
    router = Router(name="status")

    @router.message(Command("status"))
    async def status_command(message: Message) -> None:
        if message.from_user is None:
            return

        user = await db.get_user(message.from_user.id)
        if user is None:
            await message.answer("Ви ще не зареєстровані. Використайте /register")
            return

        await message.answer(format_status_message(user))

    return router
