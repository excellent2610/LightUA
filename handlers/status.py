from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command, StateFilter
from aiogram.types import Message

from db.database import Database
from utils.ads import get_ad_manager
from utils.formatting import format_status_message


def get_router(db: Database) -> Router:
    router = Router(name="status")
    ad_manager = get_ad_manager()

    @router.message(Command("status"), StateFilter("*"))
    async def status_command(message: Message) -> None:
        if message.from_user is None:
            return

        user = await db.get_user(message.from_user.id)
        if user is None:
            await ad_manager.send_with_ad(
                message.bot,
                message.chat.id,
                "Ви ще не зареєстровані. Використайте /register",
            )
            return

        await ad_manager.send_with_ad(
            message.bot,
            message.chat.id,
            format_status_message(user),
        )

    return router
