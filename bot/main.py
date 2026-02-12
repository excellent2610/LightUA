from __future__ import annotations

import asyncio

from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand

from bot.config import BOT_TOKEN
from bot.handlers import router


async def _run() -> None:
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    # Keep only актуальні команди: /start і /all.
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="обрати чергу і підчергу"),
            BotCommand(command="all", description="показати весь графік"),
        ]
    )

    await dp.start_polling(bot)


def main() -> None:
    asyncio.run(_run())

