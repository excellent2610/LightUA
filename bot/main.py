from __future__ import annotations

import asyncio

from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand

from bot.config import BOT_TOKEN
from bot.handlers import router
from bot.notifications import notification_service, notifications_router


async def _run() -> None:
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    dp.include_router(notifications_router)

    async def _on_startup(*_: object, bot: Bot, **__: object) -> None:
        await notification_service.start(bot)

    async def _on_shutdown(*_: object, **__: object) -> None:
        await notification_service.stop()

    dp.startup.register(_on_startup)
    dp.shutdown.register(_on_shutdown)

    # Keep only актуальні команди.
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="обрати чергу і підчергу"),
            BotCommand(command="all", description="показати весь графік"),
            BotCommand(command="notify_on", description="увімкнути сповіщення"),
            BotCommand(command="notify_off", description="вимкнути сповіщення"),
            BotCommand(command="notify_time", description="час попередження (хв)"),
            BotCommand(command="status", description="статус сповіщень"),
            BotCommand(command="notify_test", description="тест сповіщень"),
        ]
    )

    await dp.start_polling(bot)


def main() -> None:
    asyncio.run(_run())
