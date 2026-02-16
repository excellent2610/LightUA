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
            BotCommand(command="start", description="Обрати чергу і підчергу"),
            BotCommand(command="queue", description="Показати графік своєї черги"),
            BotCommand(command="all", description="Показати весь графік"),
            BotCommand(command="notify_on", description="Увімкнути сповіщення"),
            BotCommand(command="notify_off", description="Вимкнути сповіщення"),
            BotCommand(command="quiet_hours", description="Тиха година"),
            BotCommand(command="notify_test", description="Тест сповіщень"),
            BotCommand(command="region", description="Поточна область"),
            BotCommand(command="settings", description="Показати налаштування"),
            BotCommand(command="notify_time", description="Час попередження"),
            BotCommand(command="notify_types", description="Типи сповіщень"),

        ]
    )

    await dp.start_polling(bot)


def main() -> None:
    asyncio.run(_run())
