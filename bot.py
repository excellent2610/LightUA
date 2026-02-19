import asyncio
import contextlib
import logging

from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand

from config import BOT_TOKEN
from db.database import db
from handlers.debug import router as debug_router
from handlers.graph import router as graph_router
from handlers.notify import router as notify_router
from handlers.notify_test import router as notify_test_router
from handlers.quiet_hours import router as quiet_hours_router
from handlers.start import router as start_router
from services.scheduler import notification_loop


async def setup_bot_commands(bot: Bot) -> None:
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Запуск і налаштування"),
            BotCommand(command="graph", description="Показати графік на сьогодні"),
            BotCommand(command="notify_on", description="Увімкнути сповіщення"),
            BotCommand(command="notify_off", description="Вимкнути сповіщення"),
            BotCommand(command="notify_test", description="Тест push через 5 хв"),
            BotCommand(command="quiet_hours", description="Тихі години: 22:00-07:00 | on | off"),
            BotCommand(command="quiet_hours_on", description="Увімкнути тихі години"),
            BotCommand(command="quiet_hours_off", description="Вимкнути тихі години"),
            BotCommand(command="debug_parse", description="Діагностика парсингу"),
        ]
    )


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set. Set environment variable BOT_TOKEN before start.")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logging.info("Using SQLite DB: %s", db.path)

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    dp.include_router(start_router)
    dp.include_router(graph_router)
    dp.include_router(notify_router)
    dp.include_router(notify_test_router)
    dp.include_router(quiet_hours_router)
    dp.include_router(debug_router)

    await setup_bot_commands(bot)

    scheduler_task = asyncio.create_task(notification_loop(bot, db))
    try:
        await dp.start_polling(bot)
    finally:
        scheduler_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await scheduler_task
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
