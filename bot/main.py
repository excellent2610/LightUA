from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

from aiogram import Bot
from aiogram.types import BotCommand

try:
    from bot.config import load_settings
    from bot.dispatcher import create_dispatcher
    from bot.scheduler import LightStateScheduler
    from db.database import Database
    from parsing.alerts_parser import AlertsParser
except ModuleNotFoundError:
    # Support direct execution: python bot/main.py
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from bot.config import load_settings
    from bot.dispatcher import create_dispatcher
    from bot.scheduler import LightStateScheduler
    from db.database import Database
    from parsing.alerts_parser import AlertsParser


def configure_logging(level_name: str) -> None:
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


async def configure_bot_commands(bot: Bot) -> None:
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Почати / змінити профіль"),
            BotCommand(command="graph", description="Графік на сьогодні"),
            BotCommand(command="notify_on", description="Увімкнути сповіщення"),
            BotCommand(command="notify_off", description="Вимкнути сповіщення"),
            BotCommand(command="notify_test", description="Тест сповіщення через 5 хв"),
            BotCommand(command="quiet", description="Налаштувати тиху годину"),
            BotCommand(command="status", description="Переглянути профіль"),
            BotCommand(command="help", description="Довідка по командах"),
        ]
    )


async def run() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    logger = logging.getLogger(__name__)
    logger.info("Using DB path: %s", settings.db_path)

    database = Database(settings.db_path)
    try:
        await database.init()
    except Exception as exc:
        raise RuntimeError(f"Failed to initialize SQLite at {settings.db_path}: {exc}") from exc
    logger.info("SQLite initialized at: %s", database.db_path)

    parser = AlertsParser(timeout_seconds=settings.request_timeout_seconds)
    bot = Bot(token=settings.bot_token)
    dispatcher = create_dispatcher(db=database, parser=parser)

    scheduler = LightStateScheduler(
        bot=bot,
        db=database,
        parser=parser,
        interval_seconds=settings.poll_interval_seconds,
    )
    await scheduler.start()
    await bot.delete_webhook(drop_pending_updates=True)
    await configure_bot_commands(bot)

    try:
        await dispatcher.start_polling(bot, allowed_updates=dispatcher.resolve_used_update_types())
    finally:
        await scheduler.stop()
        await bot.session.close()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
