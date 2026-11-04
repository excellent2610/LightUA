from __future__ import annotations

import asyncio
import logging
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from aiohttp import web
from aiogram import Bot
from aiogram.types import BotCommand

if TYPE_CHECKING:
    from bot.shadow_scheduler import ShadowScheduleScheduler

try:
    from bot.config import load_settings
    from bot.dispatcher import create_dispatcher
    from bot.scheduler import LightStateScheduler
    from db.database import Database
    from parsing.alerts_parser import AlertsParser
except ModuleNotFoundError:
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from bot.config import load_settings
    from bot.dispatcher import create_dispatcher
    from bot.scheduler import LightStateScheduler
    from db.database import Database
    from parsing.alerts_parser import AlertsParser

DATABASE_APP_KEY = web.AppKey("database", Database)


# ---------------- LOGGING ----------------
def configure_logging(level_name: str) -> None:
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


# ---------------- SINGLE INSTANCE LOCK ----------------
def _is_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


@contextmanager
def single_instance_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    current_pid = os.getpid()

    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            break
        except FileExistsError:
            try:
                existing_pid = int(lock_path.read_text().strip())
            except Exception:
                existing_pid = -1

            if _is_pid_running(existing_pid):
                raise RuntimeError(f"Another instance is running (pid={existing_pid})")

            lock_path.unlink(missing_ok=True)

    try:
        os.write(fd, f"{current_pid}\n".encode())
        os.close(fd)
        yield
    finally:
        lock_path.unlink(missing_ok=True)


# ---------------- PING SERVER ----------------
async def handle_ping(request):
    return web.Response(text="OK")


async def handle_ad_click(request: web.Request) -> web.StreamResponse:
    campaign_id = request.match_info.get("campaign_id", "")
    raw_user_id = request.query.get("user_id", "")
    signature = request.query.get("signature", "")
    if (
        not campaign_id
        or len(campaign_id) > 128
        or not raw_user_id.isdecimal()
        or len(raw_user_id) > 19
        or len(signature) != 64
    ):
        raise web.HTTPNotFound()

    try:
        user_id = int(raw_user_id)
        if user_id <= 0:
            raise ValueError("Invalid user ID")
        destination = await request.app[DATABASE_APP_KEY].record_ad_click(
            campaign_id,
            user_id,
            signature,
        )
    except ValueError:
        raise web.HTTPNotFound() from None
    except Exception:
        logging.getLogger(__name__).exception("Could not record an advertising click")
        raise web.HTTPServiceUnavailable(text="Click tracking is temporarily unavailable")

    if destination is None:
        raise web.HTTPNotFound()
    raise web.HTTPFound(location=destination)


async def start_web_server(db: Database):
    app = web.Application()
    app[DATABASE_APP_KEY] = db
    app.router.add_get("/ping", handle_ping)
    app.router.add_get("/ads/click/{campaign_id}", handle_ad_click)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.environ.get("PORT", 10000))  # ВАЖЛИВО ДЛЯ RENDER
    site = web.TCPSite(runner, "0.0.0.0", port)

    await site.start()


# ---------------- BOT COMMANDS ----------------
async def configure_bot_commands(bot: Bot) -> None:
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Почати / змінити профіль"),
            BotCommand(command="graph", description="Графік на сьогодні"),
            BotCommand(command="notify_on", description="Увімкнути сповіщення"),
            BotCommand(command="notify_off", description="Вимкнути сповіщення"),
            BotCommand(command="notify_test", description="Тест сповіщення"),
            BotCommand(command="quiet", description="Тиха година"),
            BotCommand(command="status", description="Профіль"),
            BotCommand(command="help", description="Довідка"),
        ]
    )


# ---------------- MAIN RUN ----------------
async def run() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    logger = logging.getLogger(__name__)

    logger.info("Using DB path: %s", settings.db_path)

    lock_path = Path(settings.db_path).with_suffix(".lock")
    shadow_sources = {
        "alerts_energy": settings.shadow_alerts_energy_enabled,
        "kolysvitlo": settings.shadow_kolysvitlo_enabled,
        "alerts_org": settings.shadow_alerts_org_enabled,
    }
    shadow_enabled = settings.shadow_mode_enabled and any(shadow_sources.values())

    with single_instance_lock(lock_path):
        database = Database(settings.db_path, shadow_enabled=shadow_enabled)
        bot: Bot | None = None
        scheduler: LightStateScheduler | None = None
        shadow_scheduler: ShadowScheduleScheduler | None = None
        try:
            await database.init()
            parser = AlertsParser(timeout_seconds=settings.request_timeout_seconds)
            bot = Bot(token=settings.bot_token)
            dispatcher = create_dispatcher(db=database, parser=parser)
            scheduler = LightStateScheduler(
                bot=bot,
                db=database,
                parser=parser,
                interval_seconds=settings.poll_interval_seconds,
            )
            if shadow_enabled and database.shadow_schema_ready:
                from bot.shadow_scheduler import ShadowScheduleScheduler
                from parsing.source_manager import SourceScheduleManager

                source_manager = SourceScheduleManager(
                    database,
                    enabled_sources=shadow_sources,
                    timeout_seconds=settings.shadow_request_timeout_seconds,
                )
                shadow_scheduler = ShadowScheduleScheduler(source_manager)
            elif shadow_enabled:
                logger.error(
                    "Shadow monitoring is enabled but its database schema is unavailable; "
                    "continuing without shadow polling"
                )

            # Start the application services only after cleanup is guaranteed.
            await scheduler.start()
            if shadow_scheduler is not None:
                await shadow_scheduler.start()
            await start_web_server(database)

            await bot.delete_webhook(drop_pending_updates=True)
            await configure_bot_commands(bot)

            await dispatcher.start_polling(
                bot,
                allowed_updates=dispatcher.resolve_used_update_types(),
            )
        finally:
            try:
                if shadow_scheduler is not None:
                    await shadow_scheduler.stop()
            finally:
                try:
                    if scheduler is not None:
                        await scheduler.stop()
                finally:
                    try:
                        if bot is not None:
                            await bot.session.close()
                    finally:
                        await database.close()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()