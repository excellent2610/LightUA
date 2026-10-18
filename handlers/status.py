from __future__ import annotations

import asyncio
import logging

from aiogram import Router
from aiogram.filters import Command, StateFilter
from aiogram.types import Message

from db.database import Database
from parsing.alerts_parser import AlertsParser, ParserError
from utils.ads import get_ad_manager
from utils.formatting import format_light_change_message, format_status_message

logger = logging.getLogger(__name__)


def get_router(db: Database, parser: AlertsParser | None = None) -> Router:
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

        text = format_status_message(user)
        if parser is not None:
            try:
                state, _schedule = await asyncio.to_thread(
                    parser.get_current_state, user.region_slug, user.queue, user.subqueue
                )
                text += "\n\n" + format_light_change_message(state, user.region_name, user.queue, user.subqueue)
            except ParserError:
                logger.info("Current state unavailable in /status for user=%s", user.telegram_id)
            except Exception:
                logger.exception("Could not load current state in /status for user=%s", user.telegram_id)
        await ad_manager.send_with_ad(
            message.bot,
            message.chat.id,
            text,
        )

    return router
