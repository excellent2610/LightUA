from __future__ import annotations

import asyncio
import logging

from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import Message

from db.database import Database
from handlers.register import begin_registration
from parsing.alerts_parser import AlertsParser, ParserError, ScheduleNotFoundError
from utils.formatting import format_light_change_message, format_status_message

logger = logging.getLogger(__name__)


def get_router(db: Database, parser: AlertsParser) -> Router:
    router = Router(name="start")

    @router.message(CommandStart())
    async def start_command(message: Message, state: FSMContext) -> None:
        if message.from_user is None:
            return

        telegram_id = message.from_user.id
        user = await db.get_user(telegram_id)
        if user is None:
            await begin_registration(message=message, state=state, db=db, reset_existing=False)
            return

        status_line = format_status_message(user)
        light_line = "Стан світла поки недоступний."
        try:
            current_state, schedule = await asyncio.to_thread(
                parser.get_current_state,
                user.region_slug,
                user.queue,
                user.subqueue,
            )
            light_line = format_light_change_message(
                state=current_state,
                region_name=user.region_name,
                queue=user.queue,
                subqueue=user.subqueue,
            )
        except ScheduleNotFoundError:
            light_line = "ℹ️ На сьогодні планові відключення для вашої черги не знайдено."
        except ParserError:
            logger.exception("Failed to fetch current state for /start user %s", telegram_id)

        await message.answer(
            f"{light_line}\n\n{status_line}\n\n"
            "Команди:\n"
            "/graph\n"
            "/notify_on\n"
            "/notify_off\n"
            "/quiet\n"
            "/status\n"
            "/register"
        )

    return router
