from __future__ import annotations

import asyncio
import logging
import secrets

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from db.database import Database
from handlers.graph import send_graph_for_user
from parsing.alerts_parser import AlertsParser
from parsing.regions import Region, fetch_regions
from utils.formatting import normalize_region_label

logger = logging.getLogger(__name__)


class RegisterStates(StatesGroup):
    waiting_region = State()
    waiting_queue = State()
    waiting_subqueue = State()


async def begin_registration(
    message: Message,
    state: FSMContext,
    db: Database,
    reset_existing: bool,
) -> None:
    if message.from_user is None:
        return

    telegram_id = message.from_user.id

    previous_data = await state.get_data()
    previous_region_message_id = previous_data.get("region_prompt_message_id")
    if isinstance(previous_region_message_id, int):
        try:
            await message.bot.edit_message_reply_markup(
                chat_id=message.chat.id,
                message_id=previous_region_message_id,
                reply_markup=None,
            )
        except Exception:
            logger.debug("Failed to clear previous region keyboard", exc_info=True)

    await state.clear()
    if reset_existing:
        await db.delete_last_state(telegram_id)

    try:
        regions = await asyncio.to_thread(fetch_regions)
    except Exception:
        logger.exception("Failed to fetch regions from alerts.org.ua")
        await message.answer(
            "Не вдалося завантажити список областей з alerts.org.ua.\n"
            "Спробуйте пізніше командою /register."
        )
        return

    if not regions:
        await message.answer("Список областей порожній. Спробуйте пізніше.")
        return

    session_token = secrets.token_hex(4)
    cleaned_regions = [
        Region(slug=region.slug, name=_normalize_register_region_name(region.slug, region.name))
        for region in regions
    ]
    region_map = {region.slug: region.name for region in cleaned_regions}
    keyboard = _build_regions_keyboard(cleaned_regions, session_token=session_token)

    sent = await message.answer("Оберіть область:", reply_markup=keyboard)
    await state.update_data(
        region_map=region_map,
        region_session=session_token,
        region_prompt_message_id=sent.message_id,
    )
    await state.set_state(RegisterStates.waiting_region)


def get_router(db: Database, parser: AlertsParser | None = None) -> Router:
    router = Router(name="register")

    @router.message(Command(commands=["start", "register"]))
    async def register_command(message: Message, state: FSMContext) -> None:
        await begin_registration(message=message, state=state, db=db, reset_existing=True)

    @router.callback_query(RegisterStates.waiting_region, F.data.startswith("region:"))
    async def region_callback(callback: CallbackQuery, state: FSMContext) -> None:
        if callback.data is None:
            await callback.answer()
            return

        parts = callback.data.split(":", maxsplit=2)
        if len(parts) != 3:
            await callback.answer("Невірні дані кнопки. Спробуйте /register", show_alert=True)
            return

        _, token, slug = parts
        slug = slug.strip()
        state_data = await state.get_data()
        expected_token = str(state_data.get("region_session", ""))
        if token != expected_token:
            if callback.message:
                try:
                    await callback.message.edit_reply_markup(reply_markup=None)
                except Exception:
                    logger.debug("Failed to clear stale keyboard", exc_info=True)
            await callback.answer("Це застарілий список областей. Використайте /register", show_alert=True)
            return

        region_map: dict[str, str] = state_data.get("region_map", {})
        region_name = region_map.get(slug)
        if region_name is None:
            await callback.answer("Невірний вибір області", show_alert=True)
            return

        await state.update_data(region_slug=slug, region_name=region_name)
        await state.set_state(RegisterStates.waiting_queue)

        if callback.message:
            try:
                await callback.message.edit_reply_markup(reply_markup=None)
            except Exception:
                logger.debug("Failed to clear region keyboard", exc_info=True)

        if callback.message:
            await callback.message.answer(f"Обрано: {region_name}\nВведіть номер черги (наприклад: 4):")
        await callback.answer()

    @router.callback_query(F.data.startswith("region:"))
    async def stale_region_callback(callback: CallbackQuery) -> None:
        if callback.message:
            try:
                await callback.message.edit_reply_markup(reply_markup=None)
            except Exception:
                logger.debug("Failed to clear stale keyboard", exc_info=True)
        await callback.answer("Список областей застарів. Використайте /register", show_alert=True)

    @router.message(RegisterStates.waiting_region, F.text & ~F.text.startswith("/"))
    async def waiting_region_text(message: Message) -> None:
        await message.answer("Будь ласка, оберіть область кнопкою нижче.")

    @router.message(RegisterStates.waiting_queue, F.text & ~F.text.startswith("/"))
    async def queue_input(message: Message, state: FSMContext) -> None:
        value = (message.text or "").strip()
        if not value.isdigit():
            await message.answer("Черга має бути числом. Спробуйте ще раз:")
            return

        queue = int(value)
        if queue <= 0:
            await message.answer("Черга має бути додатним числом. Спробуйте ще раз:")
            return

        await state.update_data(queue=queue)
        await state.set_state(RegisterStates.waiting_subqueue)
        await message.answer("Введіть номер підчерги (наприклад: 1):")

    @router.message(RegisterStates.waiting_subqueue, F.text & ~F.text.startswith("/"))
    async def subqueue_input(message: Message, state: FSMContext) -> None:
        if message.from_user is None:
            return

        value = (message.text or "").strip()
        if not value.isdigit():
            await message.answer("Підчерга має бути числом. Спробуйте ще раз:")
            return

        subqueue = int(value)
        if subqueue <= 0:
            await message.answer("Підчерга має бути додатним числом. Спробуйте ще раз:")
            return

        data = await state.get_data()
        region_slug = data.get("region_slug")
        region_name = data.get("region_name")
        queue = data.get("queue")

        if not region_slug or not region_name or queue is None:
            await state.clear()
            await message.answer("Стан реєстрації втрачено. Повторіть /register")
            return

        telegram_id = message.from_user.id
        await db.upsert_user_registration(
            telegram_id=telegram_id,
            region_slug=region_slug,
            region_name=region_name,
            queue=int(queue),
            subqueue=subqueue,
        )
        await db.delete_last_state(telegram_id)
        await state.clear()

        await message.answer(
            "Реєстрація завершена.\n"
            f"Профіль: {normalize_region_label(region_name)} · {queue}.{subqueue}\n\n"
            "Команди:\n"
            "/graph - графік на сьогодні\n"
            "/notify_on - увімкнути сповіщення\n"
            "/notify_off - вимкнути сповіщення\n"
            "/quiet - налаштувати тиху годину\n"
            "/status - переглянути профіль"
        )
        if parser is not None:
            await send_graph_for_user(
                message=message,
                db=db,
                parser=parser,
                telegram_id=telegram_id,
                compact=True,
            )

    return router


def _build_regions_keyboard(regions: list[Region], session_token: str):
    builder = InlineKeyboardBuilder()
    for region in regions:
        builder.button(text=region.name, callback_data=f"region:{session_token}:{region.slug}")
    builder.adjust(2)
    return builder.as_markup()


def _normalize_register_region_name(slug: str, raw_name: str) -> str:
    cleaned = normalize_region_label(raw_name)
    if slug.endswith("-oblast") and "област" not in cleaned.lower():
        return f"{cleaned} область".strip()
    return cleaned
