from __future__ import annotations

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message

from db.database import Database
from utils.time_utils import parse_hhmm


class QuietStates(StatesGroup):
    waiting_start = State()
    waiting_end = State()


def get_router(db: Database) -> Router:
    router = Router(name="quiet")

    @router.message(Command("quiet"))
    async def quiet_command(message: Message, state: FSMContext) -> None:
        if message.from_user is None:
            return

        user = await db.get_user(message.from_user.id)
        if user is None:
            await message.answer("Спочатку завершіть реєстрацію: /start")
            return

        await state.clear()
        await state.set_state(QuietStates.waiting_start)
        await message.answer("Введіть початок тихої години у форматі HH:MM (наприклад 23:00):")

    @router.message(QuietStates.waiting_start, F.text & ~F.text.startswith("/"))
    async def quiet_start_input(message: Message, state: FSMContext) -> None:
        raw = (message.text or "").strip()
        parsed = parse_hhmm(raw)
        if parsed is None:
            await message.answer("Невірний формат. Введіть час як HH:MM (наприклад 23:00):")
            return

        await state.update_data(quiet_start=parsed.strftime("%H:%M"))
        await state.set_state(QuietStates.waiting_end)
        await message.answer("Введіть завершення тихої години у форматі HH:MM (наприклад 07:00):")

    @router.message(QuietStates.waiting_end, F.text & ~F.text.startswith("/"))
    async def quiet_end_input(message: Message, state: FSMContext) -> None:
        if message.from_user is None:
            return

        raw = (message.text or "").strip()
        parsed = parse_hhmm(raw)
        if parsed is None:
            await message.answer("Невірний формат. Введіть час як HH:MM (наприклад 07:00):")
            return

        data = await state.get_data()
        quiet_start = data.get("quiet_start")
        quiet_end = parsed.strftime("%H:%M")
        if not quiet_start:
            await state.clear()
            await message.answer("Стан налаштування втрачено. Спробуйте /quiet ще раз.")
            return

        if quiet_start == quiet_end:
            await message.answer("Початок і кінець не можуть бути однаковими. Спробуйте /quiet ще раз.")
            await state.clear()
            return

        updated = await db.set_quiet_hours(message.from_user.id, quiet_start, quiet_end)
        await state.clear()
        if not updated:
            await message.answer("Профіль не знайдено. Виконайте /start")
            return

        await message.answer(f"Тиха година встановлена: {quiet_start} - {quiet_end}")

    return router
