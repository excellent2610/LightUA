import re

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    ReplyKeyboardRemove,
)

from config import OBLAST_TITLES
from db.database import db
from services.parser import build_user_graph, format_outage_message, format_power_on_message

router = Router()


class StartStates(StatesGroup):
    choose_oblast = State()
    enter_queue = State()
    enter_subqueue = State()


OBLAST_BUTTONS = {
    "cherkaska": "Черкаська область",
    "kirovogradska": "Кіровоградська область",
    "kyivska": "Київська область",
}


def _oblast_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Черкаська область", callback_data="oblast:cherkaska")],
            [InlineKeyboardButton(text="Кіровоградська область", callback_data="oblast:kirovogradska")],
            [InlineKeyboardButton(text="Київська область", callback_data="oblast:kyivska")],
        ]
    )


@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(StartStates.choose_oblast)
    await message.answer("Оновлюю інтерфейс…", reply_markup=ReplyKeyboardRemove())
    await message.answer("Оберіть область:", reply_markup=_oblast_keyboard())


@router.callback_query(StartStates.choose_oblast, F.data.startswith("oblast:"))
async def process_oblast_callback(callback: CallbackQuery, state: FSMContext) -> None:
    oblast_key = callback.data.split(":", maxsplit=1)[1]
    if oblast_key not in OBLAST_BUTTONS:
        await callback.answer("Невідома область.", show_alert=True)
        return

    await state.update_data(oblast=oblast_key)
    await state.set_state(StartStates.enter_queue)
    await callback.message.answer(f"Обрано: {OBLAST_BUTTONS[oblast_key]}\nВведіть номер черги (наприклад: 4).")
    await callback.answer()


@router.message(StartStates.choose_oblast)
async def process_oblast_text(message: Message) -> None:
    await message.answer("Натисніть кнопку під повідомленням для вибору області.")


@router.message(StartStates.enter_queue)
async def process_queue(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not re.fullmatch(r"\d{1,2}", text):
        await message.answer("Номер черги має бути числом, наприклад: 4.")
        return
    await state.update_data(queue=str(int(text)))
    await state.set_state(StartStates.enter_subqueue)
    await message.answer("Введіть номер підчерги (наприклад: 1).")


@router.message(StartStates.enter_subqueue)
async def process_subqueue(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not re.fullmatch(r"\d{1,2}", text):
        await message.answer("Номер підчерги має бути числом, наприклад: 1.")
        return

    data = await state.get_data()
    oblast = data["oblast"]
    queue = data["queue"]
    subqueue = str(int(text))

    db.upsert_user(user_id=message.from_user.id, oblast=oblast, queue=queue, subqueue=subqueue)
    await state.clear()

    graph, error = build_user_graph(oblast, queue, subqueue)
    if error or not graph:
        await message.answer(
            "Налаштування збережено, але графік зараз не вдалося отримати.\n"
            f"Причина: {error or 'невідома'}",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    await message.answer(
        f"Налаштування збережено: {OBLAST_TITLES[oblast]}, {int(queue)}.{int(subqueue)}",
        reply_markup=ReplyKeyboardRemove(),
    )
    await message.answer(format_outage_message(graph))
    await message.answer(format_power_on_message(graph))
