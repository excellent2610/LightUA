from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from config import ALERTS_CF_CLEARANCE
from db.database import db
from services.parser import parse_oblast_page

router = Router()


@router.message(Command("debug_parse"))
async def cmd_debug_parse(message: Message) -> None:
    user = db.get_user(message.from_user.id)
    if not user:
        await message.answer("Спочатку налаштуйте область/чергу через /start.")
        return

    result = parse_oblast_page(user.oblast)
    queue_key = f"{int(user.queue)}.{int(user.subqueue)}"
    outage = result.data.outage.get(queue_key, [])
    power_on = result.data.power_on.get(queue_key, [])

    text = (
        f"debug_parse\n"
        f"source: {result.source_url}\n"
        f"cf_clearance_set: {'yes' if bool(ALERTS_CF_CLEARANCE) else 'no'}\n"
        f"error: {result.error or 'none'}\n"
        f"outage_keys_count: {len(result.data.outage)}\n"
        f"power_on_keys_count: {len(result.data.power_on)}\n"
        f"user_queue: {queue_key}\n"
        f"user_outage_intervals: {[(i.start, i.end) for i in outage]}\n"
        f"user_power_on_intervals: {[(i.start, i.end) for i in power_on]}\n"
        f"outage_keys_sample: {list(result.data.outage.keys())[:20]}\n"
        f"power_on_keys_sample: {list(result.data.power_on.keys())[:20]}"
    )
    if result.error and not ALERTS_CF_CLEARANCE:
        text += "\nhelp: set env ALERTS_CF_CLEARANCE from browser cookie for alerts.org.ua"
    await message.answer(text)
