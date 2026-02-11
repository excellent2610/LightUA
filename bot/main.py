from __future__ import annotations

import asyncio

from aiogram import Bot, Dispatcher

from bot.config import BOT_TOKEN
from bot.handlers import router


async def _run() -> None:
    if not BOT_TOKEN:
        raise RuntimeError(
            "Bot token is missing. Set BOT_TOKEN env var, or put token in token.txt, .env, or config.py"
        )
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)


def main() -> None:
    asyncio.run(_run())
