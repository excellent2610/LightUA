from __future__ import annotations

import logging
from datetime import datetime

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from bot.config import ADMINS
from db.database import Database
from db.models import AdStatistics
from utils.ads import click_tracking_configured, get_ad_manager

logger = logging.getLogger(__name__)
_SUPPORTED_PERIODS = {1, 7, 30}
_TELEGRAM_MESSAGE_LIMIT = 3800


def _authorized(message: Message) -> bool:
    return message.from_user is not None and message.from_user.id in ADMINS


def _period_from_command(message: Message, *, has_campaign: bool) -> tuple[int | None, str | None]:
    parts = (message.text or "").split()
    period_arg = parts[2] if has_campaign and len(parts) > 2 else (
        parts[1] if not has_campaign and len(parts) > 1 else "all"
    )
    if period_arg.casefold() in {"all", "весь"}:
        return None, None
    try:
        days = int(period_arg)
    except ValueError:
        return None, "Період має бути all, 1, 7 або 30."
    if days not in _SUPPORTED_PERIODS:
        return None, "Період має бути all, 1, 7 або 30."
    return days, None


def _period_label(days: int | None) -> str:
    if days is None:
        return "за весь час"
    if days == 1:
        return "за сьогодні"
    return f"за останні {days} днів"


def _format_datetime(value: datetime | None) -> str:
    if value is None:
        return "—"
    return value.strftime("%d.%m.%Y %H:%M %Z")


def _format_stats(stats: AdStatistics, *, include_name: bool) -> str:
    title = f"{stats.name} [{stats.campaign_id}]" if include_name else "Усі кампанії"
    status = ""
    if stats.enabled is not None:
        status = "Увімкнено" if stats.enabled else "Вимкнено"
        status = f"\nСтатус: {status}"
    return (
        f"{title}{status}\n"
        f"Покази: {stats.impressions:,} · унікальні користувачі: {stats.unique_users:,}\n"
        f"Кліки: {stats.clicks:,} · унікальні кліки: {stats.unique_clicks:,}\n"
        f"CTR: {stats.ctr:.2f}%\n"
        f"Перший показ: {_format_datetime(stats.first_impression)}\n"
        f"Останній показ: {_format_datetime(stats.last_impression)}"
    )


def _click_tracking_note() -> str:
    if click_tracking_configured():
        return "Відстежуються redirect-запити; можливі префетчі та повторні відкриття."
    return "Кліки не вимірюються: задайте PUBLIC_BASE_URL."


def _advertising_configuration_note(db: Database) -> str:
    config = get_ad_manager(db=db).load_config()
    enabled = (
        config["ads_enabled"]
        and config["show_every"] > 0
        and config["max_per_user_per_day"] > 0
    )
    return f"Покази глобально: {'увімкнені' if enabled else 'вимкнені'}."


async def _answer_in_chunks(message: Message, text: str) -> None:
    while len(text) > _TELEGRAM_MESSAGE_LIMIT:
        split_at = text.rfind("\n\n", 0, _TELEGRAM_MESSAGE_LIMIT)
        if split_at < 1:
            split_at = _TELEGRAM_MESSAGE_LIMIT
        await message.answer(text[:split_at])
        text = text[split_at:].lstrip()
    if text:
        await message.answer(text)


def get_router(db: Database) -> Router:
    router = Router()

    @router.message(Command("admin"))
    async def admin_stats(message: Message) -> None:
        if not _authorized(message):
            return

        try:
            stats = await db.get_bot_stats()
        except Exception:
            logger.exception("Could not load bot admin statistics")
            await message.answer("Не вдалося завантажити статистику. Спробуйте пізніше.")
            return
        text = (
            "📊 Статистика бота:\n\n"
            f"👥 Усього користувачів: {stats['total']}\n"
            f"🆕 Нових сьогодні: {stats['new_today']}\n"
            f"🔥 Активних за 24 год: {stats['active_24h']}\n\n"
            "Реклама: /ads [all|1|7|30]\n"
            "Кампанія: /adstats <id> [all|1|7|30]"
        )
        await message.answer(text)

    @router.message(Command("ads"))
    async def advertising_stats(message: Message) -> None:
        if not _authorized(message):
            return
        days, error = _period_from_command(message, has_campaign=False)
        if error:
            await message.answer(error)
            return

        try:
            await get_ad_manager(db=db).sync_campaigns_from_config()
            overall, campaigns = await _load_ad_statistics(db, days)
        except Exception:
            logger.exception("Could not load advertising admin statistics")
            await message.answer("Не вдалося завантажити рекламну статистику. Спробуйте пізніше.")
            return

        blocks = [
            f"📊 Реклама · {_period_label(days)}\n"
            f"{_advertising_configuration_note(db)}\n{_click_tracking_note()}\n"
            f"{_format_stats(overall, include_name=False)}"
        ]
        if campaigns:
            blocks.append("Кампанії:")
            blocks.extend(_format_stats(item, include_name=True) for item in campaigns)
        else:
            blocks.append("Кампаній ще немає.")
        await _answer_in_chunks(message, "\n\n".join(blocks))

    @router.message(Command("adstats"))
    async def advertising_campaign_stats(message: Message) -> None:
        if not _authorized(message):
            return
        parts = (message.text or "").split(maxsplit=2)
        if len(parts) < 2:
            await message.answer("Використання: /adstats <id> [all|1|7|30]")
            return
        days, error = _period_from_command(message, has_campaign=True)
        if error:
            await message.answer(error)
            return

        try:
            await get_ad_manager(db=db).sync_campaigns_from_config()
            campaigns = await db.get_ad_campaign_statistics(
                days=days,
                campaign_id=parts[1],
            )
        except Exception:
            logger.exception("Could not load advertising campaign statistics")
            await message.answer("Не вдалося завантажити статистику кампанії. Спробуйте пізніше.")
            return
        if not campaigns:
            await message.answer("Кампанію не знайдено.")
            return
        await message.answer(
            f"📊 {_period_label(days)}\n"
            f"{_advertising_configuration_note(db)}\n{_click_tracking_note()}\n"
            f"{_format_stats(campaigns[0], include_name=True)}"
        )

    return router


async def _load_ad_statistics(
    db: Database,
    days: int | None,
) -> tuple[AdStatistics, list[AdStatistics]]:
    import asyncio

    return await asyncio.gather(
        db.get_ad_overall_statistics(days=days),
        db.get_ad_campaign_statistics(days=days),
    )
