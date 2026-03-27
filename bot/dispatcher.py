from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from typing import Any, Awaitable, Callable

from aiogram import BaseMiddleware
from aiogram import Dispatcher
from aiogram.types import TelegramObject, Update

from db.database import Database
from handlers.graph import get_router as get_graph_router
from handlers.admin import get_router as get_admin_router
from handlers.notify import get_router as get_notify_router
from handlers.quiet import get_router as get_quiet_router
from handlers.register import get_router as get_register_router
from handlers.status import get_router as get_status_router
from parsing.alerts_parser import AlertsParser

logger = logging.getLogger(__name__)


class DropDuplicateUpdatesMiddleware(BaseMiddleware):
    def __init__(self, ttl_seconds: int = 120, max_entries: int = 5000) -> None:
        self.ttl_seconds = ttl_seconds
        self.max_entries = max_entries
        self._seen: OrderedDict[int, float] = OrderedDict()
        self._lock = asyncio.Lock()

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        update = event if isinstance(event, Update) else data.get("event_update")
        if not isinstance(update, Update) or update.update_id is None:
            return await handler(event, data)

        now = time.monotonic()
        async with self._lock:
            self._purge_expired(now)
            if update.update_id in self._seen:
                logger.debug("Drop duplicate update_id=%s", update.update_id)
                return None

            self._seen[update.update_id] = now
            if len(self._seen) > self.max_entries:
                self._seen.popitem(last=False)

        return await handler(event, data)

    def _purge_expired(self, now: float) -> None:
        deadline = now - self.ttl_seconds
        while self._seen:
            first_update_id, first_seen_at = next(iter(self._seen.items()))
            if first_seen_at >= deadline:
                break
            del self._seen[first_update_id]


class DropRepeatedCommandsMiddleware(BaseMiddleware):
    def __init__(self, ttl_seconds: int = 2, max_entries: int = 2000) -> None:
        self.ttl_seconds = ttl_seconds
        self.max_entries = max_entries
        self._seen: OrderedDict[tuple[int, int, str], float] = OrderedDict()
        self._lock = asyncio.Lock()

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        update = event if isinstance(event, Update) else data.get("event_update")
        if not isinstance(update, Update) or update.message is None:
            return await handler(event, data)

        message = update.message
        if message.chat is None or message.from_user is None:
            return await handler(event, data)

        text = (message.text or "").strip()
        if not text.startswith("/"):
            return await handler(event, data)

        # Keep only command name (/graph@BotName -> /graph).
        command = text.split(maxsplit=1)[0].split("@", maxsplit=1)[0].lower()
        now = time.monotonic()
        key = (message.chat.id, message.from_user.id, command)

        async with self._lock:
            self._purge_expired(now)
            previous = self._seen.get(key)
            if previous is not None and now - previous <= self.ttl_seconds:
                logger.debug("Drop repeated command %s for user=%s chat=%s", command, message.from_user.id, message.chat.id)
                return None

            self._seen[key] = now
            if len(self._seen) > self.max_entries:
                self._seen.popitem(last=False)

        return await handler(event, data)

    def _purge_expired(self, now: float) -> None:
        deadline = now - self.ttl_seconds
        while self._seen:
            first_key, first_seen_at = next(iter(self._seen.items()))
            if first_seen_at >= deadline:
                break
            del self._seen[first_key]


class UserActivityMiddleware(BaseMiddleware):
    def __init__(self, db: Database) -> None:
        self._db = db

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        update = event if isinstance(event, Update) else data.get("event_update")
        user_id = None
        if isinstance(update, Update):
            if update.message and update.message.from_user:
                user_id = update.message.from_user.id
            elif update.callback_query and update.callback_query.from_user:
                user_id = update.callback_query.from_user.id
            elif update.inline_query and update.inline_query.from_user:
                user_id = update.inline_query.from_user.id

        if user_id is not None:
            try:
                await self._db.touch_user_activity(user_id)
            except Exception:
                logger.exception("Failed to update user activity for telegram_id=%s", user_id)

        return await handler(event, data)


def create_dispatcher(db: Database, parser: AlertsParser) -> Dispatcher:
    dispatcher = Dispatcher()
    dispatcher.update.outer_middleware(DropDuplicateUpdatesMiddleware())
    dispatcher.update.outer_middleware(UserActivityMiddleware(db=db))

    dispatcher.include_router(get_register_router(db=db, parser=parser))
    dispatcher.include_router(get_admin_router(db=db))
    dispatcher.include_router(get_graph_router(db=db, parser=parser))
    dispatcher.include_router(get_notify_router(db=db))
    dispatcher.include_router(get_quiet_router(db=db))
    dispatcher.include_router(get_status_router(db=db))

    return dispatcher
