from __future__ import annotations

from aiogram import Dispatcher

from db.database import Database
from handlers.graph import get_router as get_graph_router
from handlers.notify import get_router as get_notify_router
from handlers.quiet import get_router as get_quiet_router
from handlers.register import get_router as get_register_router
from handlers.status import get_router as get_status_router
from parsing.alerts_parser import AlertsParser


def create_dispatcher(db: Database, parser: AlertsParser) -> Dispatcher:
    dispatcher = Dispatcher()

    dispatcher.include_router(get_register_router(db=db, parser=parser))
    dispatcher.include_router(get_graph_router(db=db, parser=parser))
    dispatcher.include_router(get_notify_router(db=db))
    dispatcher.include_router(get_quiet_router(db=db))
    dispatcher.include_router(get_status_router(db=db))

    return dispatcher
