from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from .api import init_router, router, _sse_callback
from .models import TaskSubmit, Priority
from .scheduler import Scheduler
from .store import TaskStore
from .worker import Worker, task

logger = logging.getLogger(__name__)


def create_app(
    persist_path: str = "data/tasks.json",
    worker_count: int = 2,
    poll_interval: float = 1.0,
    scheduler_tick: float = 5.0,
) -> FastAPI:
    app = FastAPI(title="TaskQ", version="0.1.0")
    store = TaskStore(persist_path=persist_path)
    store.on_event(_sse_callback)

    init_router(store)
    app.include_router(router)

    dashboard_path = Path(__file__).parent / "dashboard.html"

    @app.get("/", response_class=HTMLResponse)
    async def dashboard():
        return HTMLResponse(dashboard_path.read_text(encoding="utf-8"))

    scheduler = Scheduler(store, tick_interval=scheduler_tick)
    workers: list[Worker] = []

    @app.on_event("startup")
    async def startup():
        for i in range(worker_count):
            w = Worker(store, poll_interval=poll_interval)
            workers.append(w)
            asyncio.create_task(w.start())
        asyncio.create_task(scheduler.start())
        logger.info("TaskQ started with %d workers", worker_count)

    @app.on_event("shutdown")
    async def shutdown():
        await scheduler.stop()
        for w in workers:
            await w.stop()
        logger.info("TaskQ stopped")

    return app
