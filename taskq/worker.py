from __future__ import annotations

import asyncio
import logging
import time
import uuid
from datetime import datetime
from typing import Any, Callable

from .models import TaskModel, WorkerInfo
from .store import TaskStore

logger = logging.getLogger(__name__)

_task_registry: dict[str, Callable[..., Any]] = {}


def task(name: str) -> Callable:
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        _task_registry[name] = fn
        return fn
    return decorator


def get_registry() -> dict[str, Callable[..., Any]]:
    return dict(_task_registry)


class Worker:
    def __init__(
        self,
        store: TaskStore,
        worker_id: str | None = None,
        poll_interval: float = 1.0,
        heartbeat_interval: float = 5.0,
    ) -> None:
        self.store = store
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self._running = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._last_heartbeat = 0.0

    async def start(self) -> None:
        self._running = True
        self._loop = asyncio.get_running_loop()
        info = WorkerInfo(
            worker_id=self.worker_id,
            last_heartbeat=datetime.now(),
            started_at=datetime.now(),
        )
        self.store.register_worker(info)
        logger.info("Worker %s started", self.worker_id)
        await self._run_loop()

    async def stop(self) -> None:
        self._running = False
        logger.info("Worker %s stopping", self.worker_id)

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self._poll_and_execute()
            except Exception as e:
                logger.error("Worker %s error: %s", self.worker_id, e)
            self._maybe_heartbeat("idle")
            await asyncio.sleep(self.poll_interval)

    async def _poll_and_execute(self) -> None:
        t = self.store.dequeue(self.worker_id)
        if t is None:
            return
        self._maybe_heartbeat("busy", t.task_id)
        logger.info("Worker %s executing task %s (%s)", self.worker_id, t.task_id, t.task_type)
        handler = _task_registry.get(t.task_type)
        if handler is None:
            await self.store.fail(t.task_id, f"No handler registered for task type: {t.task_type}")
            self.store.worker_completed(self.worker_id, False)
            return
        try:
            result = handler(t.params)
            if asyncio.iscoroutine(result):
                result = await result
            await self.store.complete(t.task_id, result)
            self.store.worker_completed(self.worker_id, True)
            logger.info("Worker %s completed task %s", self.worker_id, t.task_id)
        except Exception as e:
            logger.error("Worker %s task %s failed: %s", self.worker_id, t.task_id, e)
            await self.store.fail(t.task_id, str(e))
            self.store.worker_completed(self.worker_id, False)

    def _maybe_heartbeat(self, status: str = "idle", current_task_id: str | None = None) -> None:
        now = time.monotonic()
        if now - self._last_heartbeat >= self.heartbeat_interval:
            self._last_heartbeat = now
            self.store.update_heartbeat(self.worker_id, status, current_task_id)
