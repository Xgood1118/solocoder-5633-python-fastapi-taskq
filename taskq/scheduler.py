from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta

from croniter import croniter

from .models import TaskSubmit, Priority
from .store import TaskStore

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, store: TaskStore, tick_interval: float = 5.0) -> None:
        self.store = store
        self.tick_interval = tick_interval
        self._running = False
        self._schedule_cache: dict[str, datetime] = {}

    async def start(self) -> None:
        self._running = True
        logger.info("Scheduler started")
        while self._running:
            try:
                await self._tick()
            except Exception as e:
                logger.error("Scheduler tick error: %s", e)
            await asyncio.sleep(self.tick_interval)

    async def stop(self) -> None:
        self._running = False
        logger.info("Scheduler stopping")

    async def _tick(self) -> None:
        await self._process_retries()
        await self._process_scheduled()
        self.store.remove_stale_workers(timeout_seconds=30)
        self.store.cleanup_expired(max_age_hours=72)

    async def _process_retries(self) -> None:
        retryable = self.store.get_retryable_tasks()
        for t in retryable:
            delay = min(2**t.retry_count, 300)
            if t.next_run_at and (datetime.now() - t.next_run_at).total_seconds() < delay:
                continue
            self.store.requeue_retry(t.task_id)
            logger.info("Requeued task %s for retry (attempt %d)", t.task_id, t.retry_count + 1)
            await self.store._emit({"event": "task_retrying", "task": t.model_dump(mode="json")})

    async def _process_scheduled(self) -> None:
        now = datetime.now()
        all_tasks = self.store.list_tasks(limit=10000)
        for t in all_tasks:
            if not t.cron_expr and not t.interval_seconds:
                continue
            if t.status not in (
                "queued",
                "completed",
            ):
                continue
            should_run = False
            if t.interval_seconds:
                if t.next_run_at and now >= t.next_run_at:
                    should_run = True
                    t.next_run_at = now + timedelta(seconds=t.interval_seconds)
            elif t.cron_expr:
                cached = self._schedule_cache.get(t.task_id)
                if cached and now >= cached:
                    should_run = True
                if t.next_run_at is None or should_run:
                    try:
                        cron = croniter(t.cron_expr, now)
                        t.next_run_at = cron.get_next(datetime)
                        self._schedule_cache[t.task_id] = t.next_run_at
                    except Exception as e:
                        logger.error("Invalid cron %s: %s", t.cron_expr, e)
                        continue
            if should_run:
                req = TaskSubmit(
                    task_type=t.task_type,
                    params=t.params,
                    priority=t.priority,
                    max_retries=t.max_retries,
                    tags=t.tags,
                )
                await self.store.submit_and_emit(req)
                logger.info("Scheduler spawned recurring task %s (%s)", t.task_id, t.task_type)
