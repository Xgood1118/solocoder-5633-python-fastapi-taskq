from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta

from croniter import croniter

from .models import TaskSubmit, Priority, TaskStatus
from .store import TaskStore

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, store: TaskStore, tick_interval: float = 5.0) -> None:
        self.store = store
        self.tick_interval = tick_interval
        self._running = False

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
        stale_workers = self.store.remove_stale_workers(timeout_seconds=30)
        if stale_workers:
            recovered = self.store.recover_orphaned_tasks(stale_workers)
            if recovered:
                logger.info("Recovered %d orphaned tasks from dead workers: %s", recovered, stale_workers)
        orphaned = self.store.check_orphaned_dependencies()
        if orphaned:
            logger.info("Marked %d tasks as dead due to missing/failed dependencies", orphaned)
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
            if t.status not in (TaskStatus.QUEUED, TaskStatus.COMPLETED):
                continue
            should_run = False
            if t.interval_seconds:
                if t.next_run_at is None:
                    t.next_run_at = now + timedelta(seconds=t.interval_seconds)
                    continue
                if now >= t.next_run_at:
                    should_run = True
                    t.next_run_at = now + timedelta(seconds=t.interval_seconds)
            elif t.cron_expr:
                try:
                    if t.next_run_at is None:
                        cron = croniter(t.cron_expr, now)
                        t.next_run_at = cron.get_next(datetime)
                        continue
                    if now >= t.next_run_at:
                        should_run = True
                        cron = croniter(t.cron_expr, t.next_run_at)
                        t.next_run_at = cron.get_next(datetime)
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
