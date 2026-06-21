from __future__ import annotations

import asyncio
import json
import threading
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Awaitable

from .models import TaskModel, TaskStatus, Priority, TaskSubmit, WorkerInfo


class TaskStore:
    def __init__(self, persist_path: str | None = "data/tasks.json") -> None:
        self._tasks: dict[str, TaskModel] = {}
        self._lock = threading.RLock()
        self._event_callbacks: list[Callable[[dict], Awaitable[None]]] = []
        self._persist_path = Path(persist_path) if persist_path else None
        self._workers: dict[str, WorkerInfo] = {}
        self._worker_lock = threading.RLock()
        self._load()

    def _load(self) -> None:
        if not self._persist_path or not self._persist_path.exists():
            return
        try:
            raw = json.loads(self._persist_path.read_text(encoding="utf-8"))
            for item in raw:
                t = TaskModel(**item)
                self._tasks[t.task_id] = t
        except Exception:
            pass

    def _save(self) -> None:
        if not self._persist_path:
            return
        try:
            self._persist_path.parent.mkdir(parents=True, exist_ok=True)
            data = [t.model_dump(mode="json") for t in self._tasks.values()]
            tmp = self._persist_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(data, default=str, ensure_ascii=False), encoding="utf-8")
            tmp.replace(self._persist_path)
        except Exception:
            pass

    def on_event(self, cb: Callable[[dict], Awaitable[None]]) -> None:
        self._event_callbacks.append(cb)

    async def _emit(self, event: dict) -> None:
        for cb in self._event_callbacks:
            try:
                await cb(event)
            except Exception:
                pass

    def submit(self, req: TaskSubmit) -> TaskModel:
        with self._lock:
            now = datetime.now()
            task = TaskModel(
                task_id=str(uuid.uuid4()),
                task_type=req.task_type,
                params=req.params,
                priority=req.priority,
                max_retries=req.max_retries,
                correlation_id=req.correlation_id,
                depends_on=req.depends_on,
                cron_expr=req.cron_expr,
                interval_seconds=req.interval_seconds,
                tags=req.tags,
                submitted_at=now,
                status=TaskStatus.QUEUED,
            )
            if req.cron_expr or req.interval_seconds:
                task.next_run_at = now
            self._tasks[task.task_id] = task
            self._save()
        return task

    async def submit_and_emit(self, req: TaskSubmit) -> TaskModel:
        task = self.submit(req)
        await self._emit({"event": "task_created", "task": task.model_dump(mode="json")})
        return task

    def dequeue(self, worker_id: str) -> TaskModel | None:
        with self._lock:
            ready: list[TaskModel] = []
            for t in self._tasks.values():
                if t.status != TaskStatus.QUEUED:
                    continue
                if not self._dependencies_met(t):
                    continue
                ready.append(t)
            if not ready:
                return None
            ready.sort(key=lambda t: (t.priority_order(), t.submitted_at))
            task = ready[0]
            task.status = TaskStatus.RUNNING
            task.started_at = datetime.now()
            task.worker_id = worker_id
            self._save()
        return task

    async def complete(self, task_id: str, result: Any = None) -> TaskModel | None:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return None
            task.status = TaskStatus.COMPLETED
            task.finished_at = datetime.now()
            task.result = result
            self._save()
        await self._emit({"event": "task_completed", "task": task.model_dump(mode="json")})
        return task

    async def fail(self, task_id: str, error: str) -> TaskModel | None:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return None
            task.error_message = error
            task.retry_count += 1
            if task.retry_count >= task.max_retries:
                task.status = TaskStatus.DEAD
                task.finished_at = datetime.now()
            else:
                task.status = TaskStatus.RETRYING
                task.next_run_at = datetime.now()
            self._save()
        await self._emit({"event": "task_failed" if task.status == TaskStatus.DEAD else "task_retrying", "task": task.model_dump(mode="json")})
        return task

    async def retry(self, task_id: str) -> TaskModel | None:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task or task.status not in (TaskStatus.DEAD, TaskStatus.FAILED):
                return None
            task.status = TaskStatus.QUEUED
            task.retry_count = 0
            task.error_message = None
            task.finished_at = None
            task.worker_id = None
            self._save()
        await self._emit({"event": "task_retried", "task": task.model_dump(mode="json")})
        return task

    async def cancel(self, task_id: str) -> TaskModel | None:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task or task.status not in (TaskStatus.QUEUED, TaskStatus.RETRYING):
                return None
            task.status = TaskStatus.DEAD
            task.finished_at = datetime.now()
            self._save()
        await self._emit({"event": "task_cancelled", "task": task.model_dump(mode="json")})
        return task

    def get(self, task_id: str) -> TaskModel | None:
        with self._lock:
            return self._tasks.get(task_id)

    def list_tasks(
        self,
        status: TaskStatus | None = None,
        task_type: str | None = None,
        correlation_id: str | None = None,
        search: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[TaskModel]:
        with self._lock:
            tasks = list(self._tasks.values())
        if status:
            tasks = [t for t in tasks if t.status == status]
        if task_type:
            tasks = [t for t in tasks if t.task_type == task_type]
        if correlation_id:
            tasks = [t for t in tasks if t.correlation_id == correlation_id]
        if search:
            search_lower = search.lower()
            tasks = [
                t for t in tasks
                if search_lower in t.task_type.lower()
                or search_lower in t.task_id.lower()
                or (t.error_message and search_lower in t.error_message.lower())
                or any(search_lower in tag.lower() for tag in t.tags)
            ]
        tasks.sort(key=lambda t: t.submitted_at, reverse=True)
        return tasks[offset : offset + limit]

    def dead_letters(self) -> list[TaskModel]:
        with self._lock:
            return [t for t in self._tasks.values() if t.status == TaskStatus.DEAD]

    def stats(self) -> dict[str, Any]:
        with self._lock:
            counts: dict[str, int] = defaultdict(int)
            type_counts: dict[str, int] = defaultdict(int)
            for t in self._tasks.values():
                counts[t.status.value] += 1
                type_counts[t.task_type] += 1
        return {
            "total": len(self._tasks),
            "by_status": dict(counts),
            "by_type": dict(type_counts),
        }

    def _dependencies_met(self, task: TaskModel) -> bool:
        for dep_id in task.depends_on:
            dep = self._tasks.get(dep_id)
            if not dep or dep.status != TaskStatus.COMPLETED:
                return False
        return True

    def register_worker(self, info: WorkerInfo) -> None:
        with self._worker_lock:
            self._workers[info.worker_id] = info

    def update_heartbeat(self, worker_id: str, status: str = "idle", current_task_id: str | None = None) -> None:
        with self._worker_lock:
            w = self._workers.get(worker_id)
            if w:
                w.last_heartbeat = datetime.now()
                w.status = status
                w.current_task_id = current_task_id

    def worker_completed(self, worker_id: str, success: bool) -> None:
        with self._worker_lock:
            w = self._workers.get(worker_id)
            if w:
                if success:
                    w.tasks_completed += 1
                else:
                    w.tasks_failed += 1
                w.status = "idle"
                w.current_task_id = None

    def list_workers(self) -> list[WorkerInfo]:
        with self._worker_lock:
            return list(self._workers.values())

    def remove_stale_workers(self, timeout_seconds: int = 30) -> None:
        now = datetime.now()
        with self._worker_lock:
            stale = [
                wid
                for wid, w in self._workers.items()
                if (now - w.last_heartbeat).total_seconds() > timeout_seconds
            ]
            for wid in stale:
                del self._workers[wid]

    def cleanup_expired(self, max_age_hours: int = 72) -> int:
        now = datetime.now()
        with self._lock:
            expired = [
                tid
                for tid, t in self._tasks.items()
                if t.status in (TaskStatus.COMPLETED, TaskStatus.DEAD)
                and t.finished_at
                and (now - t.finished_at).total_seconds() > max_age_hours * 3600
            ]
            for tid in expired:
                del self._tasks[tid]
            if expired:
                self._save()
        return len(expired)

    def get_retryable_tasks(self) -> list[TaskModel]:
        now = datetime.now()
        with self._lock:
            return [
                t for t in self._tasks.values()
                if t.status == TaskStatus.RETRYING and (t.next_run_at is None or t.next_run_at <= now)
            ]

    def requeue_retry(self, task_id: str) -> None:
        with self._lock:
            t = self._tasks.get(task_id)
            if t and t.status == TaskStatus.RETRYING:
                t.status = TaskStatus.QUEUED
                t.worker_id = None
                self._save()
