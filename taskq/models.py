from __future__ import annotations

import enum
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class TaskStatus(str, enum.Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD = "dead"


class Priority(str, enum.Enum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


_PRIORITY_ORDER = {
    Priority.URGENT: 0,
    Priority.HIGH: 1,
    Priority.NORMAL: 2,
    Priority.LOW: 3,
}


class TaskModel(BaseModel):
    task_id: str
    task_type: str
    params: dict[str, Any] = Field(default_factory=dict)
    priority: Priority = Priority.NORMAL
    status: TaskStatus = TaskStatus.QUEUED
    submitted_at: datetime = Field(default_factory=datetime.now)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    retry_count: int = 0
    max_retries: int = 3
    error_message: str | None = None
    result: Any = None
    worker_id: str | None = None
    correlation_id: str | None = None
    depends_on: list[str] = Field(default_factory=list)
    cron_expr: str | None = None
    interval_seconds: float | None = None
    next_run_at: datetime | None = None
    tags: list[str] = Field(default_factory=list)

    def priority_order(self) -> int:
        return _PRIORITY_ORDER.get(self.priority, 2)


class TaskSubmit(BaseModel):
    task_type: str
    params: dict[str, Any] = Field(default_factory=dict)
    priority: Priority = Priority.NORMAL
    max_retries: int = 3
    correlation_id: str | None = None
    depends_on: list[str] = Field(default_factory=list)
    cron_expr: str | None = None
    interval_seconds: float | None = None
    tags: list[str] = Field(default_factory=list)


class BatchSubmit(BaseModel):
    tasks: list[TaskSubmit]


class WorkerInfo(BaseModel):
    worker_id: str
    status: str = "idle"
    current_task_id: str | None = None
    last_heartbeat: datetime
    started_at: datetime
    tasks_completed: int = 0
    tasks_failed: int = 0
