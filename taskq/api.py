from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from .models import BatchSubmit, TaskStatus, TaskSubmit
from .store import TaskStore
from .worker import get_registry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api")

_store: TaskStore | None = None


def init_router(store: TaskStore) -> None:
    global _store
    _store = store


def _get_store() -> TaskStore:
    if _store is None:
        raise RuntimeError("Router not initialized")
    return _store


_sse_queues: list[asyncio.Queue] = []


async def _sse_callback(event: dict) -> None:
    dead: list[asyncio.Queue] = []
    for q in _sse_queues:
        try:
            q.put_nowait(event)
        except Exception:
            dead.append(q)
    for q in dead:
        _sse_queues.remove(q)


@router.post("/tasks", status_code=201)
async def submit_task(req: TaskSubmit) -> dict[str, Any]:
    store = _get_store()
    try:
        t = await store.submit_and_emit(req)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return t.model_dump(mode="json")


@router.post("/tasks/batch", status_code=201)
async def batch_submit(body: BatchSubmit) -> dict[str, Any]:
    store = _get_store()
    tasks = []
    for req in body.tasks:
        try:
            t = await store.submit_and_emit(req)
            tasks.append(t.model_dump(mode="json"))
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
    return {"created": len(tasks), "tasks": tasks}


@router.get("/tasks")
async def list_tasks(
    status: TaskStatus | None = None,
    task_type: str | None = None,
    correlation_id: str | None = None,
    search: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    store = _get_store()
    tasks = store.list_tasks(status=status, task_type=task_type, correlation_id=correlation_id, search=search, limit=limit, offset=offset)
    return {
        "tasks": [t.model_dump(mode="json") for t in tasks],
        "total": len(tasks),
    }


@router.get("/tasks/{task_id}")
async def get_task(task_id: str) -> dict[str, Any]:
    store = _get_store()
    t = store.get(task_id)
    if not t:
        raise HTTPException(status_code=404, detail="Task not found")
    return t.model_dump(mode="json")


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str) -> dict[str, Any]:
    store = _get_store()
    t = await store.cancel(task_id)
    if not t:
        raise HTTPException(status_code=400, detail="Cannot cancel task (not in queued/retrying/running state)")
    return t.model_dump(mode="json")


@router.post("/tasks/{task_id}/retry")
async def retry_task(task_id: str) -> dict[str, Any]:
    store = _get_store()
    t = await store.retry(task_id)
    if not t:
        raise HTTPException(status_code=400, detail="Cannot retry task (not in dead/failed state)")
    return t.model_dump(mode="json")


@router.get("/dead-letters")
async def list_dead_letters() -> dict[str, Any]:
    store = _get_store()
    tasks = store.dead_letters()
    return {
        "tasks": [t.model_dump(mode="json") for t in tasks],
        "total": len(tasks),
    }


@router.post("/dead-letters/{task_id}/retry")
async def retry_dead_letter(task_id: str) -> dict[str, Any]:
    store = _get_store()
    t = await store.retry(task_id)
    if not t:
        raise HTTPException(status_code=400, detail="Cannot retry dead letter")
    return t.model_dump(mode="json")


@router.get("/workers")
async def list_workers() -> dict[str, Any]:
    store = _get_store()
    workers = store.list_workers()
    return {
        "workers": [w.model_dump(mode="json") for w in workers],
        "total": len(workers),
    }


@router.get("/stats")
async def get_stats() -> dict[str, Any]:
    store = _get_store()
    return store.stats()


@router.get("/registry")
async def list_registry() -> dict[str, Any]:
    return {"task_types": list(get_registry().keys())}


@router.get("/stream")
async def sse_stream() -> StreamingResponse:
    q: asyncio.Queue = asyncio.Queue()
    _sse_queues.append(q)

    async def event_generator():
        try:
            while True:
                try:
                    event = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"data: {json.dumps(event, default=str, ensure_ascii=False)}\n\n"
                except asyncio.TimeoutError:
                    yield f": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            _sse_queues.remove(q)

    return StreamingResponse(event_generator(), media_type="text/event-stream")
