from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
import uuid

from productiontaskqueue.core.queue import TaskQueue
from productiontaskqueue.core.task import Task


app = FastAPI(
    title="Production Task Queue API",
    version="0.1.0",
)


# ---- request / response models ----

class SubmitTaskRequest(BaseModel):
    name: str
    payload: Dict[str, Any]


class SubmitTaskResponse(BaseModel):
    task_id: str
    status: str


# ---- initialize queue (in-memory for now) ----

queue = TaskQueue()


# ---- routes ----

@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.post("/tasks", response_model=SubmitTaskResponse)
def submit_task(request: SubmitTaskRequest):
    task_id = str(uuid.uuid4())

    task = Task(
        id=task_id,
        name=request.name,
        payload=request.payload,
    )

    queue.enqueue(task)

    return SubmitTaskResponse(
        task_id=task_id,
        status="queued",
    )


@app.get("/tasks/{task_id}")
def get_task_status(task_id: str):
    task = queue.get(task_id)

    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    return {
        "task_id": task.id,
        "name": task.name,
        "status": task.status,
        "attempts": task.attempts,
    }
