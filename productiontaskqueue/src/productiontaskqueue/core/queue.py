from typing import Dict, Optional
from threading import Lock

from .task import Task


class TaskQueue:
    """
    Simple in-memory task queue.

    This is intentionally minimal:
    - thread-safe
    - deterministic
    - easy to extend later (DB / Redis)
    """

    def __init__(self):
        self._tasks: Dict[str, Task] = {}
        self._lock = Lock()

    def enqueue(self, task: Task) -> None:
        with self._lock:
            self._tasks[task.id] = task

    def get(self, task_id: str) -> Optional[Task]:
        with self._lock:
            return self._tasks.get(task_id)

    def claim_next(self) -> Optional[Task]:
        """
        Claim the next pending task.
        Worker tarafı burayı kullanacak.
        """
        with self._lock:
            for task in self._tasks.values():
                if task.status == "pending":
                    task.mark_processing()
                    return task
        return None

    def dequeue(self) -> Optional[Task]:
        """
        Dequeue the next pending task (alias for claim_next).
        """
        return self.claim_next()