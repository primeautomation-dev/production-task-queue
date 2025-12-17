from typing import Dict, Optional

from productiontaskqueue.core.task import Task
from .base import TaskStore


class InMemoryTaskStore(TaskStore):
    """
    In-memory task store for testing and development.
    """

    def __init__(self):
        self._tasks: Dict[str, Task] = {}

    def save(self, task: Task) -> None:
        self._tasks[task.id] = task

    def get(self, task_id: str) -> Optional[Task]:
        return self._tasks.get(task_id)

    def claim_next(self) -> Optional[Task]:
        """
        Claim the next pending task.
        """
        for task in self._tasks.values():
            if task.status == "pending":
                task.mark_processing()
                return task
        return None

    def update(self, task: Task) -> None:
        if task.id in self._tasks:
            self._tasks[task.id] = task

