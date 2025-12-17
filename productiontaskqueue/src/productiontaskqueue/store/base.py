from abc import ABC, abstractmethod
from typing import Optional

from productiontaskqueue.core.task import Task


class TaskStore(ABC):
    """
    Abstract storage interface.

    Any backend (Postgres, Redis, etc.)
    must implement this contract.
    """

    @abstractmethod
    def save(self, task: Task) -> None:
        pass

    @abstractmethod
    def get(self, task_id: str) -> Optional[Task]:
        pass

    @abstractmethod
    def claim_next(self) -> Optional[Task]:
        """
        Atomically claim the next pending task.
        """
        pass

    @abstractmethod
    def update(self, task: Task) -> None:
        pass

    def create(self, task: Task) -> None:
        """
        Create/save a task (convenience alias for save).
        """
        self.save(task)

    def mark_completed(self, task_id: str) -> None:
        """
        Mark a task as completed (convenience method).
        """
        task = self.get(task_id)
        if task:
            task.mark_completed()
            self.update(task)

    def mark_failed(self, task_id: str, error: str = "") -> None:
        """
        Mark a task as failed (convenience method).
        """
        task = self.get(task_id)
        if task:
            task.mark_failed()
            self.update(task)