import time
import logging
from typing import Optional

from productiontaskqueue.core.queue import TaskQueue
from productiontaskqueue.store.base import TaskStore
from productiontaskqueue.core.task import Task


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Worker:
    """
    Production-safe worker loop.

    Responsibilities:
    - Poll queue
    - Execute task
    - Ack / Fail safely
    """

    def __init__(
        self,
        queue: TaskQueue,
        store: TaskStore,
        poll_interval: float = 1.0,
    ):
        self.queue = queue
        self.store = store
        self.poll_interval = poll_interval
        self.running = False

    def start(self) -> None:
        logger.info("Worker started")
        self.running = True

        while self.running:
            task = self._fetch_task()

            if not task:
                time.sleep(self.poll_interval)
                continue

            self._process_task(task)

    def stop(self) -> None:
        logger.info("Worker stopping")
        self.running = False

    def _fetch_task(self) -> Optional[Task]:
        try:
            return self.queue.dequeue()
        except Exception as exc:
            logger.exception("Failed to dequeue task: %s", exc)
            return None

    def _process_task(self, task: Task) -> None:
        logger.info("Processing task %s", task.id)

        try:
            task.execute()
            self.store.mark_completed(task.id)
            logger.info("Task %s completed", task.id)

        except Exception as exc:
            logger.exception("Task %s failed: %s", task.id, exc)
            self.store.mark_failed(task.id, error=str(exc))
