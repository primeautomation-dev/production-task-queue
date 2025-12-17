import time

from productiontaskqueue.core.task import Task
from productiontaskqueue.core.queue import TaskQueue
from productiontaskqueue.store import InMemoryTaskStore
from productiontaskqueue.worker import Worker


class DummyTask(Task):
    def __init__(self, task_id: str):
        super().__init__(task_id)
        self.executed = False

    def execute(self) -> None:
        self.executed = True


def test_worker_processes_task_end_to_end():
    """
    Integration test:
    enqueue -> worker executes -> store marks completed
    """

    queue = TaskQueue()
    store = InMemoryTaskStore()

    task = DummyTask(task_id="task-1")
    store.create(task)
    queue.enqueue(task)

    worker = Worker(queue=queue, store=store, poll_interval=0.01)

    # run worker briefly
    worker.running = True
    for _ in range(5):
        fetched = queue.dequeue()
        if fetched:
            worker._process_task(fetched)
        time.sleep(0.01)

    stored_task = store.get("task-1")

    assert stored_task.status == "completed"
    assert task.executed is True
