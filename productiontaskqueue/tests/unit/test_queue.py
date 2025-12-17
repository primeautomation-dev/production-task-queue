from productiontaskqueue.core.task import Task
from productiontaskqueue.core.queue import TaskQueue


class DummyTask(Task):
    def __init__(self, task_id: str):
        super().__init__(task_id)


def test_enqueue_and_dequeue_single_task():
    queue = TaskQueue()
    task = DummyTask(task_id="task-1")

    queue.enqueue(task)
    fetched = queue.dequeue()

    assert fetched is not None
    assert fetched.id == "task-1"


def test_dequeue_empty_queue_returns_none():
    queue = TaskQueue()

    fetched = queue.dequeue()

    assert fetched is None


def test_queue_fifo_order():
    queue = TaskQueue()

    task1 = DummyTask(task_id="task-1")
    task2 = DummyTask(task_id="task-2")

    queue.enqueue(task1)
    queue.enqueue(task2)

    first = queue.dequeue()
    second = queue.dequeue()

    assert first.id == "task-1"
    assert second.id == "task-2"


def test_task_not_returned_twice():
    queue = TaskQueue()
    task = DummyTask(task_id="task-1")

    queue.enqueue(task)

    first = queue.dequeue()
    second = queue.dequeue()

    assert first is not None
    assert second is None
