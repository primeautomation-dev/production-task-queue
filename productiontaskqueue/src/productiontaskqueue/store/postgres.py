import json
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from productiontaskqueue.core.task import Task
from .base import TaskStore


class PostgresTaskStore(TaskStore):
    """
    PostgreSQL-backed task store.

    Designed for:
    - crash safety
    - concurrent workers
    - at-least-once delivery
    """

    def __init__(self, dsn: str):
        self._conn = psycopg2.connect(dsn)

    def save(self, task: Task) -> None:
        with self._conn:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tasks (id, name, payload, status, attempts)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        task.id,
                        task.name,
                        json.dumps(task.payload),
                        task.status,
                        task.attempts,
                    ),
                )

    def get(self, task_id: str) -> Optional[Task]:
        with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM tasks WHERE id = %s",
                (task_id,),
            )
            row = cur.fetchone()

        if not row:
            return None

        # Handle payload: if it's a string, deserialize JSON; if dict, use as-is
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)

        return Task(
            id=row["id"],
            name=row["name"],
            payload=payload,
            status=row["status"],
            attempts=row["attempts"],
        )

    def claim_next(self) -> Optional[Task]:
        """
        Atomic task claiming using SELECT ... FOR UPDATE SKIP LOCKED
        """
        with self._conn:
            with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT * FROM tasks
                    WHERE status = 'pending'
                    ORDER BY id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """
                )
                row = cur.fetchone()

                if not row:
                    return None

                cur.execute(
                    """
                    UPDATE tasks
                    SET status = 'processing'
                    WHERE id = %s
                    """,
                    (row["id"],),
                )

        # Handle payload: if it's a string, deserialize JSON; if dict, use as-is
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)

        return Task(
            id=row["id"],
            name=row["name"],
            payload=payload,
            status="processing",
            attempts=row["attempts"],
        )

    def update(self, task: Task) -> None:
        with self._conn:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tasks
                    SET status = %s,
                        attempts = %s
                    WHERE id = %s
                    """,
                    (task.status, task.attempts, task.id),
                )
