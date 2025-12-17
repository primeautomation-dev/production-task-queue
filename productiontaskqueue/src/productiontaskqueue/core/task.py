from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class Task:
    id: str
    name: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)

    status: str = field(default="pending")
    attempts: int = field(default=0)

    def mark_processing(self):
        self.status = "processing"

    def mark_completed(self):
        self.status = "completed"

    def mark_failed(self):
        self.status = "failed"
        self.attempts += 1

    def execute(self) -> None:
        """
        Execute the task. Subclasses must override this method.
        """
        raise NotImplementedError("Task.execute() must be implemented by subclasses")