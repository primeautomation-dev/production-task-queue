"""
Core domain logic for Production Task Queue.

This package contains:
- Task definition
- Queue abstraction
"""

from .task import Task
from .queue import TaskQueue

__all__ = ["Task", "TaskQueue"]
