"""
Worker package for production task queue.

Exposes the main Worker entrypoint.
"""

from .worker import Worker

__all__ = ["Worker"]

