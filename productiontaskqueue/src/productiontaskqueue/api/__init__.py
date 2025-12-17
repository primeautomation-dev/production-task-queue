"""
API layer for Production Task Queue.

This package exposes HTTP endpoints for:
- submitting tasks
- querying task status
- basic health checks
"""

from .http import app

__all__ = ["app"]
