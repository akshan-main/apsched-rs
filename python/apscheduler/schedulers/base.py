"""Base scheduler exceptions and state constants (compatibility shim)."""

from __future__ import annotations


class SchedulerAlreadyRunningError(Exception):
    def __init__(self):
        super().__init__("Scheduler is already running")


class SchedulerNotRunningError(Exception):
    def __init__(self):
        super().__init__("Scheduler has not been started yet")


# APScheduler 3.x uses integer state constants. We mirror them so callers
# doing ``if scheduler.state == STATE_RUNNING`` keep working. The actual
# scheduler state is managed by the Rust backend; these are provided for
# type compatibility only.
STATE_STOPPED = 0
STATE_RUNNING = 1
STATE_PAUSED = 2


__all__ = [
    "SchedulerAlreadyRunningError",
    "SchedulerNotRunningError",
    "STATE_STOPPED",
    "STATE_RUNNING",
    "STATE_PAUSED",
]
