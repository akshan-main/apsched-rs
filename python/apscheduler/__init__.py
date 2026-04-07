"""apscheduler-rs: High-performance APScheduler-compatible scheduler powered by Rust."""

from apscheduler._rust import (
    EVENT_ALL,
    EVENT_ALL_JOBS_REMOVED,
    EVENT_EXECUTOR_ADDED,
    EVENT_EXECUTOR_REMOVED,
    EVENT_JOB_ADDED,
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MAX_INSTANCES,
    EVENT_JOB_MISSED,
    EVENT_JOB_MODIFIED,
    EVENT_JOB_REMOVED,
    EVENT_JOB_SUBMITTED,
    EVENT_JOBSTORE_ADDED,
    EVENT_JOBSTORE_REMOVED,
    EVENT_SCHEDULER_PAUSED,
    EVENT_SCHEDULER_RESUMED,
    EVENT_SCHEDULER_SHUTDOWN,
    EVENT_SCHEDULER_STARTED,
)

from apscheduler import util  # noqa: E402,F401  (re-export for compatibility)

__version__ = "0.1.0"

# APScheduler 3.x exposes the same version under ``release`` in some code
# paths; we mirror it for maximum compatibility.
release = __version__
version_info = tuple(int(p) if p.isdigit() else p for p in __version__.split("."))

__all__ = [
    "util",
    "release",
    "version_info",
    "__version__",
    "EVENT_ALL",
    "EVENT_ALL_JOBS_REMOVED",
    "EVENT_EXECUTOR_ADDED",
    "EVENT_EXECUTOR_REMOVED",
    "EVENT_JOB_ADDED",
    "EVENT_JOB_ERROR",
    "EVENT_JOB_EXECUTED",
    "EVENT_JOB_MAX_INSTANCES",
    "EVENT_JOB_MISSED",
    "EVENT_JOB_MODIFIED",
    "EVENT_JOB_REMOVED",
    "EVENT_JOB_SUBMITTED",
    "EVENT_JOBSTORE_ADDED",
    "EVENT_JOBSTORE_REMOVED",
    "EVENT_SCHEDULER_PAUSED",
    "EVENT_SCHEDULER_RESUMED",
    "EVENT_SCHEDULER_SHUTDOWN",
    "EVENT_SCHEDULER_STARTED",
]
