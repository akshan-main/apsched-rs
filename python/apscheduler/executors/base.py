"""Base executor interface (compatibility shim)."""

from __future__ import annotations


class BaseExecutor:
    """Base class for executors."""

    _scheduler = None
    _lock = None
    _logger = None
    _instances: dict = {}

    def start(self, scheduler, alias):
        self._scheduler = scheduler

    def shutdown(self, wait: bool = True) -> None:
        pass

    def submit_job(self, job, run_times):  # pragma: no cover - interface
        raise NotImplementedError

    def _do_submit_job(self, job, run_times):  # pragma: no cover - interface
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>"


class MaxInstancesReachedError(Exception):
    def __init__(self, job):
        super().__init__(
            f'Job "{job.id}" has reached its maximum number of instances '
            f"({job.max_instances})"
        )


__all__ = ["BaseExecutor", "MaxInstancesReachedError"]
