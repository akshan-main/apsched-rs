"""Base job store interface (compatibility shim).

Mirrors the public surface of ``apscheduler.jobstores.base`` from
APScheduler 3.x so downstream code that subclasses ``BaseJobStore`` or
catches its exception types keeps working unchanged.
"""

from __future__ import annotations


class BaseJobStore:
    """Base class for job stores. Subclass this and implement the methods."""

    def lookup_job(self, job_id):  # pragma: no cover - interface
        raise NotImplementedError

    def get_due_jobs(self, now):  # pragma: no cover - interface
        raise NotImplementedError

    def get_next_run_time(self):  # pragma: no cover - interface
        raise NotImplementedError

    def get_all_jobs(self):  # pragma: no cover - interface
        raise NotImplementedError

    def add_job(self, job):  # pragma: no cover - interface
        raise NotImplementedError

    def update_job(self, job):  # pragma: no cover - interface
        raise NotImplementedError

    def remove_job(self, job_id):  # pragma: no cover - interface
        raise NotImplementedError

    def remove_all_jobs(self):  # pragma: no cover - interface
        raise NotImplementedError

    def shutdown(self):
        pass

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>"


class JobLookupError(KeyError):
    def __init__(self, job_id):
        super().__init__(f"No job by the id of {job_id} was found")


class ConflictingIdError(KeyError):
    def __init__(self, job_id):
        super().__init__(f"Job identifier ({job_id}) conflicts with an existing job")


class TransientJobError(ValueError):
    def __init__(self, job_id):
        super().__init__(f"Adding job ({job_id}) failed due to a transient error")


__all__ = [
    "BaseJobStore",
    "JobLookupError",
    "ConflictingIdError",
    "TransientJobError",
]
