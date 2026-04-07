"""Gevent scheduler compatibility shim.

Gevent's cooperative scheduling is not required by apscheduler-rs, which
executes jobs on a native thread pool. ``GeventScheduler(...)`` returns a
:class:`BackgroundScheduler` instance for drop-in compatibility.
"""

from __future__ import annotations

from apscheduler.schedulers.background import BackgroundScheduler


def GeventScheduler(**kwargs):  # noqa: N802 — class-like factory
    """Return a :class:`BackgroundScheduler` configured for Gevent usage."""
    return BackgroundScheduler(**kwargs)


__all__ = ["GeventScheduler"]
