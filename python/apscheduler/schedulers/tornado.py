"""Tornado scheduler compatibility shim.

In apscheduler-rs the scheduler runs on a Rust-managed thread, so a
dedicated Tornado integration is unnecessary. ``TornadoScheduler(...)``
returns a :class:`BackgroundScheduler` instance — drop-in compatible with
the constructor signature APScheduler 3.x used.
"""

from __future__ import annotations

import warnings

from apscheduler.schedulers.background import BackgroundScheduler


def TornadoScheduler(io_loop=None, **kwargs):  # noqa: N802 — class-like factory
    """Return a :class:`BackgroundScheduler` configured for Tornado usage."""
    if io_loop is not None:
        warnings.warn(
            "TornadoScheduler.io_loop parameter is ignored in apscheduler-rs. "
            "The scheduler runs in a background thread.",
            stacklevel=2,
        )
    return BackgroundScheduler(**kwargs)


__all__ = ["TornadoScheduler"]
