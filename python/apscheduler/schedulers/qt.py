"""Qt scheduler compatibility shim."""

from __future__ import annotations

from apscheduler.schedulers.background import BackgroundScheduler


def QtScheduler(**kwargs):  # noqa: N802 — class-like factory
    """Return a :class:`BackgroundScheduler` configured for Qt usage."""
    return BackgroundScheduler(**kwargs)


__all__ = ["QtScheduler"]
