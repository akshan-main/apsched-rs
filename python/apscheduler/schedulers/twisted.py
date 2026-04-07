"""Twisted scheduler compatibility shim."""

from __future__ import annotations

import warnings

from apscheduler.schedulers.background import BackgroundScheduler


def TwistedScheduler(reactor=None, **kwargs):  # noqa: N802 — class-like factory
    """Return a :class:`BackgroundScheduler` configured for Twisted usage."""
    if reactor is not None:
        warnings.warn(
            "TwistedScheduler.reactor parameter is ignored in apscheduler-rs. "
            "The scheduler runs in a background thread.",
            stacklevel=2,
        )
    return BackgroundScheduler(**kwargs)


__all__ = ["TwistedScheduler"]
