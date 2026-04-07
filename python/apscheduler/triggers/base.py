"""Base trigger interface (compatibility shim)."""

from __future__ import annotations


class BaseTrigger:
    """Base class for triggers."""

    def get_next_fire_time(self, previous_fire_time, now):  # pragma: no cover
        raise NotImplementedError

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__.update(state)


__all__ = ["BaseTrigger"]
