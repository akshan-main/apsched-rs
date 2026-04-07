"""APScheduler util compatibility module.

Re-implements the most commonly used helpers from APScheduler 3.x's
``apscheduler.util`` so that downstream code expecting them keeps working.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def datetime_to_utc_timestamp(timeval: datetime | None) -> float | None:
    if timeval is None:
        return None
    if timeval.tzinfo is None:
        timeval = timeval.replace(tzinfo=timezone.utc)
    return timeval.timestamp()


def utc_timestamp_to_datetime(timestamp: float | None) -> datetime | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def datetime_repr(dateval: datetime | None) -> str:
    if dateval is None:
        return "None"
    return dateval.strftime("%Y-%m-%d %H:%M:%S %Z")


def normalize(dt: datetime) -> datetime:
    """Normalize a datetime to UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def astimezone(obj: Any) -> Any:
    """Convert a string timezone to a tzinfo, or return existing tzinfo."""
    if obj is None or hasattr(obj, "localize") or hasattr(obj, "utcoffset"):
        return obj
    if isinstance(obj, str):
        try:
            from zoneinfo import ZoneInfo

            return ZoneInfo(obj)
        except Exception:
            pass
    return obj


def convert_to_datetime(input: Any, tz: Any = None, arg_name: str = "") -> datetime:
    """Convert ``input`` to a ``datetime``, attaching ``tz`` if naive."""
    if isinstance(input, datetime):
        if input.tzinfo is None and tz is not None:
            return input.replace(tzinfo=astimezone(tz))
        return input
    if isinstance(input, str):
        return datetime.fromisoformat(input)
    raise TypeError(f"Cannot convert {input!r} to datetime")


def undefined(*args: Any, **kwargs: Any) -> None:
    """Marker for undefined values."""
    return None


def maybe_ref(ref: Any) -> Any:
    """Resolve a string callable reference, or return as-is."""
    if isinstance(ref, str):
        if ":" in ref:
            module_name, func_name = ref.rsplit(":", 1)
        elif "." in ref:
            module_name, func_name = ref.rsplit(".", 1)
        else:
            raise ValueError(f"Invalid callable ref: {ref}")
        import importlib

        module = importlib.import_module(module_name)
        return getattr(module, func_name)
    return ref


def get_callable_name(func: Any) -> str:
    """Get the name of a callable."""
    if hasattr(func, "__qualname__"):
        return func.__qualname__
    if hasattr(func, "__name__"):
        return func.__name__
    return repr(func)


def obj_to_ref(obj: Any) -> str:
    """Convert an importable object to a ``module:name`` reference string."""
    name = get_callable_name(obj)
    module = getattr(obj, "__module__", None)
    if module is None:
        raise ValueError(f"Cannot determine module of {obj!r}")
    return f"{module}:{name}"


def check_callable_args(func: Any, args: Any, kwargs: Any) -> None:
    """Check that the args/kwargs are compatible with the function signature."""
    import inspect

    try:
        sig = inspect.signature(func)
        sig.bind(*args, **kwargs)
    except TypeError as e:
        raise ValueError(f"args/kwargs incompatible with {func}: {e}") from e


__all__ = [
    "datetime_to_utc_timestamp",
    "utc_timestamp_to_datetime",
    "datetime_repr",
    "normalize",
    "astimezone",
    "convert_to_datetime",
    "undefined",
    "maybe_ref",
    "get_callable_name",
    "obj_to_ref",
    "check_callable_args",
]
