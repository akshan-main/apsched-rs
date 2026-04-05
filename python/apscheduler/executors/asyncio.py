"""AsyncIO executor implementation."""


class AsyncIOExecutor:
    """Executor that runs coroutine jobs in the scheduler's asyncio event loop.

    This executor is automatically used by AsyncIOScheduler for coroutine jobs.
    It does not need to be explicitly configured in most cases.

    Args:
        max_workers: Maximum number of concurrent coroutine jobs (default: unlimited)
    """

    def __init__(self, max_workers: int | None = None) -> None:
        self.max_workers = max_workers

    def __repr__(self) -> str:
        return f"AsyncIOExecutor(max_workers={self.max_workers})"


__all__ = ["AsyncIOExecutor"]
