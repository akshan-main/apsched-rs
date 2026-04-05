"""AsyncIO scheduler example with coroutine jobs.

The AsyncIOScheduler integrates with Python's asyncio event loop.
Jobs can be regular functions or async coroutines.
"""

import asyncio
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler


async def async_tick():
    """An async job that simulates an async I/O operation."""
    print("[async] Tick at %s" % datetime.now().strftime("%H:%M:%S.%f"))
    # Simulate async work (e.g., an HTTP request or database query)
    await asyncio.sleep(0.1)
    print("[async] Work done.")


async def fetch_data(url):
    """An async job that takes arguments."""
    print("[async] Fetching data from %s at %s" % (url, datetime.now().strftime("%H:%M:%S")))
    await asyncio.sleep(0.5)  # Simulate network I/O
    print("[async] Fetch complete for %s" % url)


def sync_tick():
    """A regular (non-async) job. These also work with AsyncIOScheduler."""
    print("[sync]  Tick at %s" % datetime.now().strftime("%H:%M:%S"))


async def main():
    scheduler = AsyncIOScheduler()

    # Async coroutine job, runs every 3 seconds
    scheduler.add_job(async_tick, "interval", seconds=3, id="async_tick")

    # Async job with arguments, runs every 10 seconds
    scheduler.add_job(
        fetch_data,
        "interval",
        seconds=10,
        args=["https://api.example.com/data"],
        id="fetch_data",
    )

    # Regular function job, runs every 5 seconds
    scheduler.add_job(sync_tick, "interval", seconds=5, id="sync_tick")

    scheduler.start()
    print("AsyncIO scheduler started. Press Ctrl+C to exit.\n")

    try:
        # Keep the event loop running
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
