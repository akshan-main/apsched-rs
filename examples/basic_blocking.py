"""Basic blocking scheduler example.

The BlockingScheduler runs in the foreground and blocks the main thread.
Use this when the scheduler is the only thing your application does.
"""

from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler


def tick():
    print("Tick! The time is: %s" % datetime.now())


if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(tick, "interval", seconds=3)
    print("Press Ctrl+C to exit")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
