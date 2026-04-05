"""Basic background scheduler example.

The BackgroundScheduler runs in a background thread, leaving the main
thread free for other work (e.g., a web server, a CLI, or interactive use).
"""

import time
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler


def tick():
    print("Background tick at: %s" % datetime.now())


def report_status(name):
    print("[%s] Status report generated at %s" % (name, datetime.now()))


if __name__ == "__main__":
    scheduler = BackgroundScheduler()

    # Simple interval job
    scheduler.add_job(tick, "interval", seconds=5)

    # Job with arguments
    scheduler.add_job(
        report_status,
        "interval",
        seconds=10,
        args=["system-monitor"],
        id="status_report",
    )

    scheduler.start()
    print("Scheduler started in the background.")
    print("Main thread is free to do other work.")
    print("Press Ctrl+C to exit.\n")

    try:
        # Simulate the main thread doing its own work
        while True:
            print("  Main thread working... (%s)" % datetime.now().strftime("%H:%M:%S"))
            time.sleep(7)
    except (KeyboardInterrupt, SystemExit):
        print("\nShutting down scheduler...")
        scheduler.shutdown()
        print("Done.")
