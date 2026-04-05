"""SQLite persistent jobs example.

This example demonstrates:
1. Adding persistent jobs that survive process restarts.
2. Using replace_existing=True to safely re-add jobs on startup.
3. Misfire handling and coalescing after downtime.

Run this script, let it tick a few times, then stop it with Ctrl+C.
Run it again -- the job will resume from where it left off, and any
missed executions during downtime will be coalesced into a single run.
"""

import os
from datetime import datetime

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.blocking import BlockingScheduler

# Store the database file next to this script
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "persistent_jobs.db")
DB_URL = "sqlite:///%s" % DB_PATH


def heartbeat():
    """A simple job that logs a timestamp."""
    print("[%s] Heartbeat" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


def daily_report():
    """A job that runs once a day at 9:00 AM."""
    print("[%s] Daily report generated!" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


def cleanup_old_files():
    """A job that runs every Sunday at midnight."""
    print("[%s] Cleaning up old files..." % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


if __name__ == "__main__":
    print("Database: %s" % DB_PATH)

    # Configure a SQLite-backed job store
    jobstores = {
        "default": SQLAlchemyJobStore(url=DB_URL),
    }

    scheduler = BlockingScheduler(jobstores=jobstores)

    # Add jobs with explicit IDs and replace_existing=True.
    # This is the recommended pattern for persistent jobs:
    # - The explicit ID ensures the job is uniquely identified across restarts.
    # - replace_existing=True means re-adding on startup updates the existing
    #   record instead of raising a ConflictingIdError.

    scheduler.add_job(
        heartbeat,
        "interval",
        seconds=5,
        id="heartbeat",
        replace_existing=True,
        misfire_grace_time=30,  # Allow up to 30 seconds of lateness
        coalesce=True,  # Collapse missed runs into one execution
    )

    scheduler.add_job(
        daily_report,
        "cron",
        hour=9,
        minute=0,
        id="daily_report",
        replace_existing=True,
        misfire_grace_time=3600,  # 1 hour grace period
        coalesce=True,
    )

    scheduler.add_job(
        cleanup_old_files,
        "cron",
        day_of_week="sun",
        hour=0,
        minute=0,
        id="weekly_cleanup",
        replace_existing=True,
        misfire_grace_time=86400,  # 24 hour grace period
        coalesce=True,
    )

    print("Persistent scheduler started with %d jobs." % len(scheduler.get_jobs()))
    print("Press Ctrl+C to exit. Jobs will persist in the SQLite database.\n")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\nShutting down...")
        scheduler.shutdown()
        print("Jobs saved to database. Run this script again to resume.")
