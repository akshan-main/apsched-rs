"""Event listener example.

Demonstrates how to monitor job execution, errors, and scheduler
lifecycle events using the event system.
"""

import time
from datetime import datetime

from apscheduler.events import (
    EVENT_ALL,
    EVENT_JOB_ADDED,
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    EVENT_JOB_REMOVED,
    EVENT_SCHEDULER_SHUTDOWN,
    EVENT_SCHEDULER_STARTED,
)
from apscheduler.schedulers.background import BackgroundScheduler


# --- Job functions ---


def successful_job():
    """A job that always succeeds."""
    print("  [job] Success at %s" % datetime.now().strftime("%H:%M:%S"))


def failing_job():
    """A job that always raises an exception."""
    raise ValueError("Something went wrong in the job!")


def slow_job():
    """A job that takes a while to complete."""
    print("  [job] Slow job started...")
    time.sleep(2)
    print("  [job] Slow job finished.")


# --- Event listeners ---


def on_job_executed(event):
    """Called when a job completes successfully."""
    print(
        "  [event] Job '%s' executed successfully at %s"
        % (event.job_id, datetime.now().strftime("%H:%M:%S"))
    )


def on_job_error(event):
    """Called when a job raises an exception."""
    print(
        "  [event] Job '%s' raised an error: %s"
        % (event.job_id, event.exception)
    )


def on_job_missed(event):
    """Called when a job's execution was missed (past misfire_grace_time)."""
    print(
        "  [event] Job '%s' missed its scheduled run time!" % event.job_id
    )


def on_job_lifecycle(event):
    """Called when jobs are added or removed."""
    print(
        "  [event] Job lifecycle event (code=%s): job_id=%s"
        % (event.code, event.job_id)
    )


def on_scheduler_event(event):
    """Called for scheduler start/shutdown events."""
    print(
        "  [event] Scheduler event (code=%s) at %s"
        % (event.code, datetime.now().strftime("%H:%M:%S"))
    )


def catch_all_listener(event):
    """A listener that catches every event (useful for logging/debugging)."""
    print("  [catch-all] Event code=%s type=%s" % (event.code, type(event).__name__))


if __name__ == "__main__":
    scheduler = BackgroundScheduler()

    # Register listeners with specific event masks

    # Listen for successful job executions
    scheduler.add_listener(on_job_executed, EVENT_JOB_EXECUTED)

    # Listen for job errors
    scheduler.add_listener(on_job_error, EVENT_JOB_ERROR)

    # Listen for missed jobs
    scheduler.add_listener(on_job_missed, EVENT_JOB_MISSED)

    # Listen for job additions and removals (combined mask with |)
    scheduler.add_listener(on_job_lifecycle, EVENT_JOB_ADDED | EVENT_JOB_REMOVED)

    # Listen for scheduler start and shutdown
    scheduler.add_listener(
        on_scheduler_event, EVENT_SCHEDULER_STARTED | EVENT_SCHEDULER_SHUTDOWN
    )

    # Uncomment the following line to see ALL events (noisy but useful for debugging):
    # scheduler.add_listener(catch_all_listener, EVENT_ALL)

    # Add jobs -- the EVENT_JOB_ADDED listener will fire for each
    scheduler.add_job(successful_job, "interval", seconds=3, id="good_job")
    scheduler.add_job(failing_job, "interval", seconds=7, id="bad_job")
    scheduler.add_job(slow_job, "interval", seconds=10, id="slow_job")

    # Start the scheduler -- EVENT_SCHEDULER_STARTED will fire
    scheduler.start()

    print("Scheduler running. Watch the event output below.")
    print("Press Ctrl+C to exit.\n")

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print("\nShutting down...")
        # EVENT_SCHEDULER_SHUTDOWN will fire
        scheduler.shutdown()
        print("Done.")
