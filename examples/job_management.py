"""Job management example.

Demonstrates the full lifecycle of job management:
- Adding jobs
- Modifying job properties
- Rescheduling (changing the trigger)
- Pausing and resuming
- Removing jobs
- Listing jobs
"""

import time
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler


def my_job(message="default"):
    """A simple job that prints a message."""
    print("  [%s] Job says: %s" % (datetime.now().strftime("%H:%M:%S"), message))


def another_job():
    """Another job for demonstration."""
    print("  [%s] Another job running" % datetime.now().strftime("%H:%M:%S"))


def print_separator(title):
    """Print a section separator."""
    print("\n--- %s ---" % title)


if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.start()

    # ===== ADD JOBS =====
    print_separator("Adding jobs")

    # Add a job with an explicit ID
    scheduler.add_job(
        my_job,
        "interval",
        seconds=3,
        id="job_alpha",
        kwargs={"message": "Hello from alpha"},
    )
    print("Added 'job_alpha' (interval, 3s)")

    # Add a cron job
    scheduler.add_job(
        another_job,
        "cron",
        second="*/10",
        id="job_beta",
    )
    print("Added 'job_beta' (cron, every 10s)")

    # Add a one-shot date job
    scheduler.add_job(
        my_job,
        "date",
        kwargs={"message": "One-shot!"},
        id="job_gamma",
    )
    print("Added 'job_gamma' (date, one-shot)")

    # Let them run for a bit
    time.sleep(8)

    # ===== LIST JOBS =====
    print_separator("Listing jobs")
    jobs = scheduler.get_jobs()
    print("Current jobs (%d):" % len(jobs))
    for job in jobs:
        print("  - %s" % job)

    # Get a specific job
    job = scheduler.get_job("job_alpha")
    print("\nDetails for 'job_alpha': %s" % job)

    # ===== MODIFY JOB =====
    print_separator("Modifying job_alpha")

    # Change the job's kwargs (the message it prints)
    scheduler.modify_job("job_alpha", kwargs={"message": "Modified message!"})
    print("Modified 'job_alpha' kwargs")

    # Change max_instances
    scheduler.modify_job("job_alpha", max_instances=3)
    print("Set max_instances=3 for 'job_alpha'")

    # Change the job name
    scheduler.modify_job("job_alpha", name="Alpha Job (renamed)")
    print("Renamed 'job_alpha'")

    time.sleep(5)

    # ===== RESCHEDULE JOB =====
    print_separator("Rescheduling job_alpha")

    # Change the trigger entirely (from 3s interval to 5s interval)
    scheduler.reschedule_job("job_alpha", trigger="interval", seconds=5)
    print("Rescheduled 'job_alpha' to 5-second interval")

    time.sleep(12)

    # ===== PAUSE JOB =====
    print_separator("Pausing job_alpha")

    scheduler.pause_job("job_alpha")
    print("Paused 'job_alpha'. It should stop printing.")

    time.sleep(8)

    # ===== RESUME JOB =====
    print_separator("Resuming job_alpha")

    scheduler.resume_job("job_alpha")
    print("Resumed 'job_alpha'. It should start printing again.")

    time.sleep(8)

    # ===== REMOVE JOB =====
    print_separator("Removing job_alpha")

    scheduler.remove_job("job_alpha")
    print("Removed 'job_alpha'")

    # Verify it's gone
    jobs = scheduler.get_jobs()
    print("Remaining jobs (%d):" % len(jobs))
    for job in jobs:
        print("  - %s" % job)

    time.sleep(5)

    # ===== REMOVE ALL JOBS =====
    print_separator("Removing all jobs")

    scheduler.remove_all_jobs()
    print("All jobs removed.")
    print("Remaining jobs: %d" % len(scheduler.get_jobs()))

    # ===== SHUTDOWN =====
    print_separator("Shutting down")
    scheduler.shutdown()
    print("Scheduler shut down. Done.")
