"""Various cron trigger examples.

Demonstrates daily, weekly, monthly, and complex cron expressions.
Each example prints what it would do and when it is scheduled.
"""

from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler


def log_job(label):
    """Generic job that prints a label and timestamp."""
    print("[%s] %s" % (datetime.now().strftime("%H:%M:%S"), label))


if __name__ == "__main__":
    scheduler = BlockingScheduler()

    # --- Daily jobs ---

    # Every day at 9:00 AM
    scheduler.add_job(
        log_job,
        "cron",
        hour=9,
        minute=0,
        second=0,
        args=["Daily 9 AM"],
        id="daily_9am",
    )

    # Every day at 6:30 PM
    scheduler.add_job(
        log_job,
        "cron",
        hour=18,
        minute=30,
        args=["Daily 6:30 PM"],
        id="daily_630pm",
    )

    # Twice daily at 8 AM and 8 PM
    scheduler.add_job(
        log_job,
        "cron",
        hour="8,20",
        minute=0,
        args=["Twice daily (8 AM / 8 PM)"],
        id="twice_daily",
    )

    # --- Weekly jobs ---

    # Every Monday at 9 AM
    scheduler.add_job(
        log_job,
        "cron",
        day_of_week="mon",
        hour=9,
        minute=0,
        args=["Monday 9 AM"],
        id="monday_9am",
    )

    # Weekdays only, every hour from 9 AM to 5 PM
    scheduler.add_job(
        log_job,
        "cron",
        day_of_week="mon-fri",
        hour="9-17",
        minute=0,
        args=["Weekday hourly (9-5)"],
        id="weekday_hourly",
    )

    # Every Saturday and Sunday at noon
    scheduler.add_job(
        log_job,
        "cron",
        day_of_week="sat,sun",
        hour=12,
        minute=0,
        args=["Weekend noon"],
        id="weekend_noon",
    )

    # --- Monthly jobs ---

    # First day of every month at midnight
    scheduler.add_job(
        log_job,
        "cron",
        day=1,
        hour=0,
        minute=0,
        args=["First of the month"],
        id="monthly_first",
    )

    # 15th of every month at 3 PM
    scheduler.add_job(
        log_job,
        "cron",
        day=15,
        hour=15,
        minute=0,
        args=["Mid-month report"],
        id="monthly_15th",
    )

    # Every quarter (Jan, Apr, Jul, Oct) on the 1st at 6 AM
    scheduler.add_job(
        log_job,
        "cron",
        month="1,4,7,10",
        day=1,
        hour=6,
        minute=0,
        args=["Quarterly report"],
        id="quarterly",
    )

    # --- Complex expressions ---

    # Every 15 minutes during business hours on weekdays
    scheduler.add_job(
        log_job,
        "cron",
        day_of_week="mon-fri",
        hour="9-17",
        minute="*/15",
        second=0,
        args=["Business hours every 15 min"],
        id="biz_15min",
    )

    # Every 5 minutes (all day, every day)
    scheduler.add_job(
        log_job,
        "cron",
        minute="*/5",
        second=0,
        args=["Every 5 minutes"],
        id="every_5min",
    )

    # At 30 seconds past every minute (demonstrates second field)
    scheduler.add_job(
        log_job,
        "cron",
        second=30,
        args=["Every minute at :30"],
        id="every_min_30s",
    )

    # Every even hour (0, 2, 4, 6, ..., 22)
    scheduler.add_job(
        log_job,
        "cron",
        hour="*/2",
        minute=0,
        second=0,
        args=["Every even hour"],
        id="even_hours",
    )

    # Specific year range: only in 2025 and 2026, daily at noon
    scheduler.add_job(
        log_job,
        "cron",
        year="2025-2026",
        hour=12,
        minute=0,
        second=0,
        args=["2025-2026 daily noon"],
        id="year_range",
    )

    # Print all registered jobs
    print("Registered %d cron jobs:\n" % len(scheduler.get_jobs()))
    scheduler.print_jobs()
    print()

    # Start the scheduler
    print("Press Ctrl+C to exit.\n")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
