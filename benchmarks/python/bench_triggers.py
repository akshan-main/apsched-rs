"""Trigger computation throughput benchmark.

Compares get_next_fire_time() performance between apscheduler-rs and APScheduler.
Run with apscheduler-rs installed to benchmark Rust backend, or with original
APScheduler to benchmark the pure-Python implementation.
"""
import time
import json
import sys
from datetime import datetime

# Detect which library we're testing
try:
    from apscheduler._rust import CronTrigger as _RsCheck
    LIBRARY = "apscheduler-rs"
except ImportError:
    LIBRARY = "APScheduler"

ITERATIONS = 100_000


def bench_cron_next_fire():
    """Benchmark CronTrigger.get_next_fire_time throughput."""
    from apscheduler.triggers.cron import CronTrigger
    trigger = CronTrigger(minute='*/5')
    now = datetime(2026, 1, 1, 0, 0, 0)

    start = time.perf_counter()
    prev = None
    for _ in range(ITERATIONS):
        nxt = trigger.get_next_fire_time(prev, now)
        prev = nxt
        if nxt is not None:
            now = nxt
    elapsed = time.perf_counter() - start
    ops_per_sec = ITERATIONS / elapsed
    return {"elapsed_s": elapsed, "ops_per_sec": ops_per_sec, "iterations": ITERATIONS}


def bench_interval_next_fire():
    """Benchmark IntervalTrigger.get_next_fire_time throughput."""
    from apscheduler.triggers.interval import IntervalTrigger
    trigger = IntervalTrigger(seconds=30)
    now = datetime(2026, 1, 1, 0, 0, 0)

    start = time.perf_counter()
    prev = None
    for _ in range(ITERATIONS):
        nxt = trigger.get_next_fire_time(prev, now)
        prev = nxt
        if nxt is not None:
            now = nxt
    elapsed = time.perf_counter() - start
    ops_per_sec = ITERATIONS / elapsed
    return {"elapsed_s": elapsed, "ops_per_sec": ops_per_sec, "iterations": ITERATIONS}


def bench_date_trigger():
    """Benchmark DateTrigger.get_next_fire_time throughput."""
    from apscheduler.triggers.date import DateTrigger
    now = datetime(2026, 1, 1, 0, 0, 0)

    start = time.perf_counter()
    for _ in range(ITERATIONS):
        trigger = DateTrigger(run_date=datetime(2026, 6, 15, 12, 0, 0))
        trigger.get_next_fire_time(None, now)
    elapsed = time.perf_counter() - start
    ops_per_sec = ITERATIONS / elapsed
    return {"elapsed_s": elapsed, "ops_per_sec": ops_per_sec, "iterations": ITERATIONS}


def main():
    print(f"=== Trigger Throughput Benchmark ({LIBRARY}) ===\n")

    results = {}

    print("Cron (*/5 minutes)...")
    r = bench_cron_next_fire()
    results["cron_next_fire"] = r
    print(f"  {r['ops_per_sec']:,.0f} ops/sec ({r['elapsed_s']:.3f}s for {ITERATIONS:,} iterations)\n")

    print("Interval (30s)...")
    r = bench_interval_next_fire()
    results["interval_next_fire"] = r
    print(f"  {r['ops_per_sec']:,.0f} ops/sec ({r['elapsed_s']:.3f}s for {ITERATIONS:,} iterations)\n")

    print("Date trigger (create + fire)...")
    r = bench_date_trigger()
    results["date_trigger"] = r
    print(f"  {r['ops_per_sec']:,.0f} ops/sec ({r['elapsed_s']:.3f}s for {ITERATIONS:,} iterations)\n")

    output = {"library": LIBRARY, "benchmarks": results}

    filename = f"bench_triggers_{LIBRARY.replace('-', '_').lower()}.json"
    with open(filename, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"Results saved to {filename}")


if __name__ == "__main__":
    main()
