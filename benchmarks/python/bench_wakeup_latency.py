"""Wakeup latency benchmark.

Measures the time between a job's scheduled fire time and its actual execution.
Run with apscheduler-rs installed to benchmark Rust backend, or with original
APScheduler to benchmark the pure-Python implementation.
"""
import time
import json
import threading
from datetime import datetime, timedelta, timezone

# Detect which library we're testing
try:
    from apscheduler._rust import CronTrigger as _RsCheck
    LIBRARY = "apscheduler-rs"
except ImportError:
    LIBRARY = "APScheduler"


def bench_wakeup_latency(iterations=20):
    """Measure latency between scheduled fire time and actual execution.

    Uses a single BackgroundScheduler and interval-based approach to avoid
    overhead of creating/destroying schedulers per iteration.
    """
    from apscheduler.schedulers.background import BackgroundScheduler

    latencies = []
    scheduler = BackgroundScheduler()
    scheduler.start()

    for i in range(iterations):
        result = {}
        event = threading.Event()

        def record_time(_r=result, _e=event):
            _r['actual'] = time.perf_counter()
            _e.set()

        # Schedule job 1 second from now to give scheduler time to pick it up
        scheduled_at = time.perf_counter() + 1.0
        run_date = datetime.now(timezone.utc) + timedelta(seconds=1)
        result['scheduled'] = scheduled_at

        scheduler.add_job(record_time, 'date', run_date=run_date, id=f'latency_{i}')
        got = event.wait(timeout=5.0)

        if got and 'actual' in result:
            latency_ms = (result['actual'] - result['scheduled']) * 1000
            latencies.append(latency_ms)

    scheduler.shutdown(wait=False)

    if latencies:
        sorted_lat = sorted(latencies)
        n = len(sorted_lat)
        avg = sum(latencies) / n
        p50 = sorted_lat[n // 2]
        p95 = sorted_lat[int(n * 0.95)]
        p99 = sorted_lat[min(int(n * 0.99), n - 1)]
        return {
            "avg_ms": round(avg, 2),
            "p50_ms": round(p50, 2),
            "p95_ms": round(p95, 2),
            "p99_ms": round(p99, 2),
            "min_ms": round(min(latencies), 2),
            "max_ms": round(max(latencies), 2),
            "samples": n,
        }
    return {"error": "no results collected"}


def main():
    print(f"=== Wakeup Latency Benchmark ({LIBRARY}) ===\n")

    print("Measuring wakeup latency (20 samples, 1s scheduled delay each)...")
    r = bench_wakeup_latency(20)

    if "error" in r:
        print(f"  ERROR: {r['error']}\n")
    else:
        print(f"  avg={r['avg_ms']:.1f}ms, p50={r['p50_ms']:.1f}ms, p95={r['p95_ms']:.1f}ms, p99={r['p99_ms']:.1f}ms")
        print(f"  min={r['min_ms']:.1f}ms, max={r['max_ms']:.1f}ms ({r['samples']} samples)\n")

    output = {"library": LIBRARY, "benchmarks": {"wakeup_latency": r}}
    filename = f"bench_latency_{LIBRARY.replace('-', '_').lower()}.json"
    with open(filename, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"Results saved to {filename}")


if __name__ == "__main__":
    main()
