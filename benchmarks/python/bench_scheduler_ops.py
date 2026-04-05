"""Scheduler operation throughput benchmark.

Measures add_job, get_jobs, and remove_job performance, plus scheduler startup time.
Run with apscheduler-rs installed to benchmark Rust backend, or with original
APScheduler to benchmark the pure-Python implementation.
"""
import time
import json

# Detect which library we're testing
try:
    from apscheduler._rust import CronTrigger as _RsCheck
    LIBRARY = "apscheduler-rs"
except ImportError:
    LIBRARY = "APScheduler"


def bench_add_jobs(n):
    """Benchmark adding N jobs."""
    from apscheduler.schedulers.background import BackgroundScheduler

    scheduler = BackgroundScheduler()
    scheduler.start()

    start = time.perf_counter()
    for i in range(n):
        scheduler.add_job(lambda: None, 'interval', seconds=3600, id=f'job_{i}')
    elapsed = time.perf_counter() - start

    scheduler.shutdown(wait=False)
    ops_per_sec = n / elapsed
    return {"elapsed_s": elapsed, "ops_per_sec": ops_per_sec, "count": n}


def bench_get_jobs(n):
    """Benchmark get_jobs with N jobs present."""
    from apscheduler.schedulers.background import BackgroundScheduler

    scheduler = BackgroundScheduler()
    scheduler.start()
    for i in range(n):
        scheduler.add_job(lambda: None, 'interval', seconds=3600, id=f'job_{i}')

    iterations = 1000
    start = time.perf_counter()
    for _ in range(iterations):
        scheduler.get_jobs()
    elapsed = time.perf_counter() - start

    scheduler.shutdown(wait=False)
    ops_per_sec = iterations / elapsed
    return {"elapsed_s": elapsed, "ops_per_sec": ops_per_sec, "job_count": n, "iterations": iterations}


def bench_remove_jobs(n):
    """Benchmark removing N jobs."""
    from apscheduler.schedulers.background import BackgroundScheduler

    scheduler = BackgroundScheduler()
    scheduler.start()
    for i in range(n):
        scheduler.add_job(lambda: None, 'interval', seconds=3600, id=f'job_{i}')

    start = time.perf_counter()
    for i in range(n):
        scheduler.remove_job(f'job_{i}')
    elapsed = time.perf_counter() - start

    scheduler.shutdown(wait=False)
    ops_per_sec = n / elapsed
    return {"elapsed_s": elapsed, "ops_per_sec": ops_per_sec, "count": n}


def bench_startup():
    """Benchmark scheduler startup time."""
    from apscheduler.schedulers.background import BackgroundScheduler

    times = []
    for _ in range(100):
        start = time.perf_counter()
        s = BackgroundScheduler()
        s.start()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        s.shutdown(wait=False)

    avg = sum(times) / len(times)
    return {"avg_s": avg, "min_s": min(times), "max_s": max(times), "iterations": 100}


def main():
    print(f"=== Scheduler Operations Benchmark ({LIBRARY}) ===\n")

    results = {}

    print("Startup time (100 iterations)...")
    r = bench_startup()
    results["startup"] = r
    print(f"  avg={r['avg_s']*1000:.2f}ms, min={r['min_s']*1000:.2f}ms, max={r['max_s']*1000:.2f}ms\n")

    for n in [1000, 10000]:
        print(f"Add {n:,} jobs...")
        r = bench_add_jobs(n)
        results[f"add_{n}"] = r
        print(f"  {r['ops_per_sec']:,.0f} ops/sec ({r['elapsed_s']:.3f}s)\n")

    for n in [1000, 10000]:
        print(f"Get jobs with {n:,} present (1000 calls)...")
        r = bench_get_jobs(n)
        results[f"get_jobs_{n}"] = r
        print(f"  {r['ops_per_sec']:,.0f} ops/sec ({r['elapsed_s']:.3f}s)\n")

    print("Remove 1,000 jobs...")
    r = bench_remove_jobs(1000)
    results["remove_1000"] = r
    print(f"  {r['ops_per_sec']:,.0f} ops/sec ({r['elapsed_s']:.3f}s)\n")

    output = {"library": LIBRARY, "benchmarks": results}
    filename = f"bench_scheduler_{LIBRARY.replace('-', '_').lower()}.json"
    with open(filename, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"Results saved to {filename}")


if __name__ == "__main__":
    main()
