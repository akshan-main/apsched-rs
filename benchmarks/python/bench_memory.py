"""Memory usage benchmark.

Measures RSS (Resident Set Size) with varying numbers of idle jobs.
Run with apscheduler-rs installed to benchmark Rust backend, or with original
APScheduler to benchmark the pure-Python implementation.
"""
import gc
import os
import time
import json
import resource

# Detect which library we're testing
try:
    from apscheduler._rust import CronTrigger as _RsCheck
    LIBRARY = "apscheduler-rs"
except ImportError:
    LIBRARY = "APScheduler"


def get_rss_mb():
    """Get current RSS in MB."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    # macOS reports ru_maxrss in bytes, Linux in KB
    if os.uname().sysname == 'Darwin':
        return usage.ru_maxrss / (1024 * 1024)
    return usage.ru_maxrss / 1024


def bench_memory(job_count):
    """Measure memory with N idle jobs."""
    from apscheduler.schedulers.background import BackgroundScheduler

    gc.collect()
    baseline = get_rss_mb()

    scheduler = BackgroundScheduler()
    scheduler.start()

    for i in range(job_count):
        scheduler.add_job(lambda: None, 'interval', seconds=86400, id=f'mem_{i}')

    gc.collect()
    time.sleep(0.5)
    after = get_rss_mb()

    scheduler.shutdown(wait=False)

    return {
        "baseline_mb": round(baseline, 2),
        "after_mb": round(after, 2),
        "delta_mb": round(after - baseline, 2),
        "per_job_kb": round(((after - baseline) * 1024) / job_count, 2) if job_count > 0 else 0,
        "job_count": job_count,
    }


def main():
    print(f"=== Memory Benchmark ({LIBRARY}) ===\n")

    results = {}
    for n in [1000, 10000, 100000]:
        print(f"{n:,} idle jobs...")
        r = bench_memory(n)
        results[f"memory_{n}"] = r
        print(f"  RSS: {r['after_mb']:.1f}MB (delta: {r['delta_mb']:.1f}MB, {r['per_job_kb']:.2f}KB/job)\n")

    output = {"library": LIBRARY, "benchmarks": results}
    filename = f"bench_memory_{LIBRARY.replace('-', '_').lower()}.json"
    with open(filename, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"Results saved to {filename}")


if __name__ == "__main__":
    main()
