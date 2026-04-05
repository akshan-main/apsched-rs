"""Head-to-head benchmark: apscheduler-rs vs APScheduler 3.11.x

Runs each benchmark in a subprocess with controlled sys.path to select
which library is active. The Rust version is loaded by prepending its
python/ directory to PYTHONPATH; the original APScheduler loads from
site-packages when that path is absent.
"""
import subprocess
import sys
import json
import os
import textwrap
import time

RS_PYTHON_DIR = "/Users/akshankrithick/projects/apsched-rs/apsched-rs/python"

TRIGGER_CODE = textwrap.dedent(r"""
import time, json, sys
from datetime import datetime, timezone

try:
    from apscheduler._rust import CronTrigger as _RC
    lib = "apscheduler-rs"
except ImportError:
    lib = "APScheduler"

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

results = {}

# --- Cron */5 minutes, 100k iterations ---
ct = CronTrigger(minute='*/5')
now = datetime(2026, 1, 1, 0, 0, 0)
start = time.perf_counter()
prev = None
for _ in range(100_000):
    nxt = ct.get_next_fire_time(prev, now)
    prev = nxt
    if nxt:
        now = nxt
elapsed = time.perf_counter() - start
results['cron_100k_ops_sec'] = round(100_000 / elapsed)
results['cron_100k_elapsed_s'] = round(elapsed, 4)

# --- Interval 30s, 100k iterations ---
it = IntervalTrigger(seconds=30)
now = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
start = time.perf_counter()
prev = None
for _ in range(100_000):
    nxt = it.get_next_fire_time(prev, now)
    prev = nxt
    if nxt:
        now = nxt
elapsed = time.perf_counter() - start
results['interval_100k_ops_sec'] = round(100_000 / elapsed)
results['interval_100k_elapsed_s'] = round(elapsed, 4)

print(json.dumps({"library": lib, "results": results}))
""")

SCHEDULER_CODE = textwrap.dedent(r"""
import time, json, gc, resource, os
from datetime import datetime, timedelta, timezone

try:
    from apscheduler._rust import CronTrigger as _RC
    lib = "apscheduler-rs"
except ImportError:
    lib = "APScheduler"

from apscheduler.schedulers.background import BackgroundScheduler

results = {}

# --- Startup time (100 iterations) ---
times = []
for _ in range(100):
    start = time.perf_counter()
    s = BackgroundScheduler()
    s.start()
    elapsed = time.perf_counter() - start
    times.append(elapsed)
    s.shutdown(wait=False)
results['startup_avg_ms'] = round(sum(times) / len(times) * 1000, 2)

# --- Add 10,000 jobs ---
s = BackgroundScheduler()
s.start()
start = time.perf_counter()
for i in range(10_000):
    s.add_job(lambda: None, 'interval', seconds=3600, id=f'j{i}')
elapsed = time.perf_counter() - start
results['add_10k_ops_sec'] = round(10_000 / elapsed)
results['add_10k_elapsed_s'] = round(elapsed, 3)

# --- Get jobs (with 10k loaded) ---
start = time.perf_counter()
for _ in range(100):
    s.get_jobs()
elapsed = time.perf_counter() - start
results['get_jobs_100x_ops_sec'] = round(100 / elapsed)

# --- Remove all 10k jobs ---
start = time.perf_counter()
s.remove_all_jobs()
elapsed = time.perf_counter() - start
results['remove_all_10k_s'] = round(elapsed, 4)
s.shutdown(wait=False)

# --- Wakeup latency ---
latencies = []
for i in range(20):
    s2 = BackgroundScheduler()
    s2.start()
    result_dict = {}
    def record(_r=result_dict): _r['t'] = time.perf_counter()
    target = time.perf_counter() + 0.1
    run_at = datetime.now(timezone.utc) + timedelta(milliseconds=100)
    result_dict['scheduled'] = target
    s2.add_job(record, 'date', run_date=run_at, id=f'lat{i}')
    time.sleep(0.5)
    s2.shutdown(wait=True)
    if 't' in result_dict:
        latencies.append((result_dict['t'] - result_dict['scheduled']) * 1000)

if latencies:
    latencies.sort()
    results['wakeup_avg_ms'] = round(sum(latencies) / len(latencies), 1)
    results['wakeup_p50_ms'] = round(latencies[len(latencies) // 2], 1)
    results['wakeup_p95_ms'] = round(latencies[int(len(latencies) * 0.95)], 1)

# --- Memory: 10k jobs ---
gc.collect()
baseline = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
if os.uname().sysname == 'Darwin':
    baseline /= (1024 * 1024)  # bytes -> MB on macOS
else:
    baseline /= 1024  # KB -> MB on Linux

s3 = BackgroundScheduler()
s3.start()
for i in range(10_000):
    s3.add_job(lambda: None, 'interval', seconds=86400, id=f'm{i}')
gc.collect()
time.sleep(0.3)
after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
if os.uname().sysname == 'Darwin':
    after /= (1024 * 1024)
else:
    after /= 1024
results['memory_10k_delta_mb'] = round(after - baseline, 1)
results['memory_per_job_bytes'] = round((after - baseline) * 1024 * 1024 / 10_000) if (after - baseline) > 0 else 0
s3.shutdown(wait=False)

print(json.dumps({"library": lib, "results": results}))
""")


def run_bench(code: str, use_rust: bool, timeout: int = 120) -> dict:
    """Run benchmark code in subprocess, return parsed JSON result."""
    env = os.environ.copy()
    if use_rust:
        # Prepend Rust python dir so it shadows the original
        existing = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = RS_PYTHON_DIR + (":" + existing if existing else "")
    else:
        # Remove any reference to the Rust python dir from PYTHONPATH
        existing = env.get("PYTHONPATH", "")
        if existing:
            parts = [p for p in existing.split(":") if RS_PYTHON_DIR not in p]
            env["PYTHONPATH"] = ":".join(parts) if parts else ""

    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True, text=True, env=env, timeout=timeout
    )
    if result.returncode != 0:
        print(f"  STDERR: {result.stderr[:500]}")
        return {"library": "ERROR", "results": {}}
    # Parse last line as JSON
    for line in reversed(result.stdout.strip().split("\n")):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    print(f"  No JSON found. stdout: {result.stdout[:500]}")
    return {"library": "ERROR", "results": {}}


def fmt_val(key: str, val) -> str:
    """Format a value nicely for the table."""
    if val is None:
        return "N/A"
    if "ops_sec" in key:
        return f"{val:,}"
    if "_ms" in key:
        return f"{val} ms"
    if "_s" in key and "ops" not in key:
        return f"{val} s"
    if "_mb" in key:
        return f"{val} MB"
    if "bytes" in key:
        return f"{val:,}"
    return str(val)


def speedup(key: str, rs_val, py_val) -> str:
    """Calculate speedup string. Higher is better for ops_sec, lower for latency/time."""
    if rs_val is None or py_val is None:
        return "N/A"
    if py_val == 0 or rs_val == 0:
        return "N/A"
    if "ops_sec" in key:
        ratio = rs_val / py_val
        return f"{ratio:.1f}x" if ratio >= 1 else f"1/{1/ratio:.1f}x"
    elif "_ms" in key or "_s" in key or "_mb" in key or "bytes" in key:
        ratio = py_val / rs_val
        return f"{ratio:.1f}x" if ratio >= 1 else f"1/{1/ratio:.1f}x"
    return "N/A"


def main():
    print("=" * 70)
    print("  Head-to-Head Benchmark: apscheduler-rs vs APScheduler 3.11.x")
    print("=" * 70)
    print()

    # --- Trigger benchmarks ---
    print("[1/4] Running trigger benchmarks (apscheduler-rs)...")
    rs_trig = run_bench(TRIGGER_CODE, use_rust=True)
    print(f"  -> {rs_trig['library']}: {rs_trig['results']}")

    print("[2/4] Running trigger benchmarks (APScheduler 3.11)...")
    py_trig = run_bench(TRIGGER_CODE, use_rust=False)
    print(f"  -> {py_trig['library']}: {py_trig['results']}")

    # --- Scheduler benchmarks ---
    print("[3/4] Running scheduler benchmarks (apscheduler-rs)...")
    rs_sched = run_bench(SCHEDULER_CODE, use_rust=True, timeout=180)
    print(f"  -> {rs_sched['library']}: {rs_sched['results']}")

    print("[4/4] Running scheduler benchmarks (APScheduler 3.11)...")
    py_sched = run_bench(SCHEDULER_CODE, use_rust=False, timeout=180)
    print(f"  -> {py_sched['library']}: {py_sched['results']}")

    # --- Merge results ---
    rs_all = {**rs_trig.get("results", {}), **rs_sched.get("results", {})}
    py_all = {**py_trig.get("results", {}), **py_sched.get("results", {})}

    all_keys = list(dict.fromkeys(list(rs_all.keys()) + list(py_all.keys())))

    # --- Print comparison table ---
    print()
    print("=" * 90)
    print("  COMPARISON TABLE")
    print("=" * 90)

    hdr_metric = "Metric".ljust(28)
    hdr_rs = "apscheduler-rs".rjust(18)
    hdr_py = "APScheduler 3.11".rjust(18)
    hdr_sp = "Speedup".rjust(10)

    print(f"| {hdr_metric} | {hdr_rs} | {hdr_py} | {hdr_sp} |")
    print(f"|{'-'*30}|{'-'*20}|{'-'*20}|{'-'*12}|")

    for key in all_keys:
        rs_v = rs_all.get(key)
        py_v = py_all.get(key)
        label = key.ljust(28)
        rs_s = fmt_val(key, rs_v).rjust(18)
        py_s = fmt_val(key, py_v).rjust(18)
        sp = speedup(key, rs_v, py_v).rjust(10)
        print(f"| {label} | {rs_s} | {py_s} | {sp} |")

    print()
    print("Higher ops/sec is better. Lower ms/s/MB is better.")
    print("Speedup > 1.0x means apscheduler-rs is faster/smaller.")
    print()


if __name__ == "__main__":
    main()
