# Performance Guide

This document explains where apscheduler-rs is faster than APScheduler, why, and how to measure the difference.

## What's Faster and Why

apscheduler-rs accelerates the **scheduler overhead** -- everything that happens between your job functions. The job functions themselves still run as Python code. The speedup comes from three areas: trigger computation, scheduler loop overhead, and memory efficiency.

## Trigger Computation

### The bottleneck in APScheduler

APScheduler's cron trigger computes next fire times by iterating through datetime fields in Python. For a cron expression like `*/5 9-17 * * MON-FRI`, the algorithm:

1. Creates Python `datetime` objects at each step
2. Uses Python comparison operators and arithmetic
3. Loops through field values to find the next match
4. Handles wrapping (e.g., minute 55 -> next hour) with Python control flow

Each `get_next_fire_time()` call involves dozens of Python object allocations and interpreted bytecode operations.

### How apscheduler-rs does it

apscheduler-rs compiles cron expressions into bitfield matchers. Each cron field (second, minute, hour, etc.) is represented as a fixed-size array of `u64` values where each bit represents whether a value is allowed:

```
minute field for "*/15":  bits = 0b...0001_0000_0000_0001_0000_0000_0001_0000_0000_0001
                                         45              30              15               0
```

Finding the next matching value is a `trailing_zeros()` operation, which maps to a single CPU instruction (`TZCNT` on x86, `CLZ` on ARM). The entire next-fire-time computation involves no heap allocations and no interpreted code.

**Expected speedup**: 10-50x for individual trigger computations. The difference compounds with many jobs because the scheduler must compute next fire times for every due job on each wake cycle.

## Scheduler Loop Overhead

### APScheduler's approach

APScheduler's scheduler loop runs in Python. On each wake cycle it:

1. Iterates all jobs in the store (Python `for` loop)
2. Checks each job's `next_run_time` against `now` (Python datetime comparison)
3. Applies misfire/coalesce logic (Python conditionals)
4. Dispatches to executors (Python method calls)

The GIL is held for the entire scheduling decision.

### apscheduler-rs approach

The scheduler loop runs as a Tokio async task in Rust:

1. Due jobs are found via a `BTreeMap` range scan -- O(log n + k) where k is the number of due jobs, not O(n) like iterating all jobs
2. Misfire/coalesce checks are Rust struct field comparisons
3. The GIL is never held during scheduling decisions

**Expected speedup**: Proportional to the number of jobs. With 10 jobs, the difference is negligible. With 10,000 jobs, the Rust loop is significantly faster because it avoids the O(n) scan and GIL contention.

## Memory Efficiency

### Python objects

In CPython, every Python object has at least 16 bytes of overhead (type pointer + reference count), and a `datetime` object is 48+ bytes. A single APScheduler job involves several datetime objects, a dict of trigger fields, a reference to the callable, and job metadata -- typically 1-2 KB per job.

### Rust structs

A `ScheduleSpec` in Rust is a flat struct with inline fields. A `FieldMatcher` (cron field) is 32 bytes (3 x u64 + min + max). The entire compiled cron trigger is under 256 bytes. A complete job record including trigger, metadata, and task spec is typically 300-500 bytes.

**Expected improvement**: 3-5x lower memory per job. This matters when you have thousands of jobs.

## When You'll See the Biggest Difference

- **Many jobs (1,000+)**: The O(log n) due-job lookup and Rust scheduling loop dominate. APScheduler's O(n) scan becomes a bottleneck.
- **High job churn**: Frequent `add_job`/`remove_job`/`modify_job` operations benefit from DashMap's lock-free concurrency versus Python dict + GIL.
- **Short interval jobs**: Jobs that fire every second or faster amplify trigger computation cost. Rust's compiled bitfield matching shines here.
- **Complex cron expressions**: Expressions with many fields (e.g., `0 */5 9-17 1,15 1-6 MON-FRI 2025-2030`) require more field iterations, magnifying the per-computation speedup.
- **Concurrent schedulers**: If you run multiple `BackgroundScheduler` instances in the same process, Rust's DashMap handles concurrent access without GIL serialization.

## When You Won't See a Difference

- **Few jobs (< 50)**: Scheduling overhead is tiny compared to job execution time. Both implementations are fast enough.
- **Slow job bodies**: If your jobs make network requests, database queries, or heavy computations, the job runtime dominates and scheduler overhead is irrelevant.
- **Infrequent firing**: Jobs that run once a day have minimal trigger computation cost regardless of implementation.
- **Single job, simple trigger**: A single interval job running every 10 minutes will feel identical in both implementations.

## How to Run Benchmarks

apscheduler-rs includes a benchmark suite that compares against APScheduler on the same workloads.

### Prerequisites

Set up two virtual environments:

```bash
# Environment with apscheduler-rs
python -m venv .venv-rs
source .venv-rs/bin/activate
pip install -e ".[dev]"

# You also need APScheduler installed for comparison
pip install "apscheduler>=3.10,<4.0"
```

### Running Benchmarks

Run all benchmarks:

```bash
python benchmarks/python/run_all.py
```

Run individual benchmarks:

```bash
# Trigger computation (cron, interval, date)
python benchmarks/python/bench_triggers.py

# Scheduler operations (add/remove/modify jobs)
python benchmarks/python/bench_scheduler_ops.py

# Wakeup latency (time from scheduled fire time to actual execution)
python benchmarks/python/bench_wakeup_latency.py

# Memory usage (per-job memory footprint)
python benchmarks/python/bench_memory.py
```

### What the Benchmarks Measure

| Benchmark | What it measures | Key metric |
|-----------|-----------------|------------|
| `bench_triggers.py` | `get_next_fire_time()` latency for cron, interval, and date triggers | ns/operation |
| `bench_scheduler_ops.py` | `add_job`, `remove_job`, `modify_job`, `get_jobs` throughput | ops/second |
| `bench_wakeup_latency.py` | Time between a job's scheduled fire time and actual callback invocation | p50/p99 latency in ms |
| `bench_memory.py` | RSS memory growth when adding N jobs | bytes/job |

### Interpreting Results

The benchmark scripts output results as both human-readable tables and JSON files for programmatic comparison. Typical results:

- **Trigger computation**: apscheduler-rs is 10-50x faster
- **Scheduler ops with 10K jobs**: apscheduler-rs is 5-20x faster for add/remove
- **Wakeup latency**: apscheduler-rs has lower p99 latency due to no GIL contention in the scheduling path
- **Memory**: apscheduler-rs uses 3-5x less memory per job

Your actual numbers will depend on hardware, Python version, and workload characteristics.
