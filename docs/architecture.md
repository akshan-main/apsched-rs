# Internal Architecture

This document describes the internal architecture of apscheduler-rs, a high-performance APScheduler-compatible scheduler powered by Rust.

## Layered Design

apscheduler-rs is structured in three layers:

```
+--------------------------------------------+
|  Python API Layer (python/apscheduler/)    |
|  Thin wrappers, re-exports, compatibility  |
+--------------------------------------------+
         |  PyO3 boundary
+--------------------------------------------+
|  PyO3 Extension (crates/pyext)             |
|  Python <-> Rust type conversion           |
|  GIL management, callable dispatch         |
+--------------------------------------------+
         |  Pure Rust
+--------------------------------------------+
|  Rust Core (crates/core, triggers, store,  |
|             executors)                      |
|  Scheduling engine, trigger math,          |
|  persistence, execution                    |
+--------------------------------------------+
```

**Python API Layer** (`python/apscheduler/`): Contains only `__init__.py` files and thin wrappers that re-export classes from the `apscheduler._rust` native module. This ensures that `from apscheduler.schedulers.blocking import BlockingScheduler` works identically to APScheduler 3.x.

**PyO3 Extension** (`crates/pyext/`): The bridge between Python and Rust. This crate uses [PyO3](https://pyo3.rs/) to expose Rust types as Python classes. It handles:
- Converting Python objects (datetimes, dicts, callables) to Rust types and back
- Managing the GIL when calling Python job functions from Rust threads
- Storing Python callable references that cannot be serialized into Rust

**Rust Core** (`crates/core/`, `crates/triggers/`, `crates/store/`, `crates/executors/`): Pure Rust crates with no Python dependency. The scheduling engine, trigger computation, persistence, and execution logic all live here.

## Why Rust Owns the Hot Path

The scheduler's performance-critical operations are:

1. **Trigger computation**: Determining the next fire time for a cron expression. This involves iterating over date/time fields to find the next matching instant. In APScheduler, this is interpreted Python with datetime arithmetic. In apscheduler-rs, cron fields are compiled into bitfield matchers where `next_match()` reduces to a `trailing_zeros()` CPU instruction.

2. **Due job retrieval**: Finding which jobs need to run right now. APScheduler iterates all jobs in Python. apscheduler-rs uses a `BTreeMap` time index that gives O(log n) range queries.

3. **Job store operations**: Adding, updating, and removing jobs. The memory store uses `DashMap` (lock-free concurrent hashmap) for O(1) lookups without Python's GIL.

4. **The scheduler loop**: The main loop that sleeps, wakes, checks for due jobs, and dispatches them. This runs on Tokio (Rust's async runtime), freeing the Python event loop from scheduling overhead.

Python still owns job execution -- your job functions run via the GIL as normal. The speedup comes from everything *around* job execution.

## Data Flow: From add_job to Execution

Here is how a job goes from `scheduler.add_job()` to actual execution:

```
1. Python: scheduler.add_job(my_func, 'interval', seconds=10)
   |
2. PyO3 layer: Convert Python args to Rust types
   - Callable -> CallableRef (ImportPath or InMemoryHandle)
   - Trigger string + kwargs -> TriggerState enum
   - Build ScheduleSpec with unique ID
   |
3. Rust core: SchedulerEngine.add_job(schedule_spec)
   - Compute initial next_fire_time via Trigger.get_next_fire_time()
   - Store in JobStore (DashMap + BTreeMap index for memory store)
   - Emit EVENT_JOB_ADDED event
   - Notify scheduler loop (tokio::sync::Notify)
   |
4. Scheduler loop (Tokio task):
   - Wakes up (notified or timer expired)
   - Calls store.get_due_jobs(now) via BTreeMap range scan
   - For each due job:
     a. Check max_instances (running_count < max_instances)
     b. Check misfire (is_misfired() with grace time)
     c. If coalesce=true, skip to latest pending fire time
     d. Create JobSpec from ScheduleSpec
     e. Submit to executor
   - Compute next_fire_time for the trigger
   - Update store with new next_run_time
   - Sleep until next_run_time (or until notified)
   |
5. Executor: PythonAwareExecutor
   - Spawns a thread (from thread pool)
   - Acquires GIL
   - Resolves CallableRef to Python callable
   - Calls the callable with deserialized args/kwargs
   - Catches exceptions
   - Sends JobResultEnvelope back via mpsc channel
   |
6. Result handling:
   - SchedulerEngine receives result
   - Decrements running_count
   - Emits EVENT_JOB_EXECUTED or EVENT_JOB_ERROR
   - Dispatches to registered event listeners
```

## Crate Structure

```
crates/
  core/           Core types and scheduler engine
    src/
      model.rs        ScheduleSpec, TaskSpec, TriggerState, etc.
      scheduler.rs    SchedulerEngine (main scheduling loop)
      event.rs        EventBus, SchedulerEvent, event constants
      traits.rs       Trigger, JobStore, Executor traits
      config.rs       SchedulerConfig, JobDefaults
      clock.rs        Clock trait (wall clock + test clock)
      error.rs        Error types

  triggers/       Trigger implementations
    src/
      cron/
        mod.rs        CronTrigger (wraps CompiledCronExpr)
        expr.rs       CompiledCronExpr (compiled bitfield cron)
        field.rs      FieldMatcher (bitfield for a single cron field)
        parser.rs     Cron expression parser
      interval.rs     IntervalTrigger
      date.rs         DateTrigger
      calendar.rs     CalendarIntervalTrigger
      base.rs         Shared utilities (jitter, etc.)

  store/           Job store implementations
    src/
      memory.rs       MemoryJobStore (DashMap + BTreeMap)
      sql.rs          SqlJobStore (SQLite via sqlx)
      base.rs         Shared store utilities

  executors/       Executor implementations
    src/
      threadpool.rs   ThreadPoolExecutor
      processpool.rs  ProcessPoolExecutor
      base.rs         Shared executor utilities

  pyext/           PyO3 Python extension
    src/
      lib.rs          Module registration
      scheduler.rs    Python scheduler classes
      triggers.rs     Python trigger wrappers
      stores.rs       Python store wrappers
      executors.rs    Python executor wrappers
      events.rs       Python event types
      convert.rs      Python <-> Rust type conversions
```

## Trigger Computation: Bitfield Cron

The cron trigger is the most performance-sensitive component. APScheduler computes next fire times by iterating through date/time fields in Python, creating datetime objects at each step. apscheduler-rs compiles cron expressions into bitfield matchers.

### FieldMatcher

Each cron field (second, minute, hour, day, month, day_of_week, year) is represented by a `FieldMatcher`: a fixed-size array of 3 `u64` values (192 bits total, enough for the year range 1970-2099).

```rust
pub struct FieldMatcher {
    bits: [u64; 3],   // 192 bits
    pub min: u32,      // e.g., 0 for seconds
    pub max: u32,      // e.g., 59 for seconds
}
```

Key operations:

- `matches(value)` -- O(1) bit check
- `next_match(from)` -- finds next set bit using `trailing_zeros()`, which maps to a single CPU instruction (`BSF`/`TZCNT` on x86)
- `first_match()` -- finds lowest set bit

### CompiledCronExpr

A `CompiledCronExpr` holds one `FieldMatcher` per cron field. Computing the next fire time walks through fields from year down to second, using `next_match()` to find the next allowed value. When a field wraps around (e.g., no matching minute in the current hour), the algorithm increments the parent field and resets children.

This is structurally similar to APScheduler's algorithm, but each field lookup is a single CPU instruction instead of a Python loop.

### Example

Cron expression `*/15 9-17 * * MON-FRI` (every 15 minutes, 9 AM to 5 PM, weekdays):

```
minutes:   bits for {0, 15, 30, 45}
hours:     bits for {9, 10, 11, 12, 13, 14, 15, 16, 17}
days:      all bits set (1-31)
months:    all bits set (1-12)
weekdays:  bits for {0, 1, 2, 3, 4}  (MON=0 through FRI=4)
seconds:   bits for {0}
```

## Memory Model

### MemoryJobStore

The in-memory store uses two data structures:

```
DashMap<String, ScheduleSpec>          -- Primary storage (job_id -> spec)
RwLock<BTreeMap<(DateTime, String)>>   -- Time index for due-job queries
```

**DashMap** is a lock-free concurrent hashmap (sharded internally). It provides O(1) lookups, inserts, and deletes without acquiring a global lock. Multiple threads can read and write simultaneously.

**BTreeMap** serves as a sorted index on `(next_run_time, job_id)`. Due job queries become range scans: `idx.range(..=now)` returns all jobs whose next run time is at or before `now`, in chronological order. This is O(log n + k) where k is the number of due jobs.

Additional maps track running instance counts and leases:

```
DashMap<String, u32>       -- running_counts (job_id -> count)
DashMap<String, JobLease>  -- leases (for multi-scheduler coordination)
```

### SqlJobStore

The SQL store uses `sqlx` with SQLite. The schema includes:

- A `next_run_time` column with a partial index (`WHERE next_run_time IS NOT NULL AND paused = 0`) for efficient due-job queries
- A `version` column for optimistic locking during job acquisition
- `acquired_by` / `lease_expires_at` columns for multi-scheduler lease management
- `job_state` stored as JSON (the full `ScheduleSpec` serialized via `serde_json`)

## Threading Model

### Tokio Runtime

The scheduler engine runs on a Tokio multi-threaded runtime. The main scheduling loop is a Tokio task that:

1. Sleeps until the next job's fire time (using `tokio::time::sleep_until`)
2. Wakes early if notified (via `tokio::sync::Notify`) when jobs are added/modified
3. Queries the store for due jobs
4. Submits them to executors
5. Processes result messages from the `mpsc` channel

### GIL Handling

Python's Global Interpreter Lock is a critical concern. apscheduler-rs minimizes GIL contention:

- **Scheduling decisions** (trigger computation, due-job queries, coalescing, misfire checks) happen entirely in Rust, without holding the GIL.
- **The GIL is acquired only when**:
  - Calling a Python job function
  - Converting Python arguments to/from Rust types at the `add_job` boundary
  - Dispatching event callbacks to Python listeners
- **For AsyncIOScheduler**, coroutine jobs are scheduled on the Python asyncio loop via `asyncio.run_coroutine_threadsafe`, which acquires the GIL briefly to schedule the coroutine and then releases it.

### Thread Pool Executor

The `PythonAwareExecutor` manages a pool of threads for running Python callables. Each thread:

1. Receives a `JobSpec` from the scheduler
2. Acquires the GIL (`Python::with_gil`)
3. Resolves the `CallableRef` to a Python callable (either by import path or from the in-memory handle store)
4. Calls the function with deserialized arguments
5. Catches any Python exception
6. Releases the GIL
7. Sends the result back via an `mpsc` channel

The thread pool size is configurable via `ThreadPoolExecutor(max_workers=N)`.
