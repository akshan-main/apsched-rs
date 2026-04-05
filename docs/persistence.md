# Persistence Guide

This document covers how apscheduler-rs persists jobs across process restarts using the SQLite-backed job store.

## SQLite Store Setup

### Basic Setup

```python
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

jobstores = {
    'default': SQLAlchemyJobStore(url='sqlite:///jobs.db')
}

scheduler = BlockingScheduler(jobstores=jobstores)
```

### Configuration Options

```python
SQLAlchemyJobStore(
    url='sqlite:///jobs.db',     # Database URL
    tablename='apscheduler_jobs', # Table name (default: 'apscheduler_jobs')
    tableschema=None,             # Schema name (for PostgreSQL)
    pickle_protocol=2,            # Pickle protocol version (for compat)
)
```

The `url` parameter accepts SQLite connection strings:

- `sqlite:///jobs.db` -- file-based database (relative path)
- `sqlite:////absolute/path/jobs.db` -- file-based database (absolute path)
- `sqlite://` or `sqlite::memory:` -- in-memory database (no persistence)

### Named Job Stores

You can use multiple job stores:

```python
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

jobstores = {
    'default': MemoryJobStore(),                          # Fast, ephemeral
    'persistent': SQLAlchemyJobStore(url='sqlite:///jobs.db'),  # Survives restart
}

scheduler = BlockingScheduler(jobstores=jobstores)

# Add a job to the persistent store
scheduler.add_job(my_func, 'interval', hours=1, jobstore='persistent')

# Add a job to the default (memory) store
scheduler.add_job(temp_func, 'interval', seconds=10)
```

## Schema Details

The SQL store automatically creates its table and index on first use. The schema is:

```sql
CREATE TABLE IF NOT EXISTS apscheduler_jobs (
    id TEXT NOT NULL PRIMARY KEY,
    next_run_time TEXT,              -- ISO 8601 datetime or NULL
    job_state TEXT NOT NULL,         -- Full ScheduleSpec as JSON
    trigger_type TEXT NOT NULL,      -- 'date', 'interval', 'cron', 'calendar_interval'
    executor TEXT NOT NULL DEFAULT 'default',
    paused INTEGER NOT NULL DEFAULT 0,
    acquired_by TEXT,               -- Scheduler ID holding the lease
    acquired_at TEXT,               -- When the lease was acquired
    lease_expires_at TEXT,          -- When the lease expires
    version INTEGER NOT NULL DEFAULT 0,
    running_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_apscheduler_jobs_next_run_time
    ON apscheduler_jobs (next_run_time)
    WHERE next_run_time IS NOT NULL AND paused = 0;
```

### Column Details

| Column | Purpose |
|--------|---------|
| `id` | Unique job identifier (user-provided or auto-generated UUID) |
| `next_run_time` | Next scheduled fire time in ISO 8601 format. NULL means the job is complete or has no scheduled time. |
| `job_state` | The full `ScheduleSpec` serialized as JSON. Contains trigger parameters, callable reference, coalesce/misfire settings, etc. |
| `trigger_type` | The trigger type string for quick filtering without deserializing `job_state`. |
| `executor` | Which executor should run this job (default: `"default"`). |
| `paused` | Whether the job is paused (0 = active, 1 = paused). Paused jobs are excluded from the `next_run_time` index. |
| `acquired_by` | Scheduler instance ID that currently holds a lease on this job. Used for multi-scheduler coordination. |
| `lease_expires_at` | When the current lease expires. Other schedulers can acquire the job after this time. |
| `version` | Optimistic locking version. Incremented on every update. |
| `running_count` | Number of currently running instances of this job. Used to enforce `max_instances`. |
| `created_at` / `updated_at` | Timestamps for auditing. |

### Partial Index

The index on `next_run_time` uses a `WHERE` clause to exclude paused jobs and jobs with no next run time. This keeps the index small and makes due-job queries fast even when the table has many paused or completed jobs.

## Restart Recovery Behavior

When a scheduler starts with a SQLite job store, the following happens:

1. **Table creation**: If the table does not exist, it is created. If it already exists, nothing changes.

2. **Job loading**: All jobs are loaded from the database. Their `next_run_time` values are preserved from the previous run.

3. **Misfire detection**: For each job whose `next_run_time` is in the past:
   - If `misfire_grace_time` is set and the job is within the grace period, it runs immediately.
   - If the job is beyond the grace period, it is treated as misfired. With `coalesce=True` (the default), the missed executions are collapsed into a single run.
   - The next fire time is then advanced to the next future occurrence.

4. **Lease cleanup**: Any stale leases from the previous scheduler instance are automatically released (the `lease_expires_at` column ensures this).

### Example: Restart Recovery

```python
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

def my_job():
    print("Running!")

scheduler = BlockingScheduler(
    jobstores={'default': SQLAlchemyJobStore(url='sqlite:///jobs.db')}
)

# First run: add the job
scheduler.add_job(
    my_job,
    'interval',
    minutes=5,
    id='my_recurring_job',      # Explicit ID for persistence
    replace_existing=True,       # Don't fail if it already exists
    misfire_grace_time=60,       # 60-second grace period
    coalesce=True,               # Collapse missed runs into one
)

try:
    scheduler.start()
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()
```

On restart:
- The job `my_recurring_job` is found in the database.
- `replace_existing=True` means `add_job` updates the existing record instead of raising an error.
- If the scheduler was down for 20 minutes, the 4 missed 5-minute intervals are coalesced into a single execution, then the schedule resumes normally.

## Callable Serialization

### Import Path References

apscheduler-rs stores callables as **import path strings** in the format `"module.path:function_name"`. For example:

```python
# This function in myapp/tasks.py
def send_report():
    ...

# Is stored as "myapp.tasks:send_report"
scheduler.add_job(send_report, 'cron', hour=9)
```

When the job fires, the scheduler resolves the import path:
1. Import the module (`myapp.tasks`)
2. Get the attribute (`send_report`)
3. Call it with the stored arguments

### What Can Be Persisted

- **Module-level functions**: Fully supported. The function must be importable by the same path when the scheduler restarts.
- **Class methods**: Supported if the class and method are importable (e.g., `"myapp.tasks:MyClass.my_method"`).
- **Built-in functions**: Supported (e.g., `print`).

### What Cannot Be Persisted

- **Lambdas**: `lambda: print("hi")` has no import path. These work with `MemoryJobStore` but fail with `SQLAlchemyJobStore`.
- **Closures**: Inner functions that capture variables cannot be serialized.
- **Dynamically created functions**: Functions created at runtime without a stable module path.
- **Decorated functions** that change `__module__` or `__qualname__`: May fail to resolve on restart.

### Backward Compatibility with Pickle

apscheduler-rs can read pickle-serialized job states from APScheduler 3.x databases. When such a job is updated (e.g., its next run time changes), it is re-serialized using import path references. This means:

1. Start with an APScheduler 3.x SQLite database.
2. Point apscheduler-rs at the same database file.
3. Jobs are loaded and deserialized from pickle.
4. On the next update cycle, they are re-serialized as JSON with import paths.

After one full cycle, all jobs are in the new format.

## Migration Between Stores

### Memory to SQLite

To migrate jobs from memory to a persistent store at runtime:

```python
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

scheduler = BackgroundScheduler(
    jobstores={
        'memory': MemoryJobStore(),
        'sqlite': SQLAlchemyJobStore(url='sqlite:///jobs.db'),
    }
)

# Add a job to memory first
scheduler.add_job(my_func, 'interval', hours=1, id='my_job', jobstore='memory')

scheduler.start()

# Later, move it to the persistent store:
# 1. Get the job details
job = scheduler.get_job('my_job', jobstore='memory')
# 2. Remove from memory
scheduler.remove_job('my_job', jobstore='memory')
# 3. Re-add to SQLite
scheduler.add_job(
    my_func, 'interval', hours=1,
    id='my_job', jobstore='sqlite',
)
```

### SQLite to Different SQLite File

To migrate to a different database file, copy the database file while the scheduler is stopped:

```bash
# Stop the scheduler
cp jobs.db jobs_backup.db

# Update your code to point to the new file
# url='sqlite:///jobs_new.db'
cp jobs.db jobs_new.db
```

### Inspecting the Database

You can inspect the job store directly with any SQLite client:

```bash
sqlite3 jobs.db

-- List all jobs
SELECT id, trigger_type, next_run_time, paused FROM apscheduler_jobs;

-- Find due jobs
SELECT id, next_run_time FROM apscheduler_jobs
WHERE next_run_time <= datetime('now') AND paused = 0;

-- View full job state
SELECT id, json_extract(job_state, '$.trigger_state') FROM apscheduler_jobs;
```
