# Migration Guide: APScheduler to apscheduler-rs

This guide covers how to migrate from [APScheduler](https://github.com/agronholm/apscheduler) 3.x to [apscheduler-rs](https://github.com/akshankrithick/apscheduler-rs), a drop-in replacement powered by a Rust scheduling engine.

## Installation

Replace the APScheduler package with apscheduler-rs:

```bash
# Before
pip install apscheduler

# After
pip install apscheduler-rs
```

If you use SQLAlchemy-backed job stores, install the optional dependency:

```bash
pip install apscheduler-rs[sqlalchemy]
```

## Import Paths Are Identical

apscheduler-rs uses the same `apscheduler` package namespace. Your existing imports work without changes:

```python
# These imports work with both APScheduler and apscheduler-rs
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.executors.asyncio import AsyncIOExecutor

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.calendarinterval import CalendarIntervalTrigger

from apscheduler.events import (
    EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED, EVENT_ALL,
    SchedulerEvent, JobEvent, JobExecutionEvent,
)
```

## Side-by-Side API Comparison

### Basic Scheduler Setup

**APScheduler:**

```python
from apscheduler.schedulers.blocking import BlockingScheduler

def my_job():
    print("Hello!")

scheduler = BlockingScheduler()
scheduler.add_job(my_job, 'interval', seconds=10)
scheduler.start()
```

**apscheduler-rs (identical):**

```python
from apscheduler.schedulers.blocking import BlockingScheduler

def my_job():
    print("Hello!")

scheduler = BlockingScheduler()
scheduler.add_job(my_job, 'interval', seconds=10)
scheduler.start()
```

### Cron Trigger

**APScheduler:**

```python
scheduler.add_job(my_job, 'cron', hour=9, minute=30, day_of_week='mon-fri')
```

**apscheduler-rs (identical):**

```python
scheduler.add_job(my_job, 'cron', hour=9, minute=30, day_of_week='mon-fri')
```

### Event Listeners

**APScheduler:**

```python
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

def my_listener(event):
    if event.exception:
        print(f"Job {event.job_id} failed")
    else:
        print(f"Job {event.job_id} succeeded")

scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
```

**apscheduler-rs (identical):**

```python
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

def my_listener(event):
    if event.exception:
        print(f"Job {event.job_id} failed")
    else:
        print(f"Job {event.job_id} succeeded")

scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
```

### Persistent Job Store

**APScheduler:**

```python
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

jobstores = {
    'default': SQLAlchemyJobStore(url='sqlite:///jobs.db')
}
scheduler = BlockingScheduler(jobstores=jobstores)
```

**apscheduler-rs (identical):**

```python
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

jobstores = {
    'default': SQLAlchemyJobStore(url='sqlite:///jobs.db')
}
scheduler = BlockingScheduler(jobstores=jobstores)
```

## Key Differences

### Performance Characteristics

apscheduler-rs delegates scheduling hot paths to Rust:

- **Trigger computation** runs as compiled Rust code using bitfield cron matching, not interpreted Python.
- **Job store operations** in memory use lock-free `DashMap` structures instead of Python dictionaries.
- **The scheduler loop** runs on Tokio (Rust async runtime), avoiding Python's GIL for scheduling decisions.
- **Job execution** still runs your Python code via the GIL -- the speedup is in the scheduler overhead, not in the jobs themselves.

### Rust Engine Under the Hood

apscheduler-rs is built with [PyO3](https://pyo3.rs/) and [maturin](https://www.maturin.rs/). The Python layer (`apscheduler.*`) is a thin wrapper that delegates to `apscheduler._rust`, a native extension module compiled from Rust. You never need to interact with the Rust layer directly.

### Callable Serialization

APScheduler 3.x uses `pickle` to serialize callables for persistent job stores. apscheduler-rs uses **import path references** (`"mymodule:my_function"`) for persistent storage, with `pickle` support for backward compatibility. This means:

- Functions defined at module level work identically.
- Lambdas and closures cannot be persisted (same limitation as APScheduler).
- When migrating an existing SQLite database, apscheduler-rs can read pickle-serialized jobs but will re-serialize them using import paths on next update.

### Python Version Support

apscheduler-rs requires Python 3.9 or later. APScheduler 3.x supports Python 3.6+.

## What Works the Same

- All three scheduler types: `BlockingScheduler`, `BackgroundScheduler`, `AsyncIOScheduler`
- All trigger types: `date`, `interval`, `cron`, `calendarinterval`
- Job stores: `MemoryJobStore`, `SQLAlchemyJobStore` (SQLite)
- Executors: `ThreadPoolExecutor`, `ProcessPoolExecutor`, `AsyncIOExecutor`
- Job management: `add_job`, `modify_job`, `reschedule_job`, `pause_job`, `resume_job`, `remove_job`, `get_jobs`, `get_job`
- Misfire handling with `misfire_grace_time`
- Coalescing (`coalesce=True/False`)
- Max instances (`max_instances`)
- Jitter (`jitter`)
- Event system with bitmask-based filtering
- Timezone support via `tzdata`

## What's Different

| Aspect | APScheduler 3.x | apscheduler-rs |
|--------|-----------------|----------------|
| Scheduler engine | Pure Python | Rust via PyO3 |
| Cron parsing | Interpreted Python | Compiled bitfield matching |
| Memory store internals | Python dict | DashMap + BTreeMap index |
| Async runtime | Python asyncio | Tokio (Rust) + Python asyncio bridge |
| Serialization default | pickle | Import path (JSON) |
| Build system | setuptools | maturin |
| Minimum Python | 3.6 | 3.9 |

## Known Incompatibilities

1. **No `GeventScheduler` or `TornadoScheduler`.** Only `BlockingScheduler`, `BackgroundScheduler`, and `AsyncIOScheduler` are implemented.

2. **No `MongoDBJobStore` or `RedisJobStore` yet.** Only `MemoryJobStore` and `SQLAlchemyJobStore` (SQLite backend) are currently supported. PostgreSQL and MySQL support via SQLAlchemy is planned but not yet available.

3. **`@scheduler.scheduled_job` decorator** is not yet supported. Use `scheduler.add_job()` instead.

4. **Custom trigger classes** written in Python cannot be plugged in directly. The trigger system runs in Rust. Use the built-in `cron`, `interval`, `date`, or `calendarinterval` triggers.

5. **`jobstore` parameter naming.** APScheduler uses `jobstore` in some APIs and `jobstores` in others. apscheduler-rs follows the same convention, but if you use undocumented internal attributes they may not exist.

6. **Pickle-only callables.** If your APScheduler setup relies on pickling complex callable objects (not just module-level functions), the persistent store may not round-trip them identically.

7. **`configure()` method.** APScheduler allows calling `scheduler.configure()` after construction. apscheduler-rs accepts configuration only through the constructor.

8. **APScheduler 4.x API.** apscheduler-rs targets the 3.x API only. The 4.x async-first API (`AsyncScheduler`, data stores, etc.) is not supported.

## Migration Checklist

- [ ] Replace `pip install apscheduler` with `pip install apscheduler-rs`
- [ ] Verify you are on Python 3.9+
- [ ] Check that you are not using `GeventScheduler`, `TornadoScheduler`, `MongoDBJobStore`, or `RedisJobStore`
- [ ] Check that you are not using custom trigger classes
- [ ] Check that you are not relying on `@scheduled_job` decorator (use `add_job` instead)
- [ ] Run your test suite -- all `apscheduler.*` imports should resolve identically
- [ ] For persistent stores, verify your jobs survive a restart (they should)
