# Feature Compatibility Matrix

This document lists every APScheduler 3.x feature and its support status in apscheduler-rs.

## Schedulers

| Scheduler | APScheduler 3.x | apscheduler-rs | Notes |
|-----------|:-:|:-:|-------|
| `BlockingScheduler` | Yes | Yes | Fully compatible |
| `BackgroundScheduler` | Yes | Yes | Fully compatible |
| `AsyncIOScheduler` | Yes | Yes | Fully compatible |
| `GeventScheduler` | Yes | No | Not planned |
| `TornadoScheduler` | Yes | No | Not planned |
| `TwistedScheduler` | Yes | No | Not planned |
| `QtScheduler` | Yes | No | Not planned |

## Triggers

| Trigger | APScheduler 3.x | apscheduler-rs | Notes |
|---------|:-:|:-:|-------|
| `DateTrigger` | Yes | Yes | Fully compatible |
| `IntervalTrigger` | Yes | Yes | Fully compatible |
| `CronTrigger` | Yes | Yes | Fully compatible, bitfield-compiled |
| `CalendarIntervalTrigger` | Yes | Yes | Fully compatible |
| `OrTrigger` (combining) | Yes | No | Not yet implemented |
| `AndTrigger` (combining) | Yes | No | Not yet implemented |
| Custom trigger classes | Yes | No | Triggers run in Rust; no Python plugin API |

### Cron Field Support

| Cron Field | APScheduler 3.x | apscheduler-rs | Notes |
|------------|:-:|:-:|-------|
| `year` | Yes | Yes | Range 1970-2099 |
| `month` | Yes | Yes | 1-12, names (JAN-DEC) |
| `day` | Yes | Yes | 1-31 |
| `week` | Yes | Yes | ISO week number |
| `day_of_week` | Yes | Yes | 0-6, names (MON-SUN) |
| `hour` | Yes | Yes | 0-23 |
| `minute` | Yes | Yes | 0-59 |
| `second` | Yes | Yes | 0-59 |
| Ranges (`1-5`) | Yes | Yes | |
| Steps (`*/15`) | Yes | Yes | |
| Lists (`1,15,30`) | Yes | Yes | |
| Last day (`last`) | Yes | No | Not yet implemented |
| Nth weekday (`3#2`) | Yes | No | Not yet implemented |

## Job Stores

| Job Store | APScheduler 3.x | apscheduler-rs | Notes |
|-----------|:-:|:-:|-------|
| `MemoryJobStore` | Yes | Yes | Uses DashMap + BTreeMap |
| `SQLAlchemyJobStore` (SQLite) | Yes | Yes | Uses Rust sqlx under the hood |
| `SQLAlchemyJobStore` (PostgreSQL) | Yes | No | Planned |
| `SQLAlchemyJobStore` (MySQL) | Yes | No | Planned |
| `MongoDBJobStore` | Yes | No | Planned |
| `RedisJobStore` | Yes | No | Planned |
| Custom job store classes | Yes | No | Stores run in Rust; no Python plugin API |

## Executors

| Executor | APScheduler 3.x | apscheduler-rs | Notes |
|----------|:-:|:-:|-------|
| `ThreadPoolExecutor` | Yes | Yes | Fully compatible |
| `ProcessPoolExecutor` | Yes | Yes | Fully compatible |
| `AsyncIOExecutor` | Yes | Yes | For coroutine jobs |
| `GeventExecutor` | Yes | No | Not planned |
| `TornadoExecutor` | Yes | No | Not planned |
| `TwistedExecutor` | Yes | No | Not planned |
| Custom executor classes | Yes | No | Executors run in Rust; no Python plugin API |

## Job Configuration

| Feature | APScheduler 3.x | apscheduler-rs | Notes |
|---------|:-:|:-:|-------|
| `id` (explicit job ID) | Yes | Yes | |
| `name` | Yes | Yes | |
| `misfire_grace_time` | Yes | Yes | Duration in seconds |
| `coalesce` | Yes | Yes | True/False |
| `max_instances` | Yes | Yes | Integer >= 1 |
| `jitter` | Yes | Yes | Random delay in seconds |
| `next_run_time` (override) | Yes | Yes | |
| `replace_existing` | Yes | Yes | |
| `executor` (named) | Yes | Yes | |
| `jobstore` (named) | Yes | Yes | |
| `args` / `kwargs` | Yes | Yes | Positional and keyword arguments |

## Job Management API

| Method | APScheduler 3.x | apscheduler-rs | Notes |
|--------|:-:|:-:|-------|
| `add_job()` | Yes | Yes | |
| `remove_job()` | Yes | Yes | |
| `modify_job()` | Yes | Yes | |
| `reschedule_job()` | Yes | Yes | Changes the trigger |
| `pause_job()` | Yes | Yes | |
| `resume_job()` | Yes | Yes | |
| `get_jobs()` | Yes | Yes | |
| `get_job()` | Yes | Yes | |
| `remove_all_jobs()` | Yes | Yes | |
| `print_jobs()` | Yes | Yes | |
| `@scheduled_job` decorator | Yes | No | Use `add_job()` instead |
| `configure()` | Yes | No | Use constructor kwargs |

## Scheduler Lifecycle

| Feature | APScheduler 3.x | apscheduler-rs | Notes |
|---------|:-:|:-:|-------|
| `start()` | Yes | Yes | |
| `shutdown(wait=True)` | Yes | Yes | |
| `shutdown(wait=False)` | Yes | Yes | |
| `pause()` | Yes | Yes | |
| `resume()` | Yes | Yes | |
| `running` property | Yes | Yes | |
| `state` property | Yes | Yes | |

## Event System

| Feature | APScheduler 3.x | apscheduler-rs | Notes |
|---------|:-:|:-:|-------|
| `add_listener()` | Yes | Yes | |
| `remove_listener()` | Yes | Yes | |
| `EVENT_SCHEDULER_STARTED` | Yes | Yes | |
| `EVENT_SCHEDULER_SHUTDOWN` | Yes | Yes | |
| `EVENT_SCHEDULER_PAUSED` | Yes | Yes | |
| `EVENT_SCHEDULER_RESUMED` | Yes | Yes | |
| `EVENT_EXECUTOR_ADDED` | Yes | Yes | |
| `EVENT_EXECUTOR_REMOVED` | Yes | Yes | |
| `EVENT_JOBSTORE_ADDED` | Yes | Yes | |
| `EVENT_JOBSTORE_REMOVED` | Yes | Yes | |
| `EVENT_ALL_JOBS_REMOVED` | Yes | Yes | |
| `EVENT_JOB_ADDED` | Yes | Yes | |
| `EVENT_JOB_REMOVED` | Yes | Yes | |
| `EVENT_JOB_MODIFIED` | Yes | Yes | |
| `EVENT_JOB_EXECUTED` | Yes | Yes | |
| `EVENT_JOB_ERROR` | Yes | Yes | |
| `EVENT_JOB_MISSED` | Yes | Yes | |
| `EVENT_JOB_SUBMITTED` | Yes | Yes | |
| `EVENT_JOB_MAX_INSTANCES` | Yes | Yes | |
| `EVENT_ALL` | Yes | Yes | Matches all events |
| Bitmask combining (`\|`) | Yes | Yes | e.g., `EVENT_JOB_EXECUTED \| EVENT_JOB_ERROR` |
| `SchedulerEvent` class | Yes | Yes | |
| `JobEvent` class | Yes | Yes | |
| `JobExecutionEvent` class | Yes | Yes | |

## Event Object Attributes

| Attribute | APScheduler 3.x | apscheduler-rs | Notes |
|-----------|:-:|:-:|-------|
| `event.code` | Yes | Yes | Event type bitmask |
| `event.job_id` | Yes | Yes | On job events |
| `event.jobstore` | Yes | Yes | On job events |
| `event.scheduled_run_time` | Yes | Yes | On execution events |
| `event.retval` | Yes | Yes | On `EVENT_JOB_EXECUTED` |
| `event.exception` | Yes | Yes | On `EVENT_JOB_ERROR` |
| `event.traceback` | Yes | No | Not available (Rust doesn't capture Python tracebacks in the same way) |
| `event.alias` | Yes | Yes | On executor/jobstore events |

## Configuration

| Feature | APScheduler 3.x | apscheduler-rs | Notes |
|---------|:-:|:-:|-------|
| Constructor kwargs | Yes | Yes | |
| `configure()` method | Yes | No | Not supported after construction |
| `gconfig` (global config) | Yes | No | |
| `job_defaults` dict | Yes | Yes | `misfire_grace_time`, `coalesce`, `max_instances` |
| `timezone` | Yes | Yes | Via `tzdata` |
| `daemon` thread mode | Yes | Yes | `BackgroundScheduler` daemon flag |

## Serialization

| Feature | APScheduler 3.x | apscheduler-rs | Notes |
|---------|:-:|:-:|-------|
| Pickle | Yes | Read-only | Can read APScheduler pickled jobs; writes as JSON |
| JSON (import paths) | No | Yes | Default format for persistence |
| Module-level functions | Yes | Yes | |
| Lambdas (persistent) | No | No | Cannot be serialized in either |
| Closures (persistent) | No | No | Cannot be serialized in either |

## Summary

**Fully supported (drop-in compatible):**
- BlockingScheduler, BackgroundScheduler, AsyncIOScheduler
- All four trigger types (date, interval, cron, calendarinterval)
- MemoryJobStore, SQLAlchemyJobStore (SQLite)
- ThreadPoolExecutor, ProcessPoolExecutor, AsyncIOExecutor
- Full event system with bitmask filtering
- All job management operations (add/remove/modify/reschedule/pause/resume)
- coalesce, misfire_grace_time, max_instances, jitter

**Not yet supported:**
- GeventScheduler, TornadoScheduler, TwistedScheduler, QtScheduler
- OrTrigger, AndTrigger (combining triggers)
- MongoDBJobStore, RedisJobStore
- PostgreSQL/MySQL backends
- `@scheduled_job` decorator
- Custom Python trigger/store/executor plugins
- `event.traceback` attribute
- `last` and `#` (nth weekday) cron expressions
