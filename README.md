# apscheduler-rs

A high-performance APScheduler-compatible scheduler runtime for Python, powered by Rust.

## Installation

```bash
pip install apscheduler-rs
```

## Quick Start

```python
from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler()

@scheduler.scheduled_job('interval', seconds=5)
def tick():
    print('Tick!')

scheduler.start()
```

## Features

- Drop-in replacement for APScheduler 3.x
- Rust-powered scheduler core for high performance
- All trigger types: date, interval, cron, calendar interval
- In-memory and SQL-backed job stores
- Thread pool and process pool executors
- AsyncIO scheduler support
- Full event system
- Multi-worker coordination

## License

MIT
