"""Django scheduler factory and singleton."""
import importlib
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

_scheduler = None
_jobs_registry = []


def _get_settings() -> dict:
    """Get APSCHEDULER settings from Django settings."""
    try:
        from django.conf import settings
        return getattr(settings, 'APSCHEDULER', {})
    except Exception:
        return {}


def _build_jobstore(config: dict):
    """Build a job store from a config dict."""
    from apscheduler.jobstores.memory import MemoryJobStore

    store_type = config.get('type', 'memory').lower()
    if store_type == 'memory':
        return MemoryJobStore()
    if store_type in ('sqlite', 'sqlalchemy', 'postgres', 'postgresql'):
        from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
        url = config.get('url')
        if url is None:
            # Try to derive from Django DATABASES
            try:
                from django.conf import settings
                default_db = settings.DATABASES.get('default', {})
                engine = default_db.get('ENGINE', '')
                if 'sqlite' in engine:
                    url = f"sqlite:///{default_db.get('NAME', 'jobs.db')}"
                elif 'postgres' in engine or 'postgresql' in engine:
                    name = default_db.get('NAME', '')
                    user = default_db.get('USER', '')
                    pwd = default_db.get('PASSWORD', '')
                    host = default_db.get('HOST', 'localhost')
                    port = default_db.get('PORT', 5432) or 5432
                    url = f"postgresql://{user}:{pwd}@{host}:{port}/{name}"
            except Exception:
                pass
        if url is None:
            raise ValueError(
                f"jobstore type {store_type!r} requires 'url' or Django DATABASES",
            )
        extra = {k: v for k, v in config.items() if k not in ('type', 'url')}
        return SQLAlchemyJobStore(url=url, **extra)
    if store_type == 'redis':
        from apscheduler._rust import RedisJobStore
        return RedisJobStore(url=config.get('url', 'redis://localhost:6379/0'))
    if store_type in ('mongodb', 'mongo'):
        from apscheduler.jobstores.mongodb import MongoDBJobStore
        return MongoDBJobStore(**{k: v for k, v in config.items() if k != 'type'})
    raise ValueError(f"Unknown jobstore type: {store_type}")


def _build_executor(config: dict):
    """Build an executor from a config dict."""
    from apscheduler.executors.pool import ProcessPoolExecutor, ThreadPoolExecutor

    exec_type = config.get('type', 'threadpool').lower()
    max_workers = config.get('max_workers', 20)
    if exec_type in ('threadpool', 'thread'):
        return ThreadPoolExecutor(max_workers=max_workers)
    if exec_type in ('processpool', 'process'):
        return ProcessPoolExecutor(max_workers=max_workers)
    raise ValueError(f"Unknown executor type: {exec_type}")


def get_scheduler():
    """Return the singleton Django scheduler, creating it if needed."""
    global _scheduler
    if _scheduler is not None:
        return _scheduler

    from apscheduler.schedulers.background import BackgroundScheduler

    config = _get_settings()
    timezone = config.get('timezone', 'UTC')
    job_defaults = config.get('job_defaults', {})

    _scheduler = BackgroundScheduler(timezone=timezone, job_defaults=job_defaults)

    # Register job stores
    jobstores = config.get('jobstores', {})
    if not jobstores:
        jobstores = {'default': {'type': 'memory'}}
    for alias, store_config in jobstores.items():
        try:
            store = _build_jobstore(store_config)
            try:
                _scheduler.remove_jobstore(alias)
            except Exception:
                pass
            _scheduler.add_jobstore(store, alias=alias)
        except Exception as e:
            logger.warning(f"Failed to register jobstore {alias}: {e}")

    # Register executors. BackgroundScheduler provides an implicit
    # threadpool executor under the 'default' alias at start() time, so
    # skip registering a user-supplied default threadpool (which would
    # otherwise collide at start). Non-default aliases and non-threadpool
    # types still go through.
    executors = config.get('executors', {})
    for alias, exec_config in executors.items():
        exec_type = (exec_config.get('type') or 'threadpool').lower()
        if alias == 'default' and exec_type in ('threadpool', 'thread'):
            # BackgroundScheduler installs its own default threadpool on start.
            continue
        try:
            executor = _build_executor(exec_config)
            try:
                _scheduler.remove_executor(alias)
            except Exception:
                pass
            _scheduler.add_executor(executor, alias=alias)
        except Exception as e:
            logger.warning(f"Failed to register executor {alias}: {e}")

    # Auto-discover tasks.py from each Django app
    if config.get('autodiscover', True):
        autodiscover_tasks()

    # Register any jobs that were declared via the @register_job decorator
    for func, args, kwargs in _jobs_registry:
        try:
            _scheduler.add_job(func, *args, **kwargs)
        except Exception as e:
            logger.warning(f"Failed to register job {func}: {e}")

    return _scheduler


def autodiscover_tasks():
    """Import tasks.py from each installed Django app."""
    try:
        from django.apps import apps
    except Exception:
        return
    for app_config in apps.get_app_configs():
        try:
            importlib.import_module(f"{app_config.name}.tasks")
            logger.info(f"Loaded tasks from {app_config.name}.tasks")
        except ImportError:
            pass
        except Exception as e:
            logger.warning(f"Failed to load tasks from {app_config.name}.tasks: {e}")


def register_job(*args, **kwargs):
    """Decorator to register a job with the Django scheduler.

    Usage:
        from apscheduler.contrib.django import register_job

        @register_job('cron', hour=2, minute=0, id='my_daily_job')
        def my_task():
            pass
    """
    def decorator(func):
        _jobs_registry.append((func, args, kwargs))
        return func
    return decorator


__all__ = ['get_scheduler', 'register_job', 'autodiscover_tasks']
