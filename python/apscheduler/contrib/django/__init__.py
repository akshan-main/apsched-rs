"""Django integration for apscheduler-rs.

Add 'apscheduler.contrib.django' to INSTALLED_APPS in your Django settings.

Configure scheduling in settings.py:

    APSCHEDULER = {
        'jobstores': {
            'default': {'type': 'sqlite', 'url': 'sqlite:///jobs.db'},
        },
        'executors': {
            'default': {'type': 'threadpool', 'max_workers': 20},
        },
        'job_defaults': {
            'coalesce': True,
            'max_instances': 1,
        },
        'timezone': 'UTC',
        'autodiscover': True,  # auto-import tasks.py from each Django app
    }

Then run the scheduler:

    python manage.py runscheduler
"""
from apscheduler.contrib.django.scheduler import (
    autodiscover_tasks,
    get_scheduler,
    register_job,
)

default_app_config = 'apscheduler.contrib.django.apps.APSchedulerConfig'

__all__ = ['get_scheduler', 'register_job', 'autodiscover_tasks', 'default_app_config']
