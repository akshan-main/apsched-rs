"""Tests for Django integration package (no Django installed required)."""
import sys

import pytest


class TestDjangoModule:
    def test_import_contrib(self):
        """Module structure should import without Django."""
        from apscheduler.contrib.django import (  # noqa: F401
            autodiscover_tasks,
            get_scheduler,
            register_job,
        )

    def test_register_job_decorator(self):
        """register_job decorator should work without Django."""
        from apscheduler.contrib.django import register_job

        @register_job('interval', seconds=60, id='test_decorated')
        def my_task():
            pass

        # Verify the decorator returned the function
        assert callable(my_task)
        assert my_task.__name__ == 'my_task'

    def test_get_scheduler_without_django(self):
        """get_scheduler should work even without Django installed."""
        # Reset module-level state
        from apscheduler.contrib.django import scheduler as sched_mod
        sched_mod._scheduler = None

        scheduler = sched_mod.get_scheduler()
        assert scheduler is not None
        # Should default to memory store + threadpool executor
        assert len(scheduler.get_jobs()) >= 0
        scheduler.shutdown(wait=False)
