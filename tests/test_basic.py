"""Basic functional tests."""
import time
import pytest
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta, timezone

class TestImports:
    def test_import_schedulers(self):
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.schedulers.asyncio import AsyncIOScheduler

    def test_import_triggers(self):
        from apscheduler.triggers.date import DateTrigger
        from apscheduler.triggers.interval import IntervalTrigger
        from apscheduler.triggers.cron import CronTrigger
        from apscheduler.triggers.calendarinterval import CalendarIntervalTrigger

    def test_import_events(self):
        from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_ALL

class TestTriggers:
    def test_date_trigger(self):
        dt = DateTrigger(run_date=datetime(2030, 1, 1))
        now = datetime(2029, 6, 1)
        nxt = dt.get_next_fire_time(None, now)
        assert nxt is not None

    def test_interval_trigger(self):
        it = IntervalTrigger(seconds=30)
        now = datetime(2026, 1, 1)
        nxt = it.get_next_fire_time(None, now)
        assert nxt is not None

    def test_cron_trigger_string(self):
        ct = CronTrigger(minute='*/5')
        now = datetime(2026, 1, 1, 12, 0, 0)
        nxt = ct.get_next_fire_time(None, now)
        assert nxt is not None

    def test_cron_trigger_int(self):
        ct = CronTrigger(hour=12, minute=30)
        now = datetime(2026, 1, 1, 0, 0, 0)
        nxt = ct.get_next_fire_time(None, now)
        assert nxt is not None

class TestScheduler:
    def test_start_shutdown(self):
        s = BackgroundScheduler()
        s.start()
        s.shutdown(wait=True)

    def test_add_remove_job(self):
        s = BackgroundScheduler()
        s.start()
        s.add_job(lambda: None, 'interval', seconds=60, id='test')
        assert len(s.get_jobs()) == 1
        s.remove_job('test')
        assert len(s.get_jobs()) == 0
        s.shutdown()

    def test_job_execution(self):
        results = []
        s = BackgroundScheduler()
        s.start()
        s.add_job(lambda: results.append(1), 'interval', seconds=1, id='exec')
        time.sleep(2.5)
        s.shutdown(wait=True)
        assert len(results) >= 2

    def test_pause_resume_job(self):
        count = [0]
        s = BackgroundScheduler()
        s.start()
        s.add_job(lambda: count.__setitem__(0, count[0] + 1), 'interval', seconds=1, id='pr')
        time.sleep(1.5)
        s.pause_job('pr')
        frozen = count[0]
        time.sleep(2)
        assert count[0] == frozen
        s.resume_job('pr')
        time.sleep(1.5)
        assert count[0] > frozen
        s.shutdown()

    def test_get_job(self):
        s = BackgroundScheduler()
        s.start()
        s.add_job(lambda: None, 'interval', seconds=60, id='lookup', name='Test')
        job = s.get_job('lookup')
        assert job.id == 'lookup'
        assert job.name == 'Test'
        assert s.get_job('missing') is None
        s.shutdown()
