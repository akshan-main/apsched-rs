"""Tests for APScheduler 3.x compatibility shims."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest


class TestUtilModule:
    def test_datetime_to_utc_timestamp_aware(self):
        from apscheduler.util import datetime_to_utc_timestamp

        t = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        assert datetime_to_utc_timestamp(t) == pytest.approx(1718452800.0)

    def test_datetime_to_utc_timestamp_naive(self):
        from apscheduler.util import datetime_to_utc_timestamp

        t = datetime(2024, 6, 15, 12, 0, 0)  # naive — treated as UTC
        assert datetime_to_utc_timestamp(t) == pytest.approx(1718452800.0)

    def test_datetime_to_utc_timestamp_none(self):
        from apscheduler.util import datetime_to_utc_timestamp

        assert datetime_to_utc_timestamp(None) is None

    def test_utc_timestamp_to_datetime_roundtrip(self):
        from apscheduler.util import (
            datetime_to_utc_timestamp,
            utc_timestamp_to_datetime,
        )

        t = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        ts = datetime_to_utc_timestamp(t)
        assert utc_timestamp_to_datetime(ts) == t

    def test_datetime_repr(self):
        from apscheduler.util import datetime_repr

        assert datetime_repr(None) == "None"
        t = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        assert "2024-06-15 12:00:00" in datetime_repr(t)

    def test_normalize_naive(self):
        from apscheduler.util import normalize

        t = datetime(2024, 6, 15, 12, 0, 0)
        n = normalize(t)
        assert n.tzinfo is timezone.utc

    def test_astimezone_string(self):
        from apscheduler.util import astimezone

        tz = astimezone("UTC")
        assert tz is not None and hasattr(tz, "utcoffset")

    def test_astimezone_none_passthrough(self):
        from apscheduler.util import astimezone

        assert astimezone(None) is None

    def test_convert_to_datetime_from_str(self):
        from apscheduler.util import convert_to_datetime

        dt = convert_to_datetime("2024-06-15T12:00:00+00:00")
        assert dt.year == 2024 and dt.hour == 12

    def test_convert_to_datetime_invalid(self):
        from apscheduler.util import convert_to_datetime

        with pytest.raises(TypeError):
            convert_to_datetime(42)

    def test_obj_to_ref_roundtrip(self):
        from apscheduler.util import datetime_to_utc_timestamp, maybe_ref, obj_to_ref

        ref = obj_to_ref(datetime_to_utc_timestamp)
        assert ref == "apscheduler.util:datetime_to_utc_timestamp"
        assert maybe_ref(ref) is datetime_to_utc_timestamp

    def test_maybe_ref_passthrough(self):
        from apscheduler.util import maybe_ref

        def f():
            pass

        assert maybe_ref(f) is f

    def test_maybe_ref_invalid(self):
        from apscheduler.util import maybe_ref

        with pytest.raises(ValueError):
            maybe_ref("not_a_ref")

    def test_get_callable_name(self):
        from apscheduler.util import get_callable_name

        def my_func():
            pass

        assert get_callable_name(my_func) == "TestUtilModule.test_get_callable_name.<locals>.my_func"

    def test_check_callable_args_ok(self):
        from apscheduler.util import check_callable_args

        def f(a, b, c=3):
            pass

        check_callable_args(f, (1, 2), {})

    def test_check_callable_args_bad(self):
        from apscheduler.util import check_callable_args

        def f(a, b):
            pass

        with pytest.raises(ValueError):
            check_callable_args(f, (1,), {})


class TestBaseClasses:
    def test_base_job_store(self):
        from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError

        class MyStore(BaseJobStore):
            def lookup_job(self, job_id):
                return None

        s = MyStore()
        assert repr(s) == "<MyStore>"
        assert s.lookup_job("x") is None

        with pytest.raises(KeyError):
            raise JobLookupError("abc")
        with pytest.raises(KeyError):
            raise ConflictingIdError("abc")

    def test_base_trigger(self):
        from apscheduler.triggers.base import BaseTrigger

        class MyTrigger(BaseTrigger):
            def get_next_fire_time(self, previous, now):
                return now

        t = MyTrigger()
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert t.get_next_fire_time(None, now) == now
        # Pickle state roundtrip
        state = t.__getstate__()
        t2 = MyTrigger()
        t2.__setstate__(state)

    def test_base_executor(self):
        from apscheduler.executors.base import BaseExecutor, MaxInstancesReachedError

        class MyExec(BaseExecutor):
            def submit_job(self, job, run_times):
                return None

        e = MyExec()
        assert "MyExec" in repr(e)
        e.shutdown()

        class FakeJob:
            id = "j1"
            max_instances = 3

        err = MaxInstancesReachedError(FakeJob())
        assert "j1" in str(err)
        assert "3" in str(err)


class TestSchedulerBase:
    def test_exceptions_and_states(self):
        from apscheduler.schedulers.base import (
            SchedulerAlreadyRunningError,
            SchedulerNotRunningError,
            STATE_PAUSED,
            STATE_RUNNING,
            STATE_STOPPED,
        )

        assert STATE_STOPPED != STATE_RUNNING != STATE_PAUSED

        with pytest.raises(Exception, match="already running"):
            raise SchedulerAlreadyRunningError()
        with pytest.raises(Exception, match="not been started"):
            raise SchedulerNotRunningError()


class TestSchedulerShims:
    def test_tornado_scheduler(self):
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.schedulers.tornado import TornadoScheduler

        s = TornadoScheduler()
        assert isinstance(s, BackgroundScheduler)

    def test_tornado_scheduler_warns_on_io_loop(self):
        from apscheduler.schedulers.tornado import TornadoScheduler

        with pytest.warns(UserWarning, match="io_loop"):
            TornadoScheduler(io_loop=object())

    def test_gevent_scheduler(self):
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.schedulers.gevent import GeventScheduler

        assert isinstance(GeventScheduler(), BackgroundScheduler)

    def test_twisted_scheduler(self):
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.schedulers.twisted import TwistedScheduler

        assert isinstance(TwistedScheduler(), BackgroundScheduler)

    def test_twisted_scheduler_warns_on_reactor(self):
        from apscheduler.schedulers.twisted import TwistedScheduler

        with pytest.warns(UserWarning, match="reactor"):
            TwistedScheduler(reactor=object())

    def test_qt_scheduler(self):
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.schedulers.qt import QtScheduler

        assert isinstance(QtScheduler(), BackgroundScheduler)


class TestVersion:
    def test_version_exported(self):
        import apscheduler

        assert isinstance(apscheduler.__version__, str)
        assert apscheduler.__version__ == apscheduler.release
        assert apscheduler.version_info[0] == 0


class TestSQLAlchemyEnginePassthrough:
    def test_url_required(self):
        from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

        with pytest.raises(ValueError):
            SQLAlchemyJobStore()

    def test_engine_passthrough(self):
        from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

        class FakeEngine:
            url = "sqlite:///:memory:"

        store = SQLAlchemyJobStore(engine=FakeEngine())
        assert store.url == "sqlite:///:memory:"


class TestCronSpecialMarkers:
    """End-to-end tests for L / W / # cron field expressions."""

    @staticmethod
    def _ts(y, m, d, h=0, mi=0, s=0):
        return datetime(y, m, d, h, mi, s, tzinfo=timezone.utc)

    def test_last_day_non_leap_feb(self):
        from apscheduler.triggers.cron import CronTrigger

        ct = CronTrigger(day="last", hour=12)
        nxt = ct.get_next_fire_time(None, self._ts(2023, 2, 1))
        assert nxt == self._ts(2023, 2, 28, 12)

    def test_last_day_leap_feb(self):
        from apscheduler.triggers.cron import CronTrigger

        ct = CronTrigger(day="last", hour=12)
        nxt = ct.get_next_fire_time(None, self._ts(2024, 2, 1))
        assert nxt == self._ts(2024, 2, 29, 12)

    def test_last_uppercase_L(self):
        from apscheduler.triggers.cron import CronTrigger

        ct = CronTrigger(day="L", hour=0, minute=0, second=0)
        nxt = ct.get_next_fire_time(None, self._ts(2024, 4, 10))
        assert nxt == self._ts(2024, 4, 30)

    def test_last_sunday_of_month(self):
        from apscheduler.triggers.cron import CronTrigger

        ct = CronTrigger(day="last sun")
        # June 2024: last Sunday is 2024-06-30.
        nxt = ct.get_next_fire_time(None, self._ts(2024, 6, 1))
        assert nxt == self._ts(2024, 6, 30)

    def test_15w_saturday_steps_back(self):
        from apscheduler.triggers.cron import CronTrigger

        ct = CronTrigger(day="15W", hour=12)
        # Aug 15 2026 is Saturday -> Friday Aug 14.
        nxt = ct.get_next_fire_time(None, self._ts(2026, 8, 1))
        assert nxt == self._ts(2026, 8, 14, 12)

    def test_15w_sunday_steps_forward(self):
        from apscheduler.triggers.cron import CronTrigger

        ct = CronTrigger(day="15W", hour=12)
        # Feb 15 2026 is Sunday -> Monday Feb 16.
        nxt = ct.get_next_fire_time(None, self._ts(2026, 2, 1))
        assert nxt == self._ts(2026, 2, 16, 12)

    def test_15w_weekday_same_day(self):
        from apscheduler.triggers.cron import CronTrigger

        ct = CronTrigger(day="15W", hour=12)
        # Sept 15 2026 is Tuesday -> same day.
        nxt = ct.get_next_fire_time(None, self._ts(2026, 9, 1))
        assert nxt == self._ts(2026, 9, 15, 12)

    def test_1w_saturday_jumps_forward(self):
        from apscheduler.triggers.cron import CronTrigger

        ct = CronTrigger(day="1W")
        # Aug 1 2026 is Saturday. Stepping back would cross months,
        # so jump forward to Monday Aug 3.
        nxt = ct.get_next_fire_time(None, self._ts(2026, 7, 31))
        assert nxt == self._ts(2026, 8, 3)

    def test_nth_weekday_third_saturday(self):
        from apscheduler.triggers.cron import CronTrigger

        # APScheduler convention: SAT = 5.
        ct = CronTrigger(day_of_week="5#3")
        # Jan 2026 Saturdays: 3, 10, 17, 24, 31 -> third = Jan 17.
        nxt = ct.get_next_fire_time(None, self._ts(2026, 1, 1))
        assert nxt == self._ts(2026, 1, 17)

    def test_nth_weekday_named(self):
        from apscheduler.triggers.cron import CronTrigger

        ct = CronTrigger(day_of_week="sat#3")
        nxt = ct.get_next_fire_time(None, self._ts(2026, 1, 1))
        assert nxt == self._ts(2026, 1, 17)

    def test_nth_weekday_rolls_to_next_month(self):
        from apscheduler.triggers.cron import CronTrigger

        ct = CronTrigger(day_of_week="sat#3")
        # Jump past Jan's 3rd Sat -> Feb 2026's 3rd Sat = Feb 21.
        nxt = ct.get_next_fire_time(None, self._ts(2026, 1, 18))
        assert nxt == self._ts(2026, 2, 21)

    def test_invalid_nth_range_errors(self):
        from apscheduler.triggers.cron import CronTrigger

        with pytest.raises(Exception):
            CronTrigger(day_of_week="0#6")

    def test_invalid_w_out_of_range_errors(self):
        from apscheduler.triggers.cron import CronTrigger

        with pytest.raises(Exception):
            CronTrigger(day="32W")
