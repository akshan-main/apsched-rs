"""Functional tests for job timeout enforcement and DAG job dependencies."""
import time
import threading
from datetime import datetime, timedelta, timezone

import pytest

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED


def _soon(ms: int = 200):
    """Return a UTC datetime ``ms`` milliseconds in the future.  Used for
    'date' triggers so the job is reliably picked up by the scheduler loop
    instead of racing with the misfire grace window."""
    return datetime.now(timezone.utc) + timedelta(milliseconds=ms)


# ---------------------------------------------------------------------------
# Feature 1: job timeout
# ---------------------------------------------------------------------------


class TestJobTimeout:
    def test_timeout_fires_error_event(self):
        """A long-running job with a timeout should produce an error event
        roughly when the timeout elapses, and the slot must free up so that
        subsequent fires happen on schedule."""
        errors = []
        executed = []
        errors_lock = threading.Lock()

        def slow_job():
            # This job deliberately runs much longer than the timeout.
            time.sleep(4.0)

        s = BackgroundScheduler()
        s.start()

        def listener(event):
            if event.code == EVENT_JOB_ERROR:
                with errors_lock:
                    errors.append((time.monotonic(), event))
            elif event.code == EVENT_JOB_EXECUTED:
                with errors_lock:
                    executed.append((time.monotonic(), event))

        s.add_listener(listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)

        s.add_job(slow_job, "interval", seconds=2, id="slow", timeout=1)
        start = time.monotonic()
        # Wait long enough for at least two fires.
        time.sleep(5.0)
        s.shutdown(wait=False)

        with errors_lock:
            # Every fire should have produced an error event (timeout).
            assert len(errors) >= 2, (
                f"expected >=2 timeout errors, got {len(errors)}: {errors}"
            )
            # The first error should fire within ~2 seconds of start
            # (job spawned ~immediately + ~1s timeout).
            first_err_dt = errors[0][0] - start
            assert first_err_dt < 3.0, (
                f"first timeout error arrived after {first_err_dt:.2f}s; expected <3s"
            )
            # All errors should mention TimeoutError in the exception text.
            for _, ev in errors:
                msg = getattr(ev, "exception", "") or ""
                assert "Timeout" in str(msg), f"expected TimeoutError, got: {msg!r}"

    def test_timeout_as_float(self):
        """Timeout accepts float seconds."""
        s = BackgroundScheduler()
        s.start()
        job = s.add_job(lambda: None, "interval", seconds=60, id="f", timeout=0.5)
        assert abs(job.timeout - 0.5) < 1e-9
        s.shutdown(wait=False)

    def test_no_timeout_is_default(self):
        s = BackgroundScheduler()
        s.start()
        job = s.add_job(lambda: None, "interval", seconds=60, id="nt")
        assert job.timeout is None
        s.shutdown(wait=False)


# ---------------------------------------------------------------------------
# Feature 2: DAG job dependencies
# ---------------------------------------------------------------------------


class TestDagDependencies:
    def test_linear_chain(self):
        """a -> b -> c: when a succeeds, b runs; when b succeeds, c runs."""
        results = []
        lock = threading.Lock()

        def job_a():
            with lock:
                results.append("a")

        def job_b():
            with lock:
                results.append("b")

        def job_c():
            with lock:
                results.append("c")

        s = BackgroundScheduler()
        s.start()
        s.add_job(job_a, "date", run_date=_soon(), id="a")
        s.add_job(job_b, depends_on=["a"], id="b")
        s.add_job(job_c, depends_on=["b"], id="c")
        time.sleep(4)
        s.shutdown(wait=True)

        with lock:
            assert results == ["a", "b", "c"], (
                f"expected DAG order, got {results}"
            )

    def test_failure_blocks_downstream_by_default(self):
        """If an upstream job fails, a downstream job without
        run_on_failure should NOT fire."""
        results = []
        lock = threading.Lock()

        def a_fails():
            raise ValueError("boom")

        def b_ok():
            with lock:
                results.append("b")

        s = BackgroundScheduler()
        s.start()
        s.add_job(a_fails, "date", run_date=_soon(), id="a_fail")
        s.add_job(b_ok, depends_on=["a_fail"], id="b_blocked")
        time.sleep(3)
        s.shutdown(wait=True)

        with lock:
            assert results == [], f"b should not have fired, got {results}"

    def test_run_on_failure_allows_downstream(self):
        """With run_on_failure=True the downstream job runs even after
        an upstream failure."""
        results = []
        lock = threading.Lock()

        def a_fails():
            raise RuntimeError("nope")

        def cleanup():
            with lock:
                results.append("cleanup")

        s = BackgroundScheduler()
        s.start()
        s.add_job(a_fails, "date", run_date=_soon(), id="a_fail2")
        s.add_job(
            cleanup,
            depends_on=["a_fail2"],
            run_on_failure=True,
            id="cleanup",
        )
        time.sleep(3)
        s.shutdown(wait=True)

        with lock:
            assert results == ["cleanup"], (
                f"cleanup should have fired, got {results}"
            )

    def test_depends_on_attribute_exposed(self):
        s = BackgroundScheduler()
        s.start()
        s.add_job(lambda: None, "date", run_date=_soon(5000), id="up1")
        job = s.add_job(
            lambda: None,
            depends_on=["up1"],
            run_on_failure=True,
            id="down1",
        )
        assert list(job.depends_on) == ["up1"]
        assert job.run_on_failure is True
        assert job.next_run_time is None
        s.shutdown(wait=False)

    def test_multi_parent_all_success(self):
        """A job that depends on two upstream jobs should only run after
        both have succeeded."""
        results = []
        lock = threading.Lock()

        def mk(tag):
            def _f():
                with lock:
                    results.append(tag)
            return _f

        s = BackgroundScheduler()
        s.start()
        s.add_job(mk("a"), "date", run_date=_soon(), id="pa")
        s.add_job(mk("b"), "date", run_date=_soon(300), id="pb")
        s.add_job(mk("c"), depends_on=["pa", "pb"], id="pc")
        time.sleep(3)
        s.shutdown(wait=True)

        with lock:
            assert "a" in results
            assert "b" in results
            assert "c" in results
            # c must come after both a and b
            assert results.index("c") > results.index("a")
            assert results.index("c") > results.index("b")
