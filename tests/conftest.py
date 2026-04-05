"""Shared test fixtures."""
import pytest
from apscheduler.schedulers.background import BackgroundScheduler

@pytest.fixture
def scheduler():
    s = BackgroundScheduler()
    yield s
    if hasattr(s, '_engine') and s._engine is not None:
        try:
            s.shutdown(wait=False)
        except Exception:
            pass
