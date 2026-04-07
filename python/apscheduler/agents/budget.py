"""Cost budget enforcement for jobs."""
from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Union

logger = logging.getLogger(__name__)


class BudgetExceededError(Exception):
    """Raised when a job tries to run but its budget has been exceeded."""
    pass


@dataclass
class CostBudget:
    """Define spending limits for a job or group of jobs.

    Example:
        budget = CostBudget(
            per_run=1.00,        # max $1 per individual job execution
            per_day=10.00,       # max $10/day across all uses of this budget
            per_month=100.00,    # max $100/month
            name='daily_briefing',
            state_file='~/.apscheduler/budgets/daily_briefing.jsonl',
        )
    """
    per_run: Optional[float] = None
    per_day: Optional[float] = None
    per_month: Optional[float] = None
    name: str = "default"
    budget_id: Optional[str] = None
    state_file: Optional[Union[str, Path]] = None

    # Internal: tracking
    _spend_log: list = field(default_factory=list, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _state_path: Optional[Path] = field(default=None, repr=False)

    def __post_init__(self):
        if self.budget_id is None:
            self.budget_id = self.name
        if self.state_file is not None:
            self._state_path = Path(os.path.expanduser(str(self.state_file)))
            self._load_from_file()
        # Register globally so MCP / listing can find this budget.
        try:
            with _REGISTRY_LOCK:
                _BUDGETS[self.name] = self
        except Exception:
            pass

    def _load_from_file(self) -> None:
        if self._state_path is None or not self._state_path.exists():
            return
        loaded: list = []
        try:
            with self._state_path.open('r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                        ts = datetime.fromisoformat(rec['timestamp'])
                        amount = float(rec['amount'])
                        loaded.append((ts, amount, rec.get('job_id')))
                    except (json.JSONDecodeError, KeyError, ValueError) as e:
                        logger.warning(f"Skipping malformed budget entry: {e}")
            # Trim to 90d
            cutoff = datetime.now(timezone.utc) - timedelta(days=90)
            self._spend_log = [entry for entry in loaded if entry[0] > cutoff]
        except OSError as e:
            logger.warning(f"Could not read budget state file {self._state_path}: {e}")

    def _append_to_file(self, ts: datetime, amount: float, job_id: Optional[str]) -> None:
        if self._state_path is None:
            return
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            rec = {
                'timestamp': ts.isoformat(),
                'amount': amount,
                'job_id': job_id,
                'budget_id': self.budget_id,
            }
            with self._state_path.open('a') as f:
                f.write(json.dumps(rec) + '\n')
        except OSError as e:
            logger.warning(f"Could not append to budget state file {self._state_path}: {e}")

    def record(self, amount_usd: float, job_id: Optional[str] = None) -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            self._spend_log.append((now, amount_usd, job_id))
            # Trim to last 90 days to bound memory
            cutoff = now - timedelta(days=90)
            self._spend_log = [entry for entry in self._spend_log if entry[0] > cutoff]
        self._append_to_file(now, amount_usd, job_id)

    def _sum_since(self, cutoff: datetime) -> float:
        with self._lock:
            return sum(entry[1] for entry in self._spend_log if entry[0] > cutoff)

    def spent_today(self) -> float:
        return self._sum_since(datetime.now(timezone.utc) - timedelta(days=1))

    def spent_this_month(self) -> float:
        return self._sum_since(datetime.now(timezone.utc) - timedelta(days=30))

    def total_spent(self) -> float:
        with self._lock:
            return sum(entry[1] for entry in self._spend_log)

    def check(self, expected_cost: float = 0.0) -> None:
        """Raise BudgetExceededError if running this job would exceed any limit."""
        if self.per_run is not None and expected_cost > self.per_run:
            raise BudgetExceededError(
                f"Budget {self.name!r}: expected cost ${expected_cost:.4f} "
                f"exceeds per_run limit ${self.per_run:.4f}"
            )
        if self.per_day is not None and self.spent_today() + expected_cost > self.per_day:
            raise BudgetExceededError(
                f"Budget {self.name!r}: spending ${expected_cost:.4f} would exceed "
                f"per_day limit ${self.per_day:.4f} (already spent ${self.spent_today():.4f})"
            )
        if self.per_month is not None and self.spent_this_month() + expected_cost > self.per_month:
            raise BudgetExceededError(
                f"Budget {self.name!r}: spending ${expected_cost:.4f} would exceed "
                f"per_month limit ${self.per_month:.4f} (already spent ${self.spent_this_month():.4f})"
            )

    def status(self) -> dict:
        return {
            'name': self.name,
            'budget_id': self.budget_id,
            'per_run_limit': self.per_run,
            'per_day_limit': self.per_day,
            'per_day_spent': self.spent_today(),
            'per_month_limit': self.per_month,
            'per_month_spent': self.spent_this_month(),
            'total_spent': self.total_spent(),
            'state_file': str(self._state_path) if self._state_path else None,
        }

    @classmethod
    def load(cls, name: str, state_file: Union[str, Path],
             per_run: Optional[float] = None,
             per_day: Optional[float] = None,
             per_month: Optional[float] = None) -> 'CostBudget':
        """Load a persisted budget from a state file (reads existing spend history)."""
        return cls(
            per_run=per_run,
            per_day=per_day,
            per_month=per_month,
            name=name,
            state_file=state_file,
        )


# Global budget registry for cross-job tracking
_BUDGETS: "dict[str, CostBudget]" = {}
_REGISTRY_LOCK = threading.Lock()


def get_budget(name: str = 'default') -> CostBudget:
    with _REGISTRY_LOCK:
        if name not in _BUDGETS:
            _BUDGETS[name] = CostBudget(name=name)
        return _BUDGETS[name]


def list_budgets() -> "dict[str, dict]":
    with _REGISTRY_LOCK:
        return {name: b.status() for name, b in _BUDGETS.items()}
