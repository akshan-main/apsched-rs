"""Cost budget enforcement for jobs."""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional


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
        )
        scheduler.add_llm_job(
            ...,
            budget=budget,
            on_budget_exceeded='pause',  # or 'skip' or 'error'
        )
    """
    per_run: Optional[float] = None
    per_day: Optional[float] = None
    per_month: Optional[float] = None
    name: str = "default"

    # Internal: tracking
    _spend_log: list = field(default_factory=list, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record(self, amount_usd: float) -> None:
        with self._lock:
            self._spend_log.append((datetime.now(timezone.utc), amount_usd))
            # Trim to last 90 days to bound memory
            cutoff = datetime.now(timezone.utc) - timedelta(days=90)
            self._spend_log = [(t, a) for (t, a) in self._spend_log if t > cutoff]

    def spent_today(self) -> float:
        cutoff = datetime.now(timezone.utc) - timedelta(days=1)
        with self._lock:
            return sum(a for (t, a) in self._spend_log if t > cutoff)

    def spent_this_month(self) -> float:
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        with self._lock:
            return sum(a for (t, a) in self._spend_log if t > cutoff)

    def total_spent(self) -> float:
        with self._lock:
            return sum(a for (t, a) in self._spend_log)

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
            'per_run_limit': self.per_run,
            'per_day_limit': self.per_day,
            'per_day_spent': self.spent_today(),
            'per_month_limit': self.per_month,
            'per_month_spent': self.spent_this_month(),
            'total_spent': self.total_spent(),
        }


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
