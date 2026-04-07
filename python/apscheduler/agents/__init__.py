"""Agent-focused scheduling features for apscheduler-rs.

This module adds LLM job support, cost budgets, webhook triggers, and other
features useful for AI agent workloads on top of the core APScheduler API.
"""
from __future__ import annotations

from typing import Optional

from apscheduler.agents.llm import (
    LLMJob,
    LLMResult,
    LLMProvider,
    AnthropicProvider,
    OpenAIProvider,
    register_llm_provider,
    PRICING,
)
from apscheduler.agents.budget import (
    CostBudget,
    BudgetExceededError,
    get_budget,
    list_budgets,
)
from apscheduler.agents.webhook import (
    WebhookTrigger,
    register_webhook_listener,
    fire_webhook,
    serve_webhooks,
)

__all__ = [
    'LLMJob',
    'LLMResult',
    'LLMProvider',
    'AnthropicProvider',
    'OpenAIProvider',
    'register_llm_provider',
    'PRICING',
    'CostBudget',
    'BudgetExceededError',
    'get_budget',
    'list_budgets',
    'WebhookTrigger',
    'register_webhook_listener',
    'fire_webhook',
    'serve_webhooks',
    'add_llm_job',
    'add_webhook_job',
]


def _add_llm_job(self, prompt: str, model: str = "claude-sonnet-4-6", schedule=None,
                 id: Optional[str] = None, budget: Optional[CostBudget] = None,
                 max_cost_per_run: Optional[float] = None, max_tokens: int = 4096,
                 provider: str = "auto", on_result=None, on_error=None,
                 trigger=None, **kwargs):
    """Add an LLM-call job to the scheduler.

    Args:
        prompt: The prompt to send to the LLM.
        model: Model name (e.g. 'claude-sonnet-4-6', 'gpt-4o').
        schedule: Either a dict of cron kwargs OR a trigger string OR None for one-shot.
        id: Job ID.
        budget: Optional CostBudget to enforce.
        max_cost_per_run: Max USD per single execution.
        max_tokens: Max tokens to generate.
        provider: 'anthropic', 'openai', 'auto', or registered provider name.
        on_result: Callback called with LLMResult on success.
        on_error: Callback called with Exception on failure.
        trigger: Override trigger (overrides schedule).
        **kwargs: Additional kwargs passed to add_job.
    """
    job_callable = LLMJob(
        prompt=prompt,
        model=model,
        provider=provider,
        max_tokens=max_tokens,
        max_cost_per_run=max_cost_per_run,
        on_result=on_result,
        on_error=on_error,
    )

    # Wrap to enforce budget
    if budget is not None:
        original = job_callable

        def budgeted_job(ctx=None):
            budget.check(0.0)  # Pre-check (we don't know exact cost yet)
            result = original(ctx)
            if isinstance(result, dict) and 'cost_usd' in result:
                budget.record(result['cost_usd'])
            return result

        job_callable = budgeted_job

    # Resolve trigger
    if trigger is None:
        if schedule is None:
            from datetime import datetime, timezone
            trigger = 'date'
            kwargs.setdefault('run_date', datetime.now(timezone.utc))
        elif isinstance(schedule, dict):
            trigger = 'cron'
            kwargs.update(schedule)
        elif isinstance(schedule, str):
            trigger = schedule

    return self.add_job(job_callable, trigger, id=id, **kwargs)


def _add_webhook_job(self, func, path: str, id: Optional[str] = None,
                     secret: Optional[str] = None, **kwargs):
    """Add a job that fires when a webhook is received at the given path.

    Example:
        scheduler.add_webhook_job(
            handle_github_event,
            path='github',  # POST /hook/github fires this
            id='gh_handler',
        )
    """
    from datetime import datetime, timezone, timedelta

    # Job uses a far-future trigger so it doesn't fire on time
    far_future = datetime.now(timezone.utc) + timedelta(days=365 * 100)
    job = self.add_job(func, 'date', run_date=far_future, id=id, **kwargs)
    register_webhook_listener(path, self, id, secret)
    return job


# Standalone function-style APIs (for users who prefer that or when monkey-patching fails)
def add_llm_job(scheduler, prompt: str, model: str = "claude-sonnet-4-6", **kwargs):
    """Functional form of scheduler.add_llm_job()."""
    return _add_llm_job(scheduler, prompt, model=model, **kwargs)


def add_webhook_job(scheduler, func, path: str, **kwargs):
    """Functional form of scheduler.add_webhook_job()."""
    return _add_webhook_job(scheduler, func, path, **kwargs)


def _install_agent_methods():
    """Monkey-patch agent methods onto the PyO3 scheduler classes."""
    installed = []
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        BackgroundScheduler.add_llm_job = _add_llm_job
        BackgroundScheduler.add_webhook_job = _add_webhook_job
        installed.append('BackgroundScheduler')
    except (ImportError, TypeError, AttributeError):
        pass
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        BlockingScheduler.add_llm_job = _add_llm_job
        BlockingScheduler.add_webhook_job = _add_webhook_job
        installed.append('BlockingScheduler')
    except (ImportError, TypeError, AttributeError):
        pass
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        AsyncIOScheduler.add_llm_job = _add_llm_job
        AsyncIOScheduler.add_webhook_job = _add_webhook_job
        installed.append('AsyncIOScheduler')
    except (ImportError, TypeError, AttributeError):
        pass
    return installed


_INSTALLED_ON = _install_agent_methods()
