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
    function_to_tool_schema,
    ToolCall,
    AgentRunResult,
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
    'add_agent_loop',
    'function_to_tool_schema',
    'ToolCall',
    'AgentRunResult',
]


def _add_llm_job(self, prompt: str, model: str = "claude-sonnet-4-6", schedule=None,
                 id: Optional[str] = None, budget: Optional[CostBudget] = None,
                 max_cost_per_run: Optional[float] = None, max_tokens: int = 4096,
                 provider: str = "auto", on_result=None, on_error=None,
                 trigger=None, tools: Optional[list] = None,
                 max_tool_iterations: int = 10, on_complete=None, **kwargs):
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
        on_result: Callback called with LLMResult (or AgentRunResult if tools given).
        on_error: Callback called with Exception on failure.
        trigger: Override trigger (overrides schedule).
        tools: Optional list of Python callables to expose as LLM tools. If set,
            runs a tool-calling agent loop instead of a single call.
        max_tool_iterations: Max iterations of the LLM <-> tool loop.
        on_complete: Alias for on_result, matches agent-loop mental model.
        **kwargs: Additional kwargs passed to add_job.
    """
    callback = on_result if on_result is not None else on_complete
    job_callable = LLMJob(
        prompt=prompt,
        model=model,
        provider=provider,
        max_tokens=max_tokens,
        max_cost_per_run=max_cost_per_run,
        on_result=callback,
        on_error=on_error,
        tools=list(tools) if tools else [],
        max_tool_iterations=max_tool_iterations,
        budget=budget,
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


def _add_agent_loop(self, initial_prompt: str, model: str = "claude-sonnet-4-6",
                    tools: Optional[list] = None, max_iterations: int = 10,
                    schedule=None, id: Optional[str] = None,
                    budget: Optional[CostBudget] = None,
                    max_cost_per_run: Optional[float] = None,
                    max_tokens: int = 4096, provider: str = "auto",
                    on_complete=None, on_error=None, trigger=None, **kwargs):
    """Add an agent-loop job: an LLM that can call Python tools until it produces a final answer.

    Example:
        scheduler.add_agent_loop(
            initial_prompt="Summarize today's open issues and post a summary",
            model="claude-sonnet-4-6",
            tools=[get_issues, post_summary],
            max_iterations=10,
            budget=CostBudget(per_run=1.00),
            schedule={'hour': 9},
            id='daily_summary_agent',
            on_complete=lambda result: print(result.final_text),
        )
    """
    return _add_llm_job(
        self,
        prompt=initial_prompt,
        model=model,
        schedule=schedule,
        id=id,
        budget=budget,
        max_cost_per_run=max_cost_per_run,
        max_tokens=max_tokens,
        provider=provider,
        on_result=on_complete,
        on_error=on_error,
        trigger=trigger,
        tools=tools or [],
        max_tool_iterations=max_iterations,
        **kwargs,
    )


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


def add_agent_loop(scheduler, initial_prompt: str, **kwargs):
    """Functional form of scheduler.add_agent_loop()."""
    return _add_agent_loop(scheduler, initial_prompt, **kwargs)


def _install_agent_methods():
    """Monkey-patch agent methods onto the PyO3 scheduler classes."""
    installed = []
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        BackgroundScheduler.add_llm_job = _add_llm_job
        BackgroundScheduler.add_webhook_job = _add_webhook_job
        BackgroundScheduler.add_agent_loop = _add_agent_loop
        installed.append('BackgroundScheduler')
    except (ImportError, TypeError, AttributeError):
        pass
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        BlockingScheduler.add_llm_job = _add_llm_job
        BlockingScheduler.add_webhook_job = _add_webhook_job
        BlockingScheduler.add_agent_loop = _add_agent_loop
        installed.append('BlockingScheduler')
    except (ImportError, TypeError, AttributeError):
        pass
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        AsyncIOScheduler.add_llm_job = _add_llm_job
        AsyncIOScheduler.add_webhook_job = _add_webhook_job
        AsyncIOScheduler.add_agent_loop = _add_agent_loop
        installed.append('AsyncIOScheduler')
    except (ImportError, TypeError, AttributeError):
        pass
    return installed


_INSTALLED_ON = _install_agent_methods()
