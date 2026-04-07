"""Tests for agent-focused features (LLM jobs, budgets, webhooks, MCP)."""
import pytest
from apscheduler.agents.budget import CostBudget, BudgetExceededError
from apscheduler.agents.llm import LLMJob, AnthropicProvider, OpenAIProvider, PRICING
from apscheduler.agents.webhook import (
    WebhookTrigger,
    register_webhook_listener,
    fire_webhook,
)
from apscheduler.agents import mcp_server


class TestLLMImports:
    def test_imports(self):
        from apscheduler.agents import LLMJob, AnthropicProvider, OpenAIProvider  # noqa: F401
        from apscheduler.agents import CostBudget, BudgetExceededError  # noqa: F401
        from apscheduler.agents import WebhookTrigger, register_webhook_listener  # noqa: F401

    def test_llm_job_construction(self):
        job = LLMJob(prompt="test", model="claude-sonnet-4-6")
        assert job.prompt == "test"
        assert job.model == "claude-sonnet-4-6"

    def test_pricing_exists(self):
        assert 'claude-sonnet-4-6' in PRICING
        assert 'gpt-4o' in PRICING


class TestCostBudget:
    def test_basic_record(self):
        b = CostBudget(per_day=10.00)
        b.record(0.50)
        assert b.spent_today() == 0.50

    def test_per_run_limit(self):
        b = CostBudget(per_run=1.00)
        with pytest.raises(BudgetExceededError):
            b.check(2.00)

    def test_per_day_limit(self):
        b = CostBudget(per_day=1.00)
        b.record(0.80)
        with pytest.raises(BudgetExceededError):
            b.check(0.30)

    def test_status_dict(self):
        b = CostBudget(per_day=10.00, name='test')
        b.record(2.50)
        status = b.status()
        assert status['name'] == 'test'
        assert status['per_day_limit'] == 10.00
        assert status['per_day_spent'] == 2.50


class TestWebhookTrigger:
    def test_webhook_registration(self):
        from apscheduler.schedulers.background import BackgroundScheduler
        s = BackgroundScheduler()
        s.start()

        called = []

        def my_handler():
            called.append(True)

        from datetime import datetime, timezone, timedelta
        far_future = datetime.now(timezone.utc) + timedelta(days=365)
        s.add_job(my_handler, 'date', run_date=far_future, id='wh_test')
        register_webhook_listener('test_path', s, 'wh_test')

        # Fire the webhook
        count = fire_webhook('test_path')
        assert count == 1

        import time
        time.sleep(2)
        s.shutdown(wait=True)
        assert len(called) >= 1


class TestSchedulerAgentMethods:
    def test_monkey_patched_methods_exist(self):
        from apscheduler.schedulers.background import BackgroundScheduler
        import apscheduler.agents  # noqa: F401
        assert hasattr(BackgroundScheduler, 'add_llm_job')
        assert hasattr(BackgroundScheduler, 'add_webhook_job')

    def test_add_webhook_job_via_method(self):
        from apscheduler.schedulers.background import BackgroundScheduler
        import apscheduler.agents  # noqa: F401
        s = BackgroundScheduler()
        s.start()
        try:
            called = []

            def handler():
                called.append(1)

            s.add_webhook_job(handler, path='method_test', id='mt_job')
            count = fire_webhook('method_test')
            assert count == 1
            import time
            time.sleep(1.5)
            assert len(called) >= 1
        finally:
            s.shutdown(wait=True)


class TestMCPServer:
    def test_tool_list(self):
        tools = mcp_server.list_tools()
        names = [t['name'] for t in tools]
        assert 'list_jobs' in names
        assert 'pause_job' in names
        assert 'trigger_job' in names
        assert 'scheduler_status' in names

    def test_tool_schemas_valid(self):
        tools = mcp_server.list_tools()
        for tool in tools:
            assert 'name' in tool
            assert 'description' in tool
            assert 'inputSchema' in tool
            assert tool['inputSchema']['type'] == 'object'
