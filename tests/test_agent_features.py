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


class TestToolCalling:
    def test_function_to_tool_schema_basic(self):
        from apscheduler.agents.llm import function_to_tool_schema

        def get_weather(city: str, units: str = "celsius") -> str:
            """Get current weather for a city."""
            return "sunny"

        schema = function_to_tool_schema(get_weather)
        assert schema['name'] == 'get_weather'
        assert 'Get current weather' in schema['description']
        assert schema['input_schema']['properties']['city']['type'] == 'string'
        assert schema['input_schema']['properties']['units']['default'] == 'celsius'
        assert 'city' in schema['input_schema']['required']
        assert 'units' not in schema['input_schema']['required']

    def test_function_to_tool_schema_int_param(self):
        from apscheduler.agents.llm import function_to_tool_schema

        def calc(a: int, b: int) -> int:
            return a + b

        schema = function_to_tool_schema(calc)
        assert schema['input_schema']['properties']['a']['type'] == 'integer'

    def test_tool_call_dataclass(self):
        from apscheduler.agents.llm import ToolCall
        tc = ToolCall(tool_name='get_weather', arguments={'city': 'NYC'}, result='72F')
        assert tc.tool_name == 'get_weather'
        assert tc.error is None


class TestAgentRunResult:
    def test_agent_run_result_dataclass(self):
        from apscheduler.agents.llm import AgentRunResult
        result = AgentRunResult(
            final_text="done",
            model="claude-sonnet-4-6",
            tool_calls=[],
            total_input_tokens=100,
            total_output_tokens=50,
            total_cost_usd=0.001,
            iterations=1,
            duration_seconds=2.0,
        )
        assert result.final_text == "done"
        assert result.iterations == 1


class TestPersistedBudget:
    def test_budget_state_file_roundtrip(self, tmp_path):
        from apscheduler.agents.budget import CostBudget
        state_file = tmp_path / 'budget.jsonl'

        b1 = CostBudget(per_day=10.00, name='persisted_test_1', state_file=str(state_file))
        b1.record(2.50)
        b1.record(1.75)

        b2 = CostBudget(per_day=10.00, name='persisted_test_1', state_file=str(state_file))
        assert abs(b2.spent_today() - 4.25) < 0.001

    def test_budget_state_file_load_classmethod(self, tmp_path):
        from apscheduler.agents.budget import CostBudget
        state_file = tmp_path / 'budget.jsonl'
        b1 = CostBudget(per_day=10.00, name='persisted_test_2', state_file=str(state_file))
        b1.record(3.00)

        b2 = CostBudget.load('persisted_test_2', str(state_file))
        assert abs(b2.spent_today() - 3.00) < 0.001


class TestAddAgentLoopMethod:
    def test_method_exists(self):
        from apscheduler.schedulers.background import BackgroundScheduler
        import apscheduler.agents  # noqa: F401
        s = BackgroundScheduler()
        assert hasattr(s, 'add_agent_loop')

    def test_add_agent_loop_signature(self):
        from apscheduler.schedulers.background import BackgroundScheduler
        import apscheduler.agents  # noqa: F401
        import inspect
        s = BackgroundScheduler()
        sig = inspect.signature(s.add_agent_loop)
        params = list(sig.parameters.keys())
        assert 'initial_prompt' in params or 'prompt' in params
        assert 'tools' in params
        assert 'max_iterations' in params or 'max_tool_iterations' in params


class TestMCPBudgetTools:
    def test_budget_tools_listed(self):
        from apscheduler.agents import mcp_server
        tools = mcp_server.list_tools()
        names = [t['name'] for t in tools]
        assert 'list_budgets' in names
        assert 'get_budget' in names
