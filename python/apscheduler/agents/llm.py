"""LLM job support - schedule LLM calls with cost tracking, retry, and observability."""
from __future__ import annotations

import inspect
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


def function_to_tool_schema(func: Callable) -> dict:
    """Convert a Python function into an LLM tool schema (JSON Schema-ish).

    Inspects the signature and docstring. Maps Python type hints to JSON schema
    types. Parameters without defaults are required.
    """
    sig = inspect.signature(func)
    type_map = {
        str: "string",
        int: "integer",
        float: "number",
        bool: "boolean",
        list: "array",
        dict: "object",
    }
    properties: dict = {}
    required: list = []
    for param_name, param in sig.parameters.items():
        py_type = param.annotation if param.annotation is not inspect.Parameter.empty else str
        type_name = type_map.get(py_type, "string")
        prop: dict = {"type": type_name}
        if param.default is not inspect.Parameter.empty:
            try:
                json.dumps(param.default)
                prop["default"] = param.default
            except (TypeError, ValueError):
                pass
        else:
            required.append(param_name)
        properties[param_name] = prop

    doc = (func.__doc__ or "").strip()
    description = doc.split("\n")[0] if doc else func.__name__
    return {
        "name": func.__name__,
        "description": description or func.__name__,
        "input_schema": {
            "type": "object",
            "properties": properties,
            "required": required,
        },
    }


@dataclass
class ToolCall:
    """Record of a single tool invocation within an agent loop."""
    tool_name: str
    arguments: dict
    result: Any = None
    error: Optional[str] = None


@dataclass
class AgentRunResult:
    """Result of an agent loop run (one or more LLM calls + tool executions)."""
    final_text: str
    model: str
    tool_calls: list = field(default_factory=list)
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_cost_usd: float = 0.0
    iterations: int = 0
    duration_seconds: float = 0.0

# Pricing data ($/1M tokens) - approximate, users can override
PRICING = {
    # Anthropic Claude
    'claude-opus-4-6': {'input': 15.00, 'output': 75.00},
    'claude-opus-4-5': {'input': 15.00, 'output': 75.00},
    'claude-sonnet-4-6': {'input': 3.00, 'output': 15.00},
    'claude-sonnet-4-5': {'input': 3.00, 'output': 15.00},
    'claude-haiku-4-5': {'input': 0.80, 'output': 4.00},
    'claude-3-5-sonnet-20241022': {'input': 3.00, 'output': 15.00},
    'claude-3-5-haiku-20241022': {'input': 0.80, 'output': 4.00},
    # OpenAI
    'gpt-4o': {'input': 2.50, 'output': 10.00},
    'gpt-4o-mini': {'input': 0.15, 'output': 0.60},
    'gpt-4-turbo': {'input': 10.00, 'output': 30.00},
    'o1-preview': {'input': 15.00, 'output': 60.00},
    'o1-mini': {'input': 3.00, 'output': 12.00},
}


@dataclass
class LLMResult:
    """Result of an LLM job execution."""
    text: str
    model: str
    input_tokens: int
    output_tokens: int
    cost_usd: float
    duration_seconds: float
    raw_response: Any = None


def _compute_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Compute USD cost from model pricing. Logs a warning if unknown model."""
    pricing = PRICING.get(model)
    if pricing is None:
        logger.warning(
            f"Model {model!r} not in PRICING table; cost will be reported as $0.00 "
            f"(tokens: {input_tokens} in / {output_tokens} out)"
        )
        return 0.0
    return (input_tokens * pricing['input'] + output_tokens * pricing['output']) / 1_000_000


class LLMProvider:
    """Base class for LLM providers."""
    name: str = "base"

    def call(self, prompt: str, model: str, max_tokens: int = 4096, **kwargs) -> LLMResult:
        raise NotImplementedError

    def run_agent_loop(
        self,
        prompt: str,
        model: str,
        tools: list,
        max_tokens: int = 4096,
        max_iterations: int = 10,
        budget=None,
        **kwargs,
    ) -> AgentRunResult:
        """Run an agent loop: LLM -> tool_use -> tool_result -> LLM -> ... -> final text.

        Subclasses must implement.
        """
        raise NotImplementedError


class AnthropicProvider(LLMProvider):
    """Anthropic Claude provider."""
    name = "anthropic"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get('ANTHROPIC_API_KEY')
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY not set")

    def call(self, prompt: str, model: str = "claude-sonnet-4-6", max_tokens: int = 4096, **kwargs) -> LLMResult:
        try:
            import anthropic
        except ImportError:
            raise ImportError("anthropic package required: pip install anthropic")

        client = anthropic.Anthropic(api_key=self.api_key)
        start = time.perf_counter()

        response = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
            **{k: v for k, v in kwargs.items() if k not in ('cost_usd', 'duration_seconds')},
        )

        duration = time.perf_counter() - start
        text = ""
        if response.content:
            for block in response.content:
                if getattr(block, 'type', None) == 'text':
                    text = block.text
                    break
            if not text and hasattr(response.content[0], 'text'):
                text = response.content[0].text
        input_tokens = response.usage.input_tokens
        output_tokens = response.usage.output_tokens
        cost = _compute_cost(model, input_tokens, output_tokens)

        return LLMResult(
            text=text,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost,
            duration_seconds=duration,
            raw_response=response,
        )

    def run_agent_loop(
        self,
        prompt: str,
        model: str = "claude-sonnet-4-6",
        tools: Optional[list] = None,
        max_tokens: int = 4096,
        max_iterations: int = 10,
        budget=None,
        **kwargs,
    ) -> AgentRunResult:
        try:
            import anthropic
        except ImportError:
            raise ImportError("anthropic package required: pip install anthropic")

        tools = tools or []
        tool_map = {t.__name__: t for t in tools}
        tool_schemas = [function_to_tool_schema(t) for t in tools]

        client = anthropic.Anthropic(api_key=self.api_key)
        start = time.perf_counter()

        messages = [{"role": "user", "content": prompt}]
        tool_call_records: list = []
        total_in = 0
        total_out = 0
        total_cost = 0.0
        final_text = ""
        iterations = 0

        filtered_kwargs = {k: v for k, v in kwargs.items() if k not in ('cost_usd', 'duration_seconds')}

        for i in range(max_iterations):
            iterations = i + 1
            create_kwargs = dict(
                model=model,
                max_tokens=max_tokens,
                messages=messages,
                **filtered_kwargs,
            )
            if tool_schemas:
                create_kwargs['tools'] = tool_schemas

            response = client.messages.create(**create_kwargs)

            total_in += response.usage.input_tokens
            total_out += response.usage.output_tokens
            total_cost += _compute_cost(model, response.usage.input_tokens, response.usage.output_tokens)

            if budget is not None:
                try:
                    budget.check(0.0)
                except Exception:
                    break

            # Collect text and tool_use blocks
            text_parts: list = []
            tool_uses: list = []
            for block in response.content:
                btype = getattr(block, 'type', None)
                if btype == 'text':
                    text_parts.append(block.text)
                elif btype == 'tool_use':
                    tool_uses.append(block)

            if text_parts:
                final_text = "\n".join(text_parts)

            # Stop if model is done
            if response.stop_reason != 'tool_use' or not tool_uses:
                break

            # Append assistant message (entire content, including tool_use blocks)
            messages.append({"role": "assistant", "content": response.content})

            # Execute tools and produce tool_result blocks
            tool_result_blocks: list = []
            for tu in tool_uses:
                tname = tu.name
                targs = tu.input or {}
                func = tool_map.get(tname)
                record = ToolCall(tool_name=tname, arguments=dict(targs))
                if func is None:
                    record.error = f"Tool {tname!r} not found"
                    tool_result_blocks.append({
                        "type": "tool_result",
                        "tool_use_id": tu.id,
                        "content": record.error,
                        "is_error": True,
                    })
                else:
                    try:
                        result = func(**targs)
                        record.result = result
                        tool_result_blocks.append({
                            "type": "tool_result",
                            "tool_use_id": tu.id,
                            "content": str(result),
                        })
                    except Exception as e:
                        record.error = str(e)
                        tool_result_blocks.append({
                            "type": "tool_result",
                            "tool_use_id": tu.id,
                            "content": f"Error: {e}",
                            "is_error": True,
                        })
                tool_call_records.append(record)

            messages.append({"role": "user", "content": tool_result_blocks})

        duration = time.perf_counter() - start
        return AgentRunResult(
            final_text=final_text,
            model=model,
            tool_calls=tool_call_records,
            total_input_tokens=total_in,
            total_output_tokens=total_out,
            total_cost_usd=total_cost,
            iterations=iterations,
            duration_seconds=duration,
        )


class OpenAIProvider(LLMProvider):
    """OpenAI provider (GPT-4, o1, etc)."""
    name = "openai"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY not set")

    def call(self, prompt: str, model: str = "gpt-4o-mini", max_tokens: int = 4096, **kwargs) -> LLMResult:
        try:
            import openai
        except ImportError:
            raise ImportError("openai package required: pip install openai")

        client = openai.OpenAI(api_key=self.api_key)
        start = time.perf_counter()

        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            **{k: v for k, v in kwargs.items() if k not in ('cost_usd', 'duration_seconds')},
        )

        duration = time.perf_counter() - start
        text = response.choices[0].message.content or ""
        input_tokens = response.usage.prompt_tokens
        output_tokens = response.usage.completion_tokens
        cost = _compute_cost(model, input_tokens, output_tokens)

        return LLMResult(
            text=text,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost,
            duration_seconds=duration,
            raw_response=response,
        )

    def run_agent_loop(
        self,
        prompt: str,
        model: str = "gpt-4o-mini",
        tools: Optional[list] = None,
        max_tokens: int = 4096,
        max_iterations: int = 10,
        budget=None,
        **kwargs,
    ) -> AgentRunResult:
        try:
            import openai
        except ImportError:
            raise ImportError("openai package required: pip install openai")

        tools = tools or []
        tool_map = {t.__name__: t for t in tools}
        tool_schemas = []
        for t in tools:
            s = function_to_tool_schema(t)
            tool_schemas.append({
                "type": "function",
                "function": {
                    "name": s["name"],
                    "description": s["description"],
                    "parameters": s["input_schema"],
                },
            })

        client = openai.OpenAI(api_key=self.api_key)
        start = time.perf_counter()

        messages: list = [{"role": "user", "content": prompt}]
        tool_call_records: list = []
        total_in = 0
        total_out = 0
        total_cost = 0.0
        final_text = ""
        iterations = 0

        filtered_kwargs = {k: v for k, v in kwargs.items() if k not in ('cost_usd', 'duration_seconds')}

        for i in range(max_iterations):
            iterations = i + 1
            create_kwargs = dict(
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                **filtered_kwargs,
            )
            if tool_schemas:
                create_kwargs['tools'] = tool_schemas

            response = client.chat.completions.create(**create_kwargs)

            total_in += response.usage.prompt_tokens
            total_out += response.usage.completion_tokens
            total_cost += _compute_cost(model, response.usage.prompt_tokens, response.usage.completion_tokens)

            if budget is not None:
                try:
                    budget.check(0.0)
                except Exception:
                    break

            msg = response.choices[0].message
            if msg.content:
                final_text = msg.content

            tool_calls = getattr(msg, 'tool_calls', None) or []
            if not tool_calls:
                break

            # Append assistant message with tool_calls
            messages.append({
                "role": "assistant",
                "content": msg.content or "",
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        },
                    }
                    for tc in tool_calls
                ],
            })

            for tc in tool_calls:
                tname = tc.function.name
                try:
                    targs = json.loads(tc.function.arguments) if tc.function.arguments else {}
                except json.JSONDecodeError:
                    targs = {}
                record = ToolCall(tool_name=tname, arguments=dict(targs))
                func = tool_map.get(tname)
                if func is None:
                    record.error = f"Tool {tname!r} not found"
                    result_str = record.error
                else:
                    try:
                        result = func(**targs)
                        record.result = result
                        result_str = str(result)
                    except Exception as e:
                        record.error = str(e)
                        result_str = f"Error: {e}"
                tool_call_records.append(record)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result_str,
                })

        duration = time.perf_counter() - start
        return AgentRunResult(
            final_text=final_text,
            model=model,
            tool_calls=tool_call_records,
            total_input_tokens=total_in,
            total_output_tokens=total_out,
            total_cost_usd=total_cost,
            iterations=iterations,
            duration_seconds=duration,
        )


_PROVIDERS: "dict[str, type[LLMProvider]]" = {
    'anthropic': AnthropicProvider,
    'openai': OpenAIProvider,
    'claude': AnthropicProvider,
    'gpt': OpenAIProvider,
}


def register_llm_provider(name: str, provider_cls: "type[LLMProvider]") -> None:
    """Register a custom LLM provider."""
    _PROVIDERS[name] = provider_cls


def _resolve_provider(name: str, model: str) -> LLMProvider:
    """Resolve a provider instance from name + model."""
    if name in _PROVIDERS:
        return _PROVIDERS[name]()
    # Auto-detect from model name
    model_lower = model.lower()
    if 'claude' in model_lower:
        return AnthropicProvider()
    if 'gpt' in model_lower or model_lower.startswith('o1') or model_lower.startswith('o3'):
        return OpenAIProvider()
    raise ValueError(f"Unknown LLM provider: {name}")


@dataclass
class LLMJob:
    """An LLM-call job. Use with scheduler.add_llm_job() or as a callable.

    Example:
        scheduler.add_llm_job(
            prompt="Summarize today's open issues",
            model="claude-sonnet-4-6",
            schedule={'hour': 9},
            id='daily_briefing',
            on_result=lambda result: print(result.text),
        )
    """
    prompt: str
    model: str = "claude-sonnet-4-6"
    provider: str = "auto"
    max_tokens: int = 4096
    max_cost_per_run: Optional[float] = None
    on_result: Optional[Callable] = None
    on_error: Optional[Callable[[Exception], None]] = None
    extra_kwargs: dict = field(default_factory=dict)
    tools: list = field(default_factory=list)
    max_tool_iterations: int = 10
    budget: Any = None

    def __call__(self, ctx=None) -> dict:
        """Execute the LLM call. Compatible with the JobContext signature.

        If `tools` is non-empty, runs an agent loop (tool_use -> tool_result -> ...).
        """
        provider_inst = _resolve_provider(self.provider, self.model)

        # Tool-calling path
        if self.tools:
            try:
                run = provider_inst.run_agent_loop(
                    prompt=self.prompt,
                    model=self.model,
                    tools=self.tools,
                    max_tokens=self.max_tokens,
                    max_iterations=self.max_tool_iterations,
                    budget=self.budget,
                    **self.extra_kwargs,
                )

                if self.max_cost_per_run is not None and run.total_cost_usd > self.max_cost_per_run:
                    raise ValueError(
                        f"Agent loop cost ${run.total_cost_usd:.4f} exceeded "
                        f"max_cost_per_run ${self.max_cost_per_run:.4f}"
                    )

                if ctx is not None:
                    try:
                        ctx.set('last_cost', run.total_cost_usd)
                        ctx.set('last_input_tokens', run.total_input_tokens)
                        ctx.set('last_output_tokens', run.total_output_tokens)
                        ctx.set('iterations', run.iterations)
                        ctx.log(
                            f"Agent loop: {run.iterations} iter, "
                            f"{run.total_input_tokens}+{run.total_output_tokens} tokens, "
                            f"{len(run.tool_calls)} tool calls, ${run.total_cost_usd:.4f}"
                        )
                    except Exception:
                        pass

                if self.on_result is not None:
                    try:
                        self.on_result(run)
                    except Exception as e:
                        logger.warning(f"on_result callback failed: {e}")

                return {
                    'final_text': run.final_text,
                    'text': run.final_text,
                    'model': run.model,
                    'input_tokens': run.total_input_tokens,
                    'output_tokens': run.total_output_tokens,
                    'cost_usd': run.total_cost_usd,
                    'duration_seconds': run.duration_seconds,
                    'iterations': run.iterations,
                    'tool_calls': [
                        {
                            'tool_name': tc.tool_name,
                            'arguments': tc.arguments,
                            'result': tc.result,
                            'error': tc.error,
                        }
                        for tc in run.tool_calls
                    ],
                }
            except Exception as e:
                if self.on_error is not None:
                    try:
                        self.on_error(e)
                    except Exception:
                        pass
                raise

        last_error = None
        for attempt in range(3):
            try:
                result = provider_inst.call(
                    prompt=self.prompt,
                    model=self.model,
                    max_tokens=self.max_tokens,
                    **self.extra_kwargs,
                )

                if self.max_cost_per_run is not None and result.cost_usd > self.max_cost_per_run:
                    raise ValueError(
                        f"LLM call cost ${result.cost_usd:.4f} exceeded "
                        f"max_cost_per_run ${self.max_cost_per_run:.4f}"
                    )

                if ctx is not None:
                    try:
                        ctx.set('last_cost', result.cost_usd)
                        ctx.set('last_input_tokens', result.input_tokens)
                        ctx.set('last_output_tokens', result.output_tokens)
                        ctx.log(f"LLM call: {result.input_tokens}+{result.output_tokens} tokens, ${result.cost_usd:.4f}")
                    except Exception:
                        pass

                if self.on_result is not None:
                    try:
                        self.on_result(result)
                    except Exception as e:
                        logger.warning(f"on_result callback failed: {e}")

                # Return a serializable dict so DAG downstream can read it
                return {
                    'text': result.text,
                    'model': result.model,
                    'input_tokens': result.input_tokens,
                    'output_tokens': result.output_tokens,
                    'cost_usd': result.cost_usd,
                    'duration_seconds': result.duration_seconds,
                }
            except Exception as e:
                last_error = e
                error_str = str(e).lower()
                # Retry on rate limit or transient errors
                if 'rate' in error_str or 'timeout' in error_str or '429' in error_str or '503' in error_str:
                    backoff = (2 ** attempt) * 1.0
                    logger.warning(f"LLM call failed (attempt {attempt+1}), retrying in {backoff}s: {e}")
                    time.sleep(backoff)
                    continue
                # Non-transient - fail fast
                if self.on_error is not None:
                    try:
                        self.on_error(e)
                    except Exception:
                        pass
                raise

        if self.on_error is not None:
            try:
                self.on_error(last_error)
            except Exception:
                pass
        raise last_error or RuntimeError("LLM call failed after retries")
