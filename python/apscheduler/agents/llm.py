"""LLM job support - schedule LLM calls with cost tracking, retry, and observability."""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

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


class LLMProvider:
    """Base class for LLM providers."""
    name: str = "base"

    def call(self, prompt: str, model: str, max_tokens: int = 4096, **kwargs) -> LLMResult:
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
        text = response.content[0].text if response.content else ""
        input_tokens = response.usage.input_tokens
        output_tokens = response.usage.output_tokens

        pricing = PRICING.get(model, {'input': 0, 'output': 0})
        cost = (input_tokens * pricing['input'] + output_tokens * pricing['output']) / 1_000_000

        return LLMResult(
            text=text,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost,
            duration_seconds=duration,
            raw_response=response,
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

        pricing = PRICING.get(model, {'input': 0, 'output': 0})
        cost = (input_tokens * pricing['input'] + output_tokens * pricing['output']) / 1_000_000

        return LLMResult(
            text=text,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost,
            duration_seconds=duration,
            raw_response=response,
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
    on_result: Optional[Callable[[LLMResult], None]] = None
    on_error: Optional[Callable[[Exception], None]] = None
    extra_kwargs: dict = field(default_factory=dict)

    def __call__(self, ctx=None) -> dict:
        """Execute the LLM call. Compatible with the JobContext signature."""
        provider_inst = _resolve_provider(self.provider, self.model)

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
