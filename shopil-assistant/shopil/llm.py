"""Thin OpenAI-compatible client (OpenRouter free models) plus a scripted mock for tests.

Every call (real or mock) is appended to logs/llm_calls.jsonl with `mock` clearly marked,
so a reviewer can see exactly which runs used a real model. API keys are never logged.
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from openai import APIStatusError, OpenAI, RateLimitError

from . import config


class LLMError(RuntimeError):
    """Model call failed after retries (network, quota, empty answer)."""


def _log(entry: dict) -> None:
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    entry = {"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"), **entry}
    with open(config.LOG_DIR / "llm_calls.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _clean(text: str) -> str:
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.S)  # some reasoning models inline their thoughts
    return text.strip()


class LLM:
    """Real model. `role` is only used for logging (actor / critic)."""
    mock = False

    def __init__(self, role: str, model: str | None = None, fallback: str | None = None):
        if not config.LLM_API_KEY:
            raise LLMError("LLM_API_KEY is not set (see .env.example)")
        self.role = role
        self.model = model or config.LLM_MODEL
        self.fallback = fallback or config.LLM_MODEL_FAST
        self.client = OpenAI(api_key=config.LLM_API_KEY, base_url=config.LLM_BASE_URL, timeout=120, max_retries=0)

    def complete(self, messages: list[dict], *, max_tokens: int = 3000, temperature: float = 0.2) -> str:
        last: Exception | None = None
        for model in dict.fromkeys([self.model, self.fallback]):      # primary, then fallback model
            for attempt, wait in enumerate((0, 4, 10)):
                if wait:
                    time.sleep(wait)
                started = time.perf_counter()
                try:
                    r = self.client.chat.completions.create(model=model, messages=messages,
                                                            max_tokens=max_tokens, temperature=temperature)
                    text = _clean(r.choices[0].message.content or "")
                    if not text:
                        raise LLMError("empty completion (reasoning used the whole token budget?)")
                    _log({"role": self.role, "model": model, "mock": False, "ok": True, "try": attempt + 1,
                          "latency_s": round(time.perf_counter() - started, 2),
                          "completion_tokens": getattr(r.usage, "completion_tokens", None)})
                    return text
                except (RateLimitError, APIStatusError, LLMError, TimeoutError, OSError) as exc:
                    last = exc
                    _log({"role": self.role, "model": model, "mock": False, "ok": False, "try": attempt + 1,
                          "error": f"{type(exc).__name__}: {str(exc)[:160]}"})
        raise LLMError(f"model call failed after retries: {last}")


class MockLLM:
    """Scripted stand-in. Results produced with it are MOCK tests and are labeled as such in the logs."""
    mock = True

    def __init__(self, role: str, script: list[str] | Callable[[list[dict]], str], name: str = "mock"):
        self.role, self.model, self._script, self._i = role, name, script, 0

    def complete(self, messages: list[dict], **_) -> str:
        out = self._script(messages) if callable(self._script) else self._script[min(self._i, len(self._script) - 1)]
        self._i += 1
        _log({"role": self.role, "model": self.model, "mock": True, "ok": True, "try": 1, "latency_s": 0.0})
        return out
