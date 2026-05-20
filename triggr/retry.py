"""Retry policy and async retry helper with exponential backoff.

Provides RetryPolicy (configurable backoff parameters), two pre-built
policies (DEFAULT, LONG_RUNNING), and the retry() coroutine used by
Processor and RetryingService.
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass, field
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int = 35
    initial_delay: float = 0.2
    max_delay: float = 5.0
    reset_retries_after: float | None = None

    def delay_for_attempt(self, attempt: int) -> float:
        delay = min(self.initial_delay * (2**attempt), self.max_delay)
        jitter = random.uniform(0, delay * 0.1)
        return delay + jitter


DEFAULT = RetryPolicy(max_retries=35, initial_delay=0.2, max_delay=5.0)
LONG_RUNNING = RetryPolicy(
    max_retries=35, initial_delay=0.2, max_delay=5.0, reset_retries_after=60.0
)


class RetriesExhausted(Exception):
    def __init__(self, attempts: int, last_error: Exception):
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(f"Retries exhausted after {attempts} attempts: {last_error}")


async def retry(
    policy: RetryPolicy,
    operation: Callable[[], Awaitable[T]],
    description: str,
    is_retryable: Callable[[Exception], bool] = lambda _: True,
    between_attempts: Callable[[], Awaitable[None]] | None = None,
) -> T:
    last_error: Exception | None = None
    for attempt in range(policy.max_retries + 1):
        try:
            return await operation()
        except Exception as e:
            last_error = e
            if not is_retryable(e) or attempt == policy.max_retries:
                break
            delay = policy.delay_for_attempt(attempt)
            logger.debug(
                "Retry %d/%d for %s after %.2fs: %s",
                attempt + 1,
                policy.max_retries,
                description,
                delay,
                e,
            )
            if between_attempts is not None:
                await between_attempts()
            await asyncio.sleep(delay)
    raise RetriesExhausted(policy.max_retries + 1, last_error)  # type: ignore[arg-type]
