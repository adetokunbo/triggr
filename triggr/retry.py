"""Retry policy and exponential backoff for async operations.

Two pre-built policies cover most cases::

    from triggr import DEFAULT, LONG_RUNNING, PollingTrigger, TriggerConfig

    config = TriggerConfig(polling_interval=30.0)

    # DEFAULT: for triggers — up to 35 retries, 0.2s → 5s backoff
    trigger = PollingTrigger(source, worker, config, retry_policy=DEFAULT)

    # LONG_RUNNING: for RetryingService — same backoff, but resets the
    # retry counter after 60s of successful operation so a service that
    # runs for hours doesn't exhaust its budget from early failures
    svc = RetryingService(factory=create_service, retry_policy=LONG_RUNNING)

To tune backoff for a specific trigger::

    from triggr import RetryPolicy, PollingTrigger, TriggerConfig

    config = TriggerConfig(polling_interval=30.0)
    policy = RetryPolicy(max_retries=10, initial_delay=1.0, max_delay=30.0)
    trigger = PollingTrigger(source, worker, config, retry_policy=policy)

``retry()`` is used internally by ``Processor`` and ``RetryingService``.
The ``between_attempts`` hook is how readiness gates are re-checked between
retries — if the gate blocks, the next attempt waits until it clears::

    from triggr import retry, RetryPolicy, EventGate

    gate = EventGate()
    policy = RetryPolicy(max_retries=5, initial_delay=0.5, max_delay=10.0)

    outcome = await retry(
        policy,
        operation=lambda: fulfillment_worker.complete(order),
        description="fulfil order",
        is_retryable=lambda e: not isinstance(e, PaymentDeclinedError),
        between_attempts=warehouse_gate.wait_until_ready,
    )
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
