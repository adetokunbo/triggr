"""Wraps a Worker with retry, staleness detection, and metrics.

``Processor`` is used internally by all trigger types — most callers
never instantiate it directly. Understanding it helps when reasoning
about what happens between a task being retrieved and its outcome being
recorded.

When a task is processed, ``Processor`` calls ``worker.complete(task)``.
If it raises, the error is classified and — if transient — retried with
exponential backoff. Between retries, the readiness gate is re-checked,
so a paused trigger stays paused even mid-retry.

Before each retry attempt, ``worker.is_stale(task)`` is called. If the
task has been cancelled or superseded since it was retrieved, the attempt
is abandoned and ``Outcome.STALE`` is returned rather than retrying::

    class FulfillmentWorker:
        async def complete(self, order: Order) -> Outcome:
            await warehouse.ship(order)
            return Outcome.SUCCESS

        async def is_stale(self, order: Order) -> bool:
            # order may have been cancelled while waiting to be processed
            return await db.is_cancelled(order.id)

``Processor`` can be used standalone for testing a worker in isolation,
without needing a full trigger::

    from triggr import Processor, DEFAULT

    processor = Processor(FulfillmentWorker(), retry_policy=DEFAULT)
    succeeded = await processor.process(order)
"""

from __future__ import annotations

import logging
import time
from typing import Awaitable, Callable, Generic, TypeVar

from .protocols import (
    ErrorClassifier,
    ErrorKind,
    NoOpMetrics,
    Outcome,
    TransientErrors,
    TriggerMetrics,
    Worker,
)
from .retry import DEFAULT, RetriesExhausted, RetryPolicy, retry

T = TypeVar("T")

logger = logging.getLogger(__name__)


class Processor(Generic[T]):
    """Wraps a Worker with retry and staleness detection.

    Independently testable — does not depend on triggers, polling,
    or lifecycle management.
    """

    def __init__(
        self,
        worker: Worker[T],
        retry_policy: RetryPolicy = DEFAULT,
        ready_gate: Callable[[], Awaitable[None]] | None = None,
        error_classifier: ErrorClassifier | None = None,
        metrics: TriggerMetrics | None = None,
        name: str = "",
    ) -> None:
        self._worker = worker
        self._retry_policy = retry_policy
        self._ready_gate = ready_gate
        self._classifier = error_classifier or TransientErrors()
        self._metrics = metrics or NoOpMetrics()
        self._logger = logging.getLogger(f"processor.{name}" if name else __name__)

    async def process(self, task: T) -> bool:
        """Process a single task with retry.

        Returns True if the task succeeded or was stale.
        """
        t0 = time.monotonic()
        try:
            outcome = await retry(
                self._retry_policy,
                lambda: self._attempt(task),
                description=self._name_of(task),
                is_retryable=self._is_retryable,
                between_attempts=self._ready_gate,
            )
        except RetriesExhausted as e:
            elapsed = time.monotonic() - t0
            self._metrics.record_error(e.last_error)
            self._metrics.record_outcome(Outcome.FAILED, elapsed)
            self._logger.error("Task %s failed after retries: %s", task, e.last_error)
            return False

        elapsed = time.monotonic() - t0
        self._metrics.record_outcome(outcome, elapsed)

        if outcome == Outcome.SUCCESS:
            self._logger.info("Task %s completed", task)
            return True
        elif outcome == Outcome.STALE:
            self._logger.debug("Task %s is stale", task)
            return True
        elif outcome == Outcome.FAILED:
            self._logger.warning("Task %s failed", task)
            return False
        else:  # NOOP
            return False

    async def _attempt(self, task: T) -> Outcome:
        if self._ready_gate is not None:
            await self._ready_gate()
        try:
            return await self._worker.complete(task)
        except Exception as e:
            self._metrics.record_error(e)
            if await self._worker.is_stale(task):
                return Outcome.STALE
            raise

    def _is_retryable(self, exc: Exception) -> bool:
        return self._classifier.classify(exc) == ErrorKind.TRANSIENT

    def _name_of(self, task: T) -> str:
        return str(task)
