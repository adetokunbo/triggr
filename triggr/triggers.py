"""Composed trigger types: the primary public API for running async work.

``PollingTrigger`` polls a ``Source`` on an interval and processes tasks
in parallel, with sliding-window concurrency bounded by ``parallelism``::

    from triggr import PollingTrigger, PollingConfig

    config = PollingConfig(polling_interval=30.0, parallelism=4)
    trigger = PollingTrigger(
        source=PendingOrderSource(),
        worker=FulfillmentWorker(),
        config=config,
    )
    trigger.run()

``StreamTrigger`` consumes an ``AsyncIterator`` — useful for event streams
where tasks arrive unpredictably rather than on a fixed schedule::

    from triggr import StreamTrigger

    trigger = StreamTrigger(
        source=payment_event_stream(),   # AsyncIterator[PaymentEvent]
        worker=PaymentWorker(),
        parallelism=4,
    )
    trigger.run()

``PeriodicTrigger`` runs a task on a fixed interval. The worker receives a
``PeriodicTask`` carrying the current timestamp; ``is_stale`` always returns
False because there is nothing to check — the task is always fresh::

    from triggr import PeriodicTrigger, PeriodicTask, Outcome

    class InventorySyncWorker:
        async def complete(self, task: PeriodicTask) -> Outcome:
            await warehouse.sync_inventory()
            return Outcome.SUCCESS

        async def is_stale(self, task: PeriodicTask) -> bool:
            return False

    trigger = PeriodicTrigger(InventorySyncWorker(), interval=60.0)
    trigger.run()

All triggers support ``pause()``, ``resume()``, ``close()``, and
``is_healthy()``. Pass ``paused=True`` to ``run()`` to start suspended::

    trigger.run(paused=True)
    # ... register other triggers ...
    trigger.resume()
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import AsyncIterator, Awaitable, Callable, Generic, TypeVar

from .gates import compose_gates
from .lifecycle import Lifecycle
from .outcome import Outcome
from .polling_loop import PollingLoop
from .processor import Processor
from .protocols import (
    ErrorClassifier,
    HasHealth,
    ReadinessGate,
    Source,
    Worker,
    TriggerMetrics,
)
from .retry import DEFAULT, RetryPolicy

T = TypeVar("T")

logger = logging.getLogger(__name__)


@dataclass
class PollingConfig:
    """Configuration for ``PollingTrigger``.

    - ``polling_interval``: seconds between poll cycles when no work is found.
      If the source returns tasks, the loop polls again immediately without waiting.
    - ``polling_jitter``: fraction of ``polling_interval`` added as random noise,
      preventing multiple triggers from polling in lockstep. At the default of 0.2,
      a 30s interval varies between 24s and 36s.
    - ``parallelism``: maximum number of tasks processed concurrently within a poll cycle.
    - ``max_silent_failures``: consecutive source errors to tolerate before logging a warning.
    """

    polling_interval: float = 30.0
    polling_jitter: float = 0.2
    parallelism: int = 4
    max_silent_failures: int = 3


def _make_gate(
    lifecycle: Lifecycle,
    ready_gate: ReadinessGate | None,
) -> Callable[[], Awaitable[None]]:
    """Combine lifecycle pause gate with an optional application readiness gate."""
    if ready_gate is None:
        return lifecycle.wait_for_not_paused
    return compose_gates(lifecycle.wait_for_not_paused, ready_gate.wait_until_ready)


class PollingTrigger(Generic[T]):
    """Poll for tasks, process them in parallel with retry.

    Composed from PollingLoop + Processor + Source.
    """

    def __init__(
        self,
        source: Source[T],
        worker: Worker[T],
        config: PollingConfig,
        retry_policy: RetryPolicy = DEFAULT,
        error_classifier: ErrorClassifier | None = None,
        metrics: TriggerMetrics | None = None,
        ready_gate: ReadinessGate | None = None,
        name: str = "",
    ) -> None:
        self._source = source
        self._lifecycle = Lifecycle(name)
        gate = _make_gate(self._lifecycle, ready_gate)
        self._processor = Processor(
            worker,
            retry_policy,
            ready_gate=gate,
            error_classifier=error_classifier,
            metrics=metrics,
            name=name,
        )
        self._loop = PollingLoop(
            callback=self._poll_once,
            lifecycle=self._lifecycle,
            interval=config.polling_interval,
            jitter=config.polling_jitter,
            max_silent_failures=config.max_silent_failures,
            error_classifier=error_classifier,
            metrics=metrics,
            name=name,
        )
        self._parallelism = config.parallelism
        self._name = name

    def run(self, paused: bool = False) -> asyncio.Task[None]:
        if paused:
            self._lifecycle.pause()
        return self._loop.start()

    async def _poll_once(self) -> bool:
        tasks = await self._source.retrieve()
        if not tasks:
            return False

        sem = asyncio.Semaphore(self._parallelism)

        async def run_with_sem(t: T) -> bool:
            async with sem:
                return await self._processor.process(t)

        results = await asyncio.gather(*(run_with_sem(t) for t in tasks))
        return any(results)

    def pause(self) -> None:
        self._lifecycle.pause()

    def resume(self) -> None:
        self._lifecycle.resume()

    def close(self) -> None:
        self._lifecycle.close()

    def is_healthy(self) -> bool:
        return self._loop.is_healthy()

    async def run_once(self) -> bool:
        """Process one batch (for testing). Trigger must be paused."""
        assert self._lifecycle.is_paused, "Trigger must be paused to call run_once"
        self._lifecycle.resume()
        try:
            return await self._poll_once()
        finally:
            self._lifecycle.pause()


class StreamTrigger(Generic[T]):
    """Process tasks from an async iterator with retry.

    Composed from Processor + async iterator.
    Supports bounded concurrency via the `parallelism` parameter.
    """

    def __init__(
        self,
        source: AsyncIterator[T],
        worker: Worker[T],
        retry_policy: RetryPolicy = DEFAULT,
        error_classifier: ErrorClassifier | None = None,
        metrics: TriggerMetrics | None = None,
        grace_period: float = 60.0,
        parallelism: int = 1,
        ready_gate: ReadinessGate | None = None,
        name: str = "",
    ) -> None:
        self._source = source
        self._lifecycle = Lifecycle(name)
        self._gate = _make_gate(self._lifecycle, ready_gate)
        self._processor = Processor(
            worker,
            retry_policy,
            ready_gate=self._gate,
            error_classifier=error_classifier,
            metrics=metrics,
            name=name,
        )
        self._task: asyncio.Task[None] | None = None
        self._last_completed_at: float | None = None
        self._grace_period = grace_period
        self._parallelism = parallelism
        self._name = name

    def run(self, paused: bool = False) -> asyncio.Task[None]:
        if paused:
            self._lifecycle.pause()
        self._task = asyncio.ensure_future(self._run())
        return self._task

    async def _run(self) -> None:
        if self._parallelism <= 1:
            await self._run_sequential()
        else:
            await self._run_concurrent()

    async def _run_sequential(self) -> None:
        try:
            async for task in self._source:
                if self._lifecycle.is_closed:
                    break
                await self._gate()
                if self._lifecycle.is_closed:
                    break
                await self._processor.process(task)
                self._last_completed_at = time.monotonic()
        except asyncio.CancelledError:
            pass

    async def _run_concurrent(self) -> None:
        sem = asyncio.Semaphore(self._parallelism)
        pending: set[asyncio.Task[None]] = set()
        try:
            async for task in self._source:
                if self._lifecycle.is_closed:
                    break
                await self._gate()
                if self._lifecycle.is_closed:
                    break
                await sem.acquire()
                t = asyncio.create_task(self._process_and_release(sem, task))
                pending.add(t)
                t.add_done_callback(pending.discard)
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        except asyncio.CancelledError:
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    async def _process_and_release(self, sem: asyncio.Semaphore, task: T) -> None:
        try:
            await self._processor.process(task)
            self._last_completed_at = time.monotonic()
        finally:
            sem.release()

    def pause(self) -> None:
        self._lifecycle.pause()

    def resume(self) -> None:
        self._lifecycle.resume()

    def close(self) -> None:
        self._lifecycle.close()
        if self._task and not self._task.done():
            self._task.cancel()

    def is_healthy(self) -> bool:
        if self._task is None or self._task.done():
            return False
        if self._last_completed_at is None:
            return True  # hasn't had a chance to complete yet
        return (time.monotonic() - self._last_completed_at) < self._grace_period


@dataclass
class PeriodicTask:
    timestamp: float

    def __str__(self) -> str:
        return f"PeriodicTask({self.timestamp})"


class PeriodicTrigger:
    """Run a task on a fixed interval. The task is never stale.

    Composed from PollingLoop + Processor.
    """

    def __init__(
        self,
        worker: Worker[PeriodicTask],
        interval: float,
        retry_policy: RetryPolicy = DEFAULT,
        error_classifier: ErrorClassifier | None = None,
        metrics: TriggerMetrics | None = None,
        ready_gate: ReadinessGate | None = None,
        name: str = "",
    ) -> None:
        self._lifecycle = Lifecycle(name)
        gate = _make_gate(self._lifecycle, ready_gate)
        self._processor = Processor(
            worker,
            retry_policy,
            ready_gate=gate,
            error_classifier=error_classifier,
            metrics=metrics,
            name=name,
        )
        self._loop = PollingLoop(
            callback=self._tick,
            lifecycle=self._lifecycle,
            interval=interval,
            jitter=0,
            error_classifier=error_classifier,
            metrics=metrics,
            name=name,
        )
        self._name = name

    async def _tick(self) -> bool:
        task = PeriodicTask(timestamp=time.time())
        await self._processor.process(task)
        return False

    def run(self, paused: bool = False) -> asyncio.Task[None]:
        if paused:
            self._lifecycle.pause()
        return self._loop.start()

    def pause(self) -> None:
        self._lifecycle.pause()

    def resume(self) -> None:
        self._lifecycle.resume()

    def close(self) -> None:
        self._lifecycle.close()

    def is_healthy(self) -> bool:
        return self._loop.is_healthy()
