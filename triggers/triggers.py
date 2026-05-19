from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import AsyncIterator, Generic, TypeVar

from .config import AutomationConfig
from .lifecycle import Lifecycle
from .outcome import TaskOutcome
from .polling_loop import PollingLoop
from .processor import TaskProcessor
from .protocols import ErrorClassifier, HasHealth, TaskSource, TaskWorker, TriggerMetrics
from .retry import AUTOMATION, RetryPolicy

T = TypeVar("T")

logger = logging.getLogger(__name__)


class PollingTaskTrigger(Generic[T]):
    """Poll for tasks, process them in parallel with retry.

    Composed from PollingLoop + TaskProcessor + TaskSource.
    """

    def __init__(
        self,
        source: TaskSource[T],
        worker: TaskWorker[T],
        config: AutomationConfig,
        retry_policy: RetryPolicy = AUTOMATION,
        error_classifier: ErrorClassifier | None = None,
        metrics: TriggerMetrics | None = None,
        name: str = "",
    ) -> None:
        self._source = source
        self._lifecycle = Lifecycle(name)
        self._processor = TaskProcessor(
            worker,
            retry_policy,
            ready_gate=self._lifecycle.wait_for_not_paused,
            error_classifier=error_classifier,
            metrics=metrics,
            name=name,
        )
        self._loop = PollingLoop(
            callback=self._poll_once,
            lifecycle=self._lifecycle,
            interval=config.polling_interval,
            jitter=config.polling_jitter,
            max_silent_failures=config.max_num_silent_polling_retries,
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
        tasks = await self._source.retrieve_tasks()
        if not tasks:
            return False

        results: list[bool] = []
        for i in range(0, len(tasks), self._parallelism):
            batch = tasks[i : i + self._parallelism]
            batch_results = await asyncio.gather(
                *(self._processor.process(t) for t in batch)
            )
            results.extend(batch_results)

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


class StreamTaskTrigger(Generic[T]):
    """Process tasks from an async iterator with retry.

    Composed from TaskProcessor + async iterator.
    Supports bounded concurrency via the `parallelism` parameter.
    """

    def __init__(
        self,
        source: AsyncIterator[T],
        worker: TaskWorker[T],
        retry_policy: RetryPolicy = AUTOMATION,
        error_classifier: ErrorClassifier | None = None,
        metrics: TriggerMetrics | None = None,
        grace_period: float = 60.0,
        parallelism: int = 1,
        name: str = "",
    ) -> None:
        self._source = source
        self._lifecycle = Lifecycle(name)
        self._processor = TaskProcessor(
            worker,
            retry_policy,
            ready_gate=self._lifecycle.wait_for_not_paused,
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
                await self._lifecycle.wait_for_not_paused()
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
                await self._lifecycle.wait_for_not_paused()
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

    Composed from PollingLoop + TaskProcessor.
    """

    def __init__(
        self,
        worker: TaskWorker[PeriodicTask],
        interval: float,
        retry_policy: RetryPolicy = AUTOMATION,
        error_classifier: ErrorClassifier | None = None,
        metrics: TriggerMetrics | None = None,
        name: str = "",
    ) -> None:
        self._lifecycle = Lifecycle(name)
        self._processor = TaskProcessor(
            worker,
            retry_policy,
            ready_gate=self._lifecycle.wait_for_not_paused,
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
