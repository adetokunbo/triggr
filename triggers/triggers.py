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
from .protocols import HasHealth, TaskSource, TaskWorker
from .retry import AUTOMATION, RetryPolicy

T = TypeVar("T")

logger = logging.getLogger(__name__)


class PollingTaskTrigger(Generic[T]):
    """Poll for tasks, process them in parallel with retry.

    Composed from PollingLoop + TaskProcessor + TaskSource.
    Replaces the inheritance-based PollingParallelTaskExecutionTrigger.
    """

    def __init__(
        self,
        source: TaskSource[T],
        worker: TaskWorker[T],
        config: AutomationConfig,
        retry_policy: RetryPolicy = AUTOMATION,
        name: str = "",
    ) -> None:
        self._source = source
        self._lifecycle = Lifecycle(name)
        self._processor = TaskProcessor(
            worker,
            retry_policy,
            ready_gate=self._lifecycle.wait_for_not_paused,
            name=name,
        )
        self._loop = PollingLoop(
            callback=self._poll_once,
            lifecycle=self._lifecycle,
            interval=config.polling_interval,
            jitter=config.polling_jitter,
            max_silent_failures=config.max_num_silent_polling_retries,
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
    Replaces the inheritance-based SourceBasedTrigger.
    """

    def __init__(
        self,
        source: AsyncIterator[T],
        worker: TaskWorker[T],
        retry_policy: RetryPolicy = AUTOMATION,
        name: str = "",
    ) -> None:
        self._source = source
        self._lifecycle = Lifecycle(name)
        self._processor = TaskProcessor(
            worker,
            retry_policy,
            ready_gate=self._lifecycle.wait_for_not_paused,
            name=name,
        )
        self._task: asyncio.Task[None] | None = None
        self._name = name

    def run(self, paused: bool = False) -> asyncio.Task[None]:
        if paused:
            self._lifecycle.pause()
        self._task = asyncio.ensure_future(self._run())
        return self._task

    async def _run(self) -> None:
        try:
            async for task in self._source:
                if self._lifecycle.is_closed:
                    break
                await self._lifecycle.wait_for_not_paused()
                if self._lifecycle.is_closed:
                    break
                await self._processor.process(task)
        except asyncio.CancelledError:
            pass

    def pause(self) -> None:
        self._lifecycle.pause()

    def resume(self) -> None:
        self._lifecycle.resume()

    def close(self) -> None:
        self._lifecycle.close()
        if self._task and not self._task.done():
            self._task.cancel()

    def is_healthy(self) -> bool:
        return self._task is not None and not self._task.done()


@dataclass
class PeriodicTask:
    timestamp: float

    def __str__(self) -> str:
        return f"PeriodicTask({self.timestamp})"


class PeriodicTrigger:
    """Run a task on a fixed interval. The task is never stale.

    Composed from PollingLoop + TaskProcessor.
    Replaces the inheritance-based PeriodicTaskTrigger.
    """

    def __init__(
        self,
        worker: TaskWorker[PeriodicTask],
        interval: float,
        retry_policy: RetryPolicy = AUTOMATION,
        name: str = "",
    ) -> None:
        self._lifecycle = Lifecycle(name)
        self._processor = TaskProcessor(
            worker,
            retry_policy,
            ready_gate=self._lifecycle.wait_for_not_paused,
            name=name,
        )
        self._loop = PollingLoop(
            callback=self._tick,
            lifecycle=self._lifecycle,
            interval=interval,
            jitter=0,
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
