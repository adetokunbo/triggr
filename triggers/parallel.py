from __future__ import annotations

import asyncio
from abc import abstractmethod
from typing import Generic, TypeVar

from .base import TriggerContext
from .polling import PollingTrigger
from .retry import AUTOMATION, RetryPolicy
from .task_based import TaskbasedTrigger, TaskOutcome

T = TypeVar("T")


class PollingParallelTaskExecutionTrigger(PollingTrigger, TaskbasedTrigger[T], Generic[T]):
    """Poll for tasks, then execute them in parallel with retry.

    The most commonly used trigger base class. Subclasses implement:
    - retrieve_tasks() -> list of tasks to process
    - complete_task(task) -> TaskOutcome
    - is_stale_task(task) -> bool
    """

    def __init__(
        self,
        context: TriggerContext,
        name: str | None = None,
        retry_policy: RetryPolicy = AUTOMATION,
    ) -> None:
        PollingTrigger.__init__(self, context, name)
        TaskbasedTrigger.__init__(self, context, name, retry_policy)

    @abstractmethod
    async def retrieve_tasks(self) -> list[T]:
        ...

    async def perform_work_if_available(self) -> bool:
        tasks = await self.retrieve_tasks()
        if not tasks:
            return False

        parallelism = self._context.config.parallelism

        async def process_batch(batch: list[T]) -> list[bool]:
            return await asyncio.gather(
                *(self.process_task_with_retry(task) for task in batch)
            )

        # Process in batches of `parallelism`
        any_succeeded = False
        for i in range(0, len(tasks), parallelism):
            batch = tasks[i : i + parallelism]
            results = await process_batch(batch)
            if any(results):
                any_succeeded = True

        return any_succeeded

    async def run_once(self) -> bool:
        """Run one task (for testing). Trigger must be paused."""
        assert not self._paused.is_set(), "Trigger must be paused to call run_once"
        tasks = await self.retrieve_tasks()
        if not tasks:
            return False
        return await self.process_task_with_retry(tasks[0])
