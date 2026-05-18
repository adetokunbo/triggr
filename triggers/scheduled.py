from __future__ import annotations

import time
from abc import abstractmethod
from dataclasses import dataclass
from typing import Generic, TypeVar

from .base import TriggerContext
from .parallel import PollingParallelTaskExecutionTrigger
from .retry import AUTOMATION, RetryPolicy
from .task_based import TaskOutcome

T = TypeVar("T")


@dataclass
class ReadyTask(Generic[T]):
    ready_at: float
    work: T

    def __str__(self) -> str:
        return f"ReadyTask(ready_at={self.ready_at}, work={self.work})"


class ScheduledTaskTrigger(PollingParallelTaskExecutionTrigger[ReadyTask[T]], Generic[T]):
    """Poll for tasks that are ready based on the current time.

    Subclasses implement:
    - list_ready_tasks(now, limit) -> list of work items ready at `now`
    - complete_task(ready_task) -> TaskOutcome
    - is_stale_task(ready_task) -> bool
    """

    def __init__(
        self,
        context: TriggerContext,
        name: str | None = None,
        retry_policy: RetryPolicy = AUTOMATION,
    ) -> None:
        super().__init__(context, name, retry_policy)

    @abstractmethod
    async def list_ready_tasks(self, now: float, limit: int) -> list[T]:
        ...

    async def retrieve_tasks(self) -> list[ReadyTask[T]]:
        now = time.time()
        limit = self._context.config.parallelism
        items = await self.list_ready_tasks(now, limit)
        return [ReadyTask(ready_at=now, work=item) for item in items]
