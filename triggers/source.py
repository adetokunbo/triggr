from __future__ import annotations

import asyncio
from abc import abstractmethod
from typing import AsyncIterator, Generic, TypeVar

from .base import TriggerContext
from .retry import AUTOMATION, RetryPolicy
from .task_based import TaskbasedTrigger

T = TypeVar("T")


class SourceBasedTrigger(TaskbasedTrigger[T], Generic[T]):
    """Trigger driven by an async iterator instead of polling.

    Subclasses implement:
    - source() -> async iterator of tasks
    - complete_task(task) -> TaskOutcome
    - is_stale_task(task) -> bool
    """

    def __init__(
        self,
        context: TriggerContext,
        name: str | None = None,
        retry_policy: RetryPolicy = AUTOMATION,
    ) -> None:
        super().__init__(context, name, retry_policy)

    @abstractmethod
    def source(self) -> AsyncIterator[T]:
        ...

    async def _run(self) -> None:
        try:
            async for task in self.source():
                if self._closed:
                    break
                await self.wait_for_not_paused()
                await self.process_task_with_retry(task)
        except asyncio.CancelledError:
            pass

    async def pause(self) -> None:
        self._paused.clear()
