from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Generic, TypeVar

from .protocols import ReadyTaskLister

T = TypeVar("T")


@dataclass
class ReadyTask(Generic[T]):
    ready_at: float
    work: T

    def __str__(self) -> str:
        return f"ReadyTask(ready_at={self.ready_at}, work={self.work})"


class ScheduledSource(Generic[T]):
    """Source adapter that wraps a ReadyLister.

    Queries for tasks that are ready at the current time, wraps them
    in ReadyTask. Plugs into PollingTrigger as a Source.
    """

    def __init__(self, lister: ReadyLister[T], parallelism: int) -> None:
        self._lister = lister
        self._parallelism = parallelism

    async def retrieve_tasks(self) -> list[ReadyTask[T]]:
        now = time.time()
        items = await self._lister.list_ready_tasks(now, self._parallelism)
        return [ReadyTask(ready_at=now, work=item) for item in items]
