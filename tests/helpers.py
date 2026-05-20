from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import AsyncIterator, Generic, TypeVar

from triggr.outcome import Outcome

T = TypeVar("T")


class RecordingWorker(Generic[T]):
    """Worker that records calls and returns configurable outcomes."""

    def __init__(
        self,
        outcome: Outcome = Outcome.SUCCESS,
        stale: bool = False,
    ) -> None:
        self.outcome = outcome
        self.stale = stale
        self.completed: list[T] = []
        self.stale_checks: list[T] = []
        self.fail_next: Exception | None = None

    async def complete(self, task: T) -> Outcome:
        if self.fail_next is not None:
            exc = self.fail_next
            self.fail_next = None
            raise exc
        self.completed.append(task)
        return self.outcome

    async def is_stale(self, task: T) -> bool:
        self.stale_checks.append(task)
        return self.stale


class FixedSource(Generic[T]):
    """Source that returns a fixed list, optionally only once."""

    def __init__(self, tasks: list[T], once: bool = False) -> None:
        self._tasks = tasks
        self._once = once
        self._returned = False
        self.call_count = 0

    async def retrieve(self) -> list[T]:
        self.call_count += 1
        if self._once and self._returned:
            return []
        self._returned = True
        return list(self._tasks)


async def async_iter(items: list[T]) -> AsyncIterator[T]:
    for item in items:
        yield item
