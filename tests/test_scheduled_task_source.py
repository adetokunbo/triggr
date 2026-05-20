from __future__ import annotations

import pytest

from triggr import ReadyTask, ScheduledTaskSource


class FixedLister:
    def __init__(self, items: list[str]) -> None:
        self._items = items
        self.call_count = 0

    async def list_ready_tasks(self, now: float, limit: int) -> list[str]:
        self.call_count += 1
        return self._items[:limit]


class TestScheduledTaskSource:
    @pytest.mark.asyncio
    async def test_wraps_items_in_ready_task(self):
        lister = FixedLister(["a", "b", "c"])
        source = ScheduledTaskSource(lister, parallelism=10)

        tasks = await source.retrieve_tasks()

        assert len(tasks) == 3
        assert all(isinstance(t, ReadyTask) for t in tasks)
        assert [t.work for t in tasks] == ["a", "b", "c"]
        assert all(t.ready_at > 0 for t in tasks)

    @pytest.mark.asyncio
    async def test_respects_parallelism_as_limit(self):
        lister = FixedLister(["a", "b", "c", "d", "e"])
        source = ScheduledTaskSource(lister, parallelism=2)

        tasks = await source.retrieve_tasks()

        assert len(tasks) == 2
        assert [t.work for t in tasks] == ["a", "b"]

    @pytest.mark.asyncio
    async def test_empty_lister(self):
        lister = FixedLister([])
        source = ScheduledTaskSource(lister, parallelism=10)

        tasks = await source.retrieve_tasks()

        assert tasks == []
        assert lister.call_count == 1
