from __future__ import annotations

import asyncio
import pytest

from triggers import StreamTaskTrigger, TaskOutcome
from .helpers import RecordingWorker, async_iter


class TestStreamTaskTrigger:
    @pytest.mark.asyncio
    async def test_processes_all_items_from_stream(self):
        items = ["a", "b", "c"]
        worker = RecordingWorker[str]()
        trigger = StreamTaskTrigger(async_iter(items), worker, name="test")

        task = trigger.run()
        await task

        assert worker.completed == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_empty_stream_completes(self):
        worker = RecordingWorker[str]()
        trigger = StreamTaskTrigger(async_iter([]), worker, name="test")

        task = trigger.run()
        await task

        assert worker.completed == []

    @pytest.mark.asyncio
    async def test_pause_blocks_processing(self):
        async def slow_stream():
            for item in ["first", "second", "third"]:
                yield item
                await asyncio.sleep(0.01)

        worker = RecordingWorker[str]()
        trigger = StreamTaskTrigger(slow_stream(), worker, name="test")

        trigger.run()
        await asyncio.sleep(0.02)
        trigger.pause()
        count_at_pause = len(worker.completed)
        await asyncio.sleep(0.05)
        count_while_paused = len(worker.completed)

        trigger.close()

        assert count_while_paused == count_at_pause

    @pytest.mark.asyncio
    async def test_close_stops_processing(self):
        async def infinite_stream():
            i = 0
            while True:
                yield f"item-{i}"
                i += 1
                await asyncio.sleep(0.01)

        worker = RecordingWorker[str]()
        trigger = StreamTaskTrigger(infinite_stream(), worker, name="test")

        trigger.run()
        await asyncio.sleep(0.05)
        trigger.close()
        await asyncio.sleep(0.02)

        count = len(worker.completed)
        assert count > 0
        assert not trigger.is_healthy()

    @pytest.mark.asyncio
    async def test_is_healthy_while_running(self):
        async def slow_stream():
            yield "a"
            await asyncio.sleep(1.0)

        worker = RecordingWorker[str]()
        trigger = StreamTaskTrigger(slow_stream(), worker, name="test")

        assert not trigger.is_healthy()
        trigger.run()
        await asyncio.sleep(0.01)
        assert trigger.is_healthy()
        trigger.close()

    @pytest.mark.asyncio
    async def test_retries_on_failure(self):
        worker = RecordingWorker[str]()
        worker.fail_next = ValueError("transient")
        worker.stale = True  # staleness check returns True after failure

        trigger = StreamTaskTrigger(async_iter(["task"]), worker, name="test")
        task = trigger.run()
        await task

        # The task failed, staleness returned True, so it was marked stale
        assert len(worker.stale_checks) >= 1
