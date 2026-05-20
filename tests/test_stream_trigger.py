from __future__ import annotations

import asyncio
import time
import pytest

from triggr import StreamTrigger, Outcome
from .helpers import RecordingWorker, async_iter


class TestStreamTrigger:
    @pytest.mark.asyncio
    async def test_processes_all_items_from_stream(self):
        items = ["a", "b", "c"]
        worker = RecordingWorker[str]()
        trigger = StreamTrigger(async_iter(items), worker, name="test")

        task = trigger.run()
        await task

        assert worker.completed == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_empty_stream_completes(self):
        worker = RecordingWorker[str]()
        trigger = StreamTrigger(async_iter([]), worker, name="test")

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
        trigger = StreamTrigger(slow_stream(), worker, name="test")

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
        trigger = StreamTrigger(infinite_stream(), worker, name="test")

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
        trigger = StreamTrigger(slow_stream(), worker, name="test")

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

        trigger = StreamTrigger(async_iter(["task"]), worker, name="test")
        task = trigger.run()
        await task

        # The task failed, staleness returned True, so it was marked stale
        assert len(worker.stale_checks) >= 1


class TestStreamConcurrency:
    @pytest.mark.asyncio
    async def test_concurrent_execution_faster_than_sequential(self):
        class SlowWorker:
            def __init__(self) -> None:
                self.completed: list[str] = []

            async def complete(self, task: str) -> Outcome:
                await asyncio.sleep(0.05)
                self.completed.append(task)
                return Outcome.SUCCESS

            async def is_stale(self, task: str) -> bool:
                return False

        items = ["a", "b", "c", "d", "e"]
        worker = SlowWorker()
        trigger = StreamTrigger(
            async_iter(items), worker, parallelism=3, name="test"
        )

        t0 = time.monotonic()
        task = trigger.run()
        await task
        elapsed = time.monotonic() - t0

        assert set(worker.completed) == set(items)
        # Sequential would be ~0.25s (5 * 0.05). With parallelism=3,
        # should be ~0.1s (ceil(5/3) * 0.05). Allow margin.
        assert elapsed < 0.20

    @pytest.mark.asyncio
    async def test_bounds_concurrency(self):
        max_concurrent = 0
        current = 0
        lock = asyncio.Lock()

        class TrackingWorker:
            async def complete(self, task: str) -> Outcome:
                nonlocal max_concurrent, current
                async with lock:
                    current += 1
                    if current > max_concurrent:
                        max_concurrent = current
                await asyncio.sleep(0.03)
                async with lock:
                    current -= 1
                return Outcome.SUCCESS

            async def is_stale(self, task: str) -> bool:
                return False

        items = [f"t{i}" for i in range(8)]
        trigger = StreamTrigger(
            async_iter(items), TrackingWorker(), parallelism=3, name="test"
        )

        task = trigger.run()
        await task

        assert max_concurrent <= 3

    @pytest.mark.asyncio
    async def test_close_cancels_inflight(self):
        class SlowWorker:
            def __init__(self) -> None:
                self.started = 0

            async def complete(self, task: str) -> Outcome:
                self.started += 1
                await asyncio.sleep(10.0)
                return Outcome.SUCCESS

            async def is_stale(self, task: str) -> bool:
                return False

        async def infinite_stream():
            i = 0
            while True:
                yield f"item-{i}"
                i += 1

        worker = SlowWorker()
        trigger = StreamTrigger(
            infinite_stream(), worker, parallelism=3, name="test"
        )

        trigger.run()
        await asyncio.sleep(0.05)
        assert worker.started >= 1
        trigger.close()
        await asyncio.sleep(0.02)

        assert not trigger.is_healthy()

    @pytest.mark.asyncio
    async def test_parallelism_1_is_sequential(self):
        order: list[str] = []

        class OrderTrackingWorker:
            async def complete(self, task: str) -> Outcome:
                order.append(f"start-{task}")
                await asyncio.sleep(0.01)
                order.append(f"end-{task}")
                return Outcome.SUCCESS

            async def is_stale(self, task: str) -> bool:
                return False

        items = ["a", "b", "c"]
        trigger = StreamTrigger(
            async_iter(items), OrderTrackingWorker(), parallelism=1, name="test"
        )

        task = trigger.run()
        await task

        # Sequential: each start-end pair is contiguous
        assert order == [
            "start-a", "end-a",
            "start-b", "end-b",
            "start-c", "end-c",
        ]
