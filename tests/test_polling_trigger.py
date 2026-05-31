from __future__ import annotations

import asyncio

import pytest

from triggr import Outcome, PollingConfig, PollingTrigger

from .helpers import FixedSource, RecordingWorker


@pytest.fixture
def config():
    return PollingConfig(polling_interval=0.01, polling_jitter=0, parallelism=4)


class TestPollingTrigger:
    @pytest.mark.asyncio
    async def test_processes_all_tasks(self, config):
        source = FixedSource(["a", "b", "c"], once=True)
        worker = RecordingWorker[str]()
        trigger = PollingTrigger(source, worker, config, name="test")
        trigger.pause()

        result = await trigger.run_once()

        assert result is True
        assert worker.completed == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_returns_false_when_no_tasks(self, config):
        source = FixedSource[str]([], once=True)
        worker = RecordingWorker[str]()
        trigger = PollingTrigger(source, worker, config, name="test")
        trigger.pause()

        result = await trigger.run_once()

        assert result is False
        assert worker.completed == []

    @pytest.mark.asyncio
    async def test_polls_repeatedly_until_closed(self, config):
        source = FixedSource(["x"])
        worker = RecordingWorker[str]()
        trigger = PollingTrigger(source, worker, config, name="test")

        trigger.run()
        await asyncio.sleep(0.05)
        trigger.close()

        assert len(worker.completed) >= 2

    @pytest.mark.asyncio
    async def test_pause_and_resume(self, config):
        source = FixedSource(["task"])
        worker = RecordingWorker[str]()
        trigger = PollingTrigger(source, worker, config, name="test")

        trigger.run()
        await asyncio.sleep(0.03)
        count_before = len(worker.completed)

        trigger.pause()
        await asyncio.sleep(0.03)
        count_while_paused = len(worker.completed)

        trigger.resume()
        await asyncio.sleep(0.03)
        count_after = len(worker.completed)

        trigger.close()

        assert count_while_paused == count_before or count_while_paused == count_before + 1
        assert count_after > count_while_paused

    @pytest.mark.asyncio
    async def test_failed_tasks_do_not_stop_loop(self, config):
        source = FixedSource(["ok", "fail"])
        fail_worker = RecordingWorker[str](outcome=Outcome.FAILED)
        trigger = PollingTrigger(source, fail_worker, config, name="test")

        trigger.run()
        await asyncio.sleep(0.03)
        trigger.close()

        assert len(fail_worker.completed) >= 2

    @pytest.mark.asyncio
    async def test_respects_parallelism(self, config):
        config.parallelism = 2
        tasks = ["a", "b", "c", "d", "e"]
        source = FixedSource(tasks, once=True)
        worker = RecordingWorker[str]()
        trigger = PollingTrigger(source, worker, config, name="test")
        trigger.pause()

        await trigger.run_once()

        assert set(worker.completed) == set(tasks)

    @pytest.mark.asyncio
    async def test_sliding_window_fills_slot_immediately(self):
        """A free parallelism slot is filled as soon as any task finishes,
        not after a full batch."""
        config = PollingConfig(polling_interval=0.01, polling_jitter=0, parallelism=2)
        loop = asyncio.get_event_loop()
        t0 = loop.time()
        marker_started_at: list[float] = []

        class Worker:
            async def complete(self, task: str) -> Outcome:
                if task == "slow":
                    await asyncio.sleep(0.15)
                elif task == "fast":
                    await asyncio.sleep(0.01)
                else:
                    marker_started_at.append(loop.time() - t0)
                return Outcome.SUCCESS

            async def is_stale(self, task: str) -> bool:
                return False

        # slow+fast start together; fast finishes at ~0.01s and frees a slot;
        # marker fills it immediately rather than waiting for slow (~0.15s).
        source = FixedSource(["slow", "fast", "marker"], once=True)
        trigger = PollingTrigger(source, Worker(), config)
        trigger.pause()
        await trigger.run_once()

        assert len(marker_started_at) == 1
        assert marker_started_at[0] < 0.1  # started well before slow would finish

    @pytest.mark.asyncio
    async def test_is_healthy_while_running(self, config):
        source = FixedSource(["x"])
        worker = RecordingWorker[str]()
        trigger = PollingTrigger(source, worker, config, name="test")

        assert not trigger.is_healthy()
        trigger.run()
        await asyncio.sleep(0.01)
        assert trigger.is_healthy()
        trigger.close()

    @pytest.mark.asyncio
    async def test_is_healthy_before_first_completion(self, config):
        source = FixedSource(["x"])
        worker = RecordingWorker[str]()
        trigger = PollingTrigger(source, worker, config, name="test")
        trigger.run()
        assert trigger.is_healthy()
        trigger.close()

    @pytest.mark.asyncio
    async def test_run_starts_paused(self, config):
        source = FixedSource(["task"])
        worker = RecordingWorker[str]()
        trigger = PollingTrigger(source, worker, config, name="test")

        trigger.run(paused=True)
        await asyncio.sleep(0.03)
        assert worker.completed == []

        trigger.resume()
        await asyncio.sleep(0.03)
        trigger.close()
        assert len(worker.completed) >= 1
