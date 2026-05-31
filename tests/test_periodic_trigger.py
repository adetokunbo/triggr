from __future__ import annotations

import asyncio

import pytest

from triggr import Outcome, PeriodicTask, PeriodicTrigger

from .helpers import RecordingWorker


class TestPeriodicTrigger:
    @pytest.mark.asyncio
    async def test_fires_repeatedly(self):
        worker = RecordingWorker[PeriodicTask]()
        trigger = PeriodicTrigger(worker, interval=0.01, name="test")

        trigger.run()
        await asyncio.sleep(0.05)
        trigger.close()

        assert len(worker.completed) >= 3

    @pytest.mark.asyncio
    async def test_tasks_have_timestamps(self):
        worker = RecordingWorker[PeriodicTask]()
        trigger = PeriodicTrigger(worker, interval=0.01, name="test")

        trigger.run()
        await asyncio.sleep(0.03)
        trigger.close()

        for task in worker.completed:
            assert isinstance(task, PeriodicTask)
            assert task.timestamp > 0

    @pytest.mark.asyncio
    async def test_pause_stops_ticking(self):
        worker = RecordingWorker[PeriodicTask]()
        trigger = PeriodicTrigger(worker, interval=0.01, name="test")

        trigger.run()
        await asyncio.sleep(0.03)
        count_before = len(worker.completed)

        trigger.pause()
        await asyncio.sleep(0.03)
        count_while_paused = len(worker.completed)

        trigger.close()

        assert count_while_paused == count_before or count_while_paused == count_before + 1

    @pytest.mark.asyncio
    async def test_resume_restarts_ticking(self):
        worker = RecordingWorker[PeriodicTask]()
        trigger = PeriodicTrigger(worker, interval=0.01, name="test")

        trigger.run()
        await asyncio.sleep(0.02)
        trigger.pause()
        await asyncio.sleep(0.02)
        count_paused = len(worker.completed)

        trigger.resume()
        await asyncio.sleep(0.03)
        count_resumed = len(worker.completed)

        trigger.close()

        assert count_resumed > count_paused

    @pytest.mark.asyncio
    async def test_is_healthy(self):
        worker = RecordingWorker[PeriodicTask]()
        trigger = PeriodicTrigger(worker, interval=0.01, name="test")

        assert not trigger.is_healthy()
        trigger.run()
        await asyncio.sleep(0.01)
        assert trigger.is_healthy()
        trigger.close()

    @pytest.mark.asyncio
    async def test_run_starts_paused(self):
        worker = RecordingWorker[PeriodicTask]()
        trigger = PeriodicTrigger(worker, interval=0.01, name="test")

        trigger.run(paused=True)
        await asyncio.sleep(0.03)
        assert worker.completed == []

        trigger.resume()
        await asyncio.sleep(0.03)
        trigger.close()
        assert len(worker.completed) >= 1

    @pytest.mark.asyncio
    async def test_failure_does_not_stop_loop(self):
        worker = RecordingWorker[PeriodicTask](outcome=Outcome.FAILED)
        trigger = PeriodicTrigger(worker, interval=0.01, name="test")

        trigger.run()
        await asyncio.sleep(0.05)
        trigger.close()

        assert len(worker.completed) >= 3
