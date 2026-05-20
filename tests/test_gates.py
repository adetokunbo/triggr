from __future__ import annotations

import asyncio
import pytest

from triggr import (
    TriggerConfig,
    CompositeGate,
    EventGate,
    PollingTrigger,
    StreamTrigger,
    PeriodicTask,
    PeriodicTrigger,
    Outcome,
    compose_gates,
)
from .helpers import FixedSource, RecordingWorker, async_iter


class TestEventGate:
    @pytest.mark.asyncio
    async def test_ready_by_default(self):
        gate = EventGate()
        # Should not block
        await asyncio.wait_for(gate.wait_until_ready(), timeout=0.01)

    @pytest.mark.asyncio
    async def test_blocks_when_not_ready(self):
        gate = EventGate()
        gate.set_not_ready()

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(gate.wait_until_ready(), timeout=0.02)

    @pytest.mark.asyncio
    async def test_unblocks_when_set_ready(self):
        gate = EventGate()
        gate.set_not_ready()

        async def set_ready_later():
            await asyncio.sleep(0.02)
            gate.set_ready()

        asyncio.ensure_future(set_ready_later())
        await asyncio.wait_for(gate.wait_until_ready(), timeout=0.1)


class TestCompositeGate:
    @pytest.mark.asyncio
    async def test_ready_when_all_ready(self):
        g1, g2 = EventGate(), EventGate()
        composite = CompositeGate(g1, g2)

        await asyncio.wait_for(composite.wait_until_ready(), timeout=0.01)

    @pytest.mark.asyncio
    async def test_blocks_when_one_not_ready(self):
        g1, g2 = EventGate(), EventGate()
        g2.set_not_ready()
        composite = CompositeGate(g1, g2)

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(composite.wait_until_ready(), timeout=0.02)

    @pytest.mark.asyncio
    async def test_unblocks_when_last_gate_ready(self):
        g1, g2 = EventGate(), EventGate()
        g1.set_not_ready()
        g2.set_not_ready()
        composite = CompositeGate(g1, g2)

        async def ready_later():
            await asyncio.sleep(0.01)
            g1.set_ready()
            await asyncio.sleep(0.01)
            g2.set_ready()

        asyncio.ensure_future(ready_later())
        await asyncio.wait_for(composite.wait_until_ready(), timeout=0.1)

    @pytest.mark.asyncio
    async def test_empty_composite_is_ready(self):
        composite = CompositeGate()
        await asyncio.wait_for(composite.wait_until_ready(), timeout=0.01)


class TestComposeGates:
    @pytest.mark.asyncio
    async def test_combines_callables(self):
        calls: list[str] = []

        async def gate_a():
            calls.append("a")

        async def gate_b():
            calls.append("b")

        combined = compose_gates(gate_a, gate_b)
        await combined()

        assert set(calls) == {"a", "b"}

    @pytest.mark.asyncio
    async def test_blocks_until_both_complete(self):
        g = EventGate()
        g.set_not_ready()

        combined = compose_gates(
            g.wait_until_ready,
            asyncio.Event().wait,  # never ready
        )

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(combined(), timeout=0.02)


class TestTriggerWithReadinessGate:
    @pytest.mark.asyncio
    async def test_polling_trigger_blocks_on_gate(self):
        gate = EventGate()
        gate.set_not_ready()
        config = TriggerConfig(polling_interval=0.01, polling_jitter=0, parallelism=4)
        source = FixedSource(["task"])
        worker = RecordingWorker[str]()

        trigger = PollingTrigger(
            source, worker, config, ready_gate=gate, name="test"
        )
        trigger.run()
        await asyncio.sleep(0.05)

        assert len(worker.completed) == 0

        gate.set_ready()
        await asyncio.sleep(0.05)

        assert len(worker.completed) >= 1
        trigger.close()

    @pytest.mark.asyncio
    async def test_stream_trigger_blocks_on_gate(self):
        gate = EventGate()
        gate.set_not_ready()
        worker = RecordingWorker[str]()

        trigger = StreamTrigger(
            async_iter(["a", "b", "c"]), worker, ready_gate=gate, name="test"
        )
        trigger.run()
        await asyncio.sleep(0.03)

        assert len(worker.completed) == 0

        gate.set_ready()
        await asyncio.sleep(0.03)

        assert len(worker.completed) >= 1
        trigger.close()

    @pytest.mark.asyncio
    async def test_periodic_trigger_blocks_on_gate(self):
        gate = EventGate()
        gate.set_not_ready()
        worker = RecordingWorker[PeriodicTask]()

        trigger = PeriodicTrigger(
            worker, interval=0.01, ready_gate=gate, name="test"
        )
        trigger.run()
        await asyncio.sleep(0.05)

        assert len(worker.completed) == 0

        gate.set_ready()
        await asyncio.sleep(0.05)

        assert len(worker.completed) >= 1
        trigger.close()
