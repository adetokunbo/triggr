from __future__ import annotations

import asyncio
import logging
import pytest

from triggr import (
    AutomationConfig,
    AutomationService,
    PeriodicTask,
    PeriodicTrigger,
    PollingTaskTrigger,
    TaskOutcome,
)
from .helpers import FixedSource, RecordingWorker


@pytest.fixture
def config():
    return AutomationConfig(polling_interval=0.01, polling_jitter=0, parallelism=4)


class FakeTrigger:
    """Minimal trigger for testing the service without real async work."""

    def __init__(self) -> None:
        self.running = False
        self.paused = False
        self.closed = False

    def run(self, paused: bool = False) -> None:
        self.running = True
        self.paused = paused

    def pause(self) -> None:
        self.paused = True

    def resume(self) -> None:
        self.paused = False

    def close(self) -> None:
        self.closed = True
        self.running = False

    def is_healthy(self) -> bool:
        return self.running and not self.closed


class TestRegistration:
    def test_register_and_iterate(self):
        svc = AutomationService()
        t1, t2 = FakeTrigger(), FakeTrigger()
        svc.register("a", t1)
        svc.register("b", t2)

        assert len(svc) == 2
        assert set(svc) == {"a", "b"}
        assert svc["a"] is t1

    def test_duplicate_name_raises(self):
        svc = AutomationService()
        svc.register("a", FakeTrigger())

        with pytest.raises(ValueError, match="already registered"):
            svc.register("a", FakeTrigger())

    def test_register_after_start_raises(self):
        svc = AutomationService()
        svc.register("a", FakeTrigger())
        svc.start_all()

        with pytest.raises(RuntimeError, match="after start_all"):
            svc.register("b", FakeTrigger())


class TestStartAll:
    def test_starts_all_triggers(self):
        svc = AutomationService()
        t1, t2 = FakeTrigger(), FakeTrigger()
        svc.register("a", t1)
        svc.register("b", t2)

        svc.start_all()

        assert t1.running
        assert t2.running

    def test_starts_paused(self):
        svc = AutomationService()
        t = FakeTrigger()
        svc.register("a", t)

        svc.start_all(paused=True)

        assert t.running
        assert t.paused


class TestHealth:
    def test_healthy_when_all_healthy(self):
        svc = AutomationService()
        t1, t2 = FakeTrigger(), FakeTrigger()
        svc.register("a", t1)
        svc.register("b", t2)
        svc.start_all()

        assert svc.is_healthy()

    def test_unhealthy_when_one_closed(self):
        svc = AutomationService()
        t1, t2 = FakeTrigger(), FakeTrigger()
        svc.register("a", t1)
        svc.register("b", t2)
        svc.start_all()

        t1.close()

        assert not svc.is_healthy()

    def test_trigger_health_by_name(self):
        svc = AutomationService()
        t1, t2 = FakeTrigger(), FakeTrigger()
        svc.register("a", t1)
        svc.register("b", t2)
        svc.start_all()

        t2.close()
        health = svc.trigger_health()

        assert health == {"a": True, "b": False}


class TestPauseResume:
    def test_pause_all(self):
        svc = AutomationService()
        t1, t2 = FakeTrigger(), FakeTrigger()
        svc.register("a", t1)
        svc.register("b", t2)
        svc.start_all()

        svc.pause_all()

        assert t1.paused
        assert t2.paused

    def test_resume_all(self):
        svc = AutomationService()
        t1, t2 = FakeTrigger(), FakeTrigger()
        svc.register("a", t1)
        svc.register("b", t2)
        svc.start_all()
        svc.pause_all()

        svc.resume_all()

        assert not t1.paused
        assert not t2.paused


class TestCloseAll:
    def test_closes_all_triggers(self):
        svc = AutomationService()
        t1, t2 = FakeTrigger(), FakeTrigger()
        svc.register("a", t1)
        svc.register("b", t2)
        svc.start_all()

        svc.close_all()

        assert t1.closed
        assert t2.closed


class TestExpectedValidation:
    def test_warns_on_missing(self, caplog):
        svc = AutomationService(expected={"a", "b", "c"})
        svc.register("a", FakeTrigger())

        with caplog.at_level(logging.WARNING):
            svc.start_all()

        assert "not registered" in caplog.text
        assert "b" in caplog.text or "c" in caplog.text

    def test_warns_on_unexpected(self, caplog):
        svc = AutomationService(expected={"a"})
        svc.register("a", FakeTrigger())
        svc.register("extra", FakeTrigger())

        with caplog.at_level(logging.WARNING):
            svc.start_all()

        assert "Unexpected" in caplog.text
        assert "extra" in caplog.text

    def test_no_warning_when_matched(self, caplog):
        svc = AutomationService(expected={"a", "b"})
        svc.register("a", FakeTrigger())
        svc.register("b", FakeTrigger())

        with caplog.at_level(logging.WARNING):
            svc.start_all()

        assert "not registered" not in caplog.text
        assert "Unexpected" not in caplog.text

    def test_raises_on_missing_when_strict(self):
        svc = AutomationService(expected={"a", "b"}, strict=True)
        svc.register("a", FakeTrigger())

        with pytest.raises(RuntimeError, match="Trigger set mismatch"):
            svc.start_all()

    def test_raises_on_unexpected_when_strict(self):
        svc = AutomationService(expected={"a"}, strict=True)
        svc.register("a", FakeTrigger())
        svc.register("extra", FakeTrigger())

        with pytest.raises(RuntimeError, match="Trigger set mismatch"):
            svc.start_all()

    def test_no_validation_without_expected(self, caplog):
        svc = AutomationService()
        svc.register("anything", FakeTrigger())

        with caplog.at_level(logging.WARNING):
            svc.start_all()

        assert caplog.text == "" or "not registered" not in caplog.text


class TestWithRealTriggers:
    @pytest.mark.asyncio
    async def test_manages_real_triggers(self, config):
        svc = AutomationService(expected={"poller", "periodic"})

        source = FixedSource(["task"])
        worker1 = RecordingWorker[str]()
        poller = PollingTaskTrigger(source, worker1, config, name="poller")

        worker2 = RecordingWorker[PeriodicTask]()
        periodic = PeriodicTrigger(worker2, interval=0.01, name="periodic")

        svc.register("poller", poller)
        svc.register("periodic", periodic)

        svc.start_all()
        await asyncio.sleep(0.05)

        assert svc.is_healthy()
        assert len(worker1.completed) >= 1
        assert len(worker2.completed) >= 1

        svc.close_all()
