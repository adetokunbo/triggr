"""Registry and lifecycle manager for a named collection of triggers.

Register triggers by name, then start, monitor, and shut them down
together::

    from triggr import TriggerService, PollingTrigger, PeriodicTrigger, PollingConfig

    config = PollingConfig(polling_interval=30.0, parallelism=4)

    svc = TriggerService()
    svc.register("orders", PollingTrigger(PendingOrderSource(), FulfillmentWorker(), config))
    svc.register("inventory-sync", PeriodicTrigger(InventorySyncWorker(), interval=60.0))
    svc.start_all()

    svc.is_healthy()        # True if all triggers are healthy
    svc.trigger_health()    # {"orders": True, "inventory-sync": True}

Pass ``expected`` to declare exactly which triggers must be registered.
``start_all()`` raises if there is any mismatch — useful for catching
typos or missing registrations at startup::

    svc = TriggerService(expected={"orders", "inventory-sync"})
    svc.register("orders", PollingTrigger(...))
    # forgot to register "inventory-sync"
    svc.start_all()   # raises RuntimeError: expected triggers not registered: {'inventory-sync'}

To shut down gracefully::

    svc.close_all()
"""

from __future__ import annotations

import logging
from typing import Iterator

from .protocols import Trigger

logger = logging.getLogger(__name__)


class TriggerService:
    """Manages a collection of named triggers.

    Handles registration, startup, health aggregation, and shutdown.
    """

    def __init__(
        self,
        expected: set[str] | None = None,
        name: str = "",
    ) -> None:
        self._expected = expected
        self._triggers: dict[str, Trigger] = {}
        self._started = False
        self._logger = logging.getLogger(f"service.{name}" if name else __name__)

    def register(self, name: str, trigger: Trigger) -> None:
        if self._started:
            raise RuntimeError("Cannot register triggers after start_all()")
        if name in self._triggers:
            raise ValueError(f"Trigger '{name}' already registered")
        self._triggers[name] = trigger

    def start_all(self, paused: bool = False) -> None:
        self._validate_expected()
        for name, trigger in self._triggers.items():
            self._logger.info("Starting trigger '%s'", name)
            trigger.run(paused=paused)
        self._started = True

    def is_healthy(self) -> bool:
        return all(t.is_healthy() for t in self._triggers.values())

    def trigger_health(self) -> dict[str, bool]:
        return {name: t.is_healthy() for name, t in self._triggers.items()}

    def pause_all(self) -> None:
        for trigger in self._triggers.values():
            trigger.pause()

    def resume_all(self) -> None:
        for trigger in self._triggers.values():
            trigger.resume()

    def close_all(self) -> None:
        for name, trigger in self._triggers.items():
            self._logger.info("Closing trigger '%s'", name)
            trigger.close()

    def __len__(self) -> int:
        return len(self._triggers)

    def __iter__(self) -> Iterator[str]:
        return iter(self._triggers)

    def __getitem__(self, name: str) -> Trigger:
        return self._triggers[name]

    def _validate_expected(self) -> None:
        if self._expected is None:
            return
        registered = set(self._triggers.keys())
        missing = self._expected - registered
        unexpected = registered - self._expected
        if missing or unexpected:
            msg_parts: list[str] = []
            if missing:
                msg_parts.append(f"expected triggers not registered: {missing}")
            if unexpected:
                msg_parts.append(f"unexpected triggers registered: {unexpected}")
            raise RuntimeError(f"Trigger set mismatch — {'; '.join(msg_parts)}")
