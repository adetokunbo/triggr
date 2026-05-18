from __future__ import annotations

import logging
from typing import Iterator

from .protocols import Trigger

logger = logging.getLogger(__name__)


class AutomationService:
    """Manages a collection of named triggers.

    Handles registration, startup, health aggregation, and shutdown.
    Optionally validates that all expected triggers are registered.
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
        for name, trigger in self._triggers.items():
            trigger.pause()

    def resume_all(self) -> None:
        for name, trigger in self._triggers.items():
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
        if missing:
            self._logger.warning("Expected triggers not registered: %s", missing)
        if unexpected:
            self._logger.warning("Unexpected triggers registered: %s", unexpected)
