"""TriggerService: registry and lifecycle manager for named triggers.

Handles registration, startup (optionally paused), health aggregation,
pause/resume, and shutdown of a collection of triggers. Optionally
validates that a declared set of triggers is registered before starting.
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
        if missing or unexpected:
            msg_parts = []
            if missing:
                msg_parts.append(f"expected triggers not registered: {missing}")
            if unexpected:
                msg_parts.append(f"unexpected triggers registered: {unexpected}")
            raise RuntimeError(f"Trigger set mismatch — {'; '.join(msg_parts)}")
