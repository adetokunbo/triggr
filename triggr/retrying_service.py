from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

from .gates import compose_gates
from .lifecycle import Lifecycle
from .polling_loop import PollingLoop
from .protocols import (
    ErrorClassifier,
    ManagedService,
    ReadinessGate,
    TriggerMetrics,
)
from .retry import LONG_RUNNING, RetryPolicy, RetriesExhausted, retry

logger = logging.getLogger(__name__)


class RetryingService:
    """Keeps a long-running service alive with two-level retry.

    Inner level: retries service instantiation with exponential backoff.
    Outer level: if inner retries exhaust, waits `restart_interval` and
    tries again indefinitely.

    Reuses PollingLoop for the outer restart loop and Lifecycle for
    pause/resume.
    """

    def __init__(
        self,
        factory: Callable[[], Awaitable[ManagedService]],
        retry_policy: RetryPolicy = LONG_RUNNING,
        restart_interval: float = 30.0,
        error_classifier: ErrorClassifier | None = None,
        metrics: TriggerMetrics | None = None,
        ready_gate: ReadinessGate | None = None,
        name: str = "",
    ) -> None:
        self._factory = factory
        self._retry_policy = retry_policy
        self._lifecycle = Lifecycle(name)
        self._current: ManagedService | None = None
        self._current_task: asyncio.Task[None] | None = None
        self._name = name
        self._logger = logging.getLogger(f"retrying.{name}" if name else __name__)

        if ready_gate is not None:
            self._gate = compose_gates(
                self._lifecycle.wait_for_not_paused,
                ready_gate.wait_until_ready,
            )
        else:
            self._gate = self._lifecycle.wait_for_not_paused

        self._loop = PollingLoop(
            callback=self._ensure_running,
            lifecycle=self._lifecycle,
            interval=restart_interval,
            jitter=0,
            error_classifier=error_classifier,
            metrics=metrics,
            name=name,
        )

    def run(self, paused: bool = False) -> asyncio.Task[None]:
        if paused:
            self._lifecycle.pause()
        return self._loop.start()

    async def _ensure_running(self) -> bool:
        if self._current is not None and self._current.is_active():
            return False  # still running

        # Service died or never started
        if self._current is not None:
            self._logger.warning("Service completed unexpectedly, restarting")
            self._current.close()
            self._current = None
            self._current_task = None

        await self._gate()
        if self._lifecycle.is_closed:
            return False

        try:
            self._current = await retry(
                self._retry_policy,
                self._factory,
                description=f"instantiate {self._name}",
                between_attempts=self._gate,
            )
            self._current_task = await self._current.start()
            self._logger.info("Service started")
        except RetriesExhausted as e:
            self._logger.error(
                "Failed to instantiate service after retries: %s", e.last_error
            )
            # Return False — outer loop will retry after restart_interval

        return False  # don't loop immediately

    def pause(self) -> None:
        self._lifecycle.pause()

    def resume(self) -> None:
        self._lifecycle.resume()

    def close(self) -> None:
        if self._current is not None:
            self._current.close()
            self._current = None
            self._current_task = None
        self._lifecycle.close()

    def is_healthy(self) -> bool:
        if not self._loop.is_healthy():
            return False
        if self._current is None:
            return True  # hasn't started yet, loop is still running
        return self._current.is_active()
