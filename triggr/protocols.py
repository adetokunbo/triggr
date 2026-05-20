"""User-provided protocols and default implementations.

Defines the interfaces callers implement to plug work into the framework:
Source, Worker, ReadyLister, ReadinessGate, ErrorClassifier,
TriggerMetrics, and ManagedService. Also provides NoOpMetrics and
AllTransient as zero-effort defaults.
"""

from __future__ import annotations

import asyncio
from enum import Enum, auto
from typing import AsyncIterator, Generic, Protocol, TypeVar, runtime_checkable

from .outcome import Outcome

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


class ErrorKind(Enum):
    TRANSIENT = auto()
    FATAL = auto()


class ErrorClassifier(Protocol):
    """Classifies exceptions as transient (retry) or fatal (don't retry)."""

    def classify(self, error: Exception) -> ErrorKind: ...


class AllTransient:
    """Default classifier — all errors are transient."""

    def classify(self, error: Exception) -> ErrorKind:
        return ErrorKind.TRANSIENT


@runtime_checkable
class HasHealth(Protocol):
    def is_healthy(self) -> bool: ...


class Worker(Protocol[T]):
    """User-provided logic for completing and staleness-checking tasks."""

    async def complete(self, task: T) -> Outcome: ...

    async def is_stale(self, task: T) -> bool: ...


class Source(Protocol[T_co]):
    """User-provided logic for retrieving tasks to process."""

    async def retrieve(self) -> list[T_co]: ...


class ReadyLister(Protocol[T_co]):
    """User-provided logic for listing time-ready tasks."""

    async def list_ready(self, now: float, limit: int) -> list[T_co]: ...


class Trigger(Protocol):
    """Common interface for all trigger types."""

    def run(self, paused: bool = False) -> None: ...

    def pause(self) -> None: ...

    def resume(self) -> None: ...

    def close(self) -> None: ...

    def is_healthy(self) -> bool: ...


class ManagedService(Protocol):
    """A long-running service that can be started, monitored, and closed."""

    async def start(self) -> asyncio.Task[None]: ...

    def close(self) -> None: ...

    def is_active(self) -> bool: ...


class ReadinessGate(Protocol):
    """A condition that must be met before work can proceed."""

    async def wait_until_ready(self) -> None: ...


class TriggerMetrics(Protocol):
    """Records metrics for trigger components."""

    def record_iteration(self, duration: float) -> None: ...

    def record_outcome(self, outcome: Outcome, duration: float) -> None: ...

    def record_error(self, error: Exception) -> None: ...


class NoOpMetrics:
    """Default metrics — discards everything."""

    def record_iteration(self, duration: float) -> None:
        pass

    def record_outcome(self, outcome: Outcome, duration: float) -> None:
        pass

    def record_error(self, error: Exception) -> None:
        pass
