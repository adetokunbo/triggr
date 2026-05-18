from __future__ import annotations

from enum import Enum, auto
from typing import AsyncIterator, Generic, Protocol, TypeVar, runtime_checkable

from .outcome import TaskOutcome

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


class TaskWorker(Protocol[T]):
    """User-provided logic for completing and staleness-checking tasks."""

    async def complete_task(self, task: T) -> TaskOutcome: ...

    async def is_stale_task(self, task: T) -> bool: ...


class TaskSource(Protocol[T_co]):
    """User-provided logic for retrieving tasks to process."""

    async def retrieve_tasks(self) -> list[T_co]: ...


class ReadyTaskLister(Protocol[T_co]):
    """User-provided logic for listing time-ready tasks."""

    async def list_ready_tasks(self, now: float, limit: int) -> list[T_co]: ...
