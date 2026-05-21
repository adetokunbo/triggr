"""User-provided protocols and default implementations.

The three core protocols to implement are ``Worker``, ``Source``, and
``ReadyLister``. The rest are optional hooks for observability and control.

``Worker`` defines what to do with each task::

    from triggr import Worker, Outcome

    class FulfillmentWorker:
        async def complete(self, order: Order) -> Outcome:
            if await warehouse.ship(order):
                return Outcome.SUCCESS
            return Outcome.FAILED

        async def is_stale(self, order: Order) -> bool:
            return await db.is_cancelled(order.id)

``Source`` defines where tasks come from::

    from triggr import Source

    class PendingOrderSource:
        async def retrieve(self) -> list[Order]:
            return await db.fetch_pending_orders(limit=50)

``ReadyLister`` is for time-based work — tasks that become ready at a
scheduled time. The framework passes the current Unix timestamp as ``now``
and the configured parallelism as ``limit``; the lister should return only
tasks whose scheduled time has passed, up to ``limit`` items::

    from triggr import ReadyLister, ScheduledSource, PollingTrigger, PollingConfig

    class ScheduledShipmentLister:
        async def list_ready(self, now: float, limit: int) -> list[Shipment]:
            # dispatch_at is a Unix timestamp set when the shipment was scheduled
            return await db.fetch_shipments(dispatch_before=now, limit=limit)

    source = ScheduledSource(lister=ScheduledShipmentLister(), parallelism=4)
    trigger = PollingTrigger(source, FulfillmentWorker(), PollingConfig())

To classify errors as transient (retry) or fatal (stop retrying)::

    from triggr import ErrorClassifier, ErrorKind

    class PaymentGatewayClassifier:
        def classify(self, error: Exception) -> ErrorKind:
            if isinstance(error, GatewayTimeoutError):
                return ErrorKind.TRANSIENT
            return ErrorKind.FATAL

``TransientErrors`` and ``NoOpMetrics`` are zero-effort defaults used
when no classifier or metrics implementation is provided.
"""

from __future__ import annotations

import asyncio
from enum import Enum, auto
from typing import Protocol, TypeVar, runtime_checkable

T = TypeVar("T")
T_contra = TypeVar("T_contra", contravariant=True)


class Outcome(Enum):
    """Outcome of a single task attempt, returned by ``Worker.complete``.

    Four values cover the cases a worker needs to signal::

        class FulfillmentWorker:
            async def complete(self, order: Order) -> Outcome:
                if order.is_already_shipped():
                    return Outcome.NOOP       # nothing to do; not an error
                if not await warehouse.ship(order):
                    return Outcome.FAILED     # failed; will be retried
                return Outcome.SUCCESS        # done

    ``STALE`` is returned by ``Processor`` when ``Worker.is_stale`` returns
    ``True`` between retry attempts — the worker does not return it directly.

    ``Processor`` treats ``SUCCESS`` and ``STALE`` as positive completions
    and ``FAILED`` and ``NOOP`` as non-completions that do not count toward
    progress.
    """

    SUCCESS = auto()
    FAILED = auto()
    NOOP = auto()
    STALE = auto()


class ErrorKind(Enum):
    TRANSIENT = auto()
    FATAL = auto()


class ErrorClassifier(Protocol):
    """Classifies exceptions as transient (retry) or fatal (don't retry)."""

    def classify(self, error: Exception) -> ErrorKind: ...


class TransientErrors:
    """Default classifier — all errors are transient."""

    def classify(self, error: Exception) -> ErrorKind:
        return ErrorKind.TRANSIENT


@runtime_checkable
class HasHealth(Protocol):
    def is_healthy(self) -> bool: ...


class Worker(Protocol[T_contra]):
    """User-provided logic for completing and staleness-checking tasks."""

    async def complete(self, task: T_contra, /) -> Outcome: ...

    async def is_stale(self, task: T_contra, /) -> bool: ...


class Source(Protocol[T]):
    """User-provided logic for retrieving tasks to process."""

    async def retrieve(self) -> list[T]: ...


class ReadyLister(Protocol[T]):
    """User-provided logic for listing time-ready tasks."""

    async def list_ready(self, now: float, limit: int) -> list[T]: ...


class Trigger(Protocol):
    """Common interface for all trigger types."""

    def run(self, paused: bool = False) -> asyncio.Task[None]: ...

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
