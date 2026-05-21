"""triggr — composable async trigger framework.

Provides building blocks for polling loops, task processing, stream-based
triggers, and long-running service management. All built on asyncio with
no external runtime dependencies.

Typical usage::

    from triggr import PollingTrigger, PollingConfig, Outcome

    class MySource:
        async def retrieve(self) -> list[str]:
            return ["work-item"]

    class MyWorker:
        async def complete(self, task: str) -> Outcome:
            print(f"processing {task}")
            return Outcome.SUCCESS

        async def is_stale(self, task: str) -> bool:
            return False

    config = PollingConfig(polling_interval=5.0)
    trigger = PollingTrigger(MySource(), MyWorker(), config)
    trigger.run()
"""

from .config import PollingConfig
from .lifecycle import Lifecycle
from .outcome import Outcome
from .polling_loop import PollingLoop
from .processor import Processor
from .protocols import (
    TransientErrors,
    ErrorClassifier,
    ErrorKind,
    HasHealth,
    ManagedService,
    NoOpMetrics,
    ReadinessGate,
    ReadyLister,
    Source,
    Worker,
    TriggerMetrics,
)
from .gates import CompositeGate, EventGate, compose_gates
from .retry import DEFAULT, LONG_RUNNING, RetryPolicy
from .scheduled import ReadyTask, ScheduledSource
from .retrying_service import RetryingService
from .trigger_service import TriggerService
from .triggers import PeriodicTask, PeriodicTrigger, PollingTrigger, StreamTrigger

__all__ = [
    "PollingConfig",
    "Lifecycle",
    "Outcome",
    "PollingLoop",
    "Processor",
    "TransientErrors",
    "ErrorClassifier",
    "ErrorKind",
    "CompositeGate",
    "EventGate",
    "compose_gates",
    "HasHealth",
    "ReadinessGate",
    "NoOpMetrics",
    "TriggerMetrics",
    "ReadyLister",
    "Source",
    "Worker",
    "RetryPolicy",
    "DEFAULT",
    "LONG_RUNNING",
    "ReadyTask",
    "ScheduledSource",
    "PeriodicTask",
    "PeriodicTrigger",
    "PollingTrigger",
    "ManagedService",
    "RetryingService",
    "TriggerService",
    "StreamTrigger",
]
