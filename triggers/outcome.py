from __future__ import annotations

from enum import Enum, auto


class TaskOutcome(Enum):
    SUCCESS = auto()
    FAILED = auto()
    NOOP = auto()
    STALE = auto()
