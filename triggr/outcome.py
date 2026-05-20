from __future__ import annotations

from enum import Enum, auto


class Outcome(Enum):
    SUCCESS = auto()
    FAILED = auto()
    NOOP = auto()
    STALE = auto()
