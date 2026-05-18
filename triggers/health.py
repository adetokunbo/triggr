from __future__ import annotations

from typing import Protocol


class HasHealth(Protocol):
    def is_healthy(self) -> bool: ...
