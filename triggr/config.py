"""Configuration dataclasses for triggr components."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class AutomationConfig:
    polling_interval: float = 30.0
    polling_jitter: float = 0.2
    parallelism: int = 4
    max_num_silent_polling_retries: int = 3
