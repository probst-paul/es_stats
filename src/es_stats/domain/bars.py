from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class RawBar:
    """
    In-memory representation of a single 1-minute bar parsed from CSV.

    Note: timestamp here is still a datetime parsed from the CSV (timezone interpretation happens in Phase 3.3).
    """
    dt: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
