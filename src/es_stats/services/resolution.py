from __future__ import annotations

from typing import Literal

from es_stats.domain.windows import WindowSpec

Resolution = Literal["1m", "30m"]


def _can_use_30m(window: WindowSpec) -> bool:
    """
    A window can be represented exactly with 30m bars iff:
    - it starts on a half-hour boundary (minute % 30 == 0)
    - it ends on the last minute of a half-hour bucket ((minute + 1) % 30 == 0)
    """
    starts_on_half_hour = window.start_minute_ct % 30 == 0
    ends_on_bucket_end = (window.end_minute_ct + 1) % 30 == 0
    return starts_on_half_hour and ends_on_bucket_end


def choose_resolution(windows: list[WindowSpec]) -> Resolution:
    """
    Pick a single bar resolution for an analysis run.

    Rule:
    - Use 30m only if every window can be satisfied exactly by 30m buckets.
    - Otherwise use 1m for the entire run.
    """
    if not windows:
        raise ValueError("choose_resolution() requires at least one window")

    return "30m" if all(_can_use_30m(w) for w in windows) else "1m"
