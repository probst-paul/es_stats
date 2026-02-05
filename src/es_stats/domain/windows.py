from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

MINUTES_PER_DAY = 24 * 60


class WindowAnchor(StrEnum):
    TRADING_DATE_CT = "TRADING_DATE_CT"


class WindowOrderRule(StrEnum):
    ANY = "ANY"
    Y_ENDS_BEFORE_X_START = "Y_ENDS_BEFORE_X_START"


@dataclass(frozen=True)
class WindowSpec:
    """
    Minute-granularity window for a trading date.

    Semantics are inclusive at both bounds:
    - start_minute_ct and end_minute_ct are in [0, 1439]
    - a non-wrapping window covers [start, end]
    - a wrapping window (start > end) crosses midnight
    """

    anchor: WindowAnchor
    start_minute_ct: int
    end_minute_ct: int
    name: str | None = None

    def __post_init__(self) -> None:
        if self.anchor != WindowAnchor.TRADING_DATE_CT:
            raise ValueError(f"Unsupported anchor: {self.anchor!r}")
        if not (0 <= self.start_minute_ct < MINUTES_PER_DAY):
            raise ValueError("start_minute_ct must be in [0, 1439]")
        if not (0 <= self.end_minute_ct < MINUTES_PER_DAY):
            raise ValueError("end_minute_ct must be in [0, 1439]")

    @property
    def spans_midnight(self) -> bool:
        return self.start_minute_ct > self.end_minute_ct

    @property
    def duration_minutes(self) -> int:
        if self.spans_midnight:
            return (
                MINUTES_PER_DAY - self.start_minute_ct
            ) + self.end_minute_ct + 1
        return self.end_minute_ct - self.start_minute_ct + 1

    def covered_minutes(self) -> set[int]:
        if not self.spans_midnight:
            return set(range(self.start_minute_ct, self.end_minute_ct + 1))
        return set(range(self.start_minute_ct, MINUTES_PER_DAY)) | set(
            range(0, self.end_minute_ct + 1)
        )


def _interval_for_ordering(window: WindowSpec) -> tuple[int, int]:
    """
    Convert to trading-date-relative interval for ordering checks.

    Wrapping windows are represented as the prior-evening segment to same-day end,
    e.g. 17:00-08:29 becomes [-420, 509].
    """
    if window.spans_midnight:
        return (window.start_minute_ct - MINUTES_PER_DAY, window.end_minute_ct)
    return (window.start_minute_ct, window.end_minute_ct)


def validate_pair(x: WindowSpec, y: WindowSpec, rule: WindowOrderRule) -> None:
    if x.anchor != y.anchor:
        raise ValueError(f"Window anchors must match, got {x.anchor} and {y.anchor}")

    if rule == WindowOrderRule.ANY:
        return

    x_minutes = x.covered_minutes()
    y_minutes = y.covered_minutes()
    if x_minutes & y_minutes:
        raise ValueError("X and Y windows overlap, but rule requires strict ordering")

    if rule == WindowOrderRule.Y_ENDS_BEFORE_X_START:
        x_start, _ = _interval_for_ordering(x)
        _, y_end = _interval_for_ordering(y)
        if y_end >= x_start:
            raise ValueError(
                "Window order invalid: Y must complete before X begins "
                f"(y_end={y_end}, x_start={x_start})"
            )
        return

    raise ValueError(f"Unsupported window order rule: {rule!r}")
