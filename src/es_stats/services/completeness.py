from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from es_stats.domain.missing_policy import MissingPolicy, WindowRole


class CoverageExclusionReason(StrEnum):
    MISSING_EXCEEDS_TOLERANCE = "MISSING_EXCEEDS_TOLERANCE"


@dataclass(frozen=True)
class CoverageResult:
    role: WindowRole
    observed_bar_count: int
    expected_bar_count: int
    missing_bar_count: int
    missing_ratio: float
    tolerance_used: float
    is_complete: bool
    exclusion_reason: CoverageExclusionReason | None


def evaluate_window_coverage(
    *,
    observed_bar_count: int,
    expected_bar_count: int,
    role: WindowRole,
    policy: MissingPolicy,
) -> CoverageResult:
    """
    Evaluate whether observed coverage is acceptable under the configured policy.

    - STRICT: requires no missing bars.
    - ALLOW_MISSING_UP_TO: allows independent X/Y missing ratios.
    """
    if observed_bar_count < 0:
        raise ValueError("observed_bar_count must be >= 0")
    if expected_bar_count < 0:
        raise ValueError("expected_bar_count must be >= 0")

    missing_bar_count = max(0, expected_bar_count - observed_bar_count)
    missing_ratio = 0.0 if expected_bar_count == 0 else (missing_bar_count / expected_bar_count)
    tolerance_used = policy.tolerance_for(role)
    is_complete = missing_ratio <= tolerance_used

    return CoverageResult(
        role=role,
        observed_bar_count=observed_bar_count,
        expected_bar_count=expected_bar_count,
        missing_bar_count=missing_bar_count,
        missing_ratio=missing_ratio,
        tolerance_used=tolerance_used,
        is_complete=is_complete,
        exclusion_reason=(
            None
            if is_complete
            else CoverageExclusionReason.MISSING_EXCEEDS_TOLERANCE
        ),
    )
