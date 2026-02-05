from __future__ import annotations

import pytest

from es_stats.domain.missing_policy import MissingPolicy, MissingPolicyMode, WindowRole
from es_stats.services.completeness import (
    CoverageExclusionReason,
    evaluate_window_coverage,
)


def test_strict_policy_requires_zero_tolerances() -> None:
    with pytest.raises(ValueError, match="STRICT mode requires"):
        MissingPolicy(mode=MissingPolicyMode.STRICT, x_tol=0.1, y_tol=0.0)


def test_missing_policy_rejects_out_of_range_tolerance() -> None:
    with pytest.raises(ValueError, match="x_tol must be in"):
        MissingPolicy(mode=MissingPolicyMode.ALLOW_MISSING_UP_TO, x_tol=-0.01, y_tol=0.0)
    with pytest.raises(ValueError, match="y_tol must be in"):
        MissingPolicy(mode=MissingPolicyMode.ALLOW_MISSING_UP_TO, x_tol=0.0, y_tol=1.01)


def test_evaluate_window_coverage_strict_pass_and_fail() -> None:
    policy = MissingPolicy(mode=MissingPolicyMode.STRICT)

    ok = evaluate_window_coverage(
        observed_bar_count=30,
        expected_bar_count=30,
        role=WindowRole.X,
        policy=policy,
    )
    assert ok.is_complete is True
    assert ok.exclusion_reason is None

    bad = evaluate_window_coverage(
        observed_bar_count=29,
        expected_bar_count=30,
        role=WindowRole.X,
        policy=policy,
    )
    assert bad.is_complete is False
    assert bad.exclusion_reason == CoverageExclusionReason.MISSING_EXCEEDS_TOLERANCE


def test_evaluate_window_coverage_allows_independent_x_and_y_tolerance() -> None:
    policy = MissingPolicy(
        mode=MissingPolicyMode.ALLOW_MISSING_UP_TO,
        x_tol=0.10,  # allow up to 10% missing in X
        y_tol=0.00,  # require full Y
    )

    x_result = evaluate_window_coverage(
        observed_bar_count=27,  # 10% missing of 30
        expected_bar_count=30,
        role=WindowRole.X,
        policy=policy,
    )
    y_result = evaluate_window_coverage(
        observed_bar_count=29,  # 3.33% missing of 30
        expected_bar_count=30,
        role=WindowRole.Y,
        policy=policy,
    )

    assert x_result.is_complete is True
    assert y_result.is_complete is False


def test_evaluate_window_coverage_passes_exactly_at_tolerance_boundary() -> None:
    policy = MissingPolicy(
        mode=MissingPolicyMode.ALLOW_MISSING_UP_TO,
        x_tol=0.20,
        y_tol=0.0,
    )
    result = evaluate_window_coverage(
        observed_bar_count=24,  # missing 6 of 30 => 20%
        expected_bar_count=30,
        role=WindowRole.X,
        policy=policy,
    )
    assert result.missing_ratio == pytest.approx(0.2)
    assert result.is_complete is True


def test_evaluate_window_coverage_rejects_negative_counts() -> None:
    policy = MissingPolicy(mode=MissingPolicyMode.STRICT)
    with pytest.raises(ValueError, match="observed_bar_count must be >= 0"):
        evaluate_window_coverage(
            observed_bar_count=-1,
            expected_bar_count=30,
            role=WindowRole.X,
            policy=policy,
        )
    with pytest.raises(ValueError, match="expected_bar_count must be >= 0"):
        evaluate_window_coverage(
            observed_bar_count=30,
            expected_bar_count=-1,
            role=WindowRole.X,
            policy=policy,
        )
