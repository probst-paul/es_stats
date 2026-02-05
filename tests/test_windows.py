from __future__ import annotations

import pytest

from es_stats.domain.windows import WindowAnchor, WindowOrderRule, WindowSpec, validate_pair


def test_window_spec_duration_for_non_wrapping_window() -> None:
    w = WindowSpec(
        anchor=WindowAnchor.TRADING_DATE_CT,
        start_minute_ct=510,  # 08:30
        end_minute_ct=569,  # 09:29
        name="RTH first hour",
    )
    assert w.spans_midnight is False
    assert w.duration_minutes == 60


def test_window_spec_duration_for_wrapping_window() -> None:
    w = WindowSpec(
        anchor=WindowAnchor.TRADING_DATE_CT,
        start_minute_ct=1020,  # 17:00
        end_minute_ct=509,  # 08:29
        name="ON",
    )
    assert w.spans_midnight is True
    assert w.duration_minutes == 930


def test_validate_pair_y_before_x_accepts_on_then_rth() -> None:
    y = WindowSpec(WindowAnchor.TRADING_DATE_CT, 1020, 509, "ON")
    x = WindowSpec(WindowAnchor.TRADING_DATE_CT, 510, 959, "RTH")
    validate_pair(x=x, y=y, rule=WindowOrderRule.Y_ENDS_BEFORE_X_START)


def test_validate_pair_rejects_overlap_for_ordered_rule() -> None:
    y = WindowSpec(WindowAnchor.TRADING_DATE_CT, 540, 600, "Y")
    x = WindowSpec(WindowAnchor.TRADING_DATE_CT, 580, 650, "X")
    with pytest.raises(ValueError, match="overlap"):
        validate_pair(x=x, y=y, rule=WindowOrderRule.Y_ENDS_BEFORE_X_START)


def test_validate_pair_rejects_bad_order() -> None:
    y = WindowSpec(WindowAnchor.TRADING_DATE_CT, 600, 700, "Y")
    x = WindowSpec(WindowAnchor.TRADING_DATE_CT, 510, 599, "X")
    with pytest.raises(ValueError, match="must complete before X begins"):
        validate_pair(x=x, y=y, rule=WindowOrderRule.Y_ENDS_BEFORE_X_START)


def test_window_spec_rejects_out_of_range_minutes() -> None:
    with pytest.raises(ValueError, match="start_minute_ct"):
        WindowSpec(WindowAnchor.TRADING_DATE_CT, -1, 10)
    with pytest.raises(ValueError, match="end_minute_ct"):
        WindowSpec(WindowAnchor.TRADING_DATE_CT, 10, 1440)
