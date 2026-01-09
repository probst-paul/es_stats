from __future__ import annotations

from datetime import UTC, datetime

import pytest

from es_stats.services.time_fields import compute_time_fields


def test_ct_minute_of_day_basic():
    # 08:30 CT -> minute 510
    # naive, interpreted in America/Chicago
    dt = datetime(2025, 1, 2, 8, 30, 0)
    tf = compute_time_fields(dt, "America/Chicago")
    assert tf.ct_minute_of_day == 8 * 60 + 30


def test_trading_date_rollover_before_1700():
    # 16:59 CT belongs to same CT calendar date
    dt = datetime(2025, 1, 2, 16, 59, 0)
    tf = compute_time_fields(dt, "America/Chicago")
    assert tf.trading_date_ct_int == 20250102


def test_trading_date_rollover_at_1700():
    # 17:00 CT belongs to next trading date
    dt = datetime(2025, 1, 2, 17, 0, 0)
    tf = compute_time_fields(dt, "America/Chicago")
    assert tf.trading_date_ct_int == 20250103


def test_aware_datetime_ignores_input_timezone():
    # Epoch instant: 2025-01-01 00:00:00 UTC
    dt = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    tf = compute_time_fields(dt, "America/Chicago")
    assert tf.ts_start_utc == int(dt.timestamp())


def test_dst_gap_is_rejected():
    # Spring forward gap in America/Chicago (nonexistent local time).
    # This exact moment can vary by year; 2025-03-09 is the US DST start in 2025.
    dt = datetime(2025, 3, 9, 2, 30, 0)
    with pytest.raises(ValueError):
        compute_time_fields(dt, "America/Chicago")
