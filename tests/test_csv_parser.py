from __future__ import annotations

from pathlib import Path

import pytest

from es_stats.services.csv_parser import CsvValidationError, read_bars_csv


def test_read_bars_csv_valid(tmp_path: Path):
    p = tmp_path / "bars.csv"
    p.write_text(
        "datetime,open,high,low,last,volume\n"
        "2025-01-01 08:30,100,101,99,100.5,10\n"
        "2025-01-01 08:31,100.5,102,100,101,20\n"
    )

    bars = read_bars_csv(p)
    assert len(bars) == 2
    assert bars[0].high >= bars[0].low


def test_read_bars_csv_missing_columns(tmp_path: Path):
    p = tmp_path / "bars.csv"
    p.write_text(
        "datetime,open,high,low,last\n2025-01-01 08:30,1,2,0.5,1.5\n")

    with pytest.raises(CsvValidationError) as exc:
        read_bars_csv(p)
    assert "Missing required columns" in str(exc.value)


def test_read_bars_csv_rejects_bad_values(tmp_path: Path):
    p = tmp_path / "bars.csv"
    p.write_text(
        "datetime,open,high,low,last,volume\n"
        "2025-01-01 08:30,100,99,101,100.5,10\n"  # high < low
    )

    with pytest.raises(CsvValidationError) as exc:
        read_bars_csv(p)
    assert "high must be >= low" in str(exc.value)


def test_read_bars_csv_accepts_epoch_seconds(tmp_path: Path):
    p = tmp_path / "bars.csv"
    p.write_text(
        "timestamp,open,high,low,last,volume\n"
        "1700000000,1,2,0.5,1.5,10\n"
    )

    bars = read_bars_csv(p)
    assert len(bars) == 1
