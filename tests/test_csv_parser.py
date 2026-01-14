from __future__ import annotations

from pathlib import Path

import pytest

from es_stats.services.csv_parser import CsvValidationError, read_bars_csv


def test_read_bars_csv_valid_with_trades(tmp_path: Path):
    p = tmp_path / "bars.csv"
    p.write_text(
        "datetime,open,high,low,last,volume,# of Trades\n"
        "2025-01-01 08:30,100,101,99,100.5,10,7\n"
    )

    res = read_bars_csv(p)
    assert res.row_count_read == 1
    assert res.row_count_rejected == 0
    assert len(res.bars) == 1
    assert res.bars[0].trades_count == 7


def test_read_bars_csv_missing_trades_column_is_fatal(tmp_path: Path):
    p = tmp_path / "bars.csv"
    p.write_text(
        "datetime,open,high,low,last,volume\n"
        "2025-01-01 08:30,100,101,99,100.5,10\n"
    )
    with pytest.raises(CsvValidationError) as e:
        read_bars_csv(p)
    assert "trades_count" in str(e.value).lower()


def test_read_bars_csv_row_missing_trades_is_rejected(tmp_path: Path):
    p = tmp_path / "bars.csv"
    p.write_text(
        "datetime,open,high,low,last,volume,# of Trades\n"
        "2025-01-01 08:30,100,101,99,100.5,10,\n"
        "2025-01-01 08:31,100,101,99,100.5,10,5\n"
    )
    res = read_bars_csv(p)
    assert res.row_count_read == 2
    assert res.row_count_rejected == 1
    assert len(res.bars) == 1
    assert res.bars[0].trades_count == 5
