from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from es_stats.repositories.instruments_repo import ensure_instrument
from es_stats.repositories.imports_repo import insert_import_run
from es_stats.repositories.sql_loader import load_sql


def _init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(load_sql("schema/001_init.sql"))
    return conn


def test_bars_1m_pk_unique(tmp_path: Path):
    conn = _init_db(tmp_path / "t.sqlite3")
    try:
        instrument_id = ensure_instrument(conn, "ES")
        import_id = insert_import_run(
            conn,
            {
                "instrument_id": instrument_id,
                "source_name": "test.csv",
                "source_hash": None,
                "input_timezone": "America/Chicago",
                "bar_interval_seconds": 60,
                "merge_policy": "skip",
                "started_at_utc": 1700000000,
                "status": "failed",
                "error_summary": None,
            },
        )

        row = (
            instrument_id,
            1700000000,
            20250101,
            0,
            1.0, 2.0, 0.5, 1.5,
            100,
            10,
            import_id,
        )

        conn.execute(
            """
            INSERT INTO bars_1m (
              instrument_id, ts_start_utc, trading_date_ct_int, ct_minute_of_day,
              open, high, low, close, volume, trades_count, source_import_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            row,
        )

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO bars_1m (
                  instrument_id, ts_start_utc, trading_date_ct_int, ct_minute_of_day,
                  open, high, low, close, volume, trades_count, source_import_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                row,
            )
    finally:
        conn.close()


def test_bars_1m_trades_non_negative(tmp_path: Path):
    conn = _init_db(tmp_path / "t.sqlite3")
    try:
        instrument_id = ensure_instrument(conn, "ES")
        import_id = insert_import_run(
            conn,
            {
                "instrument_id": instrument_id,
                "source_name": "test.csv",
                "source_hash": None,
                "input_timezone": "America/Chicago",
                "bar_interval_seconds": 60,
                "merge_policy": "skip",
                "started_at_utc": 1700000000,
                "status": "failed",
                "error_summary": None,
            },
        )

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO bars_1m (
                  instrument_id, ts_start_utc, trading_date_ct_int, ct_minute_of_day,
                  open, high, low, close, volume, trades_count, source_import_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (instrument_id, 1700000000, 20250101, 0,
                 1.0, 2.0, 0.5, 1.5, 100, -1, import_id),
            )
    finally:
        conn.close()


def test_bars_30m_bucket_minute_multiple_30(tmp_path: Path):
    conn = _init_db(tmp_path / "t.sqlite3")
    try:
        instrument_id = ensure_instrument(conn, "ES")

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO bars_30m (
                  instrument_id, bucket_start_utc, trading_date_ct_int, bucket_ct_minute_of_day,
                  open, high, low, close, volume, trades_count,
                  bar_count_1m, is_complete
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (instrument_id, 1700000000, 20250101,
                 1, 1.0, 2.0, 0.5, 1.5, 100, 10, 30, 1),
            )
    finally:
        conn.close()
