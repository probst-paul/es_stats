from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from es_stats.repositories.sql_loader import load_sql


def _init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(load_sql("schema/001_init.sql"))
    return conn


def _insert_instrument(conn: sqlite3.Connection, symbol: str = "ES") -> int:
    conn.execute("INSERT INTO instruments(symbol) VALUES (?);", (symbol,))
    return conn.execute(
        "SELECT instrument_id FROM instruments WHERE symbol = ?;",
        (symbol,),
    ).fetchone()[0]


def test_instruments_symbol_unique(tmp_path: Path):
    conn = _init_db(tmp_path / "t.sqlite3")
    try:
        _insert_instrument(conn, "ES")
        with pytest.raises(sqlite3.IntegrityError):
            _insert_instrument(conn, "ES")  # duplicate symbol
    finally:
        conn.close()


def test_bars_1m_pk_unique(tmp_path: Path):
    conn = _init_db(tmp_path / "t.sqlite3")
    try:
        instrument_id = _insert_instrument(conn, "ES")

        row = (
            instrument_id,
            1700000000,  # ts_start_utc
            20250101,    # trading_date_ct_int
            0,           # ct_minute_of_day
            1.0, 2.0, 0.5, 1.5,
            100,
        )

        conn.execute(
            """
            INSERT INTO bars_1m (
              instrument_id, ts_start_utc, trading_date_ct_int, ct_minute_of_day,
              open, high, low, close, volume
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            row,
        )

        # Duplicate PK should fail
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO bars_1m (
                  instrument_id, ts_start_utc, trading_date_ct_int, ct_minute_of_day,
                  open, high, low, close, volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                row,
            )
    finally:
        conn.close()


def test_bars_1m_fk_enforced(tmp_path: Path):
    conn = _init_db(tmp_path / "t.sqlite3")
    try:
        # instrument_id does not exist -> FK violation if PRAGMA foreign_keys is ON
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO bars_1m (
                  instrument_id, ts_start_utc, trading_date_ct_int, ct_minute_of_day,
                  open, high, low, close, volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (999999, 1700000000, 20250101, 0, 1.0, 2.0, 0.5, 1.5, 100),
            )
    finally:
        conn.close()


def test_bars_30m_pk_unique(tmp_path: Path):
    conn = _init_db(tmp_path / "t.sqlite3")
    try:
        instrument_id = _insert_instrument(conn, "ES")

        row = (
            instrument_id,
            1700000000,  # bucket_start_utc
            20250101,    # trading_date_ct_int
            0,           # bucket_ct_minute_of_day (must be multiple of 30)
            "ON",
            0,
            "a",
            1.0, 2.0, 0.5, 1.5,
            100,
            30,
            1,
            None,         # derived_from_import_id
        )

        conn.execute(
            """
            INSERT INTO bars_30m (
              instrument_id, bucket_start_utc, trading_date_ct_int, bucket_ct_minute_of_day,
              session, period_index, tpo,
              open, high, low, close, volume,
              bar_count_1m, is_complete, derived_from_import_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            row,
        )

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO bars_30m (
                  instrument_id, bucket_start_utc, trading_date_ct_int, bucket_ct_minute_of_day,
                  session, period_index, tpo,
                  open, high, low, close, volume,
                  bar_count_1m, is_complete, derived_from_import_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                row,
            )
    finally:
        conn.close()


def test_check_constraints_example(tmp_path: Path):
    conn = _init_db(tmp_path / "t.sqlite3")
    try:
        instrument_id = _insert_instrument(conn, "ES")

        # ct_minute_of_day out of range -> CHECK should fail
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO bars_1m (
                  instrument_id, ts_start_utc, trading_date_ct_int, ct_minute_of_day,
                  open, high, low, close, volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (instrument_id, 1700000000, 20250101,
                 99999, 1.0, 2.0, 0.5, 1.5, 100),
            )

        # bucket_ct_minute_of_day not multiple of 30 -> CHECK should fail
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO bars_30m (
                  instrument_id, bucket_start_utc, trading_date_ct_int, bucket_ct_minute_of_day,
                  open, high, low, close, volume,
                  bar_count_1m, is_complete
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (instrument_id, 1700000000, 20250101,
                 1, 1.0, 2.0, 0.5, 1.5, 100, 30, 1),
            )
    finally:
        conn.close()
