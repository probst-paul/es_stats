from __future__ import annotations

import psycopg
import pytest

from es_stats.repositories.imports_repo import insert_import_run
from es_stats.repositories.instruments_repo import ensure_instrument


def test_bars_1m_pk_unique(pg_conn: psycopg.Connection):
    instrument_id = ensure_instrument(pg_conn, "ES")
    import_id = insert_import_run(
        pg_conn,
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
        1.0,
        2.0,
        0.5,
        1.5,
        100,
        10,
        import_id,
    )

    pg_conn.execute(
        """
        INSERT INTO bars_1m (
          instrument_id, ts_start_utc, trading_date_ct_int, ct_minute_of_day,
          open, high, low, close, volume, trades_count, source_import_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """,
        row,
    )

    with pytest.raises(psycopg.IntegrityError):
        pg_conn.execute(
            """
            INSERT INTO bars_1m (
              instrument_id, ts_start_utc, trading_date_ct_int, ct_minute_of_day,
              open, high, low, close, volume, trades_count, source_import_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            row,
        )
    pg_conn.rollback()


def test_bars_1m_trades_non_negative(pg_conn: psycopg.Connection):
    instrument_id = ensure_instrument(pg_conn, "ES")
    import_id = insert_import_run(
        pg_conn,
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

    with pytest.raises(psycopg.IntegrityError):
        pg_conn.execute(
            """
            INSERT INTO bars_1m (
              instrument_id, ts_start_utc, trading_date_ct_int, ct_minute_of_day,
              open, high, low, close, volume, trades_count, source_import_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (instrument_id, 1700000000, 20250101, 0, 1.0, 2.0, 0.5, 1.5, 100, -1, import_id),
        )
    pg_conn.rollback()


def test_bars_30m_bucket_minute_multiple_30(pg_conn: psycopg.Connection):
    instrument_id = ensure_instrument(pg_conn, "ES")

    with pytest.raises(psycopg.IntegrityError):
        pg_conn.execute(
            """
            INSERT INTO bars_30m (
              instrument_id, bucket_start_utc, trading_date_ct_int, bucket_ct_minute_of_day,
              open, high, low, close, volume, trades_count,
              bar_count_1m, is_complete
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (instrument_id, 1700000000, 20250101, 1, 1.0, 2.0, 0.5, 1.5, 100, 10, 30, 1),
        )
    pg_conn.rollback()
