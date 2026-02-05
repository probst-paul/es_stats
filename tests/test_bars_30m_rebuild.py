from __future__ import annotations

import psycopg

from es_stats.repositories.bars_1m_repo import upsert_bars_1m
from es_stats.repositories.bars_30m_repo import rebuild_bars_30m_range
from es_stats.repositories.imports_repo import insert_import_run
from es_stats.repositories.instruments_repo import ensure_instrument


def test_rebuild_bars_30m_aggregates_trades_count(pg_conn: psycopg.Connection):
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

    rows = []
    for i in range(30):
        rows.append(
            {
                "instrument_id": instrument_id,
                "ts_start_utc": 1700000000 + 60 * i,
                "trading_date_ct_int": 20250101,
                "ct_minute_of_day": 510 + i,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 10,
                "trades_count": 2,
                "source_import_id": import_id,
            }
        )

    upsert_bars_1m(pg_conn, rows, merge_policy="skip")

    counts = rebuild_bars_30m_range(
        pg_conn,
        instrument_id=instrument_id,
        td_min=20250101,
        td_max=20250101,
        derived_from_import_id=import_id,
    )
    assert counts.inserted >= 1

    r = pg_conn.execute(
        """
        SELECT bar_count_1m, is_complete, volume, trades_count
        FROM bars_30m
        WHERE instrument_id = %s
          AND trading_date_ct_int = %s
          AND bucket_ct_minute_of_day = 510;
        """,
        (instrument_id, 20250101),
    ).fetchone()

    assert r is not None
    assert int(r[0]) == 30
    assert int(r[1]) == 1
    assert int(r[2]) == 300
    assert int(r[3]) == 60


def test_rebuild_bars_30m_partial_bucket_is_incomplete(pg_conn: psycopg.Connection):
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

    rows = []
    for i in range(10):
        rows.append(
            {
                "instrument_id": instrument_id,
                "ts_start_utc": 1700000000 + 60 * i,
                "trading_date_ct_int": 20250101,
                "ct_minute_of_day": 510 + i,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 10,
                "trades_count": 3,
                "source_import_id": import_id,
            }
        )

    upsert_bars_1m(pg_conn, rows, merge_policy="skip")
    rebuild_bars_30m_range(
        pg_conn,
        instrument_id=instrument_id,
        td_min=20250101,
        td_max=20250101,
        derived_from_import_id=import_id,
    )

    r = pg_conn.execute(
        """
        SELECT bar_count_1m, is_complete, volume, trades_count
        FROM bars_30m
        WHERE instrument_id = %s
          AND trading_date_ct_int = %s
          AND bucket_ct_minute_of_day = 510;
        """,
        (instrument_id, 20250101),
    ).fetchone()

    assert r is not None
    assert int(r[0]) == 10
    assert int(r[1]) == 0
    assert int(r[2]) == 100
    assert int(r[3]) == 30
