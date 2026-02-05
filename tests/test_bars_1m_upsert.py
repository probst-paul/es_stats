from __future__ import annotations

import psycopg

from es_stats.repositories.bars_1m_repo import upsert_bars_1m
from es_stats.repositories.imports_repo import insert_import_run
from es_stats.repositories.instruments_repo import ensure_instrument


def test_upsert_bars_1m_skip_and_overwrite(pg_conn: psycopg.Connection):
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

    base = {
        "instrument_id": instrument_id,
        "ts_start_utc": 1700000000,
        "trading_date_ct_int": 20250101,
        "ct_minute_of_day": 0,
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 100,
        "trades_count": 10,
        "source_import_id": import_id,
    }

    c1 = upsert_bars_1m(pg_conn, [base], merge_policy="skip")
    assert c1.inserted == 1
    assert c1.updated == 0

    c2 = upsert_bars_1m(pg_conn, [base], merge_policy="skip")
    assert c2.inserted == 0
    assert c2.updated == 0

    changed = dict(base)
    changed["close"] = 9.9
    changed["trades_count"] = 99
    c3 = upsert_bars_1m(pg_conn, [changed], merge_policy="overwrite")
    assert c3.inserted == 0
    assert c3.updated == 1

    row = pg_conn.execute(
        "SELECT close, trades_count FROM bars_1m WHERE instrument_id = %s AND ts_start_utc = %s;",
        (instrument_id, 1700000000),
    ).fetchone()
    assert row is not None
    assert float(row[0]) == 9.9
    assert int(row[1]) == 99
