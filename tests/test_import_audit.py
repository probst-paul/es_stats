from __future__ import annotations

import psycopg

from es_stats.repositories.imports_repo import finalize_import_run, insert_import_run
from es_stats.repositories.instruments_repo import ensure_instrument


def test_import_audit_insert_and_finalize(pg_conn: psycopg.Connection):
    instrument_id = ensure_instrument(pg_conn, "ES")

    import_id = insert_import_run(
        pg_conn,
        {
            "instrument_id": instrument_id,
            "source_name": "example.csv",
            "source_hash": None,
            "input_timezone": "America/Chicago",
            "bar_interval_seconds": 60,
            "merge_policy": "skip",
            "started_at_utc": 1700000000,
            "status": "failed",
            "error_summary": None,
        },
    )

    finalize_import_run(
        pg_conn,
        {
            "import_id": import_id,
            "finished_at_utc": 1700000010,
            "ts_min_utc": 1700000000,
            "ts_max_utc": 1700000060,
            "row_count_read": 2,
            "row_count_inserted": 0,
            "row_count_updated": 0,
            "row_count_rejected": 0,
            "status": "success",
            "error_summary": None,
        },
    )

    row = pg_conn.execute(
        "SELECT status, row_count_read, ts_min_utc, ts_max_utc FROM imports WHERE import_id = %s;",
        (import_id,),
    ).fetchone()

    assert row is not None
    assert row[0] == "success"
    assert row[1] == 2
    assert row[2] == 1700000000
    assert row[3] == 1700000060
