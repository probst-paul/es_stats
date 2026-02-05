from __future__ import annotations

import psycopg


def test_schema_init_creates_tables(pg_conn: psycopg.Connection):
    rows = pg_conn.execute(
        """
        SELECT tablename
        FROM pg_catalog.pg_tables
        WHERE schemaname = current_schema()
        ORDER BY tablename;
        """
    ).fetchall()
    table_names = {r[0] for r in rows}

    assert "instruments" in table_names
    assert "imports" in table_names
    assert "bars_1m" in table_names
    assert "bars_30m" in table_names
