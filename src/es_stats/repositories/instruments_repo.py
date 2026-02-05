from __future__ import annotations

import psycopg

from es_stats.repositories.sql_loader import load_sql


def ensure_instrument(conn: psycopg.Connection, symbol: str) -> int:
    sql = load_sql("instruments/ensure_instrument.sql")
    cur = conn.execute(sql, {"symbol": symbol})
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Failed to resolve instrument_id for symbol={symbol!r}")
    return int(row[0])
