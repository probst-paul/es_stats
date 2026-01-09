from __future__ import annotations

import sqlite3
from typing import Any

from es_stats.repositories.sql_loader import load_sql


def insert_import_run(conn: sqlite3.Connection, params: dict[str, Any]) -> int:
    sql = load_sql("imports/insert_import.sql")
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(
            "Failed to insert import run (no import_id returned).")
    return int(row[0])


def finalize_import_run(conn: sqlite3.Connection, params: dict[str, Any]) -> None:
    """
    Finalize an import audit row with bounds, counts, and status.
    """
    sql = load_sql("imports/finalize_import.sql")
    conn.execute(sql, params)
