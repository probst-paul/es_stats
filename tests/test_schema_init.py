from __future__ import annotations

import sqlite3
from pathlib import Path

from es_stats.repositories.sql_loader import load_sql


def test_schema_init_creates_tables(tmp_path: Path):
    db_path = tmp_path / "test.sqlite3"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.executescript(load_sql("schema/001_init.sql"))

        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = {r[0] for r in rows}

        assert "instruments" in table_names
        assert "imports" in table_names
        assert "bars_1m" in table_names
        assert "bars_30m" in table_names
    finally:
        conn.close()
