from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from es_stats.repositories.sql_loader import load_sql


@dataclass(frozen=True)
class RebuildCounts:
    deleted: int
    inserted: int


def rebuild_bars_30m_range(
    conn: sqlite3.Connection,
    *,
    instrument_id: int,
    td_min: int,
    td_max: int,
    derived_from_import_id: int,
) -> RebuildCounts:
    """
    Rebuild derived 30-minute bars for an instrument over a trading-date range.

    SQLite-compatible, deterministic strategy:
      1) DELETE existing bars_30m rows in range
      2) INSERT freshly aggregated rows from bars_1m for that same range

    Counts are computed via changes() after each statement (safe because:
      - DELETE has no conflicts
      - INSERT occurs after DELETE, so no PK conflicts should occur)
    """
    params = {
        "instrument_id": instrument_id,
        "td_min": td_min,
        "td_max": td_max,
        "derived_from_import_id": derived_from_import_id,
    }

    conn.execute(load_sql("bars_30m/delete_range.sql"), params)
    deleted = int(conn.execute("SELECT changes();").fetchone()[0])

    conn.execute(load_sql("bars_30m/insert_range.sql"), params)
    inserted = int(conn.execute("SELECT changes();").fetchone()[0])

    return RebuildCounts(deleted=deleted, inserted=inserted)
