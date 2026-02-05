from __future__ import annotations

from dataclasses import dataclass

import psycopg

from es_stats.repositories.sql_loader import load_sql


@dataclass(frozen=True)
class RebuildCounts:
    deleted: int
    inserted: int


def rebuild_bars_30m_range(
    conn: psycopg.Connection,
    *,
    instrument_id: int,
    td_min: int,
    td_max: int,
    derived_from_import_id: int,
) -> RebuildCounts:
    """
    Rebuild derived 30-minute bars for an instrument over a trading-date range.

    Strategy:
      1) DELETE existing bars_30m rows in range
      2) INSERT freshly aggregated rows from bars_1m for that same range
    """
    params = {
        "instrument_id": instrument_id,
        "td_min": td_min,
        "td_max": td_max,
        "derived_from_import_id": derived_from_import_id,
    }

    deleted_cur = conn.execute(load_sql("bars_30m/delete_range.sql"), params)
    inserted_cur = conn.execute(load_sql("bars_30m/insert_range.sql"), params)

    return RebuildCounts(
        deleted=int(deleted_cur.rowcount),
        inserted=int(inserted_cur.rowcount),
    )
