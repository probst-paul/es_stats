from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import psycopg

from es_stats.repositories.sql_loader import load_sql


@dataclass(frozen=True)
class UpsertCounts:
    inserted: int
    updated: int


def upsert_bars_1m(
    conn: psycopg.Connection,
    rows: Iterable[dict],
    *,
    merge_policy: str,  # "skip" | "overwrite"
) -> UpsertCounts:
    """
    Upsert canonical 1-minute bars using a temp table + set-based DML.

    Strategy:
    - load rows into TEMP tmp_bars_1m
    - count how many are new vs existing (via joins)
    - if overwrite: UPDATE existing rows from temp (set-based)
    - INSERT new rows with ON CONFLICT DO NOTHING

    Counts:
    - inserted = number of tmp rows that did not already exist in bars_1m
    - updated  = number of tmp rows that already existed (when overwrite)
      (counts key-matches; does not attempt to detect "no-op" updates)
    """
    if merge_policy not in ("skip", "overwrite"):
        raise ValueError(
            f"merge_policy must be 'skip' or 'overwrite', got: {merge_policy!r}"
        )

    conn.execute(load_sql("bars_1m/create_temp.sql"))
    conn.execute(load_sql("bars_1m/clear_temp.sql"))

    rows_list = list(rows)
    if not rows_list:
        return UpsertCounts(inserted=0, updated=0)

    # Stage into temp table (must include trades_count)
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO tmp_bars_1m (
              instrument_id, ts_start_utc, trading_date_ct_int, ct_minute_of_day,
              open, high, low, close, volume, trades_count, source_import_id
            ) VALUES (
              %(instrument_id)s, %(ts_start_utc)s, %(trading_date_ct_int)s, %(ct_minute_of_day)s,
              %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(trades_count)s, %(source_import_id)s
            );
            """,
            rows_list,
        )

    # Compute counts before mutating bars_1m
    inserted = conn.execute(
        """
        SELECT COUNT(*)
        FROM tmp_bars_1m t
        LEFT JOIN bars_1m b
          ON b.instrument_id = t.instrument_id
         AND b.ts_start_utc  = t.ts_start_utc
        WHERE b.instrument_id IS NULL;
        """
    ).fetchone()[0]

    updated = 0
    if merge_policy == "overwrite":
        updated = conn.execute(
            """
            SELECT COUNT(*)
            FROM tmp_bars_1m t
            JOIN bars_1m b
              ON b.instrument_id = t.instrument_id
             AND b.ts_start_utc  = t.ts_start_utc;
            """
        ).fetchone()[0]

        conn.execute(load_sql("bars_1m/update_existing.sql"))

    conn.execute(load_sql("bars_1m/insert_new.sql"))

    return UpsertCounts(inserted=int(inserted), updated=int(updated))
