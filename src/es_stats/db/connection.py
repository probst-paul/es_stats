from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from es_stats.config.settings import settings


def _resolve_db_path(explicit: Path | None = None) -> Path:
    """
    Resolve the SQLite DB path.

    Precedence:
      1) explicit argument (if provided)
      2) ES_STATS_DB_PATH env var (useful for tests/CI)
      3) settings.db_path
    """
    if explicit is not None:
        return explicit

    override = os.environ.get("ES_STATS_DB_PATH")
    if override:
        return Path(override)

    return settings.db_path


def connect_sqlite(db_path: Path) -> sqlite3.Connection:
    """
    Open a SQLite connection with consistent pragmas.

    Notes:
    - foreign_keys must be enabled per connection in SQLite
    - WAL improves read concurrency (useful for a web app)
    - busy_timeout reduces 'database is locked' failures under light contention
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(
        str(db_path),
        detect_types=sqlite3.PARSE_DECLTYPES,
        # Avoid thread-affinity surprises in server contexts.
        # Do not share the same connection across requests.
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row

    # Pragmas (applied per connection)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    conn.execute("PRAGMA temp_store = MEMORY;")

    return conn


def connect_default() -> sqlite3.Connection:
    """Connect using resolved default DB path (supports ES_STATS_DB_PATH override)."""
    return connect_sqlite(_resolve_db_path())


@contextmanager
def connection(db_path: Path | None = None) -> Iterator[sqlite3.Connection]:
    """
    Context manager for a short-lived connection.

    Commits on success, rolls back on exception, always closes.

    Usage:
      with connection() as conn:
          ...
    """
    resolved = _resolve_db_path(db_path)
    conn = connect_sqlite(resolved)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
