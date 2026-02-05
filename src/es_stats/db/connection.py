from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

import psycopg

from es_stats.config.settings import settings


def _resolve_database_url(explicit: str | None = None) -> str:
    """
    Resolve the Postgres connection URL.

    Precedence:
      1) explicit argument (if provided)
      2) ES_STATS_DATABASE_URL env var
      3) DATABASE_URL env var (Render default)
      4) settings.database_url
    """
    if explicit is not None and explicit.strip():
        return explicit.strip()

    for env_name in ("ES_STATS_DATABASE_URL", "DATABASE_URL"):
        override = os.environ.get(env_name)
        if override and override.strip():
            return override.strip()

    return settings.database_url


def connect_postgres(database_url: str) -> psycopg.Connection:
    """Open a PostgreSQL connection."""
    conn = psycopg.connect(database_url)
    # Keep all timestamps/epoch values interpreted in UTC.
    conn.execute("SET TIME ZONE 'UTC';")
    return conn


def connect_default() -> psycopg.Connection:
    """Connect using resolved default DB URL."""
    return connect_postgres(_resolve_database_url())


def execute_script(conn: psycopg.Connection, sql_script: str) -> None:
    """Execute a SQL script containing multiple statements."""
    for statement in sql_script.split(";"):
        stmt = statement.strip()
        if not stmt:
            continue
        conn.execute(f"{stmt};")


@contextmanager
def connection(database_url: str | None = None) -> Iterator[psycopg.Connection]:
    """
    Context manager for a short-lived connection.

    Commits on success, rolls back on exception, always closes.
    """
    conn = connect_postgres(_resolve_database_url(database_url))
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
