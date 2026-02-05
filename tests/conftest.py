from __future__ import annotations

import os
from urllib.parse import urlparse

import psycopg
import pytest

from es_stats.db.connection import execute_script
from es_stats.repositories.sql_loader import load_sql

_INTEGRATION_MODULES = {
    "test_bars_1m_upsert.py",
    "test_bars_30m_rebuild.py",
    "test_constraints.py",
    "test_import_audit.py",
    "test_import_end_to_end.py",
    "test_schema_init.py",
}


_PLACEHOLDER_PARTS = {"USER", "PASSWORD", "HOST", "DBNAME"}


def _looks_like_template_url(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.hostname or "").upper()
    path = (parsed.path or "").strip("/").upper()
    user = (parsed.username or "").upper()
    password = (parsed.password or "").upper()

    return any(
        part in _PLACEHOLDER_PARTS
        for part in (host, path, user, password)
    )


def _integration_database_url() -> str | None:
    url = os.getenv("ES_STATS_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not url:
        return None
    if _looks_like_template_url(url):
        return None
    return url


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    has_db_url = bool(_integration_database_url())
    if has_db_url:
        return

    skip_no_db = pytest.mark.skip(
        reason=(
            "Postgres integration tests require a real ES_STATS_DATABASE_URL "
            "or DATABASE_URL (not the README template value)"
        )
    )

    for item in items:
        file_name = item.location[0].split("/")[-1]
        if file_name in _INTEGRATION_MODULES:
            item.add_marker(skip_no_db)
            continue

        if file_name == "test_cli_contract.py" and item.name == "test_import_csv_accepts_valid_args":
            item.add_marker(skip_no_db)


@pytest.fixture(scope="session")
def postgres_url() -> str:
    url = _integration_database_url()
    if not url:
        pytest.skip(
            "Postgres integration tests require a real ES_STATS_DATABASE_URL "
            "or DATABASE_URL (not the README template value)"
        )
    return url


@pytest.fixture()
def pg_conn(postgres_url: str) -> psycopg.Connection:
    conn = psycopg.connect(postgres_url)
    try:
        conn.execute("SET TIME ZONE 'UTC';")
        conn.execute("DROP TABLE IF EXISTS bars_30m CASCADE;")
        conn.execute("DROP TABLE IF EXISTS bars_1m CASCADE;")
        conn.execute("DROP TABLE IF EXISTS imports CASCADE;")
        conn.execute("DROP TABLE IF EXISTS instruments CASCADE;")
        execute_script(conn, load_sql("schema/001_init.sql"))
        conn.commit()
        yield conn
    finally:
        conn.close()
