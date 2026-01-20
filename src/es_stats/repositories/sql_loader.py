from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict


class SqlFileNotFoundError(FileNotFoundError):
    pass


@dataclass(frozen=True)
class SqlLoader:
    """
    Loads SQL statements from .sql files under the repositories/sql directory.

    Usage intent:
      loader = SqlLoader()
      sql = loader.load("bars_1m/upsert.sql")
    """

    base_dir: Path

    def load(self, relative_path: str) -> str:
        rel = relative_path.strip().lstrip("/")

        # Basic guardrail: we only load .sql files
        if not rel.endswith(".sql"):
            raise ValueError(f"SQL loader expected a .sql file, got: {
                             relative_path!r}")

        full_path = (self.base_dir / rel).resolve()

        if not full_path.exists():
            raise SqlFileNotFoundError(
                f"SQL file not found: {relative_path!r} (resolved to {
                    full_path})"
            )

        return full_path.read_text(encoding="utf-8")


# Default loader and a simple cache
_SQL_BASE_DIR = Path(__file__).resolve().parent / "sql"
_loader = SqlLoader(base_dir=_SQL_BASE_DIR)
_cache: Dict[str, str] = {}


def load_sql(relative_path: str) -> str:
    """
    Load a SQL file by path relative to src/es_stats/repositories/sql/.

    Caches contents in memory for the life of the process.
    """
    key = relative_path.strip().lstrip("/")
    if key in _cache:
        return _cache[key]

    sql = _loader.load(key)
    _cache[key] = sql
    return sql
