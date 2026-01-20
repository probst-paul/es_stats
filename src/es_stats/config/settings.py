from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env(name: str, default: str) -> str:
    """Read an environment variable with a default."""
    val = os.environ.get(name)
    return default if val is None or val.strip() == "" else val.strip()


@dataclass(frozen=True)
class Settings:
    """
    Centralized, env-driven configuration.

    Design intent:
    - Keep defaults sensible for local dev.
    - Allow overrides via environment variables for Docker/VPS.
    - Avoid importing os.environ everywhere else in the codebase.
    """

    # Environmental name (local/dev/prod).
    env: str

    # Where the SQLite DB file will live.
    db_path: Path

    # Logging level (INFO, DEBUG, etc.)
    log_level: str


def load_settings() -> Settings:
    """
    Load settings from environment with safe defaults.

    Environment variables:
    - ES_STATS_ENV
    - ES_STATS_DB_PATH
    - ES_STATS_LOG_LEVEL
    """
    env = _env("ES_STATS_ENV", "local")
    db_path_str = _env("ES_STATS_DB_PATH", str(
        Path("data") / "es_stats.sqlite3"))
    log_level = _env("ES_STATS_LOG_LEVEL", "INFO").upper()

    return Settings(
        env=env,
        db_path=Path(db_path_str),
        log_level=log_level,
    )


# Module-level singleton used by app/CLI imports.
settings = load_settings()
