from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

from es_stats.domain.bars import RawBar


# Accept common header variants (lowercased/normalized)
_TS_KEYS = ("datetime", "date_time", "timestamp", "time", "date")
_OPEN_KEYS = ("open", "o")
_HIGH_KEYS = ("high", "h")
_LOW_KEYS = ("low", "l")
_CLOSE_KEYS = ("close", "last", "c")
_VOL_KEYS = ("volume", "vol", "v")


@dataclass(frozen=True)
class CsvIssue:
    line: int
    message: str


class CsvValidationError(ValueError):
    def __init__(self, issues: list[CsvIssue]):
        self.issues = issues
        super().__init__(self._format())

    def _format(self) -> str:
        head = f"CSV validation failed with {len(self.issues)} issue(s)."
        sample = "\n".join(f"  line {i.line}: {
                           i.message}" for i in self.issues[:10])
        more = "" if len(self.issues) <= 10 else f"\n  ... and {
            len(self.issues) - 10} more"
        return f"{head}\n{sample}{more}"


def _norm(s: str) -> str:
    return s.strip().lower().replace(" ", "_")


def _find_col(fieldnames: Iterable[str], candidates: tuple[str, ...]) -> str | None:
    normalized = {_norm(n): n for n in fieldnames}
    for c in candidates:
        if c in normalized:
            return normalized[c]
    return None


def _parse_dt(value: str) -> datetime:
    v = value.strip()

    # Epoch seconds (string of digits)
    if v.isdigit():
        return datetime.fromtimestamp(int(v), tz=UTC)

    # Try a small set of common formats (extend as needed)
    fmts = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%Y%m%d %H:%M:%S",
        "%Y%m%d %H:%M",
    )
    for fmt in fmts:
        try:
            return datetime.strptime(v, fmt)
        except ValueError:
            pass

    # If you later decide to add python-dateutil, this is where it would go.
    raise ValueError(f"Unrecognized datetime format: {value!r}")


def read_bars_csv(path: Path) -> list[RawBar]:
    """
    Parse and validate a CSV file into RawBar rows (in-memory only).

    Requirements (v1):
    - Must contain datetime + O/H/L/C/V columns (header names can vary via synonyms).
    - Volume must be >= 0
    - High must be >= Low
    - Numeric fields must parse cleanly
    """
    issues: list[CsvIssue] = []
    bars: list[RawBar] = []

    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise CsvValidationError(
                [CsvIssue(line=1, message="Missing header row (no fieldnames found).")])

        ts_col = _find_col(reader.fieldnames, _TS_KEYS)
        o_col = _find_col(reader.fieldnames, _OPEN_KEYS)
        h_col = _find_col(reader.fieldnames, _HIGH_KEYS)
        l_col = _find_col(reader.fieldnames, _LOW_KEYS)
        c_col = _find_col(reader.fieldnames, _CLOSE_KEYS)
        v_col = _find_col(reader.fieldnames, _VOL_KEYS)

        missing = []
        if ts_col is None:
            missing.append("datetime/timestamp")
        if o_col is None:
            missing.append("open")
        if h_col is None:
            missing.append("high")
        if l_col is None:
            missing.append("low")
        if c_col is None:
            missing.append("close")
        if v_col is None:
            missing.append("volume")

        if missing:
            raise CsvValidationError(
                [CsvIssue(line=1, message=f"Missing required columns: {', '.join(missing)}")])

        # line numbers: header is line 1
        for idx, row in enumerate(reader, start=2):
            try:
                dt = _parse_dt(row[ts_col])
                o = float(row[o_col])
                h = float(row[h_col])
                l = float(row[l_col])
                c = float(row[c_col])
                v = int(float(row[v_col]))  # handle "100.0" etc.

                if v < 0:
                    raise ValueError("volume must be >= 0")
                if h < l:
                    raise ValueError("high must be >= low")

                bars.append(RawBar(dt=dt, open=o, high=h,
                            low=l, close=c, volume=v))
            except Exception as e:
                issues.append(CsvIssue(line=idx, message=str(e)))

    if issues:
        raise CsvValidationError(issues)

    return bars
