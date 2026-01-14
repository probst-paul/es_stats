from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

from es_stats.domain.bars import RawBar


# Accept common header variants (lowercased/normalized via _norm)
_TS_KEYS = ("datetime", "date_time", "timestamp", "time", "date")
_OPEN_KEYS = ("open", "o")
_HIGH_KEYS = ("high", "h")
_LOW_KEYS = ("low", "l")
_CLOSE_KEYS = ("close", "last", "c")
_VOL_KEYS = ("volume", "vol", "v")
_TRADES_KEYS = ("#_of_trades",)


@dataclass(frozen=True)
class CsvIssue:
    line: int
    message: str


@dataclass(frozen=True)
class CsvParseResult:
    bars: list[RawBar]
    row_count_read: int
    row_count_rejected: int
    issues: list[CsvIssue]


class CsvValidationError(ValueError):
    def __init__(self, issues: list[CsvIssue]):
        self.issues = issues
        super().__init__(self._format())

    def _format(self) -> str:
        head = f"CSV validation failed with {len(self.issues)} issue(s)."
        sample = "\n".join(
            f"  line {i.line}: {i.message}" for i in self.issues[:10]
        )
        more = "" if len(self.issues) <= 10 else f"\n  ... and {
            len(self.issues) - 10} more"
        return f"{head}\n{sample}{more}"


def _norm(s: str) -> str:
    """
    Normalize header names to improve matching across vendors.

    Examples:
      "# of Trades" -> "#_of_trades"
      "Last"        -> "last"
      "Trade Count" -> "trade_count"
    """
    return s.strip().lower().replace(" ", "_")


def _find_col(fieldnames: Iterable[str], candidates: tuple[str, ...]) -> str | None:
    """
    Return the original fieldname that matches a normalized candidate, else None.
    """
    normalized = {_norm(n): n for n in fieldnames}
    for c in candidates:
        key = _norm(c)
        if key in normalized:
            return normalized[key]
    return None


def _parse_dt(value: str) -> datetime:
    v = value.strip()

    # Epoch seconds (string of digits)
    if v.isdigit():
        return datetime.fromtimestamp(int(v), tz=UTC)

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

    raise ValueError(f"Unrecognized datetime format: {value!r}")


def _req(row: dict, col: str, label: str) -> str:
    """
    Require a non-empty value; raise ValueError for row-level rejection.
    """
    val = row.get(col)
    if val is None:
        raise ValueError(f"missing {label}")
    s = str(val).strip()
    if s == "":
        raise ValueError(f"missing {label}")
    return s


def read_bars_csv(path: Path) -> CsvParseResult:
    """
    Parse and validate a CSV file into RawBar rows (in-memory only).

    Requirements (v1):
    - Must contain datetime/timestamp + O/H/L/(C or Last)/V + Trades columns
      (header names can vary via synonyms).
    - trades_count is REQUIRED as a column; if missing, the import must fail.
    - Row-level missing/blank cells (for any required field) cause that bar to be skipped.
    - Volume and trades_count must be >= 0
    - High must be >= Low
    - Numeric fields must parse cleanly

    Returns:
      CsvParseResult with bars + read/rejected counts + issues list.
      Row-level issues are NOT fatal unless all rows are rejected.
    """
    issues: list[CsvIssue] = []
    bars: list[RawBar] = []
    row_count_read = 0
    row_count_rejected = 0

    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise CsvValidationError(
                [CsvIssue(line=1, message="Missing header row (no fieldnames found).")]
            )

        ts_col = _find_col(reader.fieldnames, _TS_KEYS)
        o_col = _find_col(reader.fieldnames, _OPEN_KEYS)
        h_col = _find_col(reader.fieldnames, _HIGH_KEYS)
        l_col = _find_col(reader.fieldnames, _LOW_KEYS)
        c_col = _find_col(reader.fieldnames, _CLOSE_KEYS)
        v_col = _find_col(reader.fieldnames, _VOL_KEYS)
        t_col = _find_col(reader.fieldnames, _TRADES_KEYS)

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
            missing.append("close/last")
        if v_col is None:
            missing.append("volume")
        if t_col is None:
            missing.append("trades_count")

        # Fatal: missing required columns
        if missing:
            raise CsvValidationError(
                [CsvIssue(line=1, message=f"Missing required columns: {
                          ', '.join(missing)}")]
            )

        # line numbers: header is line 1
        for idx, row in enumerate(reader, start=2):
            row_count_read += 1
            try:
                dt_raw = _req(row, ts_col, "timestamp")
                o_raw = _req(row, o_col, "open")
                h_raw = _req(row, h_col, "high")
                l_raw = _req(row, l_col, "low")
                c_raw = _req(row, c_col, "close/last")
                v_raw = _req(row, v_col, "volume")
                t_raw = _req(row, t_col, "trades_count")

                dt = _parse_dt(dt_raw)
                o = float(o_raw)
                h = float(h_raw)
                l = float(l_raw)
                c = float(c_raw)
                v = int(float(v_raw))  # handle "100.0"
                t = int(float(t_raw))  # handle "10.0"

                if v < 0:
                    raise ValueError("volume must be >= 0")
                if t < 0:
                    raise ValueError("trades_count must be >= 0")
                if h < l:
                    raise ValueError("high must be >= low")

                bars.append(
                    RawBar(
                        dt=dt,
                        open=o,
                        high=h,
                        low=l,
                        close=c,
                        volume=v,
                        trades_count=t,
                    )
                )
            except Exception as e:
                row_count_rejected += 1
                issues.append(CsvIssue(line=idx, message=str(e)))

    # If we parsed nothing usable, treat as fatal.
    if not bars:
        raise CsvValidationError(
            issues
            or [CsvIssue(line=1, message="No valid data rows parsed (all rows rejected).")]
        )

    return CsvParseResult(
        bars=bars,
        row_count_read=row_count_read,
        row_count_rejected=row_count_rejected,
        issues=issues,
    )
