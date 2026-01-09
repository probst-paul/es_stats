from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

CT_TZ = ZoneInfo("America/Chicago")


@dataclass(frozen=True)
class TimeFields:
    ts_start_utc: int
    trading_date_ct_int: int
    ct_minute_of_day: int


def _yyyymmdd_int(d: datetime) -> int:
    return d.year * 10000 + d.month * 100 + d.day


def _localize_strict(dt_naive: datetime, tz: ZoneInfo) -> datetime:
    """
    Attach tzinfo to a naive datetime in a way that detects DST gaps.
    For ambiguous times (fall-back), we default to fold=0.

    We detect nonexistent local times by round-tripping through UTC.
    """
    if dt_naive.tzinfo is not None:
        raise ValueError("Expected naive datetime for localization.")

    # Try fold=0
    dt0 = dt_naive.replace(tzinfo=tz, fold=0)
    rt0 = dt0.astimezone(UTC).astimezone(tz).replace(tzinfo=None)
    if rt0 == dt_naive:
        return dt0

    # Try fold=1 (some ambiguous cases)
    dt1 = dt_naive.replace(tzinfo=tz, fold=1)
    rt1 = dt1.astimezone(UTC).astimezone(tz).replace(tzinfo=None)
    if rt1 == dt_naive:
        return dt1

    # Neither fold round-trips: DST gap (nonexistent local time)
    raise ValueError(f"Nonexistent local time in {tz.key}: {
                     dt_naive!r} (DST transition gap)")


def compute_time_fields(dt: datetime, input_timezone: str) -> TimeFields:
    """
    Given a datetime parsed from the CSV, compute:
      - ts_start_utc (epoch seconds, INTEGER)
      - trading_date_ct_int using CT 17:00 rollover
      - ct_minute_of_day (0..1439)

    Rules:
    - If dt is naive, interpret it in input_timezone (DST-aware; errors on DST gaps).
    - If dt is timezone-aware, treat it as an absolute instant (input_timezone is ignored).
    """
    if dt.tzinfo is None:
        tz = ZoneInfo(input_timezone)
        dt_local = _localize_strict(dt, tz)
    else:
        dt_local = dt

    dt_utc = dt_local.astimezone(UTC)
    ts_start_utc = int(dt_utc.timestamp())

    dt_ct = dt_utc.astimezone(CT_TZ)
    ct_minute_of_day = dt_ct.hour * 60 + dt_ct.minute

    # CT 17:00 rollover: >= 17:00 belongs to next trading date
    rollover = dt_ct.replace(hour=17, minute=0, second=0, microsecond=0)
    trading_date_ct = (dt_ct + timedelta(days=1)
                       ).date() if dt_ct >= rollover else dt_ct.date()
    trading_date_ct_int = trading_date_ct.year * 10000 + \
        trading_date_ct.month * 100 + trading_date_ct.day

    return TimeFields(
        ts_start_utc=ts_start_utc,
        trading_date_ct_int=trading_date_ct_int,
        ct_minute_of_day=ct_minute_of_day,
    )
