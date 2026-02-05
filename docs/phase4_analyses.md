# Phase 4.1 - V1 Analyses

This document defines the initial analytics set for Phase 4.
Scope is intentionally constrained to analyses that can share:
- window specification (`WindowSpec`)
- data resolution choice (`1m` vs `30m`)
- completeness/missing-data evaluation
- common window metrics (OHLC, range, volume, trades_count)

## Shared Terms

- **X window**: primary analysis window.
- **Y window**: reference/comparison window.
- **Trading date**: CT trading date key used in canonical bars.
- **Breach up**: `X.high > Y.high`.
- **Breach down**: `X.low < Y.low`.
- **Breach either**: up OR down.
- **Breach both**: up AND down.

---

## 1) Window Range Metrics

### Purpose
Return per-day and aggregate metrics for any window over a trading-date range.

### Inputs
- symbol (or instrument_id)
- trading_date range `[start_date, end_date]`
- window spec (X)
- missing data policy

### Per-day outputs
- open, high, low, close
- range (`high - low`)
- volume
- trades_count
- bar_count, expected_bar_count, is_complete

### Aggregate outputs
- days_in_range
- included_days
- excluded_days (with reason counts)
- mean/median/pXX for range, volume, trades_count

---

## 2) Range Break (X breaches Y)

### Purpose
Count and summarize how often one window breaches another window's high/low.
This is the direct v1 port of your `run_range_break_stats` behavior.

### Inputs
- symbol (or instrument_id)
- trading_date range
- X window (break window)
- Y window (reference window)
- direction filter: `up | down | either | both`
- optional `min_break_size` (points)
- missing data policy (independent tolerance for X and Y)

### Per-day outputs
- y_high, y_low, x_high, x_low
- broke_up (bool), broke_down (bool), broke_either (bool), broke_both (bool)
- break_up_size (`max(0, x_high - y_high)`)
- break_down_size (`max(0, y_low - x_low)`)
- included/excluded + reason

### Aggregate outputs
- total_days_evaluated
- break_up_count / pct
- break_down_count / pct
- break_either_count / pct
- break_both_count / pct
- no_break_count / pct

---

## 3) Rolling Breach Rate

### Purpose
Compute rolling breach probability over N trading days for one symbol or many symbols.

### Inputs
- symbols (1..N)
- trading_date range
- X window
- Y window
- breach direction filter (`up | down | either | both`)
- rolling_length_days (e.g., 20)
- missing data policy

### Outputs
- per symbol, per trading_date:
  - rolling_n
  - rolling_breach_count
  - rolling_breach_rate
  - rolling_excluded_days
- optional cross-symbol summary:
  - avg rolling rate
  - dispersion stats

---

## 4) Session Summary (ON / RTH / Full Day)

### Purpose
Provide a normalized daily summary for standard sessions using the same window metrics pipeline.

### Inputs
- symbol
- trading_date range
- preset windows: `ON`, `RTH`, `FULL_DAY`
- missing data policy

### Per-day outputs (per session)
- open, high, low, close, range
- volume, trades_count
- completeness fields

### Aggregate outputs (per session)
- mean/median/pXX of range
- mean/median/pXX of volume
- mean/median/pXX of trades_count
- included/excluded day counts

---

## 5) Gap + Gap Fill (N close -> N+1 open)

### Purpose
Measure regular-session open gap vs previous regular close and whether that level is filled.
This is the direct v1 port of your `run_gap_fill_stats` behavior.

### Inputs
- symbol
- trading_date range
- previous-day close window (default: prior `RTH` close)
- next-day open window (default: next `RTH` open bar)
- fill check window A (default: next `RTH` session)
- fill check window B (default: next `RTH` first hour)
- missing data policy

### Per-pair outputs (day N -> N+1)
- prev_close
- next_open
- gap_size (`next_open - prev_close`)
- gap_direction (`up | down | flat`)
- filled_in_rth (bool)
- filled_in_first_hour (bool)
- included/excluded + reason

### Aggregate outputs
- total_pairs_evaluated
- fill_rth_count / pct
- fill_first_hour_count / pct
- gap_up_count / pct
- gap_down_count / pct
- gap_flat_count / pct
- gap_up_min/max/avg
- gap_down_min/max/avg

---

## Notes for 4.2+ alignment

- All analyses must consume `WindowSpec` and shared completeness evaluation.
- For a single analysis run, choose one bar resolution globally (`1m` or `30m`) to avoid mixed-granularity drift.
- Every aggregate result should include excluded-day counts and exclusion reasons.
