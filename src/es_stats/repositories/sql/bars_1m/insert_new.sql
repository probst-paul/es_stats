INSERT OR IGNORE INTO bars_1m (
  instrument_id,
  ts_start_utc,
  trading_date_ct_int,
  ct_minute_of_day,
  open,
  high,
  low,
  close,
  volume,
  trades_count,
  source_import_id
)
SELECT
  instrument_id,
  ts_start_utc,
  trading_date_ct_int,
  ct_minute_of_day,
  open,
  high,
  low,
  close,
  volume,
  trades_count,
  source_import_id
FROM tmp_bars_1m;