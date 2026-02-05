UPDATE imports
SET
  finished_at_utc    = %(finished_at_utc)s,
  ts_min_utc         = %(ts_min_utc)s,
  ts_max_utc         = %(ts_max_utc)s,
  row_count_read     = %(row_count_read)s,
  row_count_inserted = %(row_count_inserted)s,
  row_count_updated  = %(row_count_updated)s,
  row_count_rejected = %(row_count_rejected)s,
  status             = %(status)s,
  error_summary      = %(error_summary)s
WHERE import_id = %(import_id)s;
