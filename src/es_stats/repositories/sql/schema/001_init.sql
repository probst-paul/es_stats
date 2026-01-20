PRAGMA foreign_keys = ON;

BEGIN;

-- instruments
CREATE TABLE IF NOT EXISTS instruments (
  instrument_id INTEGER PRIMARY KEY,
  symbol        TEXT    NOT NULL UNIQUE,
  description   TEXT    NULL,
  tz_exchange   TEXT    NOT NULL DEFAULT 'America/Chicago'
);

-- imports (audit trail)
CREATE TABLE IF NOT EXISTS imports (
  import_id            INTEGER PRIMARY KEY,
  instrument_id        INTEGER NOT NULL,
  source_name          TEXT    NOT NULL,
  source_hash          TEXT    NULL,
  input_timezone       TEXT    NOT NULL DEFAULT 'America/Chicago',
  bar_interval_seconds INTEGER NOT NULL DEFAULT 60,

  merge_policy         TEXT    NOT NULL CHECK (merge_policy IN ('skip','overwrite')),

  started_at_utc       INTEGER NOT NULL,
  finished_at_utc      INTEGER NULL,

  ts_min_utc           INTEGER NULL,
  ts_max_utc           INTEGER NULL,

  row_count_read       INTEGER NOT NULL DEFAULT 0,
  row_count_inserted   INTEGER NOT NULL DEFAULT 0,
  row_count_updated    INTEGER NOT NULL DEFAULT 0,
  row_count_rejected   INTEGER NOT NULL DEFAULT 0,

  status               TEXT    NOT NULL CHECK (status IN ('success','failed')),
  error_summary        TEXT    NULL,

  FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id)
);

CREATE INDEX IF NOT EXISTS idx_imports_instrument_started
  ON imports(instrument_id, started_at_utc DESC);

-- bars_1m (canonical)
CREATE TABLE IF NOT EXISTS bars_1m (
  instrument_id       INTEGER NOT NULL,
  ts_start_utc        INTEGER NOT NULL,

  trading_date_ct_int INTEGER NOT NULL,
  ct_minute_of_day    INTEGER NOT NULL CHECK (ct_minute_of_day BETWEEN 0 AND 1439),

  open                REAL    NOT NULL,
  high                REAL    NOT NULL,
  low                 REAL    NOT NULL,
  close               REAL    NOT NULL,
  volume              INTEGER NOT NULL CHECK (volume >= 0),
  trades_count        INTEGER NOT NULL CHECK (trades_count >= 0),

  source_import_id    INTEGER NULL,

  PRIMARY KEY (instrument_id, ts_start_utc),
  FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id),
  FOREIGN KEY (source_import_id) REFERENCES imports(import_id),

  CHECK (high >= low)
);

CREATE INDEX IF NOT EXISTS idx_bars_1m_inst_day_minute
  ON bars_1m(instrument_id, trading_date_ct_int, ct_minute_of_day, ts_start_utc);

CREATE INDEX IF NOT EXISTS idx_bars_1m_inst_day_ts
  ON bars_1m(instrument_id, trading_date_ct_int, ts_start_utc);

-- bars_30m (derived)
CREATE TABLE IF NOT EXISTS bars_30m (
  instrument_id          INTEGER NOT NULL,
  bucket_start_utc       INTEGER NOT NULL,

  trading_date_ct_int    INTEGER NOT NULL,
  bucket_ct_minute_of_day INTEGER NOT NULL
    CHECK (bucket_ct_minute_of_day BETWEEN 0 AND 1439)
    CHECK (bucket_ct_minute_of_day % 30 = 0),

  session                TEXT    NULL CHECK (session IN ('ON','RTH') OR session IS NULL),
  period_index           INTEGER NULL,
  tpo                    TEXT    NULL,

  open                   REAL    NOT NULL,
  high                   REAL    NOT NULL,
  low                    REAL    NOT NULL,
  close                  REAL    NOT NULL,
  volume                 INTEGER NOT NULL CHECK (volume >= 0),
  trades_count           INTEGER NOT NULL CHECK (trades_count >= 0),

  bar_count_1m           INTEGER NOT NULL CHECK (bar_count_1m BETWEEN 0 AND 30),
  is_complete            INTEGER NOT NULL DEFAULT 0 CHECK (is_complete IN (0,1)),

  derived_from_import_id INTEGER NULL,

  PRIMARY KEY (instrument_id, bucket_start_utc),
  FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id),
  FOREIGN KEY (derived_from_import_id) REFERENCES imports(import_id),

  CHECK (high >= low)
);

CREATE INDEX IF NOT EXISTS idx_bars_30m_inst_day_ts
  ON bars_30m(instrument_id, trading_date_ct_int, bucket_start_utc);

CREATE INDEX IF NOT EXISTS idx_bars_30m_inst_day_session_period
  ON bars_30m(instrument_id, trading_date_ct_int, session, period_index);

COMMIT;