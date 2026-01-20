WITH agg AS (
  SELECT
    instrument_id,
    trading_date_ct_int,
    (ct_minute_of_day - (ct_minute_of_day % 30)) AS bucket_ct_minute_of_day,
    MIN(ts_start_utc) AS bucket_start_utc,
    MAX(ts_start_utc) AS bucket_last_utc,
    MAX(high) AS high,
    MIN(low)  AS low,
    SUM(volume) AS volume,
    SUM(trades_count) AS trades_count,
    COUNT(*) AS bar_count_1m
  FROM bars_1m
  WHERE instrument_id = :instrument_id
    AND trading_date_ct_int BETWEEN :td_min AND :td_max
  GROUP BY instrument_id, trading_date_ct_int, bucket_ct_minute_of_day
),
oc AS (
  SELECT
    a.*,
    bo.open  AS open,
    bc.close AS close
  FROM agg a
  JOIN bars_1m bo
    ON bo.instrument_id = a.instrument_id
   AND bo.ts_start_utc  = a.bucket_start_utc
  JOIN bars_1m bc
    ON bc.instrument_id = a.instrument_id
   AND bc.ts_start_utc  = a.bucket_last_utc
),
classified AS (
  SELECT
    instrument_id,
    bucket_start_utc,
    trading_date_ct_int,
    bucket_ct_minute_of_day,

    CASE
      WHEN bucket_ct_minute_of_day >= 1020 OR bucket_ct_minute_of_day < 510 THEN 'ON'
      WHEN bucket_ct_minute_of_day >= 510 AND bucket_ct_minute_of_day <= 930 THEN 'RTH'
      ELSE NULL
    END AS session,

    /* NOTE: force integer period_index (SQLite / is floating division) */
    CASE
      WHEN bucket_ct_minute_of_day >= 1020 THEN CAST((bucket_ct_minute_of_day - 1020) / 30 AS INTEGER)
      WHEN bucket_ct_minute_of_day < 510  THEN CAST((420 + bucket_ct_minute_of_day) / 30 AS INTEGER)
      WHEN bucket_ct_minute_of_day >= 510 AND bucket_ct_minute_of_day <= 930
           THEN CAST((bucket_ct_minute_of_day - 510) / 30 AS INTEGER)
      ELSE NULL
    END AS period_index,

    /* Option A: base-26 Excel-style labels beyond 25: a..z, aa..az, ba.. ; A..Z, AA..AZ, ... */
    CASE
      WHEN (bucket_ct_minute_of_day >= 1020 OR bucket_ct_minute_of_day < 510) THEN
        CASE
          WHEN (
            CASE
              WHEN bucket_ct_minute_of_day >= 1020 THEN CAST((bucket_ct_minute_of_day - 1020) / 30 AS INTEGER)
              ELSE CAST((420 + bucket_ct_minute_of_day) / 30 AS INTEGER)
            END
          ) BETWEEN 0 AND 25 THEN
            CHAR(97 + (
              CASE
                WHEN bucket_ct_minute_of_day >= 1020 THEN CAST((bucket_ct_minute_of_day - 1020) / 30 AS INTEGER)
                ELSE CAST((420 + bucket_ct_minute_of_day) / 30 AS INTEGER)
              END
            ))
          WHEN (
            CASE
              WHEN bucket_ct_minute_of_day >= 1020 THEN CAST((bucket_ct_minute_of_day - 1020) / 30 AS INTEGER)
              ELSE CAST((420 + bucket_ct_minute_of_day) / 30 AS INTEGER)
            END
          ) BETWEEN 26 AND 701 THEN
            CHAR(97 + (CAST((
              CASE
                WHEN bucket_ct_minute_of_day >= 1020 THEN CAST((bucket_ct_minute_of_day - 1020) / 30 AS INTEGER)
                ELSE CAST((420 + bucket_ct_minute_of_day) / 30 AS INTEGER)
              END
            ) / 26 AS INTEGER) - 1))
            || CHAR(97 + ((
              CASE
                WHEN bucket_ct_minute_of_day >= 1020 THEN CAST((bucket_ct_minute_of_day - 1020) / 30 AS INTEGER)
                ELSE CAST((420 + bucket_ct_minute_of_day) / 30 AS INTEGER)
              END
            ) % 26))
          ELSE NULL
        END

      WHEN (bucket_ct_minute_of_day >= 510 AND bucket_ct_minute_of_day <= 930) THEN
        CASE
          WHEN CAST((bucket_ct_minute_of_day - 510) / 30 AS INTEGER) BETWEEN 0 AND 25 THEN
            CHAR(65 + CAST((bucket_ct_minute_of_day - 510) / 30 AS INTEGER))
          WHEN CAST((bucket_ct_minute_of_day - 510) / 30 AS INTEGER) BETWEEN 26 AND 701 THEN
            CHAR(65 + (CAST(CAST((bucket_ct_minute_of_day - 510) / 30 AS INTEGER) / 26 AS INTEGER) - 1))
            || CHAR(65 + (CAST((bucket_ct_minute_of_day - 510) / 30 AS INTEGER) % 26))
          ELSE NULL
        END

      ELSE NULL
    END AS tpo,

    open, high, low, close,
    volume,
    trades_count,
    bar_count_1m,
    CASE WHEN bar_count_1m = 30 THEN 1 ELSE 0 END AS is_complete
  FROM oc
)
INSERT INTO bars_30m (
  instrument_id,
  bucket_start_utc,
  trading_date_ct_int,
  bucket_ct_minute_of_day,
  session,
  period_index,
  tpo,
  open, high, low, close,
  volume,
  trades_count,
  bar_count_1m,
  is_complete,
  derived_from_import_id
)
SELECT
  instrument_id,
  bucket_start_utc,
  trading_date_ct_int,
  bucket_ct_minute_of_day,
  session,
  period_index,
  tpo,
  open, high, low, close,
  volume,
  trades_count,
  bar_count_1m,
  is_complete,
  :derived_from_import_id
FROM classified;