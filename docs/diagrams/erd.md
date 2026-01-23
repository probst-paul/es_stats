# ERD Diagrams

## Conceptual ERD

```mermaid
erDiagram
  INSTRUMENT ||--o{ IMPORT_RUN : has
  INSTRUMENT ||--o{ BAR_1M : has
  INSTRUMENT ||--o{ BAR_30M : has

  IMPORT_RUN o|--o{ BAR_1M : may_trace
  IMPORT_RUN o|--o{ BAR_30M : may_trace
````
## Physical ERD
```mermaid
erDiagram
  instruments {
    INTEGER instrument_id
    TEXT symbol
    TEXT description
    TEXT tz_exchange
  }

  imports {
    INTEGER import_id
    INTEGER instrument_id
    TEXT source_name
    TEXT source_hash
    TEXT input_timezone
    INTEGER bar_interval_seconds
    TEXT merge_policy
    INTEGER started_at_utc
    INTEGER finished_at_utc
    INTEGER ts_min_utc
    INTEGER ts_max_utc
    INTEGER row_count_read
    INTEGER row_count_inserted
    INTEGER row_count_updated
    INTEGER row_count_rejected
    TEXT status
    TEXT error_summary
  }

  bars_1m {
    INTEGER instrument_id
    INTEGER ts_start_utc
    INTEGER trading_date_ct_int
    INTEGER ct_minute_of_day
    REAL open
    REAL high
    REAL low
    REAL close
    INTEGER volume
    INTEGER source_import_id
  }

  bars_30m {
    INTEGER instrument_id
    INTEGER bucket_start_utc
    INTEGER trading_date_ct_int
    INTEGER bucket_ct_minute_of_day
    TEXT session
    INTEGER period_index
    TEXT tpo
    REAL open
    REAL high
    REAL low
    REAL close
    INTEGER volume
    INTEGER bar_count_1m
    INTEGER is_complete
    INTEGER derived_from_import_id
  }

  instruments ||--o{ imports : has
  instruments ||--o{ bars_1m : has
  instruments ||--o{ bars_30m : has

  imports o|--o{ bars_1m : may_trace
  imports o|--o{ bars_30m : may_trace
  ```