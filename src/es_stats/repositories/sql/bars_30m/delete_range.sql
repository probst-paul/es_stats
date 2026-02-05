DELETE FROM bars_30m
WHERE instrument_id = %(instrument_id)s
  AND trading_date_ct_int BETWEEN %(td_min)s AND %(td_max)s;
