INSERT INTO instruments (symbol)
VALUES (:symbol)
ON CONFLICT(symbol) DO UPDATE SET symbol = excluded.symbol
RETURNING instrument_id;