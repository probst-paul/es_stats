INSERT INTO instruments (symbol)
VALUES (%(symbol)s)
ON CONFLICT(symbol) DO UPDATE SET symbol = EXCLUDED.symbol
RETURNING instrument_id;
