CREATE TABLE IF NOT EXISTS trades (
    trade_id UUID PRIMARY KEY,
    portfolio_id UUID,
    ticker VARCHAR(10),
    side VARCHAR(4),
    quantity NUMERIC(18,2),
    price NUMERIC(18,2),
    trade_value NUMERIC(18,2),
    fees NUMERIC(18,2),
    trade_timestamp TIMESTAMP DEFAULT NOW()
);