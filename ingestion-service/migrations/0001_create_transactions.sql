CREATE TABLE IF NOT EXISTS transactions (
    hash TEXT PRIMARY KEY,
    from_address TEXT NOT NULL,
    to_address TEXT,
    amount_wei NUMERIC NOT NULL,
    timestamp BIGINT NOT NULL
);