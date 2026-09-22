CREATE TABLE crypto_trades (
    trade_id BIGINT NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    price DECIMAL(20,8),
    quantity DECIMAL(20,8),
    trade_timestamp DATETIME2,
    buyer_maker BIT,
    CONSTRAINT PK_crypto_trades
        PRIMARY KEY (trade_id, symbol)
);