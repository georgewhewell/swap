CREATE TABLE uniswap (
    "id" text PRIMARY KEY,
    "to" text,
    "sender" text,
    "log_index" bigint,
    "timestamp" timestamptz,
    "amount0_in" decimal(64,32),
    "amount0_out" decimal(64,32),
    "amount1_in" decimal(64,32),
    "amount1_out" decimal(64,32),
    "amount_usd" decimal(64,32),
    "token0" text,
    "token1" text,
    "txn_id" text,
    "block_number" bigint
);

CREATE INDEX ON uniswap ("to");
CREATE INDEX ON uniswap ("sender");
CREATE INDEX ON uniswap ("token0");
CREATE INDEX ON uniswap ("token1");
CREATE INDEX ON uniswap ("timestamp");