CREATE TABLE tokens (
    "symbol" text PRIMARY KEY,
    "name" text NOT NULL,
    "address" text NOT NULL UNIQUE
);

CREATE INDEX ON tokens ("address");
