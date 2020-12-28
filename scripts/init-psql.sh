#!/usr/bin/env bash

set -euf pipefail

DATABASE_URL=${DATABASE_URL?Need a value}

psql $DATABASE_URL -c 'drop table uniswap;' || true
psql $DATABASE_URL -c 'drop table tokens;' || true

psql $DATABASE_URL < sql/uniswap-ddl.sql
psql $DATABASE_URL < sql/tokens-ddl.sql

psql $DATABASE_URL -c 'alter database swaps set synchronous_commit TO off;'