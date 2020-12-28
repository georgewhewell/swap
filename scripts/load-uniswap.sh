#!/usr/bin/env bash

set -euo pipefail

DATABASE_URL=${DATABASE_URL?Need a value}
FIXTURE=${1:-joined.csv}

psql $DATABASE_URL << EOF

begin;

create temp table tmp_swaps
(like uniswap including defaults)
on commit drop;

\copy tmp_swaps from $FIXTURE delimiter ',' csv header

insert into uniswap
select * from tmp_swaps
on conflict do nothing;

commit;
EOF