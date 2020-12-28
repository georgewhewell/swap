#!/usr/bin/env bash

set -euo pipefail

DATABASE_URL=${DATABASE_URL?Need a value}
FIXTURE=${1:-data/tokens.csv}

psql $DATABASE_URL << EOF

begin;

create temp table tmp_tokens
(like tokens including defaults)
on commit drop;

\copy tmp_tokens from $FIXTURE delimiter ',' csv header

insert into tokens
select * from tmp_tokens
on conflict do nothing;

commit;
EOF