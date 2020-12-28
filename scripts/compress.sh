#!/usr/bin/env bash

set -euo pipefail

for f in data/uniswap/**/**/*.csv; do zstd -16 -T0 --rm "$f"; done