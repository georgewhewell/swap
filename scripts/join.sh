#!/usr/bin/env zsh

set -euo pipefail

zstdcat data/**/*.csv.zst([1]) | head -1 > joined.csv
for x in data/**/*.csv.zst; do
  echo "Unpacking $x"
  zstdcat $x | tail -n +2 -q >> joined.csv;
done
