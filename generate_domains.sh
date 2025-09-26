#!/bin/bash
# Generate full ordered 3-char domains (.com + .lk)
# Each CSV file limited to 10,000 rows

BASE_DIR="$HOME/domain_system"
OUTPUT_FILE="$BASE_DIR/pending/domains.csv"

mkdir -p "$BASE_DIR/pending"

# Clear old file
> "$OUTPUT_FILE"

# Allowed chars (a-z0-9)
CHARS=( {a..z} {0..9} )
N=${#CHARS[@]}   # 36 characters

for c1 in "${CHARS[@]}"; do
  for c2 in "${CHARS[@]}"; do
    for c3 in "${CHARS[@]}"; do
      DOMAIN="$c1$c2$c3"
      echo "$DOMAIN.com" >> "$OUTPUT_FILE"
    done
  done
done

# Split into 10k per file
split -l 10000 -d --additional-suffix=.csv "$OUTPUT_FILE" "$BASE_DIR/pending/domains_part"

rm "$OUTPUT_FILE"
echo "✅ Ordered 3-char domains generated & split into 10k-per-file CSVs in $BASE_DIR/pending/"
