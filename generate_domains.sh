#!/bin/bash
# Generate full ordered 3-6 char domains (.com + .lk)
# Each CSV file limited to 10,000 rows (streamed, not post-split)

BASE_DIR="$HOME/domain_system"
OUTPUT_DIR="$BASE_DIR/pending"

mkdir -p "$OUTPUT_DIR"

# Allowed chars (a-z0-9)
CHARS=( {a..z} {0..9} )
N=${#CHARS[@]}   # 36 characters

FILE_INDEX=0
ROW_COUNT=0
MAX_ROWS=10000
CURRENT_FILE="$OUTPUT_DIR/domains_part$(printf "%04d" $FILE_INDEX).csv"

# function to write a domain and handle splitting
write_domain() {
  local domain=$1
  echo "$domain" >> "$CURRENT_FILE"
  ROW_COUNT=$((ROW_COUNT + 1))

  if (( ROW_COUNT >= MAX_ROWS )); then
    FILE_INDEX=$((FILE_INDEX + 1))
    CURRENT_FILE="$OUTPUT_DIR/domains_part$(printf "%04d" $FILE_INDEX).csv"
    ROW_COUNT=0
  fi
}

generate_domains() {
  local length=$1
  local prefix=$2

  if [[ ${#prefix} -eq $length ]]; then
    write_domain "$prefix.com"
    write_domain "$prefix.lk"
    return
  fi

  for c in "${CHARS[@]}"; do
    generate_domains "$length" "$prefix$c"
  done
}

# Generate domains of length 3 to 6
for len in {3..6}; do
  echo "🔄 Generating $len-char domains..."
  generate_domains "$len" ""
done

echo "✅ Ordered 3-6 char domains (.com + .lk) generated in 10,000-row CSV files at $OUTPUT_DIR/"
