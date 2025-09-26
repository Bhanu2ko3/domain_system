#!/bin/bash
# Resumable CSV file processing with row-count based output splitting and restart support

BASE_DIR="$HOME/domain_system"

PENDING_DIR="$BASE_DIR/pending"
PROCESSING_DIR="$BASE_DIR/processing"
COMPLETED_DIR="$BASE_DIR/completed"
RESULTS_ACTIVE="$BASE_DIR/results/active"
RESULTS_FAIL="$BASE_DIR/results/fail"
LOGS="$BASE_DIR/logs"
STATE_DIR="$BASE_DIR/state"

RETRIES=3          # number of retry attempts per domain
TIMEOUT=4          # timeout per nslookup call (seconds)
MAX_JOBS=50        # max parallel jobs at a time
MAX_ROWS=100       # rows per output file

# 10 resolvers 
RESOLVERS=(
    "8.8.8.8"
    "8.8.4.4"
    "1.1.1.1"
    "1.0.0.1"
    "9.9.9.9"
    "149.112.112.112"
    "208.67.222.222"
    "208.67.220.220"
    "4.2.2.1"
    "4.2.2.2"
)

mkdir -p "$PROCESSING_DIR" "$RESULTS_ACTIVE" "$RESULTS_FAIL" "$LOGS" "$COMPLETED_DIR" "$STATE_DIR"

ACTIVE_STATE="$STATE_DIR/active_state"
FAIL_STATE="$STATE_DIR/fail_state"

get_tld() { domain=$1; echo "${domain##*.}"; }

is_valid_ip() {
    local IPS=$1
    for ip in ${IPS//|/ }; do   # UPDATED: split by | instead of comma
        if ! [[ $ip =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then return 1; fi
    done
    return 0
}

init_state() {
    [[ ! -f "$ACTIVE_STATE.part" ]] && echo "1" > "$ACTIVE_STATE.part"
    [[ ! -f "$ACTIVE_STATE.count" ]] && echo "0" > "$ACTIVE_STATE.count"
    [[ ! -f "$FAIL_STATE.part" ]] && echo "1" > "$FAIL_STATE.part"
    [[ ! -f "$FAIL_STATE.count" ]] && echo "0" > "$FAIL_STATE.count"

    local active_part=$(cat "$ACTIVE_STATE.part")
    local fail_part=$(cat "$FAIL_STATE.part")

    local active_file="$RESULTS_ACTIVE/active_part${active_part}.csv"
    local fail_file="$RESULTS_FAIL/fail_part${fail_part}.csv"

    local active_count=0
    [[ -f "$active_file" ]] && active_count=$(($(wc -l < "$active_file") - 1))
    ((active_count<0)) && active_count=0

    local fail_count=0
    [[ -f "$fail_file" ]] && fail_count=$(($(wc -l < "$fail_file") - 1))
    ((fail_count<0)) && fail_count=0

    echo "$active_count" > "$ACTIVE_STATE.count"
    echo "$fail_count" > "$FAIL_STATE.count"
}

get_current_file() {
    local type=$1
    local state_file results_dir file_prefix

    if [[ $type == "active" ]]; then
        state_file="$ACTIVE_STATE"; results_dir="$RESULTS_ACTIVE"; file_prefix="active_part"
    else
        state_file="$FAIL_STATE"; results_dir="$RESULTS_FAIL"; file_prefix="fail_part"
    fi

    (
        flock -x 200
        local current_part=$(cat "$state_file.part" 2>/dev/null || echo "1")
        local current_count=$(cat "$state_file.count" 2>/dev/null || echo "0")
        local current_file="$results_dir/${file_prefix}${current_part}.csv"

        (( current_count >= MAX_ROWS )) && {
            ((current_part++))
            current_count=0
            current_file="$results_dir/${file_prefix}${current_part}.csv"
            echo "$current_part" > "$state_file.part"
            echo "$current_count" > "$state_file.count"
        }

        [[ ! -f "$current_file" ]] && {
            if [[ $type == "active" ]]; then
                echo "second_level_domain,top_level_domain,ip_address,status,timestamp" > "$current_file"
            else
                echo "second_level_domain,top_level_domain,reason,timestamp" > "$current_file"
            fi
        }

        echo "$current_file"
    ) 200>/tmp/domain_state_${type}.lock
}

increment_counter() {
    local type=$1
    local state_file
    [[ $type == "active" ]] && state_file="$ACTIVE_STATE" || state_file="$FAIL_STATE"

    (
        flock -x 200
        local current_count=$(cat "$state_file.count" 2>/dev/null || echo "0")
        ((current_count++))
        echo "$current_count" > "$state_file.count"
        echo "$current_count"
    ) 200>/tmp/domain_counter_${type}.lock
}

check_domain() {
    local domain=$1
    local LOGFILE=$2
    local SUCCESS=0 attempt=0 IPS=""

    while [[ $SUCCESS -eq 0 && $attempt -lt $RETRIES ]]; do
        ((attempt++))
        for RESOLVER in "${RESOLVERS[@]}"; do
            IPS=$(timeout "$TIMEOUT" nslookup "$domain" "$RESOLVER" 2>/dev/null \
                  | awk '/^Address: / {print $2}' \
                  | tr '\n' '|' | sed 's/|$//')   # UPDATED: join IPs with |

            [[ -n "$IPS" ]] && is_valid_ip "$IPS" && { SUCCESS=1; break; }
        done
    done

    SLD="${domain%%.*}"
    TLD=$(get_tld "$domain")
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

    if [[ $SUCCESS -eq 1 ]]; then
        local active_file=$(get_current_file "active")
        echo "$SLD,$TLD,$IPS,Active,$TIMESTAMP" >> "$active_file"
        echo "$SLD,$TLD,$IPS,Active,$TIMESTAMP" >> "$LOGFILE"
        increment_counter "active" >/dev/null
    else
        local REASON="NXDOMAIN/Timeout"
        local fail_file=$(get_current_file "fail")
        echo "$SLD,$TLD,$REASON,$TIMESTAMP" >> "$fail_file"
        echo "$SLD,$TLD,$REASON,$TIMESTAMP" >> "$LOGFILE"
        increment_counter "fail" >/dev/null
    fi
}

run_job() {
    local d=$1 LOGFILE=$2
    check_domain "$d" "$LOGFILE" &
    while (( $(jobs -r | wc -l) >= MAX_JOBS )); do sleep 0.1; done
}

process_file() {
    local FILE=$1 BASENAME=$(basename "$FILE") LOGFILE="$LOGS/${BASENAME%.csv}.log"
    echo "Processing $BASENAME ..." | tee -a "$LOGFILE"
    init_state

    declare -A done
    for active_file in "$RESULTS_ACTIVE"/active_part*.csv; do
        [[ -f "$active_file" ]] || continue
        while IFS=',' read -r sld tld ip status timestamp || [[ -n "$sld" ]]; do
            [[ "$sld" == "second_level_domain" ]] && continue
            [[ -n "$sld" ]] && done["$sld"]=1
        done < "$active_file"
    done

    for fail_file in "$RESULTS_FAIL"/fail_part*.csv; do
        [[ -f "$fail_file" ]] || continue
        while IFS=',' read -r sld tld reason timestamp || [[ -n "$sld" ]]; do
            [[ "$sld" == "second_level_domain" ]] && continue
            [[ -n "$sld" ]] && done["$sld"]=1
        done < "$fail_file"
    done

    domains=()
    while IFS= read -r DOMAIN || [[ -n "$DOMAIN" ]]; do
        [[ -z "$DOMAIN" ]] && continue
        SLD="${DOMAIN%%.*}"
        [[ -n "${done[$SLD]}" ]] && continue
        domains+=("$DOMAIN")
    done < "$FILE"

    total=${#domains[@]}
    processed_total=$(($(wc -l < "$FILE") - total))
    echo "Total domains in file: $(wc -l < "$FILE")" | tee -a "$LOGFILE"
    echo "Already processed: $processed_total" | tee -a "$LOGFILE" 
    echo "Remaining to process: $total" | tee -a "$LOGFILE"

    if (( total == 0 )); then
        echo "All domains already processed!" | tee -a "$LOGFILE"
        mv "$FILE" "$COMPLETED_DIR/"
        return
    fi

    for d in "${domains[@]}"; do
        run_job "$d" "$LOGFILE"
    done
    wait

    mv "$FILE" "$COMPLETED_DIR/"
    echo "Completed $BASENAME at $(date)" | tee -a "$LOGFILE"
}

show_progress() {
    local active_part=$(cat "$ACTIVE_STATE.part" 2>/dev/null || echo "1")
    local active_count=$(cat "$ACTIVE_STATE.count" 2>/dev/null || echo "0")
    local fail_part=$(cat "$FAIL_STATE.part" 2>/dev/null || echo "1")
    local fail_count=$(cat "$FAIL_STATE.count" 2>/dev/null || echo "0")
    
    echo "================================="
    echo "CURRENT STATUS:"
    echo "Active file: active_part${active_part}.csv (rows: $active_count/$MAX_ROWS)"
    echo "Fail file: fail_part${fail_part}.csv (rows: $fail_count/$MAX_ROWS)"
    echo "================================="
}

cleanup() {
    echo ""
    echo "Script interrupted! Cleaning up..."
    wait
    show_progress
    echo "Script stopped. State saved. You can resume by running the script again."
    exit 0
}

trap cleanup SIGINT SIGTERM

echo "Starting domain processing..."
echo "Press Ctrl+C to stop gracefully"

for FILE in "$PENDING_DIR"/*.csv; do
    [ -e "$FILE" ] || continue
    process_file "$FILE"
    show_progress
done

echo "All pending files processed."

active_part=$(cat "$ACTIVE_STATE.part" 2>/dev/null || echo "1")
active_count=$(cat "$ACTIVE_STATE.count" 2>/dev/null || echo "0")
fail_part=$(cat "$FAIL_STATE.part" 2>/dev/null || echo "1")
fail_count=$(cat "$FAIL_STATE.count" 2>/dev/null || echo "0")

total_active=$((active_count + (active_part > 1 ? (active_part - 1) * MAX_ROWS : 0)))
total_fail=$((fail_count + (fail_part > 1 ? (fail_part - 1) * MAX_ROWS : 0)))

echo "Final Results:"
echo "- Active files created: $active_part"
echo "- Fail files created: $fail_part"  
echo "- Total active domains: $total_active"
echo "- Total failed domains: $total_fail"
