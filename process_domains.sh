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

# State files for counters
ACTIVE_STATE="$STATE_DIR/active_state"
FAIL_STATE="$STATE_DIR/fail_state"

get_tld() { domain=$1; echo "${domain##*.}"; }

is_valid_ip() {
    local IPS=$1
    for ip in ${IPS//,/ }; do
        if ! [[ $ip =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then return 1; fi
    done
    return 0
}

# Initialize or read state
init_state() {
    # Create state files if don't exist
    if [[ ! -f "$ACTIVE_STATE.part" ]]; then
        echo "1" > "$ACTIVE_STATE.part"
    fi
    if [[ ! -f "$ACTIVE_STATE.count" ]]; then
        echo "0" > "$ACTIVE_STATE.count"
    fi
    
    if [[ ! -f "$FAIL_STATE.part" ]]; then
        echo "1" > "$FAIL_STATE.part"
    fi
    if [[ ! -f "$FAIL_STATE.count" ]]; then
        echo "0" > "$FAIL_STATE.count"
    fi
    
    # Resume from existing state - calculate current counts from actual files
    local active_part=$(cat "$ACTIVE_STATE.part")
    local fail_part=$(cat "$FAIL_STATE.part")
    
    local active_file="$RESULTS_ACTIVE/active_part${active_part}.csv"
    local fail_file="$RESULTS_FAIL/fail_part${fail_part}.csv"
    
    # Calculate actual row count (excluding header)
    local active_count=0
    if [[ -f "$active_file" ]]; then
        active_count=$(($(wc -l < "$active_file") - 1))
        if (( active_count < 0 )); then active_count=0; fi
    fi
    
    local fail_count=0  
    if [[ -f "$fail_file" ]]; then
        fail_count=$(($(wc -l < "$fail_file") - 1))
        if (( fail_count < 0 )); then fail_count=0; fi
    fi
    
    # Update state with actual counts
    echo "$active_count" > "$ACTIVE_STATE.count"
    echo "$fail_count" > "$FAIL_STATE.count"
    
    echo "Resuming from:"
    echo "- Active: part $active_part, count $active_count/$MAX_ROWS"
    echo "- Fail: part $fail_part, count $fail_count/$MAX_ROWS"
}

# Get current file and count (thread-safe)
get_current_file() {
    local type=$1
    local state_file=""
    local results_dir=""
    local file_prefix=""
    
    if [[ $type == "active" ]]; then
        state_file="$ACTIVE_STATE"
        results_dir="$RESULTS_ACTIVE"
        file_prefix="active_part"
    else
        state_file="$FAIL_STATE"
        results_dir="$RESULTS_FAIL"
        file_prefix="fail_part"
    fi
    
    (
        flock -x 200
        local current_part=$(cat "$state_file.part" 2>/dev/null || echo "1")
        local current_count=$(cat "$state_file.count" 2>/dev/null || echo "0")
        local current_file="$results_dir/${file_prefix}${current_part}.csv"
        
        echo "DEBUG: $type - part: $current_part, count: $current_count, max: $MAX_ROWS" >&2
        
        # Check if we need to rotate BEFORE getting file
        if (( current_count >= MAX_ROWS )); then
            ((current_part++))
            current_count=0
            current_file="$results_dir/${file_prefix}${current_part}.csv"
            
            echo "$current_part" > "$state_file.part"
            echo "$current_count" > "$state_file.count"
            echo "Rotated to new $type file: ${file_prefix}${current_part}.csv" >&2
        fi
        
        # Create file with header if doesn't exist
        if [[ ! -f "$current_file" ]]; then
            if [[ $type == "active" ]]; then
                echo "second_level_domain,top_level_domain,ip_address,status,timestamp" > "$current_file"
            else
                echo "second_level_domain,top_level_domain,reason,timestamp" > "$current_file"
            fi
            echo "Created new $type file: $current_file" >&2
        fi
        
        echo "$current_file"
    ) 200>/tmp/domain_state_${type}.lock
}

# Increment counter and return new count
increment_counter() {
    local type=$1
    local state_file=""
    
    if [[ $type == "active" ]]; then
        state_file="$ACTIVE_STATE"
    else
        state_file="$FAIL_STATE"
    fi
    
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

    local SUCCESS=0
    local attempt=0
    local IPS=""

    while [[ $SUCCESS -eq 0 && $attempt -lt $RETRIES ]]; do
        ((attempt++))
        for RESOLVER in "${RESOLVERS[@]}"; do
            IPS=$(timeout "$TIMEOUT" nslookup "$domain" "$RESOLVER" 2>/dev/null \
                  | awk '/^Address: / {print $2}' \
                  | tr '\n' ',' | sed 's/,$//')
            if [[ -n "$IPS" ]] && is_valid_ip "$IPS"; then
                SUCCESS=1
                break
            fi
        done
    done

    SLD="${domain%%.*}"
    TLD=$(get_tld "$domain")
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

    if [[ $SUCCESS -eq 1 ]]; then
        local active_file=$(get_current_file "active")
        echo "$SLD,$TLD,$IPS,Active,$TIMESTAMP" >> "$active_file"
        echo "$SLD,$TLD,$IPS,Active,$TIMESTAMP" >> "$LOGFILE"
        local count=$(increment_counter "active")
        echo "Active: $domain -> $(basename "$active_file") (count: $count/$MAX_ROWS)"
    else
        local REASON="NXDOMAIN/Timeout"
        local fail_file=$(get_current_file "fail")
        echo "$SLD,$TLD,$REASON,$TIMESTAMP" >> "$fail_file"
        echo "$SLD,$TLD,$REASON,$TIMESTAMP" >> "$LOGFILE"
        local count=$(increment_counter "fail")
        echo "Failed: $domain -> $(basename "$fail_file") (count: $count/$MAX_ROWS)"
    fi
}

# Run jobs with parallel control
run_job() {
    local d=$1
    local LOGFILE=$2
    check_domain "$d" "$LOGFILE" &

    # limit jobs to MAX_JOBS
    while (( $(jobs -r | wc -l) >= MAX_JOBS )); do
        sleep 0.1
    done
}

process_file() {
    local FILE=$1
    local BASENAME=$(basename "$FILE")
    local LOGFILE="$LOGS/${BASENAME%.csv}.log"

    echo "Processing $BASENAME ..." | tee -a "$LOGFILE"
    init_state

    # Get already processed domains from ALL existing files
    echo "Checking already processed domains..."
    declare -A done
    
    # Check all active files
    for active_file in "$RESULTS_ACTIVE"/active_part*.csv; do
        if [[ -f "$active_file" ]]; then
            echo "Checking $(basename "$active_file")..."
            while IFS=',' read -r sld tld ip status timestamp || [[ -n "$sld" ]]; do
                [[ "$sld" == "second_level_domain" ]] && continue  # Skip header
                [[ -n "$sld" ]] && done["$sld"]=1
            done < "$active_file"
        fi
    done
    
    # Check all fail files  
    for fail_file in "$RESULTS_FAIL"/fail_part*.csv; do
        if [[ -f "$fail_file" ]]; then
            echo "Checking $(basename "$fail_file")..."
            while IFS=',' read -r sld tld reason timestamp || [[ -n "$sld" ]]; do
                [[ "$sld" == "second_level_domain" ]] && continue  # Skip header
                [[ -n "$sld" ]] && done["$sld"]=1
            done < "$fail_file"
        fi
    done

    # Get domains to process (skip already processed)
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

# Progress display function
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

# Add cleanup function for graceful shutdown
cleanup() {
    echo ""
    echo "Script interrupted! Cleaning up..."
    
    # Wait for running jobs to complete
    echo "Waiting for running jobs to complete..."
    wait
    
    # Show final status
    show_progress
    
    echo "Script stopped. State saved. You can resume by running the script again."
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Main processing loop
echo "Starting domain processing..."
echo "Press Ctrl+C to stop gracefully"

for FILE in "$PENDING_DIR"/*.csv; do
    [ -e "$FILE" ] || continue
    process_file "$FILE"
    show_progress
done

echo "All pending files processed."

# Final summary
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