#!/bin/bash
# Database import script for domain processing results

# Database config (use ~/.my.cnf for credentials)
DB_HOST="localhost"
DB_NAME="domain_db"
DB_USER="root"    # actual auth via .my.cnf

# Directories
BASE_DIR="$HOME/domain_system"

RESULTS_ACTIVE="$BASE_DIR/results/active"
RESULTS_FAIL="$BASE_DIR/results/fail"
COMPLETED_DIR="$BASE_DIR/completed"
PENDING_DIR="$BASE_DIR/pending"
PROCESSING_DIR="$BASE_DIR/processing"
LOGS="$BASE_DIR/logs"
DB_LOGS="$BASE_DIR/db_logs"

mkdir -p "$DB_LOGS"

# Log file
LOG_FILE="$DB_LOGS/import_$(date +%Y%m%d_%H%M%S).log"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"; }
error() { echo -e "${RED}[ERROR] $1${NC}" | tee -a "$LOG_FILE"; }
success() { echo -e "${GREEN}[SUCCESS] $1${NC}" | tee -a "$LOG_FILE"; }
warning() { echo -e "${YELLOW}[WARNING] $1${NC}" | tee -a "$LOG_FILE"; }

# Test DB connection
test_db_connection() {
    log "Testing database connection..."
    if mysql -h "$DB_HOST" -u "$DB_USER" -e "USE $DB_NAME;" 2>/dev/null; then
        success "Database connection successful"
        log "Enabling local_infile..."
        mysql -h "$DB_HOST" -u "$DB_USER" -e "SET GLOBAL local_infile = 1;" 2>/dev/null
        return 0
    else
        error "Database connection failed"
        return 1
    fi
}

# Create DB schema
create_database_schema() {
    log "Creating database schema..."
    mysql -h "$DB_HOST" -u "$DB_USER" << EOF
CREATE DATABASE IF NOT EXISTS $DB_NAME;
USE $DB_NAME;

CREATE TABLE IF NOT EXISTS active_domains (
    id INT AUTO_INCREMENT PRIMARY KEY,
    second_level_domain VARCHAR(255) NOT NULL,
    top_level_domain VARCHAR(10) NOT NULL,
    ip_address TEXT,
    status ENUM('Active', 'Inactive') DEFAULT 'Active',
    timestamp DATETIME NOT NULL,
    INDEX idx_sld (second_level_domain),
    INDEX idx_tld (top_level_domain),
    INDEX idx_timestamp (timestamp),
    UNIQUE KEY unique_domain (second_level_domain, top_level_domain)
);

CREATE TABLE IF NOT EXISTS failed_domains (
    id INT AUTO_INCREMENT PRIMARY KEY,
    second_level_domain VARCHAR(255) NOT NULL,
    top_level_domain VARCHAR(10) NOT NULL,
    reason VARCHAR(255),
    timestamp DATETIME NOT NULL,
    INDEX idx_sld (second_level_domain),
    INDEX idx_tld (top_level_domain),
    INDEX idx_reason (reason),
    INDEX idx_timestamp (timestamp),
    UNIQUE KEY unique_domain (second_level_domain, top_level_domain)
);

CREATE TABLE IF NOT EXISTS csv_files (
    id INT AUTO_INCREMENT PRIMARY KEY,
    filename VARCHAR(255) NOT NULL UNIQUE,
    status ENUM('Pending','Processing','Completed') DEFAULT 'Pending',
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    import_timestamp DATETIME,
    active_count INT DEFAULT 0,
    failed_count INT DEFAULT 0,
    INDEX idx_filename (filename),
    INDEX idx_status (status),
    INDEX idx_timestamp (timestamp)
);
EOF

    [ $? -eq 0 ] && success "Database schema created successfully" || { error "Failed to create schema"; return 1; }
}

# Import CSV via LOAD DATA LOCAL INFILE
import_csv() {
    local table="$1"
    local csv_file="$2"
    local temp_table="temp_${table}"
    local columns="$3"

    log "Importing $table from $csv_file"

    [ ! -f "$csv_file" ] && { warning "$csv_file not found"; echo 0; return; }

    local total_records=$(($(wc -l < "$csv_file") - 1))
    [ $total_records -le 0 ] && { warning "No data in $csv_file"; echo 0; return; }

    # Attempt LOAD DATA
    local imported=$(mysql --local-infile=1 -h "$DB_HOST" -u "$DB_USER" "$DB_NAME" 2>&1 << EOF
SET SESSION local_infile = 1;

CREATE TEMPORARY TABLE $temp_table ($columns);

LOAD DATA LOCAL INFILE '$csv_file'
INTO TABLE $temp_table
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

INSERT IGNORE INTO $table SELECT * FROM $temp_table;

SELECT ROW_COUNT() as imported_count;
EOF
)

    # If LOAD DATA fails, fallback
    if echo "$imported" | grep -q "ERROR\|rejected"; then
        warning "LOAD DATA failed, fallback to line-by-line insert..."
        imported=$(import_csv_line_by_line "$table" "$csv_file")
    else
        imported=$(echo "$imported" | tail -n1 | grep -o '[0-9]\+' | head -n1)
        [ -z "$imported" ] && imported=0
    fi

    success "Imported $imported records into $table"
    echo "$imported"
}

# Fallback line-by-line insert
import_csv_line_by_line() {
    local table="$1"
    local csv_file="$2"
    local imported=0

    tail -n +2 "$csv_file" | while IFS=',' read -r a b c d e; do
        # Remove quotes
        a=$(echo "$a" | sed 's/^"//;s/"$//')
        b=$(echo "$b" | sed 's/^"//;s/"$//')
        c=$(echo "$c" | sed 's/^"//;s/"$//')
        d=$(echo "$d" | sed 's/^"//;s/"$//')
        e=$(echo "$e" | sed 's/^"//;s/"$//')

        [[ -z "$a" ]] && continue

        if [ "$table" == "active_domains" ]; then
            mysql "$DB_NAME" << EOF 2>/dev/null
INSERT IGNORE INTO active_domains (second_level_domain, top_level_domain, ip_address, status, timestamp)
VALUES ('$a','$b','$c', CASE WHEN '$d'='Active' THEN 'Active' ELSE 'Inactive' END,'$e');
EOF
        else
            mysql "$DB_NAME" << EOF 2>/dev/null
INSERT IGNORE INTO failed_domains (second_level_domain, top_level_domain, reason, timestamp)
VALUES ('$a','$b','$c','$d');
EOF
        fi

        ((imported++))
    done

    echo "$imported"
}

# CSV tracking
update_csv_tracking() {
    local filename="$1"
    local status="$2"
    local active_count="$3"
    local failed_count="$4"

    mysql "$DB_NAME" << EOF
INSERT INTO csv_files (filename, status, active_count, failed_count, import_timestamp)
VALUES ('$filename','$status',$active_count,$failed_count,NOW())
ON DUPLICATE KEY UPDATE
status='$status', active_count=active_count+$active_count, failed_count=failed_count+$failed_count, import_timestamp=NOW();
EOF
}

# Import all active CSVs
import_all_active() {
    log "Starting import of all active CSVs..."
    local total=0
    local cols="second_level_domain VARCHAR(255),top_level_domain VARCHAR(10),ip_address TEXT,status VARCHAR(20),timestamp DATETIME"

    for f in "$RESULTS_ACTIVE"/active_part*.csv; do
        [ ! -f "$f" ] && continue
        local count
        count=$(import_csv "active_domains" "$f" "$cols" | tail -n1)
        total=$((total+count))
        update_csv_tracking "$(basename "$f")" "Completed" "$count" "0"
    done

    success "Total active domains imported: $total"
    echo "$total"
}

# Import all failed CSVs
import_all_failed() {
    log "Starting import of all failed CSVs..."
    local total=0
    local cols="second_level_domain VARCHAR(255),top_level_domain VARCHAR(10),reason VARCHAR(255),timestamp DATETIME"

    for f in "$RESULTS_FAIL"/fail_part*.csv; do
        [ ! -f "$f" ] && continue
        local count
        count=$(import_csv "failed_domains" "$f" "$cols" | tail -n1)
        total=$((total+count))
        update_csv_tracking "$(basename "$f")" "Completed" "0" "$count"
    done

    success "Total failed domains imported: $total"
    echo "$total"
}

# Track CSV files
track_original_files() {
    log "Tracking original CSV files..."
    for dir in "$COMPLETED_DIR" "$PENDING_DIR" "$PROCESSING_DIR"; do
        for f in "$dir"/*.csv; do
            [ ! -f "$f" ] && continue
            mysql "$DB_NAME" << EOF
INSERT IGNORE INTO csv_files (filename,status,timestamp)
VALUES ('$(basename "$f")','$(basename "$dir")',NOW());
EOF
        done
    done
}

# Generate summary
generate_summary() {
    log "Generating summary report..."
    mysql "$DB_NAME" << EOF
SELECT 'ACTIVE DOMAINS' as TABLE_NAME, COUNT(*) as TOTAL_RECORDS, COUNT(DISTINCT top_level_domain) as UNIQUE_TLDS, MIN(timestamp) as EARLIEST_RECORD, MAX(timestamp) as LATEST_RECORD FROM active_domains
UNION ALL
SELECT 'FAILED DOMAINS', COUNT(*), COUNT(DISTINCT reason), MIN(timestamp), MAX(timestamp) FROM failed_domains
UNION ALL
SELECT 'CSV FILES', COUNT(*), SUM(active_count+failed_count), MIN(timestamp), MAX(import_timestamp) FROM csv_files;
EOF
}

# Main
main() {
    log "Starting database import process..."
    test_db_connection || { error "Cannot connect to DB"; exit 1; }
    create_database_schema || { error "Failed schema creation"; exit 1; }

    log "Starting data import..."
    total_active=$(import_all_active)
    total_failed=$(import_all_failed)

    track_original_files
    generate_summary

    success "Database import completed successfully!"
    success "Log file: $LOG_FILE"

    echo ""
    echo "=== QUICK STATS ==="
    echo "Active domains imported: $total_active"
    echo "Failed domains imported: $total_failed"
    echo "Total domains: $((total_active+total_failed))"
}

# Check mysql client
[ ! $(command -v mysql) ] && { error "MySQL client not found"; exit 1; }

# Run
main
