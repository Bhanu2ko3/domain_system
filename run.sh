#!/bin/bash
# Main automation runner

BASE_DIR="$HOME/domain_system"

# Initial setup
bash "$BASE_DIR/setup.sh"

# Run generation daily at midnight
bash "$BASE_DIR/generate_domains.sh"

# Run processing continuously
bash "$BASE_DIR/process_domains.sh"

# import database every hour
bash "$BASE_DIR/db.sh"
