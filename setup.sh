#!/bin/bash
# Setup project folders

BASE_DIR="$HOME/domain_system"

mkdir -p $BASE_DIR/{pending,processing,completed,results/{active,fail},logs}

echo "Domain System Folder Structure Created at: $BASE_DIR"
