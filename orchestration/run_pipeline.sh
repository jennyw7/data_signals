#!/bin/bash
# Run the full data_signals pipeline
set -e

cd "$(dirname "$0")/../dbt"

echo "==> Installing dbt packages..."
dbt deps

echo "==> Running staging models..."
dbt run --select staging

echo "==> Running mart models..."
dbt run --select marts

echo "==> Running signal models..."
dbt run --select signals

echo "==> Running schema tests..."
dbt test

echo "==> Sending notifications..."
cd "$(dirname "$0")/.."
python notifications/slack_alert.py
python notifications/email_alert.py

echo "==> Done."
