#!/bin/bash
set -e

echo "==> Installing Python dependencies..."
pip install -r "$(dirname "$0")/requirements.txt"

echo "==> Installing dbt packages..."
cd "$(dirname "$0")/../dbt"
dbt deps

echo "==> Setup complete."
echo ""
echo "Next steps:"
echo "  1. Copy config/.env.example → config/.env and fill in your credentials"
echo "  2. Copy dbt/profiles.yml.example → ~/.dbt/profiles.yml and fill in Snowflake details"
echo "  3. Update dbt/models/staging/_sources.yml with your actual database/schema/table names"
echo "  4. Run: bash orchestration/run_pipeline.sh"
