#!/bin/bash
# Quick installer script for dbt Cloud Migration Assistant

set -e

echo "🚀 Installing dbt Cloud Migration Assistant..."
pip install -e .

echo ""
echo "✅ Installation complete!"
echo ""
echo "Next step: Run the migration"
echo "  dbt-cloud-migrate --api-key YOUR_API_KEY --account-id YOUR_ACCOUNT_ID"
echo ""
echo "For help:"
echo "  dbt-cloud-migrate --help"

