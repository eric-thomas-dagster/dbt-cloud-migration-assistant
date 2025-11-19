#!/bin/bash
# Run this after creating the GitHub repository

echo "Pushing to GitHub..."
git push -u origin main

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Successfully published to GitHub!"
    echo ""
    echo "Repository: https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant"
    echo ""
    echo "Your prospects can now use:"
    echo "  git clone https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant.git"
else
    echo ""
    echo "❌ Push failed. Make sure:"
    echo "  1. The repository exists at https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant"
    echo "  2. You have push access"
    echo "  3. You're authenticated with GitHub"
fi

