#!/bin/bash
# Script to publish the migration assistant to GitHub

set -e

REPO_URL="https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant.git"

echo "🚀 Publishing dbt Cloud Migration Assistant to GitHub..."
echo ""

# Check if git is initialized
if [ ! -d ".git" ]; then
    echo "Initializing git repository..."
    git init
fi

# Check if remote exists
if git remote get-url origin &>/dev/null; then
    echo "Remote 'origin' already exists: $(git remote get-url origin)"
    read -p "Update to $REPO_URL? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git remote set-url origin "$REPO_URL"
    fi
else
    echo "Adding remote: $REPO_URL"
    git remote add origin "$REPO_URL"
fi

# Add all files
echo "Adding files..."
git add .

# Check if there are changes to commit
if git diff --staged --quiet; then
    echo "No changes to commit."
else
    # Commit
    echo "Committing changes..."
    git commit -m "Initial commit: dbt Cloud to Dagster migration assistant"
fi

# Set branch to main
git branch -M main

# Push
echo ""
echo "Pushing to GitHub..."
echo "Repository: $REPO_URL"
echo ""
read -p "Ready to push? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    git push -u origin main
    echo ""
    echo "✅ Published to GitHub!"
    echo ""
    echo "Your prospects can now use:"
    echo "  git clone $REPO_URL"
    echo "  cd dbt-cloud-migration-assistant"
    echo "  pip install -e ."
    echo "  dbt-cloud-migrate --api-key KEY --account-id ID"
else
    echo "Push cancelled. Run 'git push -u origin main' when ready."
fi

