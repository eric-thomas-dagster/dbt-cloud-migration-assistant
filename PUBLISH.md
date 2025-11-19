# Publishing to GitHub

## Initial Setup

1. **Create the repository on GitHub:**
   - Go to https://github.com/eric-thomas-dagster
   - Click "New repository"
   - Name: `dbt-cloud-migration-assistant`
   - Make it public (or private, your choice)
   - Don't initialize with README (we already have one)

2. **Push the code:**

**Option A: Use the helper script (recommended):**
```bash
./publish-to-github.sh
```

**Option B: Manual steps:**
```bash
# Initialize git (if not already done)
git init

# Add all files
git add .

# Commit
git commit -m "Initial commit: dbt Cloud to Dagster migration assistant"

# Add remote
git remote add origin https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant.git

# Push
git branch -M main
git push -u origin main
```

## For Your Prospects

Share this simple command:

```bash
git clone https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant.git && cd dbt-cloud-migration-assistant && pip install -e . && dbt-cloud-migrate --api-key YOUR_API_KEY --account-id YOUR_ACCOUNT_ID
```

Or provide the 2-step process:

```bash
# Step 1: Clone and install
git clone https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant.git
cd dbt-cloud-migration-assistant
pip install -e .

# Step 2: Run migration
dbt-cloud-migrate --api-key YOUR_API_KEY --account-id YOUR_ACCOUNT_ID
```

## Building Distribution Packages (Optional)

If you want to distribute as a wheel file:

```bash
# Install build tools
pip install build

# Build packages
python -m build

# This creates:
# - dist/dbt-cloud-migration-assistant-0.1.0.tar.gz
# - dist/dbt-cloud-migration-assistant-0.1.0-py3-none-any.whl
```

Then prospects can install with:
```bash
pip install https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant/releases/download/v0.1.0/dbt-cloud-migration-assistant-0.1.0-py3-none-any.whl
```

Or directly from GitHub:
```bash
pip install git+https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant.git
```

