# Quick Start - 3 Steps to Migrate

## Step 1: Install

```bash
pip install -e .
```

## Step 2: Run Migration

```bash
dbt-cloud-migrate --api-key YOUR_API_KEY --account-id YOUR_ACCOUNT_ID
```

## Step 3: Follow the Instructions

The tool will guide you through the rest. After it completes:

1. Review `dagster_project/MIGRATION_SUMMARY.md`
2. Update credentials in `.env` file
3. Run `cd dagster_project && dg dev`

That's it! You now have a Dagster project migrated from dbt Cloud.

## Getting Your dbt Cloud Credentials

1. Go to [dbt Cloud](https://cloud.getdbt.com) → Account Settings → API Tokens
2. Create a new API token
3. Copy your Account ID from the URL or settings

## One-Liner (If You Have the Code)

```bash
pip install -e . && dbt-cloud-migrate --api-key YOUR_KEY --account-id YOUR_ID
```

