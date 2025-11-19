# Simple Instructions - Clone, Install, Run

## For Your Prospects

Copy and paste these instructions:

---

## Step 1: Clone and Install

```bash
git clone https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant.git
cd dbt-cloud-migration-assistant
pip install -e .
```

## Step 2: Run Migration

```bash
dbt-cloud-migrate --api-key YOUR_API_KEY --account-id YOUR_ACCOUNT_ID
```

Replace `YOUR_API_KEY` and `YOUR_ACCOUNT_ID` with your dbt Cloud credentials.

## Step 3: Follow the Instructions

The tool will guide you through the rest. When it's done:

1. Review `dagster_project/MIGRATION_SUMMARY.md`
2. Update credentials in `.env` file
3. Run `cd dagster_project && dg dev`

---

## Getting Your dbt Cloud Credentials

1. Go to https://cloud.getdbt.com
2. Account Settings → API Tokens
3. Create a new token
4. Copy your Account ID from the URL or settings

## That's It!

You now have a complete Dagster project migrated from dbt Cloud.

