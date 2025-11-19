# For Your Prospects - Simple Instructions

Copy and paste this to share with prospects:

---

## Quick Start (2 Commands)

```bash
# 1. Clone and install
git clone https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant.git
cd dbt-cloud-migration-assistant
pip install -e .

# 2. Run migration
dbt-cloud-migrate --api-key YOUR_API_KEY --account-id YOUR_ACCOUNT_ID
```

## One-Liner Version

```bash
git clone https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant.git && cd dbt-cloud-migration-assistant && pip install -e . && dbt-cloud-migrate --api-key YOUR_API_KEY --account-id YOUR_ACCOUNT_ID
```

## Getting Your dbt Cloud Credentials

1. Go to https://cloud.getdbt.com
2. Account Settings → **Service Tokens** (recommended) or API Tokens
3. Create a new **Service Token** (recommended) or Personal Access Token
4. Ensure the token has permissions to read projects, jobs, and environments
5. Copy your Account ID from the URL or settings (usually 6-8 digits)

## What Happens Next

1. The tool connects to dbt Cloud and fetches your projects, jobs, and schedules
2. It generates a complete Dagster project in `dagster_project/`
3. Review `dagster_project/MIGRATION_SUMMARY.md` for next steps
4. Start Dagster: `cd dagster_project && dg dev`

## That's It!

You now have a complete Dagster project migrated from dbt Cloud, ready to run.

---

