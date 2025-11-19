# dbt Cloud to Dagster Migration Assistant

Migrate your dbt Cloud projects to Dagster in 2 commands.

## Quick Start

```bash
# 1. Clone and install
git clone https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant.git
cd dbt-cloud-migration-assistant
pip install -e .

# 2. Run migration
dbt-cloud-migrate --api-key YOUR_API_KEY --account-id YOUR_ACCOUNT_ID
```

That's it! You'll get a complete Dagster project in the `dagster_project/` directory.

## One-Liner

```bash
git clone https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant.git && cd dbt-cloud-migration-assistant && pip install -e . && dbt-cloud-migrate --api-key YOUR_API_KEY --account-id YOUR_ACCOUNT_ID
```

## What You Need

- Python 3.8+
- dbt Cloud API key and account ID
- Access to your dbt project git repositories

## Getting Your dbt Cloud Credentials

1. Go to [dbt Cloud](https://cloud.getdbt.com) → Account Settings → API Tokens
2. Create a new API token
3. Copy your Account ID from the URL or account settings

## What You Get

After running the migration:

- ✅ Complete Dagster project with all your dbt projects, jobs, and schedules
- ✅ Component-based YAML definitions (no Python code to maintain)
- ✅ Deployment-aware configuration (works with Dagster Cloud)
- ✅ DuckDB local development target (develop without production DB)
- ✅ Migration summary and validation scripts
- ✅ Helper scripts to clone repositories and validate setup

## Next Steps

1. Review `dagster_project/MIGRATION_SUMMARY.md`
2. Update `.env` file with your credentials
3. Copy `profiles.yml.template` to `~/.dbt/profiles.yml`
4. Run `./clone_dbt_projects.sh` to clone repositories
5. Start Dagster: `cd dagster_project && dg dev`

## Features

- ✅ Automatically detects and installs required dbt adapters
- ✅ Generates dbt profiles.yml with deployment-aware target selection
- ✅ Uses Dagster 1.12+ CLI for all project scaffolding
- ✅ Component-based architecture (all YAML, no Python code)
- ✅ Maps dbt Cloud jobs → Dagster jobs
- ✅ Maps dbt Cloud schedules → Dagster schedules
- ✅ Preserves job configurations (threads, target, etc.)

## How It Works

1. Connects to dbt Cloud API and fetches your configuration
2. Uses `dg init` to scaffold Dagster project
3. Uses `dg scaffold defs` to create dbt components
4. Generates component-based YAML for jobs and schedules
5. Creates all necessary configuration files

All using Dagster's official CLI - no custom code generation!

## Requirements

- Python 3.8+
- Dagster 1.12+ (automatically installed)
- dagster-dbt 0.22.0+ (automatically installed)

## Documentation

- [INSTALL.md](INSTALL.md) - Detailed installation guide
- [DELIVERY.md](DELIVERY.md) - Information for distributing this tool
- [QUICKSTART.md](QUICKSTART.md) - 3-step quick start

## Support

For questions or issues, please open an issue on [GitHub](https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant/issues).

## License

MIT License - see LICENSE file for details
