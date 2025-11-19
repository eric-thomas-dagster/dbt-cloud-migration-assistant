# dbt Cloud to Dagster Migration Assistant

Migrate your dbt Cloud projects to Dagster in 2 commands.

## Quick Start

```bash
# 1. Clone and install
git clone https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant.git
cd dbt-cloud-migration-assistant
pip install -e .

# 2. Run migration (with automatic setup)
dbt-cloud-migrate --api-key YOUR_API_KEY --account-id YOUR_ACCOUNT_ID --auto-setup
```

That's it! The `--auto-setup` flag will automatically:
- Clone all dbt project repositories
- Copy `profiles.yml` to `~/.dbt/profiles.yml`
- Install all dependencies

You'll get a complete Dagster project ready to run!

## One-Liner (with auto-setup)

```bash
git clone https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant.git && cd dbt-cloud-migration-assistant && pip install -e . && dbt-cloud-migrate --api-key YOUR_API_KEY --account-id YOUR_ACCOUNT_ID --auto-setup
```

## Options

- `--auto-setup` - Automatically clone repos, copy profiles.yml, and install dependencies (recommended)
- `--clone-repos` - Only clone dbt project repositories
- `--copy-profiles` - Only copy profiles.yml to ~/.dbt/profiles.yml
- `--install-deps` - Only install dependencies
- `--api-base-url` - Custom API base URL for multi-tenant accounts
- `--skip-confirm` - Skip confirmation prompts

## What You Need

- Python 3.8+
- dbt Cloud API key and account ID
- Access to your dbt project git repositories

## Getting Your dbt Cloud Credentials

1. Go to [dbt Cloud](https://cloud.getdbt.com) → Account Settings → **Service Tokens** (recommended) or API Tokens
2. Create a new **Service Token** (recommended for system-level operations) or Personal Access Token
3. Ensure the token has permissions to read projects, jobs, and environments
4. Copy your Account ID from the URL or account settings (usually 6-8 digits)

### Multi-Tenant Accounts

If you have a multi-tenant account (with a custom access URL like `https://lm759.us1.dbt.com/`), you'll need to specify the API base URL:

```bash
dbt-cloud-migrate \
  --api-key YOUR_API_KEY \
  --account-id YOUR_ACCOUNT_ID \
  --api-base-url https://YOUR_PREFIX.us1.dbt.com/api/v2
```

Replace `YOUR_PREFIX` with your account prefix (found in Account Settings).

## What You Get

After running the migration:

- ✅ Complete Dagster project with all your dbt projects, jobs, and schedules
- ✅ Component-based YAML definitions (no Python code to maintain)
- ✅ Deployment-aware configuration (works with Dagster Cloud)
- ✅ DuckDB local development target (develop without production DB)
- ✅ Migration summary and validation scripts
- ✅ Helper scripts to clone repositories and validate setup

## Next Steps

If you used `--auto-setup`, you're almost done:

1. Review `dagster_project/MIGRATION_SUMMARY.md`
2. Update `.env` file with your database credentials
3. Update `~/.dbt/profiles.yml` with your database credentials
4. Start Dagster: `cd dagster_project && dg dev`

If you didn't use `--auto-setup`, you'll need to:
1. Run `./clone_dbt_projects.sh` to clone repositories
2. Copy `profiles.yml.template` to `~/.dbt/profiles.yml`
3. Install dependencies: `cd dagster_project && pip install -e .`

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
