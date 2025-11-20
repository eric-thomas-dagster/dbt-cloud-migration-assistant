# dbt Cloud to Dagster Migration Assistant

Migrate your dbt Cloud projects to Dagster in 2 commands.

## Quick Start

```bash
# 1. Clone and install
git clone https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant.git
cd dbt-cloud-migration-assistant
pip install -e .

# 2. Run migration (interactive mode - will prompt for missing info)
dbt-cloud-migrate

# Or with flags
dbt-cloud-migrate --api-key YOUR_API_KEY --account-id YOUR_ACCOUNT_ID
```

That's it! The tool will automatically:
- Clone all dbt project repositories
- Generate `profiles.yml` with all your dbt Cloud environments
- Copy `profiles.yml` to `~/.dbt/profiles.yml` (shared across all projects)
- Install all dependencies

You'll get a complete Dagster project ready to run!

**Note:** The `profiles.yml` file contains profiles for all your dbt Cloud **environments** (not projects). Multiple dbt projects can share the same `profiles.yml` file - each project references the appropriate profile name in its `dbt_project.yml`. The tool creates:
- One profile per dbt Cloud environment (e.g., `prod`, `staging`)
- Each profile includes both the environment's connection and a `local` DuckDB target for development
- A `default` profile for compatibility with standard dbt projects

## Interactive Mode

If you don't provide flags, the tool will prompt you interactively:

```bash
dbt-cloud-migrate
# Will prompt for:
# - API key (hidden input)
# - Account ID
# - Output directory (default: dagster_project)
# - Multi-tenant API URL (optional)
```

## One-Liner

```bash
git clone https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant.git && cd dbt-cloud-migration-assistant && pip install -e . && dbt-cloud-migrate
```

## Options

- `--no-auto-setup` - Skip automatic setup (don't clone repos, copy profiles, or install deps)
- `--api-base-url` - Custom API base URL for multi-tenant accounts
- `--skip-confirm` - Skip confirmation prompts
- `--output-dir` - Output directory for generated Dagster project (default: `dagster_project`)

## What You Need

- Python 3.10+
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

- ✅ Complete Dagster project with all your dbt projects, jobs, schedules, and sensors
- ✅ **One `DbtProjectComponent` per dbt Cloud project** - each project gets its own component definition
- ✅ Component-based YAML definitions (no Python code to maintain)
- ✅ Deployment-aware configuration (works with Dagster Cloud)
- ✅ DuckDB local development target (develop without production DB)
- ✅ Job completion triggers automatically converted to Dagster sensors
- ✅ Migration summary and validation scripts
- ✅ Helper scripts to clone repositories and validate setup

### Multiple dbt Projects

If you have multiple dbt projects in dbt Cloud, the tool will:
- Create a separate `DbtProjectComponent` for each project
- Each project gets its own directory: `defs/<project_name>/defs.yaml`
- All projects are registered in the same Dagster project
- Jobs and schedules are organized by their source dbt project
- **All projects share the same `profiles.yml`** - profiles are organized by environment (not project)

## Next Steps

After migration completes:

1. Review `dagster_project/MIGRATION_SUMMARY.md`
2. Update `.env` file with your database credentials (if needed)
3. Update `~/.dbt/profiles.yml` with your database credentials
4. Start Dagster: `cd dagster_project && dg dev`

## Features

- ✅ Automatically detects and installs required dbt adapters
- ✅ Generates dbt profiles.yml with deployment-aware target selection
- ✅ Uses Dagster 1.12+ CLI (`create-dagster project`) for all project scaffolding
- ✅ Component-based architecture (all YAML, no Python code)
- ✅ Maps dbt Cloud jobs → Dagster jobs
- ✅ Maps dbt Cloud schedules → Dagster schedules
- ✅ Maps dbt Cloud job completion triggers → Dagster sensors
- ✅ Supports all dbt Cloud connection types (Snowflake, BigQuery, Databricks, Spark, Athena, Trino, Synapse, Fabric, Teradata, AlloyDB, and more)
- ✅ Preserves job configurations (threads, target, etc.)

## How It Works

1. Connects to dbt Cloud API and fetches your configuration (projects, jobs, environments)
2. Uses `create-dagster project` to scaffold Dagster project (falls back to `dg init` if needed)
3. **Creates one `DbtProjectComponent` per dbt Cloud project** in `defs/<project_name>/defs.yaml`
4. Uses `dg scaffold defs` to create dbt components for each project
5. Generates component-based YAML for jobs, schedules, and sensors
6. Creates all necessary configuration files

All using Dagster's official CLI - no custom code generation!

### Project Structure Example

If you have 2 dbt projects in dbt Cloud:
- `analytics` project → `defs/analytics/defs.yaml` (DbtProjectComponent)
- `marketing` project → `defs/marketing/defs.yaml` (DbtProjectComponent)

Both are registered in the same Dagster project and can share resources.

## Requirements

- Python 3.10+
- Dagster 1.12+ (automatically installed)
- dagster-dbt 0.22.0+ (automatically installed)

## Troubleshooting

### "Command not found: dbt-cloud-migrate"

Make sure you installed the package:
```bash
pip install -e .
```

### "Dagster CLI not found"

The tool will automatically install Dagster, but if you see this error:
```bash
pip install 'dagster[cli]>=1.12.0'
```

### Multi-Tenant Account Issues

If you have a multi-tenant account and get authentication errors, make sure to use the `--api-base-url` flag with your account's custom API endpoint.

## Support

For questions or issues, please open an issue on [GitHub](https://github.com/eric-thomas-dagster/dbt-cloud-migration-assistant/issues).

## License

MIT License - see LICENSE file for details
