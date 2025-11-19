# Delivery Guide for Prospects

## How to Deliver This Tool

### Option 1: Git Repository (Recommended)

1. **Push to a Git repository** (GitHub, GitLab, etc.)
2. **Share the repository URL** with prospects
3. **Provide this one-liner**:

```bash
git clone <repository-url> && cd dbt-cloud-migration-assistant && pip install -e . && dbt-cloud-migrate --api-key YOUR_KEY --account-id YOUR_ID
```

### Option 2: Package Distribution

1. **Build distribution packages**:
   ```bash
   pip install build
   python -m build
   ```
   This creates:
   - `dist/dbt-cloud-migration-assistant-0.1.0.tar.gz` (source distribution)
   - `dist/dbt-cloud-migration-assistant-0.1.0-py3-none-any.whl` (wheel)

2. **Share the `.whl` file** with prospects

3. **Provide installation instructions**:
   ```bash
   pip install dbt-cloud-migration-assistant-0.1.0-py3-none-any.whl
   dbt-cloud-migrate --api-key YOUR_KEY --account-id YOUR_ID
   ```

### Option 3: Zip Archive

1. **Create a zip file** of the entire directory (excluding `__pycache__`, `.git`, etc.)
2. **Share the zip file** with prospects
3. **Provide instructions**:
   ```bash
   unzip dbt-cloud-migration-assistant.zip
   cd dbt-cloud-migration-assistant
   pip install -e .
   dbt-cloud-migrate --api-key YOUR_KEY --account-id YOUR_ID
   ```

## Quick Start Instructions for Prospects

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)
- dbt Cloud API credentials

### Installation & Usage

**Option 1: One-liner (simplest)**

```bash
# If from Git repository:
git clone <repository-url> && cd dbt-cloud-migration-assistant && ./install.sh && dbt-cloud-migrate --api-key YOUR_KEY --account-id YOUR_ID

# If from wheel file:
pip install dbt-cloud-migration-assistant-*.whl && dbt-cloud-migrate --api-key YOUR_KEY --account-id YOUR_ID

# If from zip file:
unzip dbt-cloud-migration-assistant.zip && cd dbt-cloud-migration-assistant && ./install.sh && dbt-cloud-migrate --api-key YOUR_KEY --account-id YOUR_ID
```

**Option 2: Step by step**

```bash
# 1. Install
pip install -e .

# 2. Run migration
dbt-cloud-migrate --api-key YOUR_KEY --account-id YOUR_ID
```

### Getting dbt Cloud API Credentials

1. Log in to [dbt Cloud](https://cloud.getdbt.com)
2. Go to **Account Settings → API Tokens**
3. Create a new API token
4. Copy your **Account ID** from the URL or account settings

### What Happens Next

After running the command, the tool will:

1. ✅ Connect to dbt Cloud and fetch your projects, jobs, and environments
2. ✅ Ask for git repository URLs (or discover them automatically)
3. ✅ Generate a complete Dagster project in `dagster_project/` directory
4. ✅ Create all necessary configuration files
5. ✅ Generate a migration summary report

### Next Steps After Migration

1. Review `dagster_project/MIGRATION_SUMMARY.md`
2. Update `.env` file with your credentials
3. Copy `profiles.yml.template` to `~/.dbt/profiles.yml`
4. Run `./clone_dbt_projects.sh` to clone repositories
5. Run `./validate_migration.sh` to validate setup
6. Start Dagster: `cd dagster_project && dg dev`

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

### "Permission denied" on scripts

Make scripts executable:
```bash
chmod +x clone_dbt_projects.sh validate_migration.sh
```

## Support

For issues or questions, refer to:
- `README.md` - Full documentation
- `INSTALL.md` - Detailed installation guide
- `MIGRATION_SUMMARY.md` - Generated after migration

