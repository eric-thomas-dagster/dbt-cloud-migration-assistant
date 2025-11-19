# Installation Guide

## Quick Start (One-Liner)

```bash
pip install git+https://github.com/your-org/dbt-cloud-migration-assistant.git && dbt-cloud-migrate --api-key YOUR_KEY --account-id YOUR_ID
```

## Installation Options

### Option 1: Install from Git Repository (Recommended)

If you have the repository URL:

```bash
# Clone the repository
git clone https://github.com/your-org/dbt-cloud-migration-assistant.git
cd dbt-cloud-migration-assistant

# Install the package
pip install -e .

# Run the migration
dbt-cloud-migrate --api-key YOUR_API_KEY --account-id YOUR_ACCOUNT_ID
```

### Option 2: Install from Local Directory

If you received the code as a zip file or directory:

```bash
# Extract/unzip the code
cd dbt-cloud-migration-assistant

# Install the package
pip install -e .

# Run the migration
dbt-cloud-migrate --api-key YOUR_API_KEY --account-id YOUR_ACCOUNT_ID
```

### Option 3: Install as a Package Archive

If you received a `.whl` or `.tar.gz` file:

```bash
# Install from wheel
pip install dbt-cloud-migration-assistant-*.whl

# Or install from source distribution
pip install dbt-cloud-migration-assistant-*.tar.gz

# Run the migration
dbt-cloud-migrate --api-key YOUR_API_KEY --account-id YOUR_ACCOUNT_ID
```

## Prerequisites

- Python 3.8 or higher
- pip (Python package manager)
- dbt Cloud API access (API key and account ID)

## Verify Installation

After installation, verify the CLI is available:

```bash
dbt-cloud-migrate --help
```

## Getting Your dbt Cloud API Credentials

1. Log in to dbt Cloud
2. Navigate to Account Settings → API Tokens
3. Create a new API token with appropriate permissions
4. Copy your Account ID from the URL or account settings

## Next Steps

After running the migration, see the generated `MIGRATION_SUMMARY.md` in the output directory for next steps.

