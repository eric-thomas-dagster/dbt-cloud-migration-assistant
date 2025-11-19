"""Generate dbt profiles.yml from dbt Cloud environment configurations"""

from typing import List, Dict, Any
import yaml


def generate_profiles_yml(environments: List[Dict[str, Any]]) -> str:
    """
    Generate dbt profiles.yml content from dbt Cloud environments

    Args:
        environments: List of dbt Cloud environment dictionaries

    Returns:
        YAML string for profiles.yml
    """
    profiles = {}

    for env in environments:
        env_name = env.get("name", "default")
        connection = env.get("connection", {})
        
        if not connection:
            continue

        connection_type = (
            connection.get("type")
            or connection.get("connection_type")
            or "postgres"
        ).lower()

        # Map connection type to dbt profile type
        profile_type = connection_type
        if connection_type == "sqlserver":
            profile_type = "sqlserver"
        elif connection_type == "databricks":
            profile_type = "databricks"

        # Build profile configuration
        profile_config = {
            "type": profile_type,
        }

        # Add connection-specific fields
        if connection_type == "snowflake":
            profile_config.update({
                "account": connection.get("account", "{{ env_var('DBT_SNOWFLAKE_ACCOUNT') }}"),
                "user": connection.get("user") or connection.get("username", "{{ env_var('DBT_SNOWFLAKE_USER') }}"),
                "password": "{{ env_var('DBT_SNOWFLAKE_PASSWORD') }}",
                "role": connection.get("role", "{{ env_var('DBT_SNOWFLAKE_ROLE', '') }}"),
                "database": connection.get("database", "{{ env_var('DBT_SNOWFLAKE_DATABASE') }}"),
                "warehouse": connection.get("warehouse", "{{ env_var('DBT_SNOWFLAKE_WAREHOUSE') }}"),
                "schema": connection.get("schema", "{{ env_var('DBT_SNOWFLAKE_SCHEMA') }}"),
            })
        elif connection_type == "bigquery":
            profile_config.update({
                "type": "bigquery",
                "method": "service-account",
                "project": connection.get("project", "{{ env_var('DBT_BIGQUERY_PROJECT') }}"),
                "dataset": connection.get("schema") or connection.get("dataset", "{{ env_var('DBT_BIGQUERY_DATASET') }}"),
                "keyfile": "{{ env_var('DBT_BIGQUERY_KEYFILE') }}",
            })
        elif connection_type == "postgres" or connection_type == "redshift":
            profile_config.update({
                "host": connection.get("host", "{{ env_var('DBT_POSTGRES_HOST') }}"),
                "user": connection.get("user") or connection.get("username", "{{ env_var('DBT_POSTGRES_USER') }}"),
                "password": "{{ env_var('DBT_POSTGRES_PASSWORD') }}",
                "port": connection.get("port", 5432),
                "dbname": connection.get("database", "{{ env_var('DBT_POSTGRES_DATABASE') }}"),
                "schema": connection.get("schema", "{{ env_var('DBT_POSTGRES_SCHEMA') }}"),
            })
        elif connection_type == "databricks":
            profile_config.update({
                "host": connection.get("host", "{{ env_var('DBT_DATABRICKS_HOST') }}"),
                "http_path": connection.get("http_path", "{{ env_var('DBT_DATABRICKS_HTTP_PATH') }}"),
                "token": "{{ env_var('DBT_DATABRICKS_TOKEN') }}",
                "schema": connection.get("schema", "{{ env_var('DBT_DATABRICKS_SCHEMA') }}"),
            })
        else:
            # Generic profile - use environment variables
            profile_config.update({
                "host": "{{ env_var('DBT_HOST') }}",
                "user": "{{ env_var('DBT_USER') }}",
                "password": "{{ env_var('DBT_PASSWORD') }}",
                "database": connection.get("database", "{{ env_var('DBT_DATABASE') }}"),
                "schema": connection.get("schema", "{{ env_var('DBT_SCHEMA') }}"),
            })

        # Add threads if specified
        if connection.get("threads"):
            profile_config["threads"] = connection.get("threads")

        # Add local dev target (DuckDB) for local development
        # Output names: use environment name for production, 'local' for DuckDB
        env_output_name = env_name.lower().replace(" ", "_")
        outputs = {
            env_output_name: profile_config,  # Production/staging target named after environment
            "local": {
                "type": "duckdb",
                "path": "{{ env_var('DBT_DUCKDB_PATH', 'local_dev.duckdb') }}",
                "schema": "{{ env_var('DBT_DUCKDB_SCHEMA', 'dev') }}",
            }
        }
        
        # Set target based on deployment (local for development, environment name for production)
        # In Dagster Cloud, you can use DAGSTER_CLOUD_DEPLOYMENT_NAME to select the right target
        # Example: target: "{{ env_var('DAGSTER_CLOUD_DEPLOYMENT_NAME', 'local') }}"
        # For now, default to 'local' for local development with DuckDB
        profiles[env_output_name] = {
            "outputs": outputs,
            "target": "local"  # Default to local DuckDB for development
        }

    # If no environments, create a template
    if not profiles:
        profiles = {
            "default": {
                "outputs": {
                    "prod": {  # Production target
                        "type": "postgres",
                        "host": "{{ env_var('DBT_HOST') }}",
                        "user": "{{ env_var('DBT_USER') }}",
                        "password": "{{ env_var('DBT_PASSWORD') }}",
                        "port": 5432,
                        "dbname": "{{ env_var('DBT_DATABASE') }}",
                        "schema": "{{ env_var('DBT_SCHEMA') }}",
                    },
                    "local": {  # Local development target (DuckDB)
                        "type": "duckdb",
                        "path": "{{ env_var('DBT_DUCKDB_PATH', 'local_dev.duckdb') }}",
                        "schema": "{{ env_var('DBT_DUCKDB_SCHEMA', 'dev') }}",
                    }
                },
                "target": "local"  # Default to local DuckDB for development
            }
        }

    # Add deployment-aware target selection comment at the top
    profiles_yaml = yaml.dump(profiles, default_flow_style=False, sort_keys=False)
    
    # Prepend deployment-aware configuration instructions
    # Following the pattern from hooli-data-eng-pipelines demo project
    header = """# dbt profiles.yml - Deployment-Aware Configuration
#
# Default target is 'local' (DuckDB) for local development.
# 
# The dbt components are already configured to use deployment-aware target selection!
# They automatically use DAGSTER_CLOUD_DEPLOYMENT_NAME to select the right target.
#
# This matches the pattern from the Dagster demo project:
# https://github.com/dagster-io/hooli-data-eng-pipelines/blob/master/hooli-data-eng/src/hooli_data_eng/defs/dbt/resources.py
#
# How it works:
#   - Local development: Uses 'local' target (DuckDB) when DAGSTER_CLOUD_DEPLOYMENT_NAME is not set
#   - Dagster Cloud deployments: Uses deployment name as target (e.g., 'prod', 'staging')
#
# To make profiles.yml deployment-aware, update the 'target' field:
#   target: "{{ env_var('DAGSTER_CLOUD_DEPLOYMENT_NAME', 'local') }}"
#
# See MIGRATION_SUMMARY.md for more details.

"""
    
    return header + profiles_yaml

