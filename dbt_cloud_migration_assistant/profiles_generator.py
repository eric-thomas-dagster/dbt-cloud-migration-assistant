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

        # Extract connection details - they can be in different formats:
        # 1. Direct format: connection.type, connection.host, etc.
        # 2. Nested format: connection.connection_details.fields.{field}.value
        connection_details = connection.get("connection_details", {})
        fields = connection_details.get("fields", {}) if connection_details else {}
        
        # Helper function to get field value (handles both formats)
        def get_field_value(field_name: str, default=None):
            # Try nested format first (connection_details.fields)
            if fields:
                field_data = fields.get(field_name, {})
                if isinstance(field_data, dict):
                    return field_data.get("value", default)
            # Try direct format
            return connection.get(field_name, default)
        
        # Get connection type
        connection_type = (
            get_field_value("type")
            or connection.get("type")
            or connection.get("connection_type")
            or "postgres"
        )
        if connection_type:
            connection_type = connection_type.lower()
        else:
            connection_type = "postgres"

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
                "account": get_field_value("account") or connection.get("account", "{{ env_var('DBT_SNOWFLAKE_ACCOUNT') }}"),
                "user": get_field_value("user") or connection.get("user") or connection.get("username", "{{ env_var('DBT_SNOWFLAKE_USER') }}"),
                "password": "{{ env_var('DBT_SNOWFLAKE_PASSWORD') }}",
                "role": get_field_value("role") or connection.get("role", "{{ env_var('DBT_SNOWFLAKE_ROLE', '') }}"),
                "database": get_field_value("database") or connection.get("database", "{{ env_var('DBT_SNOWFLAKE_DATABASE') }}"),
                "warehouse": get_field_value("warehouse") or connection.get("warehouse", "{{ env_var('DBT_SNOWFLAKE_WAREHOUSE') }}"),
                "schema": get_field_value("schema") or connection.get("schema", "{{ env_var('DBT_SNOWFLAKE_SCHEMA') }}"),
            })
        elif connection_type == "bigquery":
            # BigQuery uses service account JSON - we'll use environment variables for the keyfile
            project_id = get_field_value("project_id") or connection.get("project_id") or connection.get("project")
            dataset = get_field_value("dataset") or connection.get("dataset") or connection.get("schema")
            location = get_field_value("location") or connection.get("location")
            
            profile_config.update({
                "type": "bigquery",
                "method": "service-account",
                "project": project_id or "{{ env_var('DBT_BIGQUERY_PROJECT') }}",
                "dataset": dataset or "{{ env_var('DBT_BIGQUERY_DATASET') }}",
                "keyfile": "{{ env_var('DBT_BIGQUERY_KEYFILE') }}",
            })
            
            # Add optional BigQuery fields
            if location:
                profile_config["location"] = location
            if get_field_value("priority"):
                profile_config["priority"] = get_field_value("priority")
            if get_field_value("maximum_bytes_billed"):
                profile_config["maximum_bytes_billed"] = get_field_value("maximum_bytes_billed")
        elif connection_type == "postgres" or connection_type == "redshift":
            profile_config.update({
                "host": get_field_value("host") or connection.get("host", "{{ env_var('DBT_POSTGRES_HOST') }}"),
                "user": get_field_value("user") or connection.get("user") or connection.get("username", "{{ env_var('DBT_POSTGRES_USER') }}"),
                "password": "{{ env_var('DBT_POSTGRES_PASSWORD') }}",
                "port": get_field_value("port") or connection.get("port", 5432),
                "dbname": get_field_value("database") or connection.get("database") or connection.get("dbname", "{{ env_var('DBT_POSTGRES_DATABASE') }}"),
                "schema": get_field_value("schema") or connection.get("schema", "{{ env_var('DBT_POSTGRES_SCHEMA') }}"),
            })
        elif connection_type == "databricks":
            profile_config.update({
                "host": get_field_value("host") or connection.get("host", "{{ env_var('DBT_DATABRICKS_HOST') }}"),
                "http_path": get_field_value("http_path") or connection.get("http_path", "{{ env_var('DBT_DATABRICKS_HTTP_PATH') }}"),
                "token": "{{ env_var('DBT_DATABRICKS_TOKEN') }}",
                "schema": get_field_value("schema") or connection.get("schema", "{{ env_var('DBT_DATABRICKS_SCHEMA') }}"),
            })
        else:
            # Generic profile - use environment variables
            profile_config.update({
                "host": get_field_value("host") or connection.get("host", "{{ env_var('DBT_HOST') }}"),
                "user": get_field_value("user") or connection.get("user") or connection.get("username", "{{ env_var('DBT_USER') }}"),
                "password": "{{ env_var('DBT_PASSWORD') }}",
                "database": get_field_value("database") or connection.get("database", "{{ env_var('DBT_DATABASE') }}"),
                "schema": get_field_value("schema") or connection.get("schema", "{{ env_var('DBT_SCHEMA') }}"),
            })

        # Add threads if specified
        threads = get_field_value("threads") or connection.get("threads")
        if threads:
            profile_config["threads"] = threads

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

    # Always add a 'default' profile for compatibility with standard dbt projects
    # This ensures projects that reference 'profile: default' in dbt_project.yml will work
    if "default" not in profiles:
        # Use the first environment's configuration, or create a minimal default
        if profiles:
            # Use the first profile's structure but name it 'default'
            first_profile_name = list(profiles.keys())[0]
            first_profile = profiles[first_profile_name]
            profiles["default"] = first_profile.copy()
        else:
            # Create a minimal default profile if no environments exist
            profiles["default"] = {
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

    # If no environments, create a template (legacy fallback)
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

