"""Detect dbt adapters and environment variables from dbt Cloud environments"""

from typing import List, Dict, Any, Set, Optional


# Mapping of dbt Cloud connection types to dbt adapter packages
ADAPTER_MAPPING = {
    "snowflake": "dbt-snowflake",
    "bigquery": "dbt-bigquery",
    "postgres": "dbt-postgres",
    "redshift": "dbt-redshift",
    "databricks": "dbt-databricks",
    "spark": "dbt-spark",
    "duckdb": "dbt-duckdb",
    "sqlserver": "dbt-sqlserver",
    "oracle": "dbt-oracle",
    "athena": "dbt-athena",
    "trino": "dbt-trino",
    "clickhouse": "dbt-clickhouse",
    "dremio": "dbt-dremio",
    "exasol": "dbt-exasol",
    "firebolt": "dbt-firebolt",
    "materialize": "dbt-materialize",
    "mysql": "dbt-mysql",
    "teradata": "dbt-teradata",
    "vertica": "dbt-vertica",
}


def detect_adapters(environments: List[Dict[str, Any]]) -> Set[str]:
    """
    Detect required dbt adapters from dbt Cloud environments

    Args:
        environments: List of dbt Cloud environment dictionaries

    Returns:
        Set of required dbt adapter package names
    """
    adapters = set()

    for env in environments:
        # Check connection details
        connection = env.get("connection", {})
        if connection:
            # Try to get connection type from various possible fields
            connection_type = (
                connection.get("type")
                or connection.get("connection_type")
                or connection.get("adapter_type")
            )

            if connection_type:
                # Normalize to lowercase
                connection_type = connection_type.lower()

                # Map to adapter package
                adapter = ADAPTER_MAPPING.get(connection_type)
                if adapter:
                    adapters.add(adapter)
                else:
                    # Try direct match if not in mapping
                    if connection_type.startswith("dbt-"):
                        adapters.add(connection_type)
                    else:
                        # Try common patterns
                        potential_adapter = f"dbt-{connection_type}"
                        adapters.add(potential_adapter)

        # Also check project-level adapter if available
        project = env.get("project")
        if project:
            adapter_type = project.get("adapter_type")
            if adapter_type:
                adapter_type = adapter_type.lower()
                adapter = ADAPTER_MAPPING.get(adapter_type)
                if adapter:
                    adapters.add(adapter)

    return adapters


def extract_environment_variables(
    environments: List[Dict[str, Any]], jobs: List[Dict[str, Any]]
) -> Dict[str, str]:
    """
    Extract environment variables from dbt Cloud environments and jobs

    Args:
        environments: List of dbt Cloud environment dictionaries
        jobs: List of dbt Cloud job dictionaries

    Returns:
        Dictionary of environment variable names to values (or placeholders)
    """
    env_vars = {}

    for env in environments:
        env_name = env.get("name", "").upper().replace(" ", "_").replace("-", "_")

        # Extract connection credentials (we'll use placeholders for security)
        connection = env.get("connection", {})
        if connection:
            # Database connection variables
            if connection.get("account"):
                env_vars[f"DBT_{env_name}_ACCOUNT"] = connection.get("account", "")
            if connection.get("database"):
                env_vars[f"DBT_{env_name}_DATABASE"] = connection.get("database", "")
            if connection.get("schema"):
                env_vars[f"DBT_{env_name}_SCHEMA"] = connection.get("schema", "")
            if connection.get("warehouse"):
                env_vars[f"DBT_{env_name}_WAREHOUSE"] = connection.get("warehouse", "")
            if connection.get("user") or connection.get("username"):
                env_vars[f"DBT_{env_name}_USER"] = (
                    connection.get("user") or connection.get("username", "")
                )
            # Password should be set manually
            if connection.get("password"):
                env_vars[f"DBT_{env_name}_PASSWORD"] = "<SET_MANUALLY>"

            # Connection-specific variables
            connection_type = (
                connection.get("type")
                or connection.get("connection_type")
                or "postgres"
            ).lower()

            if connection_type == "snowflake":
                if connection.get("role"):
                    env_vars[f"DBT_{env_name}_ROLE"] = connection.get("role", "")
            elif connection_type == "bigquery":
                if connection.get("project"):
                    env_vars[f"DBT_{env_name}_PROJECT"] = connection.get("project", "")
                if connection.get("keyfile"):
                    env_vars[f"DBT_{env_name}_KEYFILE"] = "<SET_MANUALLY>"
            elif connection_type == "databricks":
                if connection.get("host"):
                    env_vars[f"DBT_{env_name}_HOST"] = connection.get("host", "")
                if connection.get("http_path"):
                    env_vars[f"DBT_{env_name}_HTTP_PATH"] = connection.get(
                        "http_path", ""
                    )
                if connection.get("token"):
                    env_vars[f"DBT_{env_name}_TOKEN"] = "<SET_MANUALLY>"

        # Extract custom environment variables if available
        custom_env_vars = env.get("custom_environment_variables", {})
        if custom_env_vars:
            for key, value in custom_env_vars.items():
                env_vars[key] = value

    # Extract job-level environment variables
    for job in jobs:
        job_env_vars = job.get("environment_variables", {})
        if job_env_vars:
            for key, value in job_env_vars.items():
                # Use job name as prefix if not already set
                if key not in env_vars:
                    env_vars[key] = value

    # Add common dbt variables
    if "DBT_PROFILES_DIR" not in env_vars:
        env_vars["DBT_PROFILES_DIR"] = "~/.dbt"

    return env_vars

