"""CLI interface for dbt Cloud migration assistant"""

import click
from typing import Dict
from .dbt_cloud_client import DbtCloudClient
from .git_discovery import discover_git_repo, prompt_for_git_repo, validate_git_url
from .dagster_generator import DagsterProjectGenerator
from .adapter_detector import detect_adapters, extract_environment_variables


@click.command()
@click.option(
    "--api-key",
    required=True,
    help="dbt Cloud API key",
    envvar="DBT_CLOUD_API_KEY",
)
@click.option(
    "--account-id",
    required=True,
    type=int,
    help="dbt Cloud account ID",
    envvar="DBT_CLOUD_ACCOUNT_ID",
)
@click.option(
    "--output-dir",
    default="dagster_project",
    help="Output directory for generated Dagster project",
)
@click.option(
    "--skip-confirm",
    is_flag=True,
    help="Skip confirmation prompts",
)
def main(api_key: str, account_id: int, output_dir: str, skip_confirm: bool):
    """
    Migrate dbt Cloud projects, jobs, and schedules to Dagster.

    This tool fetches your dbt Cloud configuration and generates a Dagster
    project with equivalent jobs and schedules.
    """
    click.echo("🚀 Starting dbt Cloud to Dagster migration...")
    click.echo("")

    # Initialize client
    try:
        client = DbtCloudClient(api_key, account_id)
        click.echo("✓ Connected to dbt Cloud API")
    except Exception as e:
        click.echo(f"✗ Failed to connect to dbt Cloud: {e}", err=True)
        raise click.Abort()

    # Fetch data from dbt Cloud
    click.echo("📥 Fetching projects, jobs, and environments...")
    try:
        projects = client.get_projects()
        click.echo(f"  Found {len(projects)} project(s)")

        jobs = client.get_jobs()
        click.echo(f"  Found {len(jobs)} job(s)")

        environments = client.get_environments()
        click.echo(f"  Found {len(environments)} environment(s)")
    except Exception as e:
        click.echo(f"✗ Failed to fetch data: {e}", err=True)
        raise click.Abort()

    if not projects:
        click.echo("⚠ No projects found in dbt Cloud", err=True)
        raise click.Abort()

    # Discover or prompt for git repositories
    click.echo("")
    click.echo("🔍 Discovering git repositories...")
    project_repos: Dict[int, str] = {}

    for project in projects:
        project_id = project.get("id")
        project_name = project.get("name", f"project_{project_id}")

        # Try to discover repo
        repo_url = discover_git_repo(project)
        if repo_url:
            click.echo(f"  ✓ Found repo for {project_name}: {repo_url}")
            project_repos[project_id] = repo_url
        else:
            # Prompt user
            if not skip_confirm:
                repo_url = prompt_for_git_repo(project_name, project_id)
                project_repos[project_id] = repo_url
            else:
                click.echo(
                    f"  ⚠ No repo found for {project_name}, skipping...", err=True
                )

    if not project_repos:
        click.echo("✗ No git repositories configured", err=True)
        raise click.Abort()

    # Detect adapters and environment variables
    click.echo("")
    click.echo("🔍 Analyzing environments for dbt adapters and configuration...")
    required_adapters = detect_adapters(environments)
    if required_adapters:
        click.echo(f"  Detected dbt adapters: {', '.join(sorted(required_adapters))}")
    else:
        click.echo("  ⚠ No dbt adapters detected (will use dbt-core only)")
    
    env_vars = extract_environment_variables(environments, jobs)
    if env_vars:
        click.echo(f"  Found {len(env_vars)} environment variable(s) to configure")

    # Generate Dagster project
    click.echo("")
    click.echo(f"📦 Generating Dagster project in '{output_dir}'...")
    click.echo("  Using Dagster 1.12+ CLI (dg) for project scaffolding...")
    click.echo("  - Scaffolding dbt components with 'dg scaffold defs'")
    click.echo("  - Registering custom job/schedule components with 'dg scaffold component'")
    try:
        generator = DagsterProjectGenerator(output_dir)
        generator.generate_project(projects, jobs, environments, project_repos)
        click.echo("✓ Dagster project generated successfully using Dagster CLI")
    except Exception as e:
        click.echo(f"✗ Failed to generate project: {e}", err=True)
        click.echo("  Make sure Dagster 1.12+ is installed: pip install 'dagster[cli]>=1.12.0'", err=True)
        raise click.Abort()

    # Summary
    click.echo("")
    click.echo("✅ Migration complete!")
    click.echo("")
    click.echo("📄 Migration Summary: See MIGRATION_SUMMARY.md for details")
    click.echo("")
    click.echo("Next steps:")
    click.echo(f"  1. Review the generated project in '{output_dir}/'")
    click.echo("  2. Update the .env file with your database credentials")
    if required_adapters:
        click.echo(f"  3. Install dependencies (includes adapters: {', '.join(sorted(required_adapters))}):")
    else:
        click.echo("  3. Install dependencies:")
    click.echo(f"     cd {output_dir} && pip install -e .")
    click.echo("  4. Run './clone_dbt_projects.sh' to clone all dbt project repositories")
    click.echo("  5. Copy 'profiles.yml.template' to '~/.dbt/profiles.yml' and update credentials")
    click.echo("  6. Run './validate_migration.sh' to validate the migration")
    click.echo("  7. Start Dagster: dg dev (or dagster dev)")
    click.echo("")
    click.echo("Note: All definitions are component-based YAML (no Python code generation!)")
    click.echo("      Jobs and schedules use custom components in defs/jobs/ and defs/schedules/")


if __name__ == "__main__":
    main()

