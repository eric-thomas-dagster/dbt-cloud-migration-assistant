"""Git repository discovery and validation"""

import re
from typing import Optional
import click


def validate_git_url(url: str) -> bool:
    """Validate if a string looks like a valid git URL"""
    patterns = [
        r"^https://.*\.git$",
        r"^git@.*:.*\.git$",
        r"^https://.*$",  # Allow URLs without .git suffix
        r"^git@.*:.*$",  # Allow SSH URLs without .git suffix
    ]
    return any(re.match(pattern, url) for pattern in patterns)


def discover_git_repo(project: dict) -> Optional[str]:
    """
    Attempt to discover git repository from project metadata

    Args:
        project: dbt Cloud project dictionary

    Returns:
        Git repository URL if found, None otherwise
    """
    # Check repository_url field
    repo_url = project.get("repository_url")
    if repo_url and validate_git_url(repo_url):
        return repo_url

    # Check other potential fields
    for field in ["git_url", "repo_url", "repository"]:
        if field in project:
            value = project.get(field)
            if value and validate_git_url(str(value)):
                return str(value)

    return None


def prompt_for_git_repo(project_name: str, project_id: int) -> str:
    """
    Prompt user for git repository URL

    Args:
        project_name: Name of the dbt project
        project_id: ID of the dbt project

    Returns:
        Git repository URL
    """
    click.echo(f"\nProject: {project_name} (ID: {project_id})")
    while True:
        repo_url = click.prompt(
            "Enter the git repository URL for this project",
            type=str,
        )
        if validate_git_url(repo_url):
            return repo_url
        else:
            click.echo("Invalid git URL format. Please try again.")
            click.echo("Examples:")
            click.echo("  - https://github.com/user/repo.git")
            click.echo("  - git@github.com:user/repo.git")

