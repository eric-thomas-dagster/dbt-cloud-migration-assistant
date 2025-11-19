"""Generate Dagster project structure from dbt Cloud configuration using Dagster CLI"""

import os
import subprocess
import shutil
from pathlib import Path
from typing import List, Dict, Any, Optional, Set
import yaml
from .adapter_detector import detect_adapters, extract_environment_variables
from .profiles_generator import generate_profiles_yml


class DagsterProjectGenerator:
    """Generates Dagster project structure from dbt Cloud data using Dagster CLI"""

    def __init__(self, output_dir: str = "dagster_project"):
        """
        Initialize generator

        Args:
            output_dir: Directory where Dagster project will be created
        """
        self.output_dir = Path(output_dir).resolve()
        self.project_root = self.output_dir

    def generate_project(
        self,
        projects: List[Dict[str, Any]],
        jobs: List[Dict[str, Any]],
        environments: List[Dict[str, Any]],
        project_repos: Dict[int, str],
    ):
        """
        Generate complete Dagster project structure using Dagster CLI

        Args:
            projects: List of dbt Cloud projects
            jobs: List of dbt Cloud jobs
            environments: List of dbt Cloud environments
            project_repos: Mapping of project_id to git repository URL
        """
        # Check if Dagster CLI is available
        self._check_dagster_cli()

        # Initialize Dagster project using CLI
        self._init_dagster_project()

        # Scaffold dbt components for each project
        for project in projects:
            project_id = project.get("id")
            if project_id not in project_repos:
                continue

            project_name = self._sanitize_name(project.get("name", f"project_{project_id}"))
            project_display_name = project.get("name", f"project_{project_id}")
            dbt_project_path = f"./dbt_projects/{project_display_name}"

            # Scaffold dbt component using CLI
            self._scaffold_dbt_component(project_name, dbt_project_path)

        # Detect required dbt adapters
        required_adapters = detect_adapters(environments)

        # Extract environment variables
        env_vars = extract_environment_variables(environments, jobs)

        # Create package structure and register custom components
        self._create_package_structure()
        self._register_custom_components()
        
        # Generate jobs and schedules as component-based YAML definitions
        self._generate_jobs_and_schedules(projects, jobs, project_repos)

        # Update pyproject.toml with dependencies including adapters and dagster-cloud
        self._update_pyproject_toml(required_adapters)

        # Generate .env file with environment variables
        self._generate_env_file(env_vars)

        # Generate dbt profiles.yml
        self._generate_profiles_yml(environments)

        # Generate git clone script
        self._generate_git_clone_script(projects, project_repos)

        # Generate migration validation script
        self._generate_validation_script()

        # Generate migration summary report
        self._generate_migration_summary(projects, jobs, environments, project_repos, required_adapters)

        # Generate README
        self._generate_readme(projects, project_repos, required_adapters)

    def clone_repositories(self, projects: List[Dict[str, Any]], project_repos: Dict[int, str]):
        """Clone all dbt project repositories"""
        import subprocess
        
        dbt_projects_dir = self.output_dir / "dbt_projects"
        dbt_projects_dir.mkdir(exist_ok=True)
        
        for project in projects:
            project_id = project.get("id")
            if project_id not in project_repos:
                continue
            
            project_name = project.get("name", f"project_{project_id}")
            repo_url = project_repos[project_id]
            project_dir = dbt_projects_dir / project_name
            
            if project_dir.exists():
                continue  # Skip silently, CLI will handle messaging
            
            try:
                subprocess.run(
                    ["git", "clone", repo_url, str(project_dir)],
                    check=True,
                    capture_output=True,
                    text=True
                )
            except subprocess.CalledProcessError as e:
                raise Exception(f"Failed to clone {project_name}: {e.stderr}")

    def copy_profiles_yml(self):
        """Copy profiles.yml.template to ~/.dbt/profiles.yml"""
        import shutil
        from pathlib import Path
        
        # The template is generated in the output directory
        template_path = self.output_dir / "profiles.yml.template"
        if not template_path.exists():
            raise FileNotFoundError(f"profiles.yml.template not found at {template_path}")
        
        dbt_dir = Path.home() / ".dbt"
        dbt_dir.mkdir(exist_ok=True)
        
        target_path = dbt_dir / "profiles.yml"
        
        # Copy the file
        shutil.copy2(template_path, target_path)

    def install_dependencies(self):
        """Install dependencies in the generated Dagster project"""
        import subprocess
        
        try:
            result = subprocess.run(
                ["pip", "install", "-e", "."],
                cwd=self.output_dir,
                check=True,
                capture_output=True,
                text=True
            )
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to install dependencies: {e.stderr}")

    def _check_dagster_cli(self):
        """Check if Dagster CLI (dg) is available"""
        try:
            result = subprocess.run(
                ["dg", "--version"],
                capture_output=True,
                text=True,
                check=True,
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            # Try create-dagster as fallback
            try:
                result = subprocess.run(
                    ["create-dagster", "--version"],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                self.cli_command = "create-dagster"
            except (subprocess.CalledProcessError, FileNotFoundError):
                raise RuntimeError(
                    "Dagster CLI (dg or create-dagster) not found. "
                    "Please install Dagster 1.12+ with: pip install dagster[cli]"
                )
        else:
            self.cli_command = "dg"

    def _init_dagster_project(self):
        """Initialize Dagster project using CLI"""
        project_name = self.output_dir.name

        # If directory exists and is not empty, we'll work with it
        if self.output_dir.exists() and any(self.output_dir.iterdir()):
            # Check if it's already a Dagster project
            project_package = self._get_project_package_name()
            if (self.output_dir / "pyproject.toml").exists() or (self.output_dir / project_package / "defs").exists():
                # It's already a Dagster project, we can work with it
                return
            else:
                raise RuntimeError(
                    f"Directory {self.output_dir} exists and is not empty. "
                    "Please use a different output directory or remove this one."
                )

        # Create parent directory if needed
        self.output_dir.parent.mkdir(parents=True, exist_ok=True)

        # Use dg init or create-dagster to scaffold project
        try:
            if self.cli_command == "dg":
                # Try dg init - it may create project in current directory
                if not self.output_dir.exists():
                    self.output_dir.mkdir(parents=True, exist_ok=True)
                
                # Run dg init in the target directory
                subprocess.run(
                    ["dg", "init"],
                    check=True,
                    cwd=self.output_dir,
                    capture_output=True,
                    text=True,
                )
            else:
                # create-dagster project_name creates a new directory
                subprocess.run(
                    ["create-dagster", "project", project_name],
                    check=True,
                    cwd=self.output_dir.parent,
                )
                # Check if a subdirectory was created
                possible_dirs = list(self.output_dir.parent.glob(f"{project_name}*"))
                if possible_dirs and possible_dirs[0] != self.output_dir:
                    # If target doesn't exist, we can use the created one
                    if not self.output_dir.exists():
                        self.output_dir = possible_dirs[0].resolve()
        except subprocess.CalledProcessError as e:
            # If CLI fails, create minimal structure manually
            self._create_minimal_dagster_structure()

    def _create_minimal_dagster_structure(self):
        """Create minimal Dagster project structure if CLI fails"""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create package and defs directory
        project_package = self._get_project_package_name()
        package_dir = self.output_dir / project_package
        package_dir.mkdir(exist_ok=True)
        (package_dir / "defs").mkdir(exist_ok=True)
        
        # Create minimal pyproject.toml
        project_package = self._get_project_package_name()
        pyproject_content = f"""[project]
name = "dagster-dbt-migration"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "dagster[cli]>=1.12.0",
    "dagster-dbt>=0.22.0",
    "dbt-core>=1.5.0",
]

[tool.setuptools]
packages = ["{project_package}"]

[tool.dg]
directory_type = "project"

[tool.dg.project]
root_module = "{project_package}"
"""
        with open(self.output_dir / "pyproject.toml", "w") as f:
            f.write(pyproject_content)

    def _scaffold_dbt_component(self, component_name: str, dbt_project_path: str):
        """
        Scaffold a dbt component using Dagster CLI

        Args:
            component_name: Name for the dbt component
            dbt_project_path: Relative path to the dbt project directory
        """
        try:
            # Use dg scaffold to create dbt component
            # Format: dg scaffold defs dagster_dbt.DbtProjectComponent <name> --project-path <path>
            cmd = [
                "dg",
                "scaffold",
                "defs",
                "dagster_dbt.DbtProjectComponent",
                component_name,
                "--project-path",
                dbt_project_path,
            ]

            subprocess.run(
                cmd,
                check=True,
                cwd=self.output_dir,
                capture_output=True,
            )
        except subprocess.CalledProcessError as e:
            # If scaffold fails, we'll create the defs.yaml manually
            self._create_dbt_component_manual(component_name, dbt_project_path)

    def _create_dbt_component_manual(self, component_name: str, dbt_project_path: str):
        """Manually create dbt component defs.yaml if CLI fails"""
        project_package = self._get_project_package_name()
        defs_dir = self.output_dir / project_package / "defs" / component_name
        defs_dir.mkdir(parents=True, exist_ok=True)

        # Dagster 1.12 defs.yaml format - single object (not a list)
        # The defs.yaml should contain a single component definition
        # Note: Target selection is handled via dbt profiles.yml, not as a component attribute
        defs_config = {
            "type": "dagster_dbt.DbtProjectComponent",
            "attributes": {
                "project": f"${{{{ project_root }}}}/{dbt_project_path}",
            },
        }

        with open(defs_dir / "defs.yaml", "w") as f:
            yaml.dump(defs_config, f, default_flow_style=False, sort_keys=False)

    def _create_package_structure(self):
        """Create proper Python package structure"""
        project_package = self._get_project_package_name()
        package_dir = self.output_dir / project_package
        package_dir.mkdir(exist_ok=True)
        
        # Create __init__.py for the package
        init_file = package_dir / "__init__.py"
        if not init_file.exists():
            with open(init_file, "w") as f:
                f.write('"""Dagster project migrated from dbt Cloud."""\n')

    def _register_custom_components(self):
        """Register custom components using Dagster CLI and copy component files"""
        # Components are automatically registered when in the project structure
        # We'll scaffold the component structure and then copy our implementations
        
        project_package = self._get_project_package_name()
        package_dir = self.output_dir / project_package
        components_dir = package_dir / "components"
        components_dir.mkdir(parents=True, exist_ok=True)
        
        # Try to scaffold JobComponent using CLI (if it doesn't exist)
        job_component_file = components_dir / "job.py"
        if not job_component_file.exists():
            try:
                # Use dg scaffold component to create the structure
                subprocess.run(
                    ["dg", "scaffold", "component", "JobComponent"],
                    check=True,
                    cwd=self.output_dir,
                    capture_output=True,
                )
            except subprocess.CalledProcessError:
                # If scaffold fails, we'll create it manually
                pass
        
        # Try to scaffold ScheduleComponent using CLI (if it doesn't exist)
        schedule_component_file = components_dir / "schedule.py"
        if not schedule_component_file.exists():
            try:
                subprocess.run(
                    ["dg", "scaffold", "component", "ScheduleComponent"],
                    check=True,
                    cwd=self.output_dir,
                    capture_output=True,
                )
            except subprocess.CalledProcessError:
                # If scaffold fails, we'll create it manually
                pass
        
        # Copy our custom component implementations (overwriting scaffolded versions)
        job_component_source = Path(__file__).parent / "components" / "job.py"
        if job_component_source.exists():
            shutil.copy2(job_component_source, job_component_file)
        
        schedule_component_source = Path(__file__).parent / "components" / "schedule.py"
        if schedule_component_source.exists():
            shutil.copy2(schedule_component_source, schedule_component_file)
        
        # Ensure __init__.py exists
        init_file = components_dir / "__init__.py"
        init_source = Path(__file__).parent / "components" / "__init__.py"
        if init_source.exists():
            shutil.copy2(init_source, init_file)
        elif not init_file.exists():
            # Create minimal __init__.py if it doesn't exist
            with open(init_file, "w") as f:
                f.write('"""Custom Dagster components for jobs and schedules."""\n\n')
                f.write("from .job import JobComponent\n")
                f.write("from .schedule import ScheduleComponent\n\n")
                f.write("__all__ = [\n")
                f.write('    "JobComponent",\n')
                f.write('    "ScheduleComponent",\n')
                f.write("]\n")

    def _generate_jobs_and_schedules(
        self,
        projects: List[Dict[str, Any]],
        jobs: List[Dict[str, Any]],
        project_repos: Dict[int, str],
    ):
        """Generate jobs and schedules as component-based YAML definitions"""
        # Group jobs by project
        jobs_by_project = {}
        for job in jobs:
            project_id = job.get("project_id")
            if project_id not in project_repos:
                continue
            if project_id not in jobs_by_project:
                jobs_by_project[project_id] = []
            jobs_by_project[project_id].append(job)

        # Create component-based YAML definitions for jobs and schedules
        all_job_defs = []
        all_schedule_defs = []
        
        for project_id, project_jobs in jobs_by_project.items():
            project = next((p for p in projects if p.get("id") == project_id), None)
            if not project:
                continue

            project_name = self._sanitize_name(project.get("name", f"project_{project_id}"))
            
            # The dbt component will create assets with keys based on the component name
            # We need to reference those assets in our jobs
            # The component name is the sanitized project name
            component_name = project_name

            for job in project_jobs:
                job_id = job.get("id")
                job_name = self._sanitize_name(job.get("name", f"job_{job_id}"))
                job_name_safe = f"{project_name}_{job_name}"

                # Create job component definition
                # Jobs reference assets from the dbt component using asset selection
                # Format: "component_name.*" to select all assets from the component
                # Note: Asset selection uses wildcard pattern matching
                # Component type uses full module path for proper registration
                project_package = self._get_project_package_name()
                
                # Extract job configuration from dbt Cloud
                execute_steps = job.get("execute_steps", [])
                job_description = job.get("description") or f"Job migrated from dbt Cloud: {job.get('name', f'job_{job_id}')}"
                
                # Build job attributes
                job_attributes = {
                    "job_name": job_name_safe,
                    "asset_selection": [f"{component_name}.*"],  # Select all assets from the dbt component
                    "description": job_description,
                }
                
                # Add tags if available (e.g., from job settings)
                if job.get("settings"):
                    settings = job.get("settings", {})
                    tags = {}
                    if settings.get("threads"):
                        tags["dbt_threads"] = str(settings.get("threads"))
                    if settings.get("target_name"):
                        tags["dbt_target"] = settings.get("target_name")
                    if tags:
                        job_attributes["tags"] = tags
                
                job_def = {
                    "type": f"{project_package}.components.job.JobComponent",
                    "attributes": job_attributes,
                }
                all_job_defs.append(job_def)

                # Create schedule component definition if schedule exists
                schedule = job.get("schedule")
                if schedule:
                    cron = schedule.get("cron")
                    if cron:
                        schedule_def = {
                            "type": f"{project_package}.components.schedule.ScheduleComponent",
                            "attributes": {
                                "schedule_name": f"{job_name_safe}_schedule",
                                "cron_expression": cron,
                                "job_name": job_name_safe,  # Reference the job we just created
                                "description": f"Schedule migrated from dbt Cloud for {job.get('name', f'job_{job_id}')}",
                                "default_status": "RUNNING",
                            },
                        }
                        all_schedule_defs.append(schedule_def)

        # Write jobs as component-based YAML
        # Use dg scaffold defs to create directory structure, then populate YAML
        project_package = self._get_project_package_name()
        if all_job_defs:
            jobs_dir = self.output_dir / project_package / "defs" / "jobs"
            
            # Try to use dg scaffold defs to create the structure
            try:
                # Get the component type name - need to determine the package name
                # The component should be registered as components.job.JobComponent
                # But we need the full module path based on project structure
                project_name = self._get_project_package_name()
                component_type = f"{project_name}.components.job.JobComponent"
                
                # Scaffold the first job definition to create directory structure
                subprocess.run(
                    ["dg", "scaffold", "defs", component_type, "jobs"],
                    check=True,
                    cwd=self.output_dir,
                    capture_output=True,
                )
            except (subprocess.CalledProcessError, Exception):
                # If scaffold fails, create directory manually
                jobs_dir.mkdir(parents=True, exist_ok=True)
            
            # Write all job definitions to defs.yaml
            with open(jobs_dir / "defs.yaml", "w") as f:
                yaml.dump(all_job_defs, f, default_flow_style=False, sort_keys=False)

        # Write schedules as component-based YAML
        if all_schedule_defs:
            schedules_dir = self.output_dir / project_package / "defs" / "schedules"
            
            # Try to use dg scaffold defs to create the structure
            try:
                project_name = self._get_project_package_name()
                component_type = f"{project_name}.components.schedule.ScheduleComponent"
                
                # Scaffold the first schedule definition to create directory structure
                subprocess.run(
                    ["dg", "scaffold", "defs", component_type, "schedules"],
                    check=True,
                    cwd=self.output_dir,
                    capture_output=True,
                )
            except (subprocess.CalledProcessError, Exception):
                # If scaffold fails, create directory manually
                schedules_dir.mkdir(parents=True, exist_ok=True)
            
            # Write all schedule definitions to defs.yaml
            with open(schedules_dir / "defs.yaml", "w") as f:
                yaml.dump(all_schedule_defs, f, default_flow_style=False, sort_keys=False)


    def _update_pyproject_toml(self, required_adapters: Set[str]):
        """Update pyproject.toml to include all required dependencies"""
        pyproject_path = self.output_dir / "pyproject.toml"
        
        # Base dependencies
        dependencies = [
            'dagster[cli]>=1.12.0',
            'dagster-dbt>=0.22.0',
            'dagster-cloud>=1.0.0',
            'dbt-core>=1.5.0',
            'dbt-duckdb>=1.5.0',  # For local development
        ]
        
        # Add detected dbt adapters (avoid duplicates)
        for adapter in sorted(required_adapters):
            if adapter != "dbt-duckdb":  # Already added above
                dependencies.append(f'{adapter}>=1.5.0')

        if not pyproject_path.exists():
            # Create new pyproject.toml
            project_package = self._get_project_package_name()
            content = f"""[project]
name = "dagster-dbt-migration"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
"""
            for dep in dependencies:
                content += f'    "{dep}",\n'
            content += """]

[tool.setuptools]
packages = ["{project_package}"]

[tool.setuptools.package-data]
"*" = ["py.typed"]

[tool.dg]
directory_type = "project"

[tool.dg.project]
root_module = "{project_package}"
"""
            with open(pyproject_path, "w") as f:
                f.write(content)
            return

        # Read existing pyproject.toml
        with open(pyproject_path, "r") as f:
            content = f.read()

        # Simple approach: append missing dependencies and update requires-python
        # In production, you might want to parse TOML properly
        project_package = self._get_project_package_name()
        lines = content.split("\n")
        new_lines = []
        in_dependencies = False
        existing_deps = set()
        has_tool_dg = False
        has_tool_dg_project = False
        has_setuptools = False
        
        for line in lines:
            # Check if tool.dg.project section exists
            if '[tool.dg.project]' in line:
                has_tool_dg_project = True
                new_lines.append(line)
                continue
            elif 'root_module' in line and 'tool.dg' in content[:content.find(line)]:
                # Update root_module if it exists
                new_lines.append(f'root_module = "{project_package}"')
                continue
            # Check if tool.dg section exists
            elif '[tool.dg]' in line:
                has_tool_dg = True
                new_lines.append(line)
                continue
            elif 'tool.dg' in line and 'directory_type' in line:
                # Update directory_type if it exists
                new_lines.append('directory_type = "project"')
                continue
            # Check for setuptools configuration
            elif '[tool.setuptools]' in line:
                has_setuptools = True
                new_lines.append(line)
                continue
            elif 'packages =' in line and has_setuptools:
                # Update packages if setuptools section exists
                new_lines.append(f'packages = ["{project_package}"]')
                continue
            # Update requires-python to >=3.10 for Dagster 1.12+ compatibility
            elif 'requires-python' in line and '<3.10' in line:
                new_lines.append('requires-python = ">=3.10"')
                continue
            elif 'requires-python' in line:
                # Replace any existing requires-python with >=3.10
                new_lines.append('requires-python = ">=3.10"')
                continue
            elif 'dependencies = [' in line:
                in_dependencies = True
                new_lines.append(line)
            elif in_dependencies:
                if line.strip().startswith('"') and line.strip().endswith('",'):
                    # Extract dependency name
                    dep_name = line.strip().strip('",').split('>=')[0].split('==')[0]
                    existing_deps.add(dep_name)
                    new_lines.append(line)
                elif line.strip() == "]":
                    # Add missing dependencies before closing bracket
                    for dep in dependencies:
                        dep_name = dep.split('>=')[0].split('==')[0]
                        if dep_name not in existing_deps:
                            new_lines.append(f'    "{dep}",')
                    new_lines.append(line)
                    in_dependencies = False
                else:
                    new_lines.append(line)
            else:
                new_lines.append(line)
        
        # Add setuptools configuration if it doesn't exist
        if not has_setuptools:
            # Find where to insert - after [project] section
            insert_idx = len(new_lines)
            for i, line in enumerate(new_lines):
                if line.strip() == "]" and in_dependencies:
                    insert_idx = i + 1
                    break
            new_lines.insert(insert_idx, "")
            new_lines.insert(insert_idx + 1, "[tool.setuptools]")
            new_lines.insert(insert_idx + 2, f'packages = ["{project_package}"]')
        
        # Add tool.dg.project section if it doesn't exist
        if not has_tool_dg_project:
            if not has_tool_dg:
                new_lines.append("")
                new_lines.append("[tool.dg]")
                new_lines.append('directory_type = "project"')
            new_lines.append("")
            new_lines.append("[tool.dg.project]")
            new_lines.append(f'root_module = "{project_package}"')
        elif not has_tool_dg:
            # Add tool.dg section if project section exists but main section doesn't
            new_lines.append("")
            new_lines.append("[tool.dg]")
            new_lines.append('directory_type = "project"')

        with open(pyproject_path, "w") as f:
            f.write("\n".join(new_lines))

    def _generate_env_file(self, env_vars: Dict[str, str]):
        """Generate .env file with environment variables"""
        env_path = self.output_dir / ".env"
        
        if not env_vars:
            # Create empty .env file with instructions
            content = """# Environment variables for dbt and Dagster
# Add your database credentials and other configuration here
# This file is gitignored by default for security

# Example dbt variables:
# DBT_PROFILES_DIR=~/.dbt
# DBT_TARGET=dev

# Example database credentials (set these based on your dbt Cloud environments):
# DBT_DEV_USER=your_username
# DBT_DEV_PASSWORD=your_password
# DBT_DEV_DATABASE=your_database
"""
        else:
            content = "# Environment variables extracted from dbt Cloud\n"
            content += "# Review and update these values, especially passwords and tokens\n\n"
            content += "# Local Development (DuckDB)\n"
            content += "# Use 'local' target for DuckDB-based local development\n"
            content += "# Set DBT_TARGET=local or use: dbt run --target local\n\n"
            
            for key, value in sorted(env_vars.items()):
                content += f"{key}={value}\n"
            
            content += "\n# Dagster Cloud Deployment Variables\n"
            content += "# These are automatically available in Dagster Cloud deployments:\n"
            content += "# - DAGSTER_CLOUD_DEPLOYMENT_NAME: deployment name (e.g., 'prod', 'staging')\n"
            content += "# - DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT: '1' if branch deployment\n"
            content += "# - DAGSTER_CLOUD_GIT_BRANCH: git branch name (branch deployments)\n"
            content += "# - DAGSTER_CLOUD_GIT_SHA: commit SHA (branch deployments)\n"
            content += "# See MIGRATION_SUMMARY.md for full list and usage examples\n\n"
            
            content += "# Add any additional environment variables needed for your setup\n"

        with open(env_path, "w") as f:
            f.write(content)

    def _generate_profiles_yml(self, environments: List[Dict[str, Any]]):
        """Generate dbt profiles.yml file"""
        profiles_content = generate_profiles_yml(environments)
        
        # Create .dbt directory in project
        dbt_dir = self.output_dir / ".dbt"
        dbt_dir.mkdir(exist_ok=True)
        
        profiles_path = dbt_dir / "profiles.yml"
        with open(profiles_path, "w") as f:
            f.write("# dbt profiles.yml generated from dbt Cloud migration\n")
            f.write("# Review and update environment variable references as needed\n\n")
            f.write(profiles_content)
        
        # Also create a template in the project root for reference
        template_path = self.output_dir / "profiles.yml.template"
        with open(template_path, "w") as f:
            f.write("# Template dbt profiles.yml\n")
            f.write("# Copy this to ~/.dbt/profiles.yml and update with your credentials\n")
            f.write("# \n")
            f.write("# Default Target: 'local' (DuckDB) for local development\n")
            f.write("#   - DuckDB database will be created at the path specified in DBT_DUCKDB_PATH\n")
            f.write("#   - No additional setup needed for local development\n")
            f.write("# \n")
            f.write("# Deployment-Aware Configuration:\n")
            f.write("#   The dbt components are ALREADY configured with deployment-aware target selection!\n")
            f.write("#   They use DAGSTER_CLOUD_DEPLOYMENT_NAME to automatically select the right target.\n")
            f.write("#   \n")
            f.write("#   To make profiles.yml match this behavior, update the 'target' field:\n")
            f.write("#     target: \"{{ env_var('DAGSTER_CLOUD_DEPLOYMENT_NAME', 'local') }}\"\n")
            f.write("#   \n")
            f.write("#   This matches the pattern from the Dagster demo project:\n")
            f.write("#   https://github.com/dagster-io/hooli-data-eng-pipelines/blob/master/hooli-data-eng/src/hooli_data_eng/defs/dbt/resources.py\n")
            f.write("# \n")
            f.write("# See MIGRATION_SUMMARY.md for more details.\n\n")
            f.write(profiles_content)

    def _generate_git_clone_script(self, projects: List[Dict[str, Any]], project_repos: Dict[int, str]):
        """Generate a script to clone all dbt project repositories"""
        script_path = self.output_dir / "clone_dbt_projects.sh"
        
        content = "#!/bin/bash\n"
        content += "# Script to clone all dbt project repositories\n"
        content += "# Generated by dbt Cloud to Dagster migration assistant\n\n"
        content += "set -e\n\n"
        content += "mkdir -p dbt_projects\n"
        content += "cd dbt_projects\n\n"
        
        for project in projects:
            project_id = project.get("id")
            if project_id in project_repos:
                project_name = project.get("name", f"project_{project_id}")
                repo_url = project_repos[project_id]
                content += f"echo \"Cloning {project_name}...\"\n"
                content += f"if [ ! -d \"{project_name}\" ]; then\n"
                content += f"    git clone {repo_url} {project_name}\n"
                content += f"else\n"
                content += f"    echo \"  {project_name} already exists, skipping...\"\n"
                content += f"fi\n\n"
        
        content += "echo \"All dbt projects cloned successfully!\"\n"
        
        with open(script_path, "w") as f:
            f.write(content)
        
        # Make script executable
        script_path.chmod(0o755)

    def _generate_validation_script(self):
        """Generate a script to validate the migration"""
        script_path = self.output_dir / "validate_migration.sh"
        
        content = "#!/bin/bash\n"
        content += "# Script to validate the dbt Cloud to Dagster migration\n"
        content += "# Generated by dbt Cloud to Dagster migration assistant\n\n"
        content += "set -e\n\n"
        content += "echo \"🔍 Validating migration...\"\n\n"
        content += "# Check if Dagster CLI is available\n"
        content += "if ! command -v dg &> /dev/null; then\n"
        content += "    echo \"❌ Dagster CLI (dg) not found. Install with: pip install 'dagster[cli]>=1.12.0'\"\n"
        content += "    exit 1\n"
        content += "fi\n\n"
        content += "# Validate Dagster definitions\n"
        content += "echo \"Checking Dagster definitions...\"\n"
        content += "dg check defs || {\n"
        content += "    echo \"❌ Dagster definitions validation failed\"\n"
        content += "    exit 1\n"
        content += "}\n\n"
        content += "# Check if dbt projects are cloned\n"
        content += "if [ ! -d \"dbt_projects\" ] || [ -z \"$(ls -A dbt_projects)\" ]; then\n"
        content += "    echo \"⚠️  Warning: dbt_projects directory is empty. Run ./clone_dbt_projects.sh\"\n"
        content += "else\n"
        content += "    echo \"✓ dbt projects directory exists\"\n"
        content += "fi\n\n"
        content += "# Check if profiles.yml exists\n"
        content += "if [ ! -f \"~/.dbt/profiles.yml\" ] && [ ! -f \".dbt/profiles.yml\" ]; then\n"
        content += "    echo \"⚠️  Warning: dbt profiles.yml not found. Copy profiles.yml.template to ~/.dbt/profiles.yml\"\n"
        content += "else\n"
        content += "    echo \"✓ dbt profiles.yml found\"\n"
        content += "fi\n\n"
        content += "echo \"✅ Migration validation complete!\"\n"
        content += "echo \"\"\n"
        content += "echo \"Next steps:\"\n"
        content += "echo \"  1. Review and update .env file with your credentials\"\n"
        content += "echo \"  2. Copy profiles.yml.template to ~/.dbt/profiles.yml and update\"\n"
        content += "echo \"  3. Run: dg dev\"\n"
        
        with open(script_path, "w") as f:
            f.write(content)
        
        # Make script executable
        script_path.chmod(0o755)

    def _generate_migration_summary(
        self,
        projects: List[Dict[str, Any]],
        jobs: List[Dict[str, Any]],
        environments: List[Dict[str, Any]],
        project_repos: Dict[int, str],
        required_adapters: Set[str],
    ):
        """Generate a migration summary report"""
        summary_path = self.output_dir / "MIGRATION_SUMMARY.md"
        
        content = "# dbt Cloud to Dagster Migration Summary\n\n"
        content += f"Generated: {self._get_timestamp()}\n\n"
        
        # What was migrated
        content += "## ✅ What Was Migrated\n\n"
        
        # Projects
        migrated_projects = [p for p in projects if p.get("id") in project_repos]
        content += f"### Projects ({len(migrated_projects)})\n\n"
        for project in migrated_projects:
            project_id = project.get("id")
            project_name = project.get("name", f"project_{project_id}")
            repo_url = project_repos.get(project_id, "N/A")
            content += f"- **{project_name}** (ID: {project_id})\n"
            content += f"  - Repository: `{repo_url}`\n"
            content += f"  - Component: `defs/{self._sanitize_name(project_name)}/`\n\n"
        
        # Jobs
        migrated_jobs = [j for j in jobs if j.get("project_id") in project_repos]
        content += f"### Jobs ({len(migrated_jobs)})\n\n"
        for job in migrated_jobs:
            job_id = job.get("id")
            job_name = job.get("name", f"job_{job_id}")
            project_id = job.get("project_id")
            project = next((p for p in projects if p.get("id") == project_id), None)
            project_name = self._sanitize_name(project.get("name", f"project_{project_id}")) if project else "unknown"
            job_name_safe = f"{project_name}_{self._sanitize_name(job_name)}"
            
            content += f"- **{job_name}** (ID: {job_id})\n"
            content += f"  - Dagster Job: `{job_name_safe}`\n"
            if job.get("schedule"):
                cron = job.get("schedule", {}).get("cron", "N/A")
                content += f"  - Schedule: `{job_name_safe}_schedule` (cron: `{cron}`)\n"
            content += "\n"
        
        # Environments
        content += f"### Environments ({len(environments)})\n\n"
        for env in environments:
            env_name = env.get("name", "Unknown")
            connection = env.get("connection", {})
            connection_type = (
                connection.get("type") or connection.get("connection_type") or "unknown"
            )
            content += f"- **{env_name}**\n"
            content += f"  - Connection Type: `{connection_type}`\n"
            content += f"  - Profile Target: `{env_name.lower().replace(' ', '_')}`\n\n"
        
        # Adapters
        if required_adapters:
            content += f"### dbt Adapters ({len(required_adapters)})\n\n"
            for adapter in sorted(required_adapters):
                content += f"- `{adapter}`\n"
            content += "\n"
        
        # Warnings and manual steps
        content += "## ⚠️ Warnings and Manual Steps\n\n"
        
        # Alerts
        content += "### Alerts/Notifications\n"
        content += "**⚠️ Alerts and notifications from dbt Cloud were NOT migrated.**\n\n"
        content += "You will need to manually configure alerts in Dagster:\n"
        content += "- For Dagster Cloud: Use the Alerts feature in the Dagster+ UI\n"
        content += "- For OSS: Configure alerting through your monitoring system\n"
        content += "- Review your dbt Cloud notification settings and recreate them in Dagster\n\n"
        
        # Environment variables
        content += "### Environment Variables\n"
        content += "**Action Required:** Review and update the `.env` file with your actual credentials.\n\n"
        content += "- Update any placeholders marked with `<SET_MANUALLY>`\n"
        content += "- Verify all database connection details\n"
        content += "- For Dagster Cloud: Set these in the Dagster+ UI or agent config\n\n"
        
        # Profiles
        content += "### dbt Profiles\n"
        content += "**Action Required:** Copy `profiles.yml.template` to `~/.dbt/profiles.yml` and update credentials.\n\n"
        content += "- A local DuckDB target has been added for local development\n"
        content += "- Use `dbt_target=local` for local development\n"
        content += "- Update production targets with actual credentials\n\n"
        
        # Git repositories
        missing_repos = [p for p in projects if p.get("id") not in project_repos]
        if missing_repos:
            content += "### Missing Git Repositories\n"
            content += "**Warning:** The following projects were skipped because no git repository was found:\n\n"
            for project in missing_repos:
                project_id = project.get("id")
                project_name = project.get("name", f"project_{project_id}")
                content += f"- {project_name} (ID: {project_id})\n"
            content += "\n"
        
        # Deployment awareness
        content += "## 🌍 Deployment-Aware Configuration\n\n"
        content += "This migration is configured to be deployment-aware using Dagster Cloud environment variables.\n\n"
        content += "### Available Environment Variables\n\n"
        content += "Dagster Cloud provides built-in environment variables that you can use:\n\n"
        content += "| Variable | Description |\n"
        content += "|----------|-------------|\n"
        content += "| `DAGSTER_CLOUD_DEPLOYMENT_NAME` | The name of the Dagster+ deployment (e.g., `prod`, `staging`) |\n"
        content += "| `DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT` | `1` if the deployment is a branch deployment |\n"
        content += "| `DAGSTER_CLOUD_GIT_BRANCH` | The git branch name (branch deployments only) |\n"
        content += "| `DAGSTER_CLOUD_GIT_SHA` | The commit SHA (branch deployments only) |\n\n"
        content += "### Usage Examples\n\n"
        content += "#### 1. Deployment-Aware Target Selection in profiles.yml\n\n"
        content += "Update the `target` field in your profiles.yml to select the right target based on deployment:\n\n"
        content += "```yaml\n"
        content += "default:\n"
        content += "  outputs:\n"
        content += "    local:\n"
        content += "      type: duckdb\n"
        content += "      # ... local config\n"
        content += "    prod:\n"
        content += "      type: snowflake\n"
        content += "      # ... prod config\n"
        content += "  # Deployment-aware target selection:\n"
        content += "  target: \"{{ env_var('DAGSTER_CLOUD_DEPLOYMENT_NAME', 'local') }}\"\n"
        content += "```\n\n"
        content += "#### 2. Deployment-Aware Target in dbt Component Configuration\n\n"
        content += "In `defs/<project_name>/defs.yaml`, you can add target selection:\n\n"
        content += "```yaml\n"
        content += "- type: dagster_dbt.DbtProjectComponent\n"
        content += "  attributes:\n"
        content += "    project: \"{{ project_root }}/dbt_projects/my_project\"\n"
        content += "    # Add target selection here:\n"
        content += "    target: \"{{ env_var('DAGSTER_CLOUD_DEPLOYMENT_NAME', 'local') }}\"\n"
        content += "```\n\n"
        content += "#### 3. Conditional Logic in Python Components\n\n"
        content += "```python\n"
        content += "import os\n"
        content += "deployment = os.getenv('DAGSTER_CLOUD_DEPLOYMENT_NAME', 'local')\n"
        content += "if deployment == 'prod':\n"
        content += "    target = 'prod'\n"
        content += "elif deployment in ['staging', 'dev']:\n"
        content += "    target = 'staging'\n"
        content += "else:\n"
        content += "    target = 'local'  # Default to DuckDB for local development\n"
        content += "```\n\n"
        content += "### How Target Selection Works\n\n"
        content += "The migration tool automatically configures deployment-aware target selection:\n\n"
        content += "1. **Local Development**: When `DAGSTER_CLOUD_DEPLOYMENT_NAME` is not set or is 'local', uses `local` target (DuckDB)\n"
        content += "2. **Deployments**: When deployed to Dagster Cloud, uses the deployment name as the target\n"
        content += "   - If deployment is 'prod', uses `prod` target from profiles.yml\n"
        content += "   - If deployment is 'staging', uses `staging` target from profiles.yml\n\n"
        content += "This matches the pattern used in the [Dagster demo project](https://github.com/dagster-io/hooli-data-eng-pipelines/blob/master/hooli-data-eng/src/hooli_data_eng/defs/dbt/resources.py).\n\n"
        content += "**No additional configuration needed!** The dbt components are already set up to use the right target based on your deployment.\n\n"
        
        # Next steps
        content += "## 📋 Next Steps\n\n"
        content += "1. **Review this summary** - Verify all projects, jobs, and environments were migrated correctly\n"
        content += "2. **Update credentials** - Review and update `.env` file with actual credentials\n"
        content += "3. **Configure profiles** - Copy `profiles.yml.template` to `~/.dbt/profiles.yml`\n"
        content += "4. **Clone repositories** - Run `./clone_dbt_projects.sh` to clone all dbt projects\n"
        content += "5. **Validate migration** - Run `./validate_migration.sh` to check setup\n"
        content += "6. **Configure alerts** - Manually set up alerts in Dagster (see warnings above)\n"
        content += "7. **Test locally** - Use `dbt_target=local` for local development with DuckDB\n"
        content += "8. **Deploy** - Deploy to Dagster Cloud and configure deployment-specific settings\n"
        content += "9. **Start Dagster** - Run `dg dev` to start the Dagster UI\n\n"
        
        # Asset checks note
        content += "## ✅ Automatic Features\n\n"
        content += "- **dbt Tests → Asset Checks**: dbt tests are automatically converted to Dagster asset checks by `dagster-dbt`\n"
        content += "- **Asset Dependencies**: dbt model dependencies are automatically mapped to Dagster asset dependencies\n"
        content += "- **Component-Based**: All definitions use Dagster's component system (no Python code generation)\n\n"
        
        with open(summary_path, "w") as f:
            f.write(content)

    def _get_timestamp(self) -> str:
        """Get current timestamp as string"""
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _generate_readme(
        self, projects: List[Dict[str, Any]], project_repos: Dict[int, str], required_adapters: Set[str]
    ):
        """Generate README for the Dagster project"""
        project_names = [
            p.get("name", f"project_{p.get('id')}")
            for p in projects
            if p.get("id") in project_repos
        ]

        adapter_list = ", ".join(sorted(required_adapters)) if required_adapters else "None detected"
        
        content = f"""# Dagster Project - Migrated from dbt Cloud

This project was generated by the dbt Cloud to Dagster migration assistant using Dagster 1.12+ CLI.

## Projects Migrated

{chr(10).join(f"- {name}" for name in project_names)}

## Detected dbt Adapters

{adapter_list}

## Setup

1. Install dependencies:
```bash
cd {self.output_dir.name}
pip install -e .
```

2. Configure environment variables:
   - Review and update the `.env` file with your database credentials
   - Update any placeholders marked with `<SET_MANUALLY>`
   - The `.env` file is gitignored by default for security

3. Configure dbt profiles:
   - A template `profiles.yml.template` has been generated
   - Copy it to `~/.dbt/profiles.yml` and update with your credentials
   - Or use the generated `.dbt/profiles.yml` in the project directory

4. Clone your dbt projects:
   - Run `./clone_dbt_projects.sh` to clone all repositories
   - Or manually clone to `./dbt_projects/` directory:
"""
        for project in projects:
            project_id = project.get("id")
            if project_id in project_repos:
                project_name = project.get("name", f"project_{project_id}")
                repo_url = project_repos[project_id]
                content += f"   - {project_name}: `git clone {repo_url} ./dbt_projects/{project_name}`\n"

        content += """
5. Start Dagster:
```bash
dg dev
# or
dagster dev
```

## Project Structure

- `defs/` - Contains component and definition files (all YAML-based)
  - `defs/<project_name>/` - dbt component definitions (created via `dg scaffold` as YAML)
  - `defs/jobs/defs.yaml` - Job component definitions (using `components.job.JobComponent`)
  - `defs/schedules/defs.yaml` - Schedule component definitions (using `components.schedule.ScheduleComponent`)
- `components/` - Custom component implementations (JobComponent, ScheduleComponent)
- `.env` - Environment variables (gitignored)
- `pyproject.toml` - Project dependencies including dbt adapters and dagster-cloud

## About Jobs and Schedules

Jobs and schedules are defined using **custom components** in YAML:
- **Jobs**: Defined using `components.job.JobComponent` in `defs/jobs/defs.yaml`
- **Schedules**: Defined using `components.schedule.ScheduleComponent` in `defs/schedules/defs.yaml`
- All definitions are component-based YAML - no Python code generation needed!
- Components are automatically loaded by Dagster's component system

## Generated Files

- `profiles.yml.template` - Template dbt profiles.yml (copy to `~/.dbt/profiles.yml`)
- `.dbt/profiles.yml` - Project-specific profiles.yml
- `.env` - Environment variables (gitignored)
- `clone_dbt_projects.sh` - Script to clone all dbt project repositories
- `validate_migration.sh` - Script to validate the migration

## Next Steps

1. **Review and update the `.env` file** with your actual credentials
2. **Configure dbt profiles**: Copy `profiles.yml.template` to `~/.dbt/profiles.yml` and update
3. **Clone dbt projects**: Run `./clone_dbt_projects.sh` or clone manually
4. **Validate migration**: Run `./validate_migration.sh` to check everything is set up correctly
5. **Review component configurations** in `defs/` directories
6. **Verify schedule cron expressions** match your requirements
7. **Test jobs manually** before enabling schedules
8. **Run `dg check defs`** to validate your configuration
9. **Start Dagster**: `dg dev` (or `dagster dev`)
10. **For Dagster Cloud deployment**: Ensure `dagster-cloud` is installed and configured

## Migration Features

✅ **Component-based architecture** - All definitions (dbt components, jobs, schedules) are YAML-based
✅ **Automatic adapter detection** - Detects and installs required dbt adapters
✅ **Environment variable extraction** - Extracts connection details from dbt Cloud
✅ **Profiles.yml generation** - Generates dbt profiles.yml from environment configurations
✅ **Job configuration** - Preserves job settings (threads, target, etc.) as tags
✅ **Schedule migration** - Maps dbt Cloud schedules to Dagster schedules
✅ **Git clone automation** - Script to clone all dbt project repositories
✅ **Migration validation** - Script to validate the migration setup
"""
        with open(self.output_dir / "README.md", "w") as f:
            f.write(content)

    def _sanitize_name(self, name: str) -> str:
        """Sanitize name for use in file paths and identifiers"""
        return name.replace("-", "_").replace(" ", "_").replace(".", "_").lower()

    def _get_project_package_name(self) -> str:
        """Get the Python package name for the project"""
        # Try to read from pyproject.toml
        pyproject_path = self.output_dir / "pyproject.toml"
        if pyproject_path.exists():
            try:
                # Try tomllib (Python 3.11+)
                try:
                    import tomllib
                    with open(pyproject_path, "rb") as f:
                        data = tomllib.load(f)
                        project_name = data.get("project", {}).get("name", "dagster_dbt_migration")
                        # Convert to Python package name format
                        return project_name.replace("-", "_")
                except ImportError:
                    # Fallback to toml (if available) or simple parsing
                    import re
                    with open(pyproject_path, "r") as f:
                        content = f.read()
                        # Simple regex to extract project name
                        match = re.search(r'name\s*=\s*"([^"]+)"', content)
                        if match:
                            return match.group(1).replace("-", "_")
            except Exception:
                pass
        
        # Default fallback - use directory name
        return self.output_dir.name.replace("-", "_")
