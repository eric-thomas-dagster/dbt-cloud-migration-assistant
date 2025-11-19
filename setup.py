"""Setup script for dbt Cloud Migration Assistant"""

from setuptools import setup, find_packages

# Read README for long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="dbt-cloud-migration-assistant",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Migrate dbt Cloud projects to dbt Core + Dagster",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/dbt-cloud-migration-assistant",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "click>=8.0.0",
        "requests>=2.28.0",
        "pyyaml>=6.0",
        "dagster[cli]>=1.12.0",
        "dagster-dbt>=0.22.0",
        "dbt-core>=1.5.0",
    ],
    entry_points={
        "console_scripts": [
            "dbt-cloud-migrate=dbt_cloud_migration_assistant.cli:main",
        ],
    },
)

