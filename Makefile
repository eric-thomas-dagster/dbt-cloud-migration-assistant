.PHONY: install build dist clean test

# Install the package in development mode
install:
	pip install -e .

# Build distribution packages
build:
	python -m build

# Create source distribution and wheel
dist: build
	@echo "Distribution packages created in dist/"

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	find . -type d -name __pycache__ -exec rm -r {} +
	find . -type f -name "*.pyc" -delete

# Run tests (if you add tests)
test:
	python -m pytest tests/ -v

# Create a standalone installer script
create-installer:
	@echo "#!/bin/bash" > install.sh
	@echo "# Quick installer for dbt Cloud Migration Assistant" >> install.sh
	@echo "pip install -e ." >> install.sh
	@chmod +x install.sh
	@echo "Created install.sh"

