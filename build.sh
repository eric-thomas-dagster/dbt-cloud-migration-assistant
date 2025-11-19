#!/bin/bash
# Build script for creating distribution packages

set -e

echo "🔨 Building dbt Cloud Migration Assistant..."

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf build/ dist/ *.egg-info/

# Install build tools if needed
if ! command -v python -m build &> /dev/null; then
    echo "Installing build tools..."
    pip install build
fi

# Build distribution packages
echo "Building distribution packages..."
python -m build

echo ""
echo "✅ Build complete!"
echo ""
echo "Distribution packages created in dist/:"
ls -lh dist/
echo ""
echo "To install from wheel:"
echo "  pip install dist/dbt-cloud-migration-assistant-*.whl"
echo ""
echo "To install from source:"
echo "  pip install dist/dbt-cloud-migration-assistant-*.tar.gz"

