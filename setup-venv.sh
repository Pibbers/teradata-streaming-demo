#!/bin/bash
# ============================================================
# Setup virtual environment for Teradata Streaming Demo
# ============================================================
# Creates a Python virtual environment and installs all
# required dependencies for running the demo scripts.
#
# Usage:
#   bash setup-venv.sh
#
# After setup, activate the venv with:
#   source venv/bin/activate
#
# Or demos will auto-source it if available.
# ============================================================

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$PROJECT_ROOT/venv"

echo "=================================================="
echo "  Setting up Python virtual environment"
echo "=================================================="

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 not found. Please install Python 3.8 or later."
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
echo "✓ Using Python $PYTHON_VERSION"

# Create virtual environment
if [ -d "$VENV_DIR" ]; then
    echo "✓ Virtual environment already exists at $VENV_DIR"
else
    echo "→ Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
    echo "✓ Virtual environment created"
fi

# Upgrade pip
echo "→ Upgrading pip, setuptools, wheel..."
"$VENV_DIR/bin/pip" install --upgrade pip setuptools wheel > /dev/null 2>&1
echo "✓ pip upgraded"

# Install requirements
echo "→ Installing dependencies from requirements.txt..."
"$VENV_DIR/bin/pip" install -r "$PROJECT_ROOT/requirements.txt" > /dev/null 2>&1
echo "✓ Dependencies installed"

echo ""
echo "=================================================="
echo "  Setup complete!"
echo "=================================================="
echo ""
echo "To activate the virtual environment, run:"
echo "  source venv/bin/activate"
echo ""
echo "To verify installation, run:"
echo "  source venv/bin/activate && python -c \"import fastavro, confluent_kafka, boto3; print('All dependencies loaded successfully')\""
echo ""
