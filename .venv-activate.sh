#!/bin/bash
# ============================================================
# Helper script to activate Python virtual environment
# ============================================================
# This script checks if a virtual environment exists and
# activates it. It's meant to be sourced by demo scripts.
#
# Usage (in a demo script):
#   source .venv-activate.sh
#
# It will:
#   1. Check if venv/ exists
#   2. If yes, activate it
#   3. If no, check if python3 is available
#   4. If not available, error out with helpful message
# ============================================================

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" 2>/dev/null && pwd)"
VENV_DIR="$PROJECT_ROOT/venv"

# Try to activate venv if it exists
if [ -d "$VENV_DIR" ]; then
    if [ -f "$VENV_DIR/bin/activate" ]; then
        source "$VENV_DIR/bin/activate"
        # Optional: uncomment for debugging
        # echo "[DEBUG] Activated venv at $VENV_DIR"
    fi
elif ! command -v python3 &> /dev/null; then
    echo "ERROR: Neither virtual environment nor system python3 found."
    echo ""
    echo "Please run setup first:"
    echo "  bash setup-venv.sh"
    echo ""
    exit 1
fi
