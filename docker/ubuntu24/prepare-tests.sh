#!/usr/bin/env bash
# Install the SQL harness in a venv on the Ubuntu 24.04 host.
set -euo pipefail
repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
venv=$HOME/.venvs/starrocks
python3 -m venv "$venv"
"$venv/bin/pip" install --upgrade pip
"$venv/bin/pip" install -r "$repo/test/requirements.txt"
mkdir -p "$HOME/build-logs"
"$venv/bin/pip" freeze >"$HOME/build-logs/sql-harness-requirements.txt"
cd "$repo/test"
"$venv/bin/python" run.py --help
