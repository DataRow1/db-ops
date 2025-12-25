#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

uv python install 3.11
rm -rf .venv build dist

uv venv --python 3.11
source .venv/bin/activate

uv pip install -e ".[cli]"
uv pip install "pyinstaller==6.17.0"

pyinstaller --noconfirm --clean dbops.spec

./dist/dbops/dbops --help >/dev/null
echo "✅ build ok: dist/dbops/dbops"