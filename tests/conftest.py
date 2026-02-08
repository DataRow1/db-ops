from __future__ import annotations

import sys
from pathlib import Path

# Ensure tests always import the local src tree, not an older installed wheel.
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))
