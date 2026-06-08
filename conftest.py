"""pytest configuration for the repo.

Makes the repo root importable so `import app...` works when running `pytest`
from the repo root. This repo has no src/ layout; tests live in tests/ mirroring
app/ (see PROJECT.md conventions), so we prepend this file's directory (the repo
root) to sys.path rather than relying on import-mode magic.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
