"""Tests for the RIS importer (scripts/import_ris_network.py)."""

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
IMPORTER = REPO_ROOT / "scripts" / "import_ris_network.py"

# Two identical entries (same DOI) plus one unique entry. EndNote RIS exports
# routinely repeat records; the importer must collapse them before ingest so the
# pipeline does not create twin nodes for the same paper.
_DUP_RIS = """\
TY  - JOUR
TI  - Duplicate Entry Paper On Microbiota
DO  - 10.1234/dup-test-abc
ER  -

TY  - JOUR
TI  - Duplicate Entry Paper On Microbiota
DO  - 10.1234/dup-test-abc
ER  -

TY  - JOUR
TI  - A Different Unique Paper
ER  -
"""


def test_importer_collapses_duplicate_records(tmp_path):
    ris = tmp_path / "dup.ris"
    ris.write_text(_DUP_RIS, encoding="utf-8")

    # --dry-run parses and dedups but writes nothing and makes no network calls,
    # so this is deterministic and isolated from the live graph.
    result = subprocess.run(
        [sys.executable, str(IMPORTER), str(ris), "--dry-run"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "dropped 1 duplicate record(s)" in result.stdout
    assert "records   : 2" in result.stdout
    assert "=== dry-run: nothing written ===" in result.stdout
