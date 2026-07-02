"""Tests for the RIS importer (scripts/import_ris_network.py)."""

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
IMPORTER = REPO_ROOT / "scripts" / "import_ris_network.py"

sys.path.insert(0, str(REPO_ROOT / "scripts"))
from import_ris_network import RisRecord  # noqa: E402


def test_to_input_passes_ris_keywords_as_fallback_floor():
    # RIS KW values ride along in the input dict so resolve_identity can keep
    # them when enrichment resolves no keywords of its own.
    record = RisRecord(
        title="A Title Only Paper On Gut Microbiota",
        keywords=["gut microbiome", "diet"],
    )
    assert record.to_input()["keywords"] == ["gut microbiome", "diet"]


def test_to_input_omits_keywords_when_absent():
    record = RisRecord(title="A Paper With No Keywords At All Here")
    assert "keywords" not in record.to_input()


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
