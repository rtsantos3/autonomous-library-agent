"""Tests for the RIS importer (scripts/import_ris_network.py) and RIS sink."""

import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
IMPORTER = REPO_ROOT / "scripts" / "import_ris_network.py"

sys.path.insert(0, str(REPO_ROOT / "scripts"))
import import_ris_network  # noqa: E402
from import_ris_network import RisRecord  # noqa: E402

from pipeline import ris_sink, trellis  # noqa: E402
from pipeline.ris_sink import RisSink, format_record, sink_from_env  # noqa: E402


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


# ---------------------------------------------------------------------------
# RIS sink: formatting, dedup, path resolution
# ---------------------------------------------------------------------------

_PAPER = {
    "title": "Fast community detection by SCORE",
    "authors": ["Jiashun Jin", "Ke, Zheng Tracy"],
    "year": "2015",
    "venue": "The Annals of Statistics",
    "doi": "10.1214/14-AOS1265",
    "pmid": "123",
    "keywords": ["networks"],
    "abstract": "Spectral   clustering\non ratios.",
}


def test_format_record_renders_ris_fields_and_requires_title():
    text = format_record(_PAPER)
    lines = text.splitlines()
    assert lines[0] == "TY  - JOUR"
    assert "TI  - Fast community detection by SCORE" in lines
    assert "AU  - Jin, Jiashun" in lines  # "Given Family" -> "Family, Given"
    assert "AU  - Ke, Zheng Tracy" in lines  # already comma form: kept
    assert "PY  - 2015" in lines
    assert "JO  - The Annals of Statistics" in lines
    assert "DO  - 10.1214/14-AOS1265" in lines
    assert "UR  - https://doi.org/10.1214/14-AOS1265" in lines
    assert "AN  - PMID:123" in lines
    assert "KW  - networks" in lines
    assert "AB  - Spectral clustering on ratios." in lines  # one line
    assert lines[-2:] == ["ER  - ", ""]  # record closes, then a blank separator
    with pytest.raises(ValueError):
        format_record({"title": ""})


def test_append_dedups_by_doi_case_insensitive_and_by_title(tmp_path):
    sink = RisSink(tmp_path / "lib.ris")
    assert sink.append(_PAPER) is True
    assert sink.append({"title": "other", "doi": "10.1214/14-aos1265"}) is False
    assert sink.append({"title": "A Title Only Paper"}) is True
    assert sink.append({"title": "a title only paper"}) is False
    assert sink.path.read_text(encoding="utf-8").count("ER  -") == 2


def test_dedup_survives_a_new_sink_instance(tmp_path):
    path = tmp_path / "lib.ris"
    RisSink(path).append(_PAPER)
    RisSink(path).append({"title": "Title Only Survives Reload"})
    fresh = RisSink(path)
    assert fresh.append(dict(_PAPER)) is False
    assert fresh.append({"title": "title only survives reload"}) is False
    assert path.read_text(encoding="utf-8").count("ER  -") == 2


def test_sink_path_precedence(tmp_path, monkeypatch):
    monkeypatch.delenv("RIS_SINK_PATH", raising=False)
    assert RisSink().path == ris_sink.DEFAULT_PATH
    monkeypatch.setenv("RIS_SINK_PATH", str(tmp_path / "env.ris"))
    assert RisSink().path == tmp_path / "env.ris"
    assert RisSink(tmp_path / "explicit.ris").path == tmp_path / "explicit.ris"


def test_sink_from_env_is_off_without_env(tmp_path, monkeypatch):
    monkeypatch.delenv("RIS_SINK_PATH", raising=False)
    assert sink_from_env() is None
    monkeypatch.setenv("RIS_SINK_PATH", str(tmp_path / "env.ris"))
    assert isinstance(sink_from_env(), RisSink)


# ---------------------------------------------------------------------------
# Intake hook (live Trellis + network)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_ingest_batch_feeds_every_resolved_paper_to_sink(ephemeral_trellis, tmp_path):
    from pipeline.ingestion import ingest_batch

    sink = RisSink(tmp_path / "lib.ris")
    outcomes, _ = ingest_batch(["10.1214/14-AOS1265"], workers=1, ris_sink=sink)
    assert outcomes[0].resolve is not None
    text = sink.path.read_text(encoding="utf-8")
    assert "do  - 10.1214/14-aos1265" in text.lower()


@pytest.mark.integration
def test_sink_failure_is_logged_not_fatal(ephemeral_trellis, caplog):
    from pipeline.ingestion import ingest_batch

    def broken_sink(paper):
        raise RuntimeError("disk full")

    with caplog.at_level(logging.WARNING):
        outcomes, _ = ingest_batch(
            ["10.1214/14-AOS1265"], workers=1, ris_sink=broken_sink
        )
    assert not outcomes[0].errors
    assert "ris_sink failed" in caplog.text


# ---------------------------------------------------------------------------
# Importer drop-folder behaviour (dry-run: no network, no graph writes)
# ---------------------------------------------------------------------------

_ONE_RIS = "TY  - JOUR\nTI  - Some Paper In The Drop Folder\nER  -\n"


def _run_importer(*args, workspace):
    env = dict(os.environ, TRELLIS_WORKSPACE=str(workspace))
    env.pop("RIS_SINK_PATH", None)
    return subprocess.run(
        [sys.executable, str(IMPORTER), *map(str, args)],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        env=env,
    )


def test_importer_never_reads_the_sink_output(tmp_path):
    folder = tmp_path / "references"
    folder.mkdir()
    (folder / "in.ris").write_text(_ONE_RIS, encoding="utf-8")
    (folder / "library.ris").write_text(format_record(_PAPER), encoding="utf-8")
    result = _run_importer(
        folder, "--ris-sink", folder / "library.ris", "--dry-run", workspace=tmp_path
    )
    assert result.returncode == 0, result.stderr
    assert "files     : 1" in result.stdout
    assert "Fast community detection" not in result.stdout


def test_importer_skips_files_in_the_archive(tmp_path):
    folder = tmp_path / "references"
    (folder / "imported").mkdir(parents=True)
    (folder / "in.ris").write_text(_ONE_RIS, encoding="utf-8")
    (folder / "imported" / "old.ris").write_text(_ONE_RIS, encoding="utf-8")
    result = _run_importer(
        folder, "--archive", folder / "imported", "--dry-run", workspace=tmp_path
    )
    assert result.returncode == 0, result.stderr
    assert "files     : 1" in result.stdout


def test_notes_dir_outside_workspace_exits_2(tmp_path):
    workspace = tmp_path / "ws"
    workspace.mkdir()
    ris = tmp_path / "in.ris"
    ris.write_text(_ONE_RIS, encoding="utf-8")
    result = _run_importer(
        ris, "--notes-dir", tmp_path / "elsewhere", "--dry-run", workspace=workspace
    )
    assert result.returncode == 2
    assert "outside the workspace" in result.stdout


def test_write_stub_note_names_and_never_overwrites(tmp_path):
    record = RisRecord(
        title="Fast community detection by SCORE",
        authors=["Jin, Jiashun"],
        year="2015",
        doi="10.1214/14-aos1265",
    )
    path = import_ris_network.write_stub_note(record, tmp_path, "in.ris")
    assert path.name == "jin-2015-fast-community-detection-by-score.md"
    body = path.read_text(encoding="utf-8")
    assert "# Fast community detection by SCORE" in body
    assert "https://doi.org/10.1214/14-aos1265" in body
    path.write_text("my edits", encoding="utf-8")
    assert import_ris_network.write_stub_note(record, tmp_path, "in.ris") is None
    assert path.read_text(encoding="utf-8") == "my edits"


class _FakeResponse:
    def __init__(self, items):
        self._items = items

    def json(self):
        return {"message": {"items": self._items}}


def test_crossref_doi_requires_exact_title_match(monkeypatch):
    items = [
        {"DOI": "10.9/near", "title": ["phyloseq: an R package (preprint)"]},
        {"DOI": "10.1371/JOURNAL.PONE.0061217", "title": ["PhyloSeq: An R Package"]},
    ]
    monkeypatch.setattr(
        import_ris_network, "http_get", lambda *a, **k: _FakeResponse(items)
    )
    lookup = import_ris_network.crossref_doi_for_title
    assert lookup("phyloseq: an R package") == "10.1371/journal.pone.0061217"
    assert lookup("phyloseq: an R package for something else") == ""


def test_project_slug_env_overrides_add_reference_parent(monkeypatch):
    calls = []
    monkeypatch.setattr(
        trellis, "_run_json", lambda *args: calls.append(args) or {"node": {}}
    )
    monkeypatch.delenv("TRELLIS_PROJECT_SLUG", raising=False)
    trellis.add_reference("A Paper Title For Parent Test", uri="https://doi.org/10.1/x")
    assert calls[-1][calls[-1].index("--parent") + 1] == "microbiome-research-library"
    monkeypatch.setenv("TRELLIS_PROJECT_SLUG", "references")
    trellis.add_reference("A Paper Title For Parent Test", uri="https://doi.org/10.1/x")
    assert calls[-1][calls[-1].index("--parent") + 1] == "references"


# ---------------------------------------------------------------------------
# Regression tests for the PR #9 review findings
# ---------------------------------------------------------------------------


def _main(monkeypatch, *argv):
    monkeypatch.setattr(sys, "argv", ["import_ris_network.py", *map(str, argv)])
    monkeypatch.setattr(import_ris_network, "load_dotenv", lambda: None)
    # main() writes TRELLIS_PROJECT_SLUG for --parent; register it with
    # monkeypatch first so teardown restores it and it cannot leak to later tests.
    monkeypatch.setenv("TRELLIS_PROJECT_SLUG", trellis.PROJECT_SLUG)
    return import_ris_network.main()


def test_default_sink_path_is_collected_when_no_sink_configured(
    tmp_path, monkeypatch, capsys
):
    # Review finding 1: with neither --ris-sink nor RIS_SINK_PATH, a file that
    # happens to sit at the default sink path is ordinary input.
    folder = tmp_path / "references"
    folder.mkdir()
    default = folder / "library.ris"
    default.write_text(_ONE_RIS, encoding="utf-8")
    monkeypatch.setattr(ris_sink, "DEFAULT_PATH", default)
    monkeypatch.delenv("RIS_SINK_PATH", raising=False)
    monkeypatch.setenv("TRELLIS_WORKSPACE", str(tmp_path))
    assert _main(monkeypatch, folder, "--dry-run") == 0
    assert "files     : 1" in capsys.readouterr().out


def test_append_dedups_stored_url_and_prefix_doi_forms(tmp_path):
    # Review finding 2: stored DOIs in URL or doi: form must match a bare DOI.
    path = tmp_path / "lib.ris"
    path.write_text(
        "TY  - JOUR\nTI  - Stored A\nDO  - https://doi.org/10.1/ABC\nER  - \n\n"
        "TY  - JOUR\nTI  - Stored B\nDO  - doi:10.2/XYZ.\nER  - \n\n",
        encoding="utf-8",
    )
    sink = RisSink(path)
    assert sink.append({"title": "New A", "doi": "10.1/abc"}) is False
    assert (
        sink.append({"title": "New B", "doi": "https://dx.doi.org/10.2/xyz"}) is False
    )
    assert path.read_text(encoding="utf-8").count("ER  -") == 2


def test_archive_never_overwrites_same_basename(tmp_path):
    # Review finding 3: two inputs with one basename must both survive.
    for name, body in (("a", "first"), ("b", "second")):
        (tmp_path / name).mkdir()
        (tmp_path / name / "paper.ris").write_text(body, encoding="utf-8")
    archive = tmp_path / "imported"
    import_ris_network.archive_files(
        [tmp_path / "a" / "paper.ris", tmp_path / "b" / "paper.ris"], archive
    )
    bodies = sorted(p.read_text(encoding="utf-8") for p in archive.iterdir())
    assert bodies == ["first", "second"]


def test_note_bind_failure_is_an_error_and_blocks_archive(
    tmp_path, monkeypatch, capsys
):
    # Review finding 4: a note failure after node creation is reported and
    # counted, so the RIS file stays in the drop folder for a retry.
    from types import SimpleNamespace

    folder = tmp_path / "references"
    folder.mkdir()
    ris = folder / "in.ris"
    ris.write_text(_ONE_RIS, encoding="utf-8")
    monkeypatch.setenv("TRELLIS_WORKSPACE", str(tmp_path))
    monkeypatch.delenv("RIS_SINK_PATH", raising=False)
    monkeypatch.setattr(trellis, "get_node", lambda slug: {"slug": slug})
    outcome = SimpleNamespace(
        errors=[],
        link=None,
        upsert=SimpleNamespace(created=True, slug="some-paper"),
    )
    monkeypatch.setattr(
        import_ris_network, "ingest_batch", lambda items, **kw: ([outcome], None)
    )

    def refuse_bind(*args, **kwargs):
        raise RuntimeError("bind refused")

    monkeypatch.setattr(trellis, "update_node", refuse_bind)
    code = _main(
        monkeypatch,
        folder,
        "--parent",
        "references",
        "--no-crossref",
        "--notes-dir",
        folder,
        "--archive",
        folder / "imported",
    )
    out = capsys.readouterr().out
    assert code == 0
    assert "note error (bind refused)" in out
    assert "errors=1" in out
    assert ris.exists()  # not archived
    assert "archive   : skipped" in out
