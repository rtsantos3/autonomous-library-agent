import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def empty_workspace(tmp_path):
    workspace = tmp_path / "selected library"
    workspace.mkdir()
    try:
        subprocess.run(
            ["trellis", "init"], cwd=workspace, check=True, capture_output=True
        )
    except FileNotFoundError:
        pytest.skip("trellis is not available")
    return workspace


def invoke(request, **environment):
    result = subprocess.run(
        [sys.executable, "-m", "pipeline.local_agent"],
        input=json.dumps(request),
        text=True,
        capture_output=True,
        cwd=ROOT,
        env={**os.environ, **environment},
        check=False,
    )
    return result.returncode, json.loads(result.stdout)


@pytest.mark.integration
def test_preview_binds_explicit_workspace_and_hashes_exact_ris(
    empty_workspace, tmp_path
):
    project = json.loads(
        subprocess.run(
            ["trellis", "add", "project", "selected-parent", "--json"],
            cwd=empty_workspace,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    project = project.get("node", project)
    node = json.loads(
        subprocess.run(
            [
                "trellis",
                "add",
                "reference",
                "Selected paper",
                "--parent",
                "selected-parent",
                "--metadata",
                json.dumps({"reference": {"doi": "10.1234/selected"}}),
                "--json",
            ],
            cwd=empty_workspace,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    node = node.get("node", node)
    ris = tmp_path / "selected.ris"
    ris.write_text(
        "TY  - JOUR\nTI  - Explicit paper selection\nDO  - 10.1234/selected\nER  -\n"
    )
    request = {
        "operation": "preview",
        "workspace": str(empty_workspace),
        "ris_paths": [str(ris)],
    }
    inherited = str(tmp_path / "unrelated library")
    Path(inherited).mkdir()
    subprocess.run(["trellis", "init"], cwd=inherited, check=True, capture_output=True)
    code, first = invoke(
        request, TRELLIS_WORKSPACE=inherited, TRELLIS_PROJECT_SLUG="not-the-parent"
    )
    assert code == 0
    receipt = first["receipt"]
    assert receipt["inputs"]["workspace"] == str(empty_workspace)
    assert receipt["inputs"]["parent"] is None
    assert receipt["inputs"]["records"] == [
        {"title": "Explicit paper selection", "doi": "10.1234/selected"}
    ]
    assert first["read_only"] is True
    assert first["records"] == [{"existing_id": node["id"]}]
    unrelated = subprocess.run(
        ["trellis", "find", "--json"],
        cwd=inherited,
        check=True,
        capture_output=True,
        text=True,
    )
    assert json.loads(unrelated.stdout) == []
    code, repeated = invoke(request)
    assert code == 0
    assert repeated["receipt"]["inputs_sha256"] == receipt["inputs_sha256"]
    assert repeated["receipt"]["engine"] == receipt["engine"]
    ris.write_text(ris.read_text().replace("10.1234/selected", "10.1234/changed"))
    code, changed = invoke(request)
    assert code == 0
    assert changed["receipt"]["inputs_sha256"] != receipt["inputs_sha256"]
    assert (
        changed["receipt"]["inputs"]["ris_inputs"][0]["sha256"]
        != receipt["inputs"]["ris_inputs"][0]["sha256"]
    )
    graph = subprocess.run(
        ["trellis", "find", "--json"],
        cwd=empty_workspace,
        check=True,
        capture_output=True,
        text=True,
    )
    assert {n["id"] for n in json.loads(graph.stdout)} == {project["id"], node["id"]}


@pytest.mark.integration
def test_missing_workspace_and_directory_import_are_rejected(empty_workspace, tmp_path):
    missing = tmp_path / "not initialized"
    missing.mkdir()
    code, result = invoke(
        {
            "operation": "ingest",
            "workspace": str(missing),
            "records": ["10.1234/selected"],
        }
    )
    assert code == 1
    assert "existing initialized Trellis" in result["error"]
    assert not (missing / ".trellis").exists()
    code, result = invoke(
        {
            "operation": "preview",
            "workspace": str(empty_workspace),
            "ris_paths": [str(tmp_path)],
        }
    )
    assert code == 1
    assert "not a directory" in result["error"]


@pytest.mark.integration
def test_ingestion_requires_an_explicit_existing_parent(empty_workspace):
    code, result = invoke(
        {
            "operation": "ingest",
            "workspace": str(empty_workspace),
            "records": ["10.1234/selected"],
        }
    )
    assert code == 1
    assert "existing parent node is required" in result["error"]
    graph = subprocess.run(
        ["trellis", "find", "--json"],
        cwd=empty_workspace,
        check=True,
        capture_output=True,
        text=True,
    )
    assert json.loads(graph.stdout) == []
