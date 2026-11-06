"""One explicit local request per process; JSON in, receipt and outcome JSON out."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
from dataclasses import asdict
from importlib.metadata import version
from pathlib import Path
from typing import Any

from pipeline import ingestion, trellis

ROOT = Path(__file__).resolve().parents[1]


def _digest(value) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _absolute_path(value: object) -> Path:
    if not isinstance(value, str) or not Path(value).expanduser().is_absolute():
        raise ValueError("Use an explicit absolute path")
    return Path(value).expanduser().resolve(strict=True)


def _engine_identity() -> dict:
    paths = sorted((ROOT / "pipeline").rglob("*.py"))
    paths.append(ROOT / "scripts" / "import_ris_network.py")
    files = {
        path.relative_to(ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in paths
    }
    git = (
        subprocess.run(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
        )
        if shutil.which("git")
        else None
    )
    return {
        "source_sha256": _digest(files),
        "git_revision": git.stdout.strip() if git and git.returncode == 0 else None,
        "python": platform.python_version(),
        "python_executable": str(Path(sys.executable).resolve()),
        "trellis_executable": shutil.which(trellis.TRELLIS_BIN),
        "dependencies": {
            name: version(name)
            for name in ("requests", "python-dotenv", "pyyaml", "rapidfuzz")
        },
    }


def _inputs(request: dict) -> tuple[list, list[dict]]:
    records = request.get("records") or []
    ris_paths = request.get("ris_paths") or []
    if not isinstance(records, list) or not isinstance(ris_paths, list):
        raise ValueError("records and ris_paths must be arrays")
    records = list(records)
    artifacts = []
    if ris_paths:
        from scripts.import_ris_network import parse_ris_text

        for value in ris_paths:
            path = _absolute_path(value)
            if not path.is_file() or path.suffix.lower() != ".ris":
                raise ValueError(
                    "Each RIS input must name one existing .ris file, not a directory"
                )
            data = path.read_bytes()
            parsed = [
                record.to_input()
                for record in parse_ris_text(data.decode("utf-8", errors="replace"))
            ]
            if not parsed:
                raise ValueError(f"No paper records found in RIS file: {path}")
            records.extend(parsed)
            artifacts.append(
                {
                    "path": str(path),
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "records": len(parsed),
                }
            )
    if not records:
        raise ValueError(
            "Specify paper records or exact RIS files; workspace scanning is not supported"
        )
    for record in records:
        ingestion.parse_input(ingestion._as_raw_input(record))
    return records, artifacts


def run_request(request: Any) -> dict:
    """Target binding is process-local, never a global library registration."""
    if not isinstance(request, dict):
        raise ValueError("Request must be an object")
    unknown = set(request) - {
        "operation",
        "workspace",
        "parent",
        "records",
        "ris_paths",
    }
    if unknown:
        raise ValueError(f"Unknown request fields: {', '.join(sorted(unknown))}")
    operation = request.get("operation")
    if operation not in ("preview", "ingest"):
        raise ValueError("operation must be preview or ingest")
    workspace = _absolute_path(request.get("workspace"))
    if not workspace.is_dir() or not (workspace / ".trellis" / "trellis.db").is_file():
        raise ValueError(
            "Target must be an existing initialized Trellis directory; no workspace is created"
        )
    parent = request.get("parent")
    if parent is not None and (not isinstance(parent, str) or not parent.strip()):
        raise ValueError("parent must be an existing node slug or UUID, or null")
    if operation == "ingest" and parent is None:
        raise ValueError("An existing parent node is required for reference ingestion")
    records, artifacts = _inputs(request)
    os.environ["TRELLIS_WORKSPACE"] = str(workspace)
    os.environ["TRELLIS_PROJECT_SLUG"] = parent or ""
    # An inherited RIS sink must not write to an unrelated library or vault.
    os.environ.pop("RIS_SINK_PATH", None)
    if parent:
        trellis.get_node(parent)
    engine = _engine_identity()
    if not engine["trellis_executable"]:
        raise ValueError("trellis executable is not available on PATH")
    inputs = {
        "workspace": str(workspace),
        "parent": parent,
        "records": records,
        "ris_inputs": artifacts,
    }
    receipt = {
        "operation": operation,
        "inputs": inputs,
        "inputs_sha256": _digest(inputs),
        "engine": engine,
    }
    if operation == "preview":
        index = trellis.build_node_index()
        matches = []
        for record in records:
            parsed = ingestion.parse_input(ingestion._as_raw_input(record))
            existing = trellis.dedup_check_indexed(
                index,
                doi=parsed.doi or "",
                pmid=parsed.pmid or "",
                title=parsed.title or "",
            )
            matches.append({"existing_id": existing.get("id") if existing else None})
        return {"receipt": receipt, "read_only": True, "records": matches}
    with contextlib.redirect_stdout(sys.stderr):
        outcomes, metrics = ingestion.ingest_batch(records, workers=1)
    results = []
    for outcome in outcomes:
        result = {
            "created": outcome.upsert.created if outcome.upsert else False,
            "errors": outcome.errors,
        }
        if outcome.upsert:
            node = trellis.get_node(outcome.upsert.slug)
            citations = (node.get("metadata") or {}).get("reference", {}).get(
                "outbound_citations"
            ) or {}
            result.update(
                {
                    "node_id": node.get("id"),
                    "slug": outcome.upsert.slug,
                    "pipeline_status": next(
                        (
                            str(t).split(":", 1)[1]
                            for t in node.get("tags", [])
                            if str(t).startswith("pipeline:")
                        ),
                        None,
                    ),
                    "citations_stored": len(citations.get("items") or []),
                    "citation_providers": citations.get("providers") or {},
                    "citations_sha256": _digest(citations.get("items") or []),
                    "links_processed": outcome.link.linked if outcome.link else 0,
                }
            )
        results.append(result)
    return {
        "receipt": receipt,
        "read_only": False,
        "outcomes": results,
        "metrics": asdict(metrics),
    }


def main() -> int:
    try:
        result = run_request(json.load(sys.stdin))
    except Exception as error:
        print(json.dumps({"error": str(error)}))
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
