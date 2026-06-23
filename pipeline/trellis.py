"""
Trellis CLI wrapper for the autonomous library agent pipeline.

All mutations go through the trellis CLI. No LLM involvement here —
this module is pure data transport between the pipeline and the graph store.
"""
from __future__ import annotations

import json
import subprocess
import unicodedata
from pathlib import Path
from typing import Optional

import os

TRELLIS_BIN = "trellis"
# Trellis workspace root — the directory containing .trellis/
# Override with TRELLIS_WORKSPACE env var when the workspace is not the repo root.
PROJECT_ROOT = os.environ.get(
    "TRELLIS_WORKSPACE",
    str(Path(__file__).resolve().parents[2])  # LAD_library/ when cloned inside it
)
PROJECT_SLUG = "microbiome-research-library"
ACTOR = "daedalus"

PIPELINE_TAGS = {
    "pipeline:queued",
    "pipeline:scaffolded",
    "pipeline:digesting",
    "pipeline:digested",
    "pipeline:partial",
    "pipeline:needs-review",
    "pipeline:failed",
}


# ---------------------------------------------------------------------------
# Subprocess primitives
# ---------------------------------------------------------------------------

def _run(*args: str) -> str:
    result = subprocess.run(
        [TRELLIS_BIN, *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"trellis {args[0]!r} failed (exit {result.returncode}):\n"
            f"  stdout: {result.stdout.strip()}\n"
            f"  stderr: {result.stderr.strip()}"
        )
    return result.stdout.strip()


def _run_json(*args: str) -> object:
    raw = _run(*args)
    return json.loads(raw) if raw else {}


def _unwrap_node(data: object) -> dict:
    if isinstance(data, dict) and "node" in data:
        return data["node"]
    return data or {}


def _unwrap_list(data: object) -> list[dict]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("results", data.get("nodes", []))
    return []


def _norm_title(t: str) -> str:
    if not t:
        return ""
    t = unicodedata.normalize("NFKD", t.lower().strip())
    t = t.encode("ascii", "ignore").decode("ascii").rstrip(".")
    return " ".join(t.split())


def _normalize_doi_uri(s: str) -> str:
    """Normalize DOI URI forms to canonical https://doi.org/<doi>."""
    if not s:
        return ""
    value = s.strip()
    lower = value.lower()
    for prefix in ("doi:", "https://doi.org/", "http://dx.doi.org/"):
        if lower.startswith(prefix):
            value = value[len(prefix):]
            break
    return f"https://doi.org/{value.strip()}"


# ---------------------------------------------------------------------------
# Read operations
# ---------------------------------------------------------------------------

def get_node(slug_or_uuid: str) -> dict:
    return _unwrap_node(_run_json("get", slug_or_uuid, "--json"))


def find_nodes(text: str = None, tag: str = None, limit: int = None) -> list[dict]:
    args = ["find"]
    if text:
        args += ["--text", text]
    if tag:
        args += ["--tag", tag]
    if limit is not None:
        args += ["--limit", str(limit)]
    args.append("--json")
    try:
        raw = _run(*args)
    except RuntimeError:
        return []
    return _unwrap_list(json.loads(raw)) if raw else []


def grep_nodes(query: str) -> list[dict]:
    """Full-text grep across all node fields including metadata JSON."""
    try:
        raw = _run("grep", query, "--json")
    except RuntimeError:
        return []
    if not raw:
        return []
    parsed = json.loads(raw)
    return parsed if isinstance(parsed, list) else []


def get_by_pipeline_status(status: str) -> list[dict]:
    return find_nodes(tag=f"pipeline:{status}")


# ---------------------------------------------------------------------------
# Write operations
# ---------------------------------------------------------------------------

def add_reference(
    title: str,
    uri: str = None,
    abstract: str = None,
    citation: str = None,
    metadata: dict = None,
    tags: list[str] = None,
    file_path: str = None,
    actor_id: str = ACTOR,
) -> dict:
    """
    Add a new reference node. metadata should be the full metadata_ dict.

    Always pass uri as https://doi.org/<doi>. URI is set-once at create time;
    the Trellis CLI does not support updating it.
    """
    args = ["add", "reference", title]
    if uri:
        args += ["--uri", uri]
    if abstract:
        args += ["--abstract", abstract]
    if citation:
        args += ["--citation", citation]
    if metadata is not None:
        args += ["--metadata", json.dumps(metadata)]
    if tags:
        args += ["--tags", ",".join(tags)]
    if file_path:
        args += ["--file", file_path]
    args += ["--parent", PROJECT_SLUG, "--actor-id", actor_id, "--json"]
    return _unwrap_node(_run_json(*args))


def update_node(
    slug_or_uuid: str,
    tags: list[str] = None,
    abstract: str = None,
    citation: str = None,
    metadata: dict = None,
    actor_id: str = ACTOR,
) -> dict:
    """
    Update a node's bound fields. Only provided fields are changed (PATCH semantics).
    metadata replaces the entire metadata_ JSON blob when provided.
    description is intentionally excluded — not written by the pipeline.
    """
    args = ["update", slug_or_uuid]
    if tags is not None:
        args += ["--tags", ",".join(tags)]
    if abstract is not None:
        args += ["--abstract", abstract]
    if citation is not None:
        args += ["--citation", citation]
    if metadata is not None:
        args += ["--metadata", json.dumps(metadata)]
    args += ["--actor-id", actor_id, "--json"]
    return _unwrap_node(_run_json(*args))


def add_child_node(
    node_type: str,
    title: str,
    description: str = None,
    parent: str = None,
    tags: list[str] = None,
    actor_id: str = ACTOR,
) -> dict:
    args = ["add", node_type, title]
    if description:
        args += ["--description", description]
    if parent:
        args += ["--parent", parent]
    if tags:
        args += ["--tags", ",".join(tags)]
    args += ["--actor-id", actor_id, "--json"]
    return _unwrap_node(_run_json(*args))


def link_nodes(
    source: str,
    target: str,
    relation: str = "references",
    actor_id: str = ACTOR,
) -> dict:
    """
    Returns {"ok": True} on success or idempotent duplicate.
    Returns {"ok": False, "error": str} on real failure.
    Never raises.
    """
    try:
        _run_json("link", source, target, "--relation", relation, "--actor-id", actor_id, "--json")
        return {"ok": True}
    except RuntimeError as e:
        msg = str(e)
        lower = msg.lower()
        if "already" in lower or "exist" in lower or "duplicate" in lower:
            return {"ok": True, "idempotent": True}
        return {"ok": False, "error": msg}


def annotate_node(slug_or_uuid: str, note: str, actor_id: str = ACTOR) -> dict:
    return _unwrap_node(
        _run_json("annotate", slug_or_uuid, note, "--actor-id", actor_id, "--json")
    )


# ---------------------------------------------------------------------------
# Pipeline state
# ---------------------------------------------------------------------------

def set_pipeline_status(slug_or_uuid: str, status: str, actor_id: str = ACTOR) -> dict:
    """Replace the pipeline:* tag on a node, preserving all other tags."""
    new_tag = f"pipeline:{status}"
    if new_tag not in PIPELINE_TAGS:
        raise ValueError(f"Unknown pipeline status {status!r}. Valid: {sorted(PIPELINE_TAGS)}")
    node = get_node(slug_or_uuid)
    existing = node.get("tags") or []
    kept = [t for t in existing if not t.startswith("pipeline:")]
    kept.append(new_tag)
    return update_node(slug_or_uuid, tags=kept, actor_id=actor_id)


# ---------------------------------------------------------------------------
# Dedup chain
# ---------------------------------------------------------------------------

def find_by_s2id(s2id: str) -> Optional[dict]:
    nodes = find_nodes(tag=f"s2id:{s2id}")
    return nodes[0] if nodes else None


def find_by_doi(doi: str) -> Optional[dict]:
    """Check uri field first, then grep metadata for alt_dois."""
    uri = _normalize_doi_uri(doi)
    for node in find_nodes(text=uri):
        if _normalize_doi_uri(node.get("uri", "")) == uri:
            return node
    # Catches alt_dois stored in metadata
    for node in grep_nodes(doi):
        if node.get("slug"):
            return node
    return None


def find_by_pmid(pmid: str) -> Optional[dict]:
    nodes = find_nodes(tag=f"pmid:{pmid}")
    return nodes[0] if nodes else None


def find_by_title(title: str) -> Optional[dict]:
    nt = _norm_title(title)
    if not nt or len(nt) < 10:
        return None
    for node in find_nodes(text=nt):
        if _norm_title(node.get("title", "")) == nt:
            return node
    return None


def dedup_check(
    s2id: str = None,
    doi: str = None,
    pmid: str = None,
    title: str = None,
) -> Optional[dict]:
    """Full dedup chain. Returns existing node or None."""
    if s2id:
        node = find_by_s2id(s2id)
        if node:
            return node
    if doi:
        node = find_by_doi(doi)
        if node:
            return node
    if pmid:
        node = find_by_pmid(pmid)
        if node:
            return node
    if title:
        node = find_by_title(title)
        if node:
            return node
    return None
