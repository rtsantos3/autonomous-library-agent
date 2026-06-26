"""
Trellis CLI wrapper for the autonomous library agent pipeline.

All mutations go through the trellis CLI. No LLM involvement here —
this module is pure data transport between the pipeline and the graph store.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import sqlite3
import subprocess
import time
import unicodedata
from pathlib import Path
from typing import Optional

TRELLIS_BIN = "trellis"
_REPO_ROOT = Path(__file__).resolve().parents[1]
_CONFIG_PATH = _REPO_ROOT / "config.yml"
# Default workspace — the directory above the repo (the "upper directory", i.e.
# the library this agent sits next to). One agent serves many libraries; point it
# at a specific one via config.yml's `workspace:` or the TRELLIS_WORKSPACE env var.
_DEFAULT_WORKSPACE = str(_REPO_ROOT.parent)


def _config_workspace() -> Optional[str]:
    # Non-secret tuneable persisted by setup.sh. Read lazily and degrade
    # gracefully (missing file / pyyaml absent) so the agent still runs on the
    # default workspace even before setup has created config.yml.
    try:
        import yaml
    except ImportError:
        return None
    try:
        with open(_CONFIG_PATH) as fh:
            cfg = yaml.safe_load(fh) or {}
    except (OSError, yaml.YAMLError):
        return None
    ws = cfg.get("workspace")
    if ws and str(ws).strip():
        return str(ws).strip()
    return None


def _workspace() -> str:
    # Resolved per call rather than frozen at import, so tests (and deploys) can
    # repoint the workspace without reloading this module. Precedence:
    #   TRELLIS_WORKSPACE env (explicit override: tests/CI/one-off)
    #   -> config.yml `workspace:` (persisted by setup.sh)
    #   -> default to the repo's parent directory.
    env = os.environ.get("TRELLIS_WORKSPACE")
    if env:
        return env
    return _config_workspace() or _DEFAULT_WORKSPACE


# Back-compat module constant: the workspace as resolved at import time.
PROJECT_ROOT = _workspace()
PROJECT_SLUG = "microbiome-research-library"
ACTOR = "daedalus"
logger = logging.getLogger(__name__)

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
    last_result = None
    for attempt in range(5):
        try:
            result = subprocess.run(
                [TRELLIS_BIN, *args],
                cwd=_workspace(),
                capture_output=True,
                text=True,
                timeout=120,
            )
        except subprocess.TimeoutExpired as exc:
            command = " ".join([TRELLIS_BIN, *args])
            raise RuntimeError(
                f"{command} timed out after {exc.timeout} seconds"
            ) from exc
        if result.returncode == 0:
            return result.stdout.strip()

        last_result = result
        combined = f"{result.stdout}\n{result.stderr}".lower()
        if "locked" not in combined and "busy" not in combined:
            break
        if attempt < 4:
            time.sleep(0.1 * (2**attempt))

    result = last_result
    if result is not None and result.returncode != 0:
        raise RuntimeError(
            f"trellis {args[0]!r} failed (exit {result.returncode}):\n"
            f"  stdout: {result.stdout.strip()}\n"
            f"  stderr: {result.stderr.strip()}"
        )
    return ""


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
            value = value[len(prefix) :]
            break
    return f"https://doi.org/{value.strip()}"


def _doi_key(s: str) -> Optional[str]:
    """Normalize DOI input to the bare lowercase DOI used for index keys."""
    uri = _normalize_doi_uri(s or "")
    prefix = "https://doi.org/"
    if not uri.startswith(prefix):
        return None
    doi = uri[len(prefix) :].strip().lower()
    return doi or None


def _node_identifier(node: dict) -> Optional[str]:
    return node.get("id") or node.get("uuid") or node.get("slug")


def _reference_metadata(node: dict) -> dict:
    metadata = node.get("metadata") or {}
    if not isinstance(metadata, dict):
        return {}
    reference = metadata.get("reference") or {}
    return reference if isinstance(reference, dict) else {}


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
    except RuntimeError as exc:
        logger.warning("find_nodes text=%r tag=%r failed: %s", text, tag, exc)
        return []
    return _unwrap_list(json.loads(raw)) if raw else []


def grep_nodes(query: str) -> list[dict]:
    """Full-text grep across all node fields including metadata JSON."""
    try:
        raw = _run("grep", query, "--json")
    except RuntimeError as exc:
        logger.warning("grep_nodes query=%r failed: %s", query, exc)
        return []
    if not raw:
        return []
    parsed = json.loads(raw)
    return parsed if isinstance(parsed, list) else []


def get_by_pipeline_status(status: str) -> list[dict]:
    # `trellis find` defaults to a 100-row cap; without an explicit limit a full
    # backfill scan would silently see only the first 100 matching nodes. Match
    # the bound used by build_node_index so the whole status cohort is returned.
    return find_nodes(tag=f"pipeline:{status}", limit=5000)


# ---------------------------------------------------------------------------
# Batch index
# ---------------------------------------------------------------------------


def build_node_index() -> dict:
    """
    Build an in-memory lookup index for Trellis nodes in one subprocess call.

    This is used by citation linking to avoid running the full dedup subprocess
    chain once per cited paper.
    """
    index = {
        "by_s2id": {},
        "by_doi": {},
        "by_pmid": {},
        "by_title": {},
        "pending_citations": {},
    }
    for node in find_nodes(limit=5000):
        tags = node.get("tags") or []
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",") if t.strip()]

        reference = _reference_metadata(node)
        doi_values = [
            node.get("uri", ""),
            reference.get("uri", ""),
            reference.get("doi", ""),
        ]
        doi_values.extend(reference.get("alt_dois") or [])
        for doi_value in doi_values:
            doi = _doi_key(doi_value)
            if doi:
                index["by_doi"].setdefault(doi, node)

        for tag in tags:
            tag = str(tag)
            if tag.startswith("s2id:"):
                s2id = tag.split(":", 1)[1].strip()
                if s2id:
                    index["by_s2id"].setdefault(s2id, node)
            elif tag.startswith("pmid:"):
                pmid = tag.split(":", 1)[1].strip()
                if pmid:
                    index["by_pmid"].setdefault(pmid, node)

        title = _norm_title(node.get("title", ""))
        if title:
            index["by_title"].setdefault(title, node)

        source = _node_identifier(node)
        items = (reference.get("outbound_citations") or {}).get("items") or []
        if source and isinstance(items, list):
            for item in items:
                if not isinstance(item, dict):
                    continue
                citation_doi = _doi_key(item.get("doi", ""))
                if citation_doi:
                    index["pending_citations"].setdefault(citation_doi, []).append(
                        (source, node)
                    )

    return index


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

    If uri is provided, preserve it in metadata.reference.uri. The Trellis CLI
    does not support a --uri option for add.
    """
    if uri:
        metadata = dict(metadata or {})
        reference = dict(metadata.get("reference") or {})
        reference["uri"] = uri
        metadata["reference"] = reference

    args = ["add", "reference", title]
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


_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)


def _is_uuid(s: str) -> bool:
    return bool(s and _UUID_RE.match(s))


def _resolve_to_uuid(slug_or_uuid: str) -> Optional[str]:
    """Return UUID for a node, resolving slug → UUID via get_node if needed."""
    if _is_uuid(slug_or_uuid):
        return slug_or_uuid
    try:
        node = get_node(slug_or_uuid)
        return node.get("id")
    except RuntimeError:
        return None


def _uuid_hex(uuid: str) -> str:
    return str(uuid).replace("-", "").lower()


def _trellis_db_path() -> Path:
    return Path(_workspace()) / ".trellis" / "trellis.db"


def build_edge_index() -> set[tuple[str, str, str]]:
    """
    Read all Trellis edges directly from SQLite.

    Workaround for Trellis#69: create_edge currently has no uniqueness
    constraint and the CLI has no edge-listing command. Remove this once
    Trellis enforces edge uniqueness.
    """
    path = _trellis_db_path()
    if not path.exists():
        return set()
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
            rows = conn.execute(
                "SELECT source_id,target_id,relationship FROM edges"
            ).fetchall()
    except sqlite3.Error as exc:
        logger.warning("build_edge_index failed for %s: %s", path, exc)
        raise
    return {
        (_uuid_hex(source_id), _uuid_hex(target_id), relationship)
        for source_id, target_id, relationship in rows
    }


def _edge_exists(src_uuid: str, tgt_uuid: str, relationship: str) -> bool:
    """
    Read one edge directly from SQLite.

    Workaround for Trellis#69: create_edge blindly inserts duplicates. Remove
    this once Trellis enforces edge uniqueness.
    """
    path = _trellis_db_path()
    if not path.exists():
        return False
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM edges
                WHERE source_id = ? AND target_id = ? AND relationship = ?
                LIMIT 1
                """,
                (_uuid_hex(src_uuid), _uuid_hex(tgt_uuid), relationship),
            ).fetchone()
    except sqlite3.Error as exc:
        logger.warning("_edge_exists failed for %s: %s", path, exc)
        raise
    return row is not None


def link_nodes(
    source: str,
    target: str,
    relation: str = "references",
    actor_id: str = ACTOR,
    edge_index: set[tuple[str, str, str]] = None,
    edge_lock=None,
) -> dict:
    """
    Returns {"ok": True} on success or idempotent duplicate.
    Returns {"ok": False, "error": str} on real failure.
    Never raises.

    Always uses --source-uuid / --target-uuid / --relationship to avoid
    slug ambiguity errors. Resolves slugs to UUIDs when necessary.
    """
    reserved = False
    key = None
    try:
        src_uuid = source if _is_uuid(source) else _resolve_to_uuid(source)
        tgt_uuid = target if _is_uuid(target) else _resolve_to_uuid(target)
        if not src_uuid or not tgt_uuid:
            return {
                "ok": False,
                "error": f"Could not resolve UUIDs: src={source} tgt={target}",
            }
        key = (_uuid_hex(src_uuid), _uuid_hex(tgt_uuid), relation)
        if edge_index is not None:
            lock_context = (
                edge_lock if edge_lock is not None else contextlib.nullcontext()
            )
            with lock_context:
                if key in edge_index:
                    return {"ok": True, "idempotent": True}
                # Reserve before the subprocess to avoid duplicate edges when
                # persistent-agent batches include duplicate/deduped sources.
                # Trellis#69: the CLI currently has no edge uniqueness guard.
                edge_index.add(key)
                reserved = True
        else:
            try:
                if _edge_exists(src_uuid, tgt_uuid, relation):
                    return {"ok": True, "idempotent": True}
            except sqlite3.Error as e:
                return {
                    "ok": False,
                    "error": f"edge existence check failed: {e}",
                }

        _run_json(
            "link",
            "--source-uuid",
            src_uuid,
            "--target-uuid",
            tgt_uuid,
            "--relationship",
            relation,
            "--actor-id",
            actor_id,
            "--json",
        )
        return {"ok": True}
    except RuntimeError as e:
        if reserved and edge_index is not None and key is not None:
            lock_context = (
                edge_lock if edge_lock is not None else contextlib.nullcontext()
            )
            with lock_context:
                edge_index.discard(key)
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
        raise ValueError(
            f"Unknown pipeline status {status!r}. Valid: {sorted(PIPELINE_TAGS)}"
        )
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
    """Check URI/metadata DOI fields first, then grep metadata for alt_dois."""
    uri = _normalize_doi_uri(doi)
    bare_doi = _doi_key(doi)
    candidates = find_nodes(text=uri)
    if bare_doi:
        seen = {
            node.get("id") or node.get("uuid") or node.get("slug")
            for node in candidates
        }
        for node in find_nodes(text=bare_doi):
            identity = node.get("id") or node.get("uuid") or node.get("slug")
            if identity not in seen:
                candidates.append(node)
                seen.add(identity)

    for node in candidates:
        reference = _reference_metadata(node)
        metadata_uri = reference.get("uri", "")
        metadata_doi = reference.get("doi", "")
        if _normalize_doi_uri(node.get("uri", "")) == uri:
            return node
        if _normalize_doi_uri(metadata_uri) == uri:
            return node
        if bare_doi and _doi_key(metadata_doi) == bare_doi:
            return node
    # Catches alt_dois stored in metadata
    for node in grep_nodes(doi):
        if node.get("slug"):
            return node
    return None


def reverse_materialize(
    new_slug: str,
    doi: str = None,
    index: dict = None,
) -> int:
    """
    Create pending citation edges from nodes that already cited this DOI before
    the target node existed in Trellis.
    """
    if not new_slug or not index or "pending_citations" not in index or not doi:
        return 0

    key = _doi_key(doi)
    if not key:
        return 0

    created = 0
    new_uuid = new_slug if _is_uuid(new_slug) else _resolve_to_uuid(new_slug)
    for waiting_slug_or_id, _node in (index.get("pending_citations") or {}).get(
        key, []
    ):
        if not waiting_slug_or_id:
            continue
        waiting_uuid = None
        if new_uuid and _is_uuid(new_uuid):
            waiting_uuid = (
                waiting_slug_or_id
                if _is_uuid(waiting_slug_or_id)
                else _resolve_to_uuid(waiting_slug_or_id)
            )
        if (
            waiting_uuid and waiting_uuid == new_uuid
        ) or waiting_slug_or_id == new_slug:
            continue
        # link_nodes does a direct edge-existence read here; see Trellis#69.
        result = link_nodes(waiting_slug_or_id, new_slug, "references")
        if result.get("ok"):
            created += 1
    return created


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


def dedup_check_indexed(
    index: dict,
    s2id: str = None,
    doi: str = None,
    pmid: str = None,
    title: str = None,
) -> Optional[dict]:
    """Full dedup chain against a pre-built node index."""
    if s2id:
        node = (index.get("by_s2id") or {}).get(s2id)
        if node:
            return node
    if doi:
        key = _doi_key(doi)
        if key:
            node = (index.get("by_doi") or {}).get(key)
            if node:
                return node
    if pmid:
        node = (index.get("by_pmid") or {}).get(str(pmid))
        if node:
            return node
    if title:
        nt = _norm_title(title)
        if nt and len(nt) >= 10:
            node = (index.get("by_title") or {}).get(nt)
            if node:
                return node
    return None
