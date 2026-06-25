#!/usr/bin/env python3
"""
Paper ingestion pipeline — scoped to a single paper.

Usage:
    from ingest import ingest_paper

    slug = ingest_paper(doi="10.1038/s41564-023-01464-1")
    slug = ingest_paper(title="Some paper title", doi="10.x/yy")
    slug = ingest_paper(doi="10.x/yy", source="rss", depth=0)

Returns the Trellis slug on success, None on failure.
Idempotent: if DOI already exists, returns existing slug without re-adding.
"""

import json
import re
import subprocess
import unicodedata
from typing import Optional

PIPELINE_TAGS = [
    "pipeline:queued",
    "pipeline:scaffolded",
    "pipeline:digested",
    "pipeline:partial",
    "pipeline:needs-review",
    "pipeline:failed",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clean(t: str) -> str:
    if not t:
        return ""
    t = re.sub(r"[\r\n]+", " ", t)
    t = " ".join(t.split())
    t = t.replace('"', "'").replace("\\", "")
    return t[:2000]


def _norm_title(t: str) -> str:
    """Normalize title for comparison: lowercase, strip unicode/punct, collapse whitespace."""
    if not t:
        return ""
    t = unicodedata.normalize("NFKD", t.lower().strip())
    t = t.encode("ascii", "ignore").decode("ascii")
    t = t.rstrip(".")
    # collapse whitespace
    t = " ".join(t.split())
    return t


def _trellis(*args: str, json_output: bool = True) -> subprocess.CompletedProcess:
    """Run a trellis CLI command."""
    cmd = ["trellis"] + list(args)
    if json_output and "--json" not in cmd:
        cmd.append("--json")
    return subprocess.run(cmd, capture_output=True, text=True, timeout=15)


def _doi_exists(doi: str) -> Optional[str]:
    """
    DOI dedup (live): returns existing slug if DOI found, else None.
    trellis grep <doi> searches all fields (uri, metadata, tags).
    Avoid calling this in bulk loops — use load_existing_dois() instead.
    """
    r = _trellis("grep", doi)
    if r.returncode != 0 or not r.stdout.strip():
        return None
    try:
        nodes = json.loads(r.stdout)
    except (json.JSONDecodeError, ValueError):
        return None
    if nodes:
        return nodes[0].get("slug")
    return None


def load_existing_dois() -> dict:
    """
    Bulk-load all existing DOIs into a dict {doi_lowercase: slug}.
    Scans all pipeline states so queued/digested nodes also dedupe correctly.
    """
    dois = {}
    for tag in PIPELINE_TAGS:
        r = _trellis("find", "--tag", tag)
        try:
            nodes = json.loads(r.stdout) if r.stdout.strip() else []
        except (json.JSONDecodeError, ValueError):
            nodes = []
        for n in nodes:
            slug = n.get("slug", "")
            uri = n.get("uri", "") or ""
            meta = (
                n.get("metadata", {}) if isinstance(n.get("metadata", {}), dict) else {}
            )
            ref = meta.get("reference", {}) if isinstance(meta, dict) else {}
            # Handle both "doi:10.x/yy" and "https://doi.org/10.x/yy"
            if uri.startswith("doi:"):
                doi = uri[4:].strip()
                dois[doi.lower()] = slug
            elif "doi.org/" in uri:
                doi = uri.split("doi.org/")[-1].strip()
                dois[doi.lower()] = slug
            elif isinstance(ref, dict) and ref.get("doi"):
                dois[str(ref["doi"]).strip().lower()] = slug
            elif (
                isinstance(ref, dict)
                and ref.get("url")
                and "doi.org/" in str(ref["url"])
            ):
                doi = str(ref["url"]).split("doi.org/")[-1].strip()
                dois[doi.lower()] = slug
    return dois


def _title_exists(title: str, _cache: dict = None) -> Optional[str]:
    """
    Title dedup: returns existing slug if a normalized-title match found, else None.
    Uses trellis find --text, then compares _norm_title of results.
    """
    nt = _norm_title(title)
    if not nt or len(nt) < 10:
        return None
    r = _trellis("find", "--text", nt)
    if r.returncode != 0 or not r.stdout.strip():
        return None
    try:
        nodes = json.loads(r.stdout)
    except (json.JSONDecodeError, ValueError):
        return None
    for n in nodes:
        existing_nt = _norm_title(n.get("title", ""))
        if existing_nt == nt:
            return n.get("slug")
    return None


def load_existing_titles() -> dict:
    """
    Bulk-load all existing reference titles into a dict {norm_title: slug}.
    Call once at startup to avoid per-paper trellis find calls.
    """
    titles = {}
    for tag in PIPELINE_TAGS:
        r = _trellis("find", "--tag", tag)
        try:
            nodes = json.loads(r.stdout) if r.stdout.strip() else []
        except (json.JSONDecodeError, ValueError):
            nodes = []
        for n in nodes:
            nt = _norm_title(n.get("title", ""))
            if not nt:
                meta = (
                    n.get("metadata", {})
                    if isinstance(n.get("metadata", {}), dict)
                    else {}
                )
                ref = meta.get("reference", {}) if isinstance(meta, dict) else {}
                if isinstance(ref, dict):
                    nt = _norm_title(ref.get("title", ""))
            if nt:
                titles[nt] = n.get("slug")
    return titles


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def ingest_paper(
    doi: str,
    title: Optional[str] = None,
    abstract: Optional[str] = None,
    year: Optional[str] = None,
    venue: Optional[str] = None,
    authors: Optional[str] = None,
    source: str = "manual",
    depth: int = 0,
    parent: str = "microbiome-research-library",
    actor: str = "daedalus",
    extra_tags: Optional[list] = None,
    title_cache: Optional[dict] = None,
    doi_cache: Optional[dict] = None,
) -> Optional[str]:
    """
    Ingest a single paper into Trellis.

    Required: doi (must start with '10.')
    Optional: title, abstract, year, venue, authors, source, depth,
              parent, actor, extra_tags, title_cache, doi_cache

    title_cache: dict of {norm_title: slug} from load_existing_titles().
    doi_cache: dict of {doi_lower: slug} from load_existing_dois().

    Returns: Trellis slug on success, None on failure.
    Idempotent: safe to call repeatedly with same DOI or title.

    Pipeline state after this call: pipeline:scaffolded
    (Metadata enrichment from external APIs happens in the scaffold stage.)
    """
    doi = doi.strip()
    if not doi.startswith("10."):
        return None

    # --- Dedup gate ---
    # 1. DOI exact match (cache first, live fallback)
    dl = doi.lower()
    if doi_cache and dl in doi_cache:
        return doi_cache[dl]
    existing = _doi_exists(doi)
    if existing:
        return existing

    # 2. Normalized title match
    if title:
        nt = _norm_title(title)
        if title_cache and nt in title_cache:
            return title_cache[nt]
        # Fallback: live query (slower, for ad-hoc calls without cache)
        existing = _title_exists(title)
        if existing:
            return existing

    # --- Build metadata ---
    meta = {
        "reference": {
            "schema": "reference-v1",
            "title": _clean(title) if title else f"DOI:{doi}",
        }
    }
    if doi:
        meta["reference"]["doi"] = doi
    if year:
        meta["reference"]["year"] = year
    if venue:
        meta["reference"]["venue"] = _clean(venue)
    if authors:
        meta["reference"]["authors"] = _clean(authors)
    meta_json = json.dumps(meta, ensure_ascii=False)

    # --- Build tags ---
    tags = [
        "pipeline:scaffolded",
        f"source:{source}",
        f"depth:{depth}",
    ]
    if year and year.isdigit():
        tags.append(f"year:{year}")
    if extra_tags:
        tags.extend(extra_tags)

    # --- Title ---
    paper_title = _clean(title) if title else f"DOI:{doi}"

    # --- Build command ---
    cmd = [
        "trellis",
        "add",
        "reference",
        paper_title,
    ]
    if abstract:
        cmd.extend(["--abstract", _clean(abstract)])
    cmd.extend(
        [
            "--metadata",
            meta_json,
            "--tags",
            ",".join(tags),
            "--parent",
            parent,
            "--actor-id",
            actor,
            "--json",
        ]
    )

    # --- Execute ---
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)

    if r.returncode == 0:
        try:
            data = json.loads(r.stdout)
            # trellis add returns {"ok": true, "node": {...}}
            node = data.get("node", data)
            return node.get("slug")
        except (json.JSONDecodeError, ValueError):
            return _doi_exists(doi)

    # Failure
    return None


# ---------------------------------------------------------------------------
# CLI entry point (for testing / one-off use)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Ingest a single paper into Trellis")
    p.add_argument("--doi", required=True, help="DOI (required)")
    p.add_argument("--title", default=None)
    p.add_argument("--abstract", default=None)
    p.add_argument("--year", default=None)
    p.add_argument("--venue", default=None)
    p.add_argument("--authors", default=None)
    p.add_argument("--source", default="manual")
    p.add_argument("--depth", type=int, default=0)
    args = p.parse_args()

    slug = ingest_paper(
        doi=args.doi,
        title=args.title,
        abstract=args.abstract,
        year=args.year,
        venue=args.venue,
        authors=args.authors,
        source=args.source,
        depth=args.depth,
    )
    if slug:
        print(f"OK: {slug}")
    else:
        print("FAIL")
        exit(1)
