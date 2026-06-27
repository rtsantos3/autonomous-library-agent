#!/usr/bin/env python3
"""One-time migration: remove cross-wired reference nodes.

Strategy
--------
The stored ``metadata.reference.citation`` is only a cheap candidate narrower:
some records have stale citation strings even though their DOI is correct. The
authoritative check resolves ``metadata.reference.doi`` and compares that
paper's title to the node title.

Reads use SQLite in read-only mode for candidate discovery. Network access is
limited to DOI verification. All writes go through the Trellis CLI wrappers in
``pipeline.trellis``. Dry-run by default; pass ``--apply`` to unlink incident
edges and soft-delete only DOI-verified cross-wired nodes.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from pipeline import trellis  # noqa: E402
from pipeline.trellis import _workspace  # noqa: E402

MIGRATION_ACTOR = "crosswired-doi-removal"
MIN_SIGNIFICANT_TITLE_TOKENS = 5
MAX_CROSSWIRED_OVERLAP = 0.40
MAX_DIFFERENT_TITLE_SIMILARITY = 0.45
HTTP_TIMEOUT_SECONDS = 8
REQUEST_DELAY_SECONDS = 0.2
RATE_LIMIT_DELAY_SECONDS = 1.0
STOPWORDS = {
    "the",
    "and",
    "of",
    "in",
    "for",
    "a",
    "an",
    "to",
    "on",
    "with",
    "from",
    "via",
}


@dataclass(frozen=True)
class Edge:
    id: str
    source_id: str
    target_id: str
    relationship: str


@dataclass(frozen=True)
class CandidateNode:
    id: str
    title: str
    doi: str
    citation: str
    citation_overlap: float
    edges: tuple[Edge, ...]


class Decision(Enum):
    REMOVE = "remove"
    KEEP = "keep"
    MANUAL = "manual"


@dataclass(frozen=True)
class VerifiedNode:
    candidate: CandidateNode
    decision: Decision
    doi_title: str | None
    title_similarity: float | None
    title_similarity_forward: float | None
    title_similarity_reverse: float | None


def _dash(uuid_hex: str) -> str:
    """Format a dashless 32-hex id as a canonical UUID for Trellis CLI flags."""
    if not uuid_hex or "-" in uuid_hex:
        return uuid_hex
    return (
        f"{uuid_hex[:8]}-{uuid_hex[8:12]}-{uuid_hex[12:16]}-"
        f"{uuid_hex[16:20]}-{uuid_hex[20:]}"
    )


def _db_path() -> Path:
    return Path(_workspace()) / ".trellis" / "trellis.db"


def _decode_metadata(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _reference(metadata: dict) -> dict:
    reference = metadata.get("reference") or {}
    return reference if isinstance(reference, dict) else {}


def _norm(value: object) -> list[str]:
    text = html.unescape(str(value or "")).lower()
    text = "".join(char if char.isalnum() else " " for char in text)
    normalized = re.sub(r"\s+", " ", text).strip()
    return normalized.split()


def _significant_tokens(value: object) -> list[str]:
    return [
        token for token in _norm(value) if len(token) >= 3 and token not in STOPWORDS
    ]


def _token_overlap(title: str, comparison: str) -> float | None:
    title_tokens = _significant_tokens(title)
    if len(title_tokens) < MIN_SIGNIFICANT_TITLE_TOKENS:
        return None
    comparison_tokens = set(_significant_tokens(comparison))
    if not comparison_tokens:
        return None
    matches = sum(1 for token in title_tokens if token in comparison_tokens)
    return matches / len(title_tokens)


def _bidirectional_title_similarity(
    node_title: str, doi_title: str
) -> tuple[float, float, float] | None:
    node_tokens = set(_significant_tokens(node_title))
    doi_tokens = set(_significant_tokens(doi_title))
    if not node_tokens or not doi_tokens:
        return None
    intersection = node_tokens & doi_tokens
    forward = len(intersection) / len(node_tokens)
    reverse = len(intersection) / len(doi_tokens)
    return max(forward, reverse), forward, reverse


def _load_edges_by_node(cur: sqlite3.Cursor) -> dict[str, list[Edge]]:
    edges_by_node: dict[str, list[Edge]] = {}
    for row in cur.execute(
        """
        SELECT id, source_id, target_id, relationship
        FROM edges
        """
    ):
        edge = Edge(
            id=row["id"],
            source_id=row["source_id"],
            target_id=row["target_id"],
            relationship=row["relationship"],
        )
        edges_by_node.setdefault(edge.source_id, []).append(edge)
        if edge.target_id != edge.source_id:
            edges_by_node.setdefault(edge.target_id, []).append(edge)
    return edges_by_node


def stage1_candidates() -> list[CandidateNode]:
    db = sqlite3.connect(f"file:{_db_path()}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    cur = db.cursor()
    edges_by_node = _load_edges_by_node(cur)

    candidates: list[CandidateNode] = []
    try:
        for row in cur.execute(
            """
            SELECT id, title, metadata_
            FROM nodes
            WHERE COALESCE(status, '') != 'deleted'
              AND type = 'reference'
            ORDER BY title, id
            """
        ):
            metadata = _decode_metadata(row["metadata_"])
            reference = _reference(metadata)
            doi = str(reference.get("doi") or "").strip()
            citation = str(reference.get("citation") or "").strip()
            if not doi or not citation:
                continue
            title = str(row["title"] or "")
            overlap = _token_overlap(title, citation)
            if overlap is None or overlap >= MAX_CROSSWIRED_OVERLAP:
                continue
            node_edges = tuple(
                sorted(edges_by_node.get(row["id"], ()), key=lambda edge: edge.id)
            )
            candidates.append(
                CandidateNode(
                    id=row["id"],
                    title=title,
                    doi=doi,
                    citation=citation,
                    citation_overlap=overlap,
                    edges=node_edges,
                )
            )
    finally:
        db.close()
    return candidates


def _request_json(url: str, headers: dict[str, str] | None = None) -> dict | None:
    request = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            status = response.status
            body = response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 429:
            time.sleep(RATE_LIMIT_DELAY_SECONDS)
        return None
    except (OSError, TimeoutError, urllib.error.URLError):
        return None
    if status == 429:
        time.sleep(RATE_LIMIT_DELAY_SECONDS)
        return None
    if status < 200 or status >= 300:
        return None
    try:
        data = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _semantic_scholar_title(doi: str) -> str | None:
    encoded_doi = urllib.parse.quote(doi, safe="/:")
    url = (
        "https://api.semanticscholar.org/graph/v1/paper/"
        f"DOI:{encoded_doi}?fields=title"
    )
    headers = {}
    api_key = os.environ.get("S2_API_KEY")
    if api_key:
        headers["x-api-key"] = api_key
    data = _request_json(url, headers=headers)
    title = data.get("title") if data else None
    return str(title).strip() if title else None


def _crossref_title(doi: str) -> str | None:
    encoded_doi = urllib.parse.quote(doi, safe="/:")
    url = f"https://api.crossref.org/works/{encoded_doi}"
    data = _request_json(url)
    message = data.get("message") if data else None
    if not isinstance(message, dict):
        return None
    titles = message.get("title")
    if not isinstance(titles, list) or not titles:
        return None
    title = titles[0]
    return str(title).strip() if title else None


def _resolve_doi_title(doi: str) -> str | None:
    title = _semantic_scholar_title(doi)
    if title:
        return title
    time.sleep(REQUEST_DELAY_SECONDS)
    return _crossref_title(doi)


def stage2_verify(candidates: list[CandidateNode]) -> list[VerifiedNode]:
    doi_title_cache: dict[str, str | None] = {}
    verified: list[VerifiedNode] = []
    for candidate in candidates:
        normalized_doi = candidate.doi.lower()
        if normalized_doi not in doi_title_cache:
            doi_title_cache[normalized_doi] = _resolve_doi_title(candidate.doi)
            time.sleep(REQUEST_DELAY_SECONDS)

        doi_title = doi_title_cache[normalized_doi]
        if not doi_title:
            verified.append(
                VerifiedNode(
                    candidate=candidate,
                    decision=Decision.MANUAL,
                    doi_title=None,
                    title_similarity=None,
                    title_similarity_forward=None,
                    title_similarity_reverse=None,
                )
            )
            continue

        similarity = _bidirectional_title_similarity(candidate.title, doi_title)
        if similarity is not None and similarity[0] < MAX_DIFFERENT_TITLE_SIMILARITY:
            decision = Decision.REMOVE
        else:
            decision = Decision.KEEP
        sim, forward, reverse = (
            similarity if similarity is not None else (None, None, None)
        )
        verified.append(
            VerifiedNode(
                candidate=candidate,
                decision=decision,
                doi_title=doi_title,
                title_similarity=sim,
                title_similarity_forward=forward,
                title_similarity_reverse=reverse,
            )
        )
    return verified


def plan() -> list[VerifiedNode]:
    return stage2_verify(stage1_candidates())


def _unlink(edge: Edge) -> None:
    trellis._run_json(
        "unlink",
        "--source-uuid",
        _dash(edge.source_id),
        "--target-uuid",
        _dash(edge.target_id),
        "--relationship",
        edge.relationship,
        "--all",
        "--actor-id",
        MIGRATION_ACTOR,
        "--json",
    )


def _remove_node(node_id: str) -> None:
    trellis._run_json(
        "rm",
        "--uuid",
        _dash(node_id),
        "--force",
        "--actor-id",
        MIGRATION_ACTOR,
        "--json",
    )


def _unique_edges(nodes: list[VerifiedNode]) -> list[Edge]:
    by_key: dict[tuple[str, str, str], Edge] = {}
    for node in nodes:
        for edge in node.candidate.edges:
            key = (edge.source_id, edge.target_id, edge.relationship)
            by_key.setdefault(key, edge)
    return sorted(
        by_key.values(),
        key=lambda edge: (edge.source_id, edge.target_id, edge.relationship),
    )


def _edge_count(nodes: list[VerifiedNode]) -> int:
    return len({edge.id for node in nodes for edge in node.candidate.edges})


def _format_score(score: float | None) -> str:
    return "n/a" if score is None else f"{score:.2f}"


def _format_similarity(node: VerifiedNode) -> str:
    return (
        f"sim: {_format_score(node.title_similarity)} "
        f"(fwd {_format_score(node.title_similarity_forward)}/"
        f"rev {_format_score(node.title_similarity_reverse)})"
    )


def run(apply: bool) -> int:
    verified = plan()
    removals = [node for node in verified if node.decision is Decision.REMOVE]
    kept = [node for node in verified if node.decision is Decision.KEEP]
    manual = [node for node in verified if node.decision is Decision.MANUAL]
    edges = _unique_edges(removals)
    edge_count = _edge_count(removals)

    print("WILL REMOVE (doi != title)")
    for node in removals:
        candidate = node.candidate
        print(f"\nid: {_dash(candidate.id)}")
        print(f"node title: {candidate.title}")
        print(f"resolved doi title: {node.doi_title}")
        print(_format_similarity(node))
        print(f"edge count (in+out): {len(candidate.edges)}")
    if not removals:
        print("none")

    print("\nKEPT (stale citation, doi ok)")
    print(f"count: {len(kept)}")
    for node in kept:
        print(f"- node title: {node.candidate.title}")
        print(f"  resolved doi title: {node.doi_title}")
        print(f"  {_format_similarity(node)}")

    print("\nMANUAL (doi unresolved)")
    for node in manual:
        candidate = node.candidate
        print(f"\nid: {_dash(candidate.id)}")
        print(f"doi: {candidate.doi}")
        print(f"node title: {candidate.title}")
        print(_format_similarity(node))
        print(f"edge count (in+out): {len(candidate.edges)}")
    if not manual:
        print("none")

    if apply:
        for edge in edges:
            _unlink(edge)
        for node in removals:
            _remove_node(node.candidate.id)

    if apply:
        print(
            f"\nSummary: {len(removals)} removed, {len(kept)} kept, "
            f"{len(manual)} manual; {edge_count} edge(s) unlinked."
        )
    else:
        print(
            f"\nSummary: {len(removals)} would be removed, {len(kept)} kept, "
            f"{len(manual)} manual; {edge_count} edge(s) would be unlinked."
        )
    if verified and not removals and not kept and manual:
        print(
            "All DOI resolutions failed; this may be an offline run. "
            "No nodes were classified for removal."
        )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="unlink incident edges and soft-delete cross-wired nodes",
    )
    args = parser.parse_args()
    return run(apply=args.apply)


if __name__ == "__main__":
    raise SystemExit(main())
