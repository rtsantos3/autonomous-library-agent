#!/usr/bin/env python3
"""
One-time migration: remove FOREIGN identifier tags (and the phantom citation
edges they spawned) from contaminated nodes.

Root cause
----------
``pipeline.ingestion._make_tags`` carries forward a node's existing non-pipeline
tags verbatim and then appends the freshly-resolved ``s2id:``/``pmid:`` tags. An
``s2id:``/``pmid:`` tag is an *identity* tag: it should describe the node itself
and nothing else. When the April pre-pipeline duplicate-DOI scaffolding was
deduped, some surviving nodes inherited a sibling's identifier tag; every
subsequent re-ingest then preserved that foreign tag while adding the correct
one, so a node ended up advertising several papers' identities at once (one node
carried 10 distinct ``s2id:`` tags).

``pipeline.trellis.build_node_index`` keys ``by_s2id`` / ``by_pmid`` off those
tags, so a foreign ``pmid:20368178`` on the pig-feed paper made every paper that
cites Turnbaugh 2009 link to the pig paper instead. The pig node accrued 222
inbound ``references`` edges against a true S2 cited-by count of 2.

What this fixes
---------------
For each node whose ``s2id:``/``pmid:`` tags disagree with its own
``metadata.reference`` identity:

  1. Strip the foreign identifier tags (keep the node's own + all other tags).
  2. Drop inbound ``references`` edges that are justified ONLY by a foreign tag,
     i.e. the citing source does not reference this node by its OWN identity
     (own s2_id / doi / pmid / normalized title). Genuine citations are kept.

Reads go through SQLite (read-only) for cheap whole-graph planning. Every WRITE
goes through the Trellis CLI (pipeline.trellis), never raw SQL, so the tags JSON
blob and the tag_links table stay in sync. Read-only by default; pass --apply to
mutate. Idempotent: a second pass after a completed run is a no-op.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from pipeline import trellis  # noqa: E402
from pipeline.trellis import _doi_key, _norm_title, _workspace  # noqa: E402

MIGRATION_ACTOR = "identity-tag-decontamination"


# --- declaration: identity helpers -------------------------------------------
def _dash(uuid_hex: str) -> str:
    """Format a dashless 32-hex node/edge id as a canonical UUID for the CLI."""
    u = uuid_hex
    if not u or "-" in u:
        return u
    return f"{u[:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:]}"


def _own_identity(node_title: str, ref: dict) -> dict:
    """The set of identifiers that legitimately belong to THIS node."""
    s2 = ref.get("s2_id")
    pmid = ref.get("pmid")
    dois = set()
    for v in [ref.get("doi"), ref.get("uri"), *(ref.get("alt_dois") or [])]:
        k = _doi_key(v or "")
        if k:
            dois.add(k)
    return {
        "s2": s2,
        "pmid": str(pmid) if pmid else None,
        "dois": dois,
        "title": _norm_title(node_title or ""),
    }


def _item_matches_identity(item: dict, ident: dict) -> bool:
    """True when a citing paper's outbound-citation item refers to ident's node."""
    if ident["s2"] and item.get("s2_id") == ident["s2"]:
        return True
    if ident["pmid"] and str(item.get("pmid")) == ident["pmid"]:
        return True
    di = _doi_key(item.get("doi") or "")
    if di and di in ident["dois"]:
        return True
    it = _norm_title(item.get("title") or "")
    if it and len(it) >= 10 and it == ident["title"]:
        return True
    return False


# --- body: plan from a read-only SQLite snapshot -----------------------------
def _db_path() -> str:
    return os.path.join(_workspace(), ".trellis", "trellis.db")


def plan() -> dict:
    db = sqlite3.connect(f"file:{_db_path()}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    cur = db.cursor()

    # Load every alive node's identity + tags in two sweeps.
    nodes: dict[str, dict] = {}
    for r in cur.execute(
        "SELECT id, title, metadata_ FROM nodes WHERE status != 'deleted'"
    ):
        ref = {}
        if r["metadata_"]:
            try:
                ref = (json.loads(r["metadata_"]) or {}).get("reference") or {}
            except json.JSONDecodeError:
                ref = {}
        nodes[r["id"]] = {
            "id": r["id"],
            "title": r["title"],
            "ident": _own_identity(r["title"], ref),
            "items": (ref.get("outbound_citations") or {}).get("items") or [],
            "tags": [],
        }
    for r in cur.execute(
        "SELECT owner_id, tag FROM tag_links WHERE owner_type = 'node'"
    ):
        n = nodes.get(r["owner_id"])
        if n is not None:
            n["tags"].append(r["tag"])

    # Contaminated = node carrying an s2id:/pmid: tag that is not its own.
    contaminated: dict[str, dict] = {}
    for nid, n in nodes.items():
        own = n["ident"]
        foreign = []
        for t in n["tags"]:
            if t.startswith("s2id:") and own["s2"] and t.split(":", 1)[1] != own["s2"]:
                foreign.append(t)
            elif (
                t.startswith("pmid:")
                and own["pmid"]
                and t.split(":", 1)[1] != own["pmid"]
            ):
                foreign.append(t)
        if foreign:
            kept = [t for t in n["tags"] if t not in foreign]
            contaminated[nid] = {"node": n, "foreign": foreign, "kept_tags": kept}

    # For each contaminated target, classify its inbound references edges.
    phantom_edges: list[tuple] = []  # (source_id, target_id)
    kept_edges = 0
    for nid, c in contaminated.items():
        ident = c["node"]["ident"]
        for e in cur.execute(
            "SELECT source_id FROM edges "
            "WHERE target_id = ? AND relationship = 'references'",
            (nid,),
        ):
            src = nodes.get(e["source_id"])
            if src and any(_item_matches_identity(it, ident) for it in src["items"]):
                kept_edges += 1  # genuine citation by this node's own identity
            else:
                phantom_edges.append((e["source_id"], nid))

    db.close()
    return {
        "contaminated": contaminated,
        "phantom_edges": phantom_edges,
        "kept_edges": kept_edges,
        "total_nodes": len(nodes),
    }


# --- end: apply via the Trellis CLI ------------------------------------------
def _unlink(source_id: str, target_id: str) -> None:
    trellis._run_json(
        "unlink",
        "--source-uuid",
        _dash(source_id),
        "--target-uuid",
        _dash(target_id),
        "--relationship",
        "references",
        "--all",
        "--actor-id",
        MIGRATION_ACTOR,
        "--json",
    )


def run(apply: bool) -> int:
    p = plan()
    contaminated = p["contaminated"]
    phantom = p["phantom_edges"]

    per_target = defaultdict(int)
    for _, tgt in phantom:
        per_target[tgt] += 1

    for nid, c in sorted(
        contaminated.items(), key=lambda kv: per_target[kv[0]], reverse=True
    ):
        title = (c["node"]["title"] or "")[:60]
        print(
            f"{'APPLY' if apply else 'DRY '} {title!r}: "
            f"-{len(c['foreign'])} foreign id-tag(s) {c['foreign']}, "
            f"-{per_target[nid]} phantom inbound edge(s)"
        )

    if apply:
        # Edges first (so a re-run sees fewer), then tags.
        for src, tgt in phantom:
            _unlink(src, tgt)
        for nid, c in contaminated.items():
            trellis.update_node(
                _dash(nid), tags=c["kept_tags"], actor_id=MIGRATION_ACTOR
            )

    print(
        f"\n{'Applied' if apply else 'Would change'}: "
        f"{len(contaminated)} contaminated node(s) of {p['total_nodes']} alive, "
        f"strip {sum(len(c['foreign']) for c in contaminated.values())} foreign "
        f"id-tag(s), unlink {len(phantom)} phantom edge(s), "
        f"keep {p['kept_edges']} genuine inbound edge(s)."
    )
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--apply", action="store_true", help="perform the cleanup (default: dry run)"
    )
    args = ap.parse_args()
    return run(apply=args.apply)


if __name__ == "__main__":
    raise SystemExit(main())
