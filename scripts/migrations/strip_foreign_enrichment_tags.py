#!/usr/bin/env python3
"""
One-time migration: strip enrichment tags cross-wired from the Hermans 2024
zebrafish/OKR paper (pmid:38621210) onto 7 unrelated gut-microbiome nodes.

Root cause
----------
The April bulk import cross-wired pmid:38621210 (Hermans 2024, "A 3D-Printed
Device to Measure the Zebrafish Optokinetic Response") into several gut-
microbiome nodes. ``_make_tags`` at the time carried forward ALL existing
topical tags verbatim; subsequent re-ingests therefore preserved the foreign
tags (kw:okr, kw:optokinetic-response, mesh-major:nystagmus-optokinetic, etc.)
alongside the correct ones.  The earlier ``stale-citation-clear`` migration
removed the foreign ``citation`` string but left those topical tags intact.

The underlying ``_make_tags`` function has been fixed to drop and re-derive all
source-derived topical namespaces (mesh:, kw:, field:, type:, year:) on every
re-ingest, so this class of contamination cannot recur.

This migration removes the residual foreign tags from the 7 affected nodes.

Detection
---------
``pmid:38621210`` is the authoritative signal: these nodes carry ONLY the
foreign PMID, not their own (all 7 are preprints with no PMID of their own).

Tags removed
------------
  pmid:38621210, kw:okr, kw:optic-nerve, kw:optokinetic-response,
  kw:regeneration, kw:vision, mesh-major:nystagmus-optokinetic,
  mesh:nystagmus-optokinetic, mesh:printing-three-dimensional, mesh-q:physiology

Read-only by default; pass --apply to mutate. Idempotent: a second run is a
no-op (no matching tags remain). All writes go through the Trellis CLI.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.trellis import _workspace, update_node  # noqa: E402

ACTOR_ID = "strip-foreign-enrichment-tags"

# Tags originating exclusively from the cross-wired zebrafish OKR paper.
FOREIGN_TAGS: frozenset[str] = frozenset(
    {
        "pmid:38621210",
        "kw:okr",
        "kw:optic-nerve",
        "kw:optokinetic-response",
        "kw:regeneration",
        "kw:vision",
        "mesh-major:nystagmus-optokinetic",
        "mesh:nystagmus-optokinetic",
        "mesh:printing-three-dimensional",
        "mesh-q:physiology",
    }
)


def _db_path() -> str:
    return os.path.join(_workspace(), ".trellis", "trellis.db")


def _dash_uuid(raw: str) -> str:
    """Convert a 32-hex SQLite UUID to the dashed form the Trellis CLI expects."""
    h = raw.replace("-", "")
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def _candidates(db: sqlite3.Connection) -> list[dict]:
    """Return live nodes that carry the foreign PMID tag."""
    rows = db.execute(
        "SELECT n.id, n.slug, n.title FROM nodes n "
        "JOIN tag_links tl ON tl.owner_id = n.id "
        "WHERE COALESCE(n.status, '') != 'deleted' "
        "AND tl.tag = 'pmid:38621210' "
        "GROUP BY n.id"
    ).fetchall()
    result = []
    for r in rows:
        tags = [
            x[0]
            for x in db.execute(
                "SELECT tag FROM tag_links WHERE owner_id = ?", (r["id"],)
            ).fetchall()
        ]
        foreign_present = [t for t in tags if t in FOREIGN_TAGS]
        if foreign_present:
            result.append(
                {
                    "id": r["id"],
                    "uuid": _dash_uuid(r["id"]),
                    "slug": r["slug"],
                    "title": r["title"],
                    "all_tags": tags,
                    "foreign_tags": foreign_present,
                }
            )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true", help="Mutate the graph (default: dry run)"
    )
    args = parser.parse_args()

    db = sqlite3.connect(f"file:{_db_path()}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    candidates = _candidates(db)
    db.close()

    if not candidates:
        print("No affected nodes found — already clean.")
        return 0

    mode = "APPLY" if args.apply else "DRY RUN"
    print(f"=== {mode}: {len(candidates)} node(s) with foreign enrichment tags ===\n")

    for c in candidates:
        clean_tags = [t for t in c["all_tags"] if t not in FOREIGN_TAGS]
        print(f"  {c['slug'][:70]}")
        print(f"    title : {c['title'][:65]}")
        print(f"    remove: {sorted(c['foreign_tags'])}")
        print(f"    keep  : {len(clean_tags)} tags remaining")
        if args.apply:
            try:
                update_node(c["slug"], tags=clean_tags, actor_id=ACTOR_ID)
            except RuntimeError as exc:
                if "Ambiguous slug" not in str(exc):
                    raise
                # Duplicate slug: fall back to UUID (unambiguous).
                update_node(c["uuid"], tags=clean_tags, actor_id=ACTOR_ID)
            print("    → updated")
        print()

    if not args.apply:
        print("Dry run complete. Pass --apply to mutate.")
    else:
        print(f"Done. Stripped foreign enrichment tags from {len(candidates)} node(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
