#!/usr/bin/env python3
"""
One-time migration: collapse camelCase ``type:*`` tags into their canonical
hyphenated form.

Semantic Scholar emits publication types in camelCase ("JournalArticle") while
PubMed/Crossref emit spaced ("Journal Article"). Before pipeline._utils
.pub_type_slug existed, both flowed through plain slugify() and produced
divergent tags ("type:journalarticle" vs "type:journal-article"), so the same
node accrued two tags for one concept. The source path is fixed, but existing
nodes keep the stale camelCase tag because re-ingestion preserves existing
non-pipeline tags — hence this backfill.

Read-only by default (dry run). Pass --apply to mutate. All writes go through
the Trellis CLI (pipeline.trellis), never raw SQL, so tags JSON and tag_links
stay in sync. Idempotent: re-running after a completed pass is a no-op.
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from pipeline import trellis  # noqa: E402

# Reuse the same camelCase->canonical map the live pipeline self-heals with, so
# the migration and _scaffold_tags can never drift apart.
from pipeline._utils import _CAMEL_TYPE_SLUGS, canonical_type_tag  # noqa: E402

CAMEL_MAP = _CAMEL_TYPE_SLUGS

MIGRATION_ACTOR = "type-tag-migration"


def canonicalize_tags(tags: list[str]) -> list[str]:
    """Rewrite stale camelCase type:* tags to canonical form, preserving order
    and dropping duplicates that result from the collapse."""
    out: list[str] = []
    for tag in tags:
        tag = canonical_type_tag(tag)
        if tag not in out:
            out.append(tag)
    return out


def affected_nodes() -> dict[str, dict]:
    """Map node id -> node for every node carrying a stale camelCase type tag.
    Deduped by id since a node may carry more than one stale variant."""
    nodes: dict[str, dict] = {}
    for bad in CAMEL_MAP:
        for node in trellis.find_nodes(tag=f"type:{bad}", limit=5000):
            nid = node.get("id") or node.get("uuid") or node.get("slug")
            if nid:
                nodes[nid] = node
    return nodes


def run(apply: bool) -> int:
    nodes = affected_nodes()
    changed = 0
    for node in nodes.values():
        ident = node.get("id") or node.get("slug")
        current = node.get("tags") or []
        # find_nodes results may be summaries without tags; fetch the full node.
        if not current:
            current = trellis.get_node(ident).get("tags") or []
        new_tags = canonicalize_tags(current)
        if new_tags == current:
            continue
        changed += 1
        removed = [t for t in current if t not in new_tags]
        added = [t for t in new_tags if t not in current]
        slug = node.get("slug") or ident
        print(f"{'APPLY' if apply else 'DRY '} {slug}: -{removed} +{added}")
        if apply:
            trellis.update_node(ident, tags=new_tags, actor_id=MIGRATION_ACTOR)
    print(
        f"\n{'Applied' if apply else 'Would change'} {changed} node(s) "
        f"out of {len(nodes)} carrying a stale camelCase type tag."
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="perform the rewrite (default is a dry run)",
    )
    args = parser.parse_args()
    return run(apply=args.apply)


if __name__ == "__main__":
    raise SystemExit(main())
