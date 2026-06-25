"""
Integration test: 50 foundational interconnected microbiome papers (2005-2013).
Uses ingest_batch() for parallelized processing.
Reports cross-links materialized.
"""

import sys
import time
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")
load_dotenv(Path(__file__).resolve().parents[3] / ".env")

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.trellis import find_nodes, get_node
from pipeline.ingestion import format_metrics_table, ingest_batch

DOIS = [
    "10.1038/nature07540",
    "10.1016/j.cell.2012.10.052",
    "10.1038/nature08821",
    "10.1038/nature09944",
    "10.1126/scitranslmed.3000322",
    "10.1038/nature11234",
    "10.1038/nature11053",
    "10.1126/science.1177486",
    "10.1038/nmeth.f.303",
    "10.1186/gb-2011-12-6-r60",
    "10.1371/journal.pcbi.1002606",
    "10.1038/nature05414",
    "10.1038/4441022a",
    "10.1371/journal.pcbi.1002358",
    "10.1038/nbt.2676",
    "10.1128/AEM.71.12.8228-8235.2005",
    "10.1126/science.1198719",
    "10.1126/SCIENCE.1110591",
    "10.1073/pnas.1005963107",
    "10.1053/j.gastro.2009.08.042",
    "10.1016/j.chom.2008.02.015",
    "10.1038/nature11225",
    "10.1126/science.1206025",
    "10.1126/science.1208344",
    "10.1073/pnas.1002611107",
    "10.1038/nmeth.1650",
    "10.2337/db10-0253",
    "10.1126/science.1183605",
    "10.1038/ismej.2009.37",
    "10.1073/pnas.1000080107",
    "10.1038/ismej.2012.8",
    "10.1128/AEM.03006-05",
    "10.1126/scitranslmed.3003605",
    "10.1093/nar/gkr1044",
    "10.1146/annurev-micro-090110-102830",
    "10.1093/nar/gkn663",
    "10.1194/jlr.R500013-JLR200",
    "10.1038/nature12347",
    "10.1038/ijo.2008.155",
    "10.1073/pnas.0901529106",
    "10.1093/nar/gkm160",
    "10.1053/j.gastro.2011.10.001",
    "10.1073/pnas.1002601107",
    "10.1093/bioinformatics/btp636",
    "10.1016/j.cmet.2009.02.002",
    "10.1038/oby.2009.167",
    "10.1073/pnas.1000081107",
    "10.2337/db08-1637",
    "10.1016/j.cmet.2007.10.013",
    "10.1371/journal.pone.0035240",
]


def run():
    print(f"\n{'='*60}")
    print(f"Batch pipeline test — {len(DOIS)} interconnected papers")
    print(f"Date: {date.today()}")
    print(f"{'='*60}\n")

    queued_before = len(find_nodes(tag="pipeline:queued"))

    t0 = time.time()
    outcomes, metrics = ingest_batch(DOIS, workers=8)
    elapsed = time.time() - t0

    ok = [o for o in outcomes if not o.errors]
    failed = [o for o in outcomes if o.errors]
    created = sum(1 for o in ok if o.upsert and o.upsert.created)
    updated = sum(1 for o in ok if o.upsert and not o.upsert.created)
    total_stored = sum(o.citation_store.stored for o in ok if o.citation_store)
    total_linked = sum(o.link.linked for o in ok if o.link)
    total_skipped = sum(o.link.skipped for o in ok if o.link)

    queued_after = len(find_nodes(tag="pipeline:queued"))

    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"  Time elapsed:     {elapsed:.1f}s ({elapsed/len(DOIS):.1f}s per paper)")
    print(f"  Processed:        {len(ok)}/{len(DOIS)}")
    print(f"  Failed:           {len(failed)}")
    print(f"  Created new:      {created}")
    print(f"  Updated existing: {updated}")
    print(f"  Citations stored: {total_stored}")
    print(f"  Edges linked:     {total_linked}")
    print(f"  Edges skipped:    {total_skipped}")
    print(f"  Queued delta:     {queued_after - queued_before} (expect 0 — no stubs)")

    if failed:
        print("\n  Failures:")
        for o in failed:
            doi = o.parse.doi if o.parse else "?"
            print(f"    {doi}: {o.errors}")

    # Cross-link check — are edges being created between our 50?
    slugs = {o.upsert.slug for o in ok if o.upsert and o.upsert.slug}
    print(f"\n  Checking cross-links among {len(slugs)} processed nodes...")
    cross_edges = []
    cross_failures = 0
    for o in ok:
        if not o.upsert or not o.upsert.slug:
            continue
        try:
            node = get_node(o.upsert.slug)
            meta = (node.get("metadata") or {}).get("reference") or {}
            items = (meta.get("outbound_citations") or {}).get("items") or []
            for item in items:
                item_doi = (item.get("doi") or "").lower()
                if any(
                    item_doi == d.lower()
                    for d in DOIS
                    if d.lower() != (o.parse.doi or "").lower()
                ):
                    cross_edges.append((o.upsert.slug, item_doi))
        except Exception as exc:
            cross_failures += 1
            doi = o.parse.doi if o.parse else "?"
            print(
                f"    ERROR checking cross-links for {o.upsert.slug} ({doi}): {exc!r}"
            )

    print(f"  Cross-citations within batch (in metadata): {len(cross_edges)}")
    print(f"  Cross-link check failures: {cross_failures}")
    if cross_edges:
        for src, tgt in cross_edges[:10]:
            print(f"    {src[:50]} → {tgt}")
        if len(cross_edges) > 10:
            print(f"    ... and {len(cross_edges)-10} more")

    print()
    print(format_metrics_table(metrics))

    print(f"\n{'='*60}")
    print("DONE")


if __name__ == "__main__":
    run()
