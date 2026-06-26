"""
Integration test: 25 foundational interconnected microbiome papers.
Uses ingest_batch() for parallelized processing.
Reports cross-links materialized.
"""

import importlib.util
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

_BENCH_LOG_PATH = Path(__file__).resolve().parents[2] / "tests" / "_bench_log.py"
_BENCH_LOG_SPEC = importlib.util.spec_from_file_location("_bench_log", _BENCH_LOG_PATH)
_bench_log = importlib.util.module_from_spec(_BENCH_LOG_SPEC)
_BENCH_LOG_SPEC.loader.exec_module(_bench_log)
log_benchmark_result = _bench_log.log_benchmark_result

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
]


def run():
    lines = [
        f"\n{'='*60}",
        f"Batch pipeline test — {len(DOIS)} interconnected papers",
        f"Date: {date.today()}",
        f"{'='*60}\n",
    ]

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

    lines.extend(
        [
            f"\n{'='*60}",
            "SUMMARY",
            f"{'='*60}",
            f"  Time elapsed:     {elapsed:.1f}s ({elapsed/len(DOIS):.1f}s per paper)",
            f"  Processed:        {len(ok)}/{len(DOIS)}",
            f"  Failed:           {len(failed)}",
            f"  Created new:      {created}",
            f"  Updated existing: {updated}",
            f"  Citations stored: {total_stored}",
            f"  Edges linked:     {total_linked}",
            f"  Edges skipped:    {total_skipped}",
            f"  Queued delta:     {queued_after - queued_before} (expect 0 — no stubs)",
        ]
    )

    if failed:
        lines.append("\n  Failures:")
        for o in failed:
            doi = o.parse.doi if o.parse else "?"
            lines.append(f"    {doi}: {o.errors}")

    # Cross-link check — are edges being created between our 25?
    slugs = {o.upsert.slug for o in ok if o.upsert and o.upsert.slug}
    lines.append(f"\n  Checking cross-links among {len(slugs)} processed nodes...")
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
            lines.append(
                f"    ERROR checking cross-links for {o.upsert.slug} ({doi}): {exc!r}"
            )

    lines.append(f"  Cross-citations within batch (in metadata): {len(cross_edges)}")
    lines.append(f"  Cross-link check failures: {cross_failures}")
    if cross_edges:
        for src, tgt in cross_edges[:10]:
            lines.append(f"    {src[:50]} → {tgt}")
        if len(cross_edges) > 10:
            lines.append(f"    ... and {len(cross_edges)-10} more")

    lines.extend(
        [
            "",
            format_metrics_table(metrics),
            f"\n{'='*60}",
        ]
    )
    report = "\n".join(lines)
    print(report)

    summary = {
        "processed": len(ok),
        "total": len(DOIS),
        "failed": len(failed),
        "created": created,
        "updated": updated,
        "citations_stored": total_stored,
        "edges_linked": total_linked,
        "edges_skipped": total_skipped,
        "cross_citations": len(cross_edges),
        "cross_link_failures": cross_failures,
        "elapsed_seconds": round(elapsed, 2),
        "per_paper_seconds": round(elapsed / len(DOIS), 2),
    }
    paths = log_benchmark_result("benchmark_25", len(DOIS), summary, metrics, report)
    print(f"\nLogged: {paths['jsonl']}\n        {paths['txt']}")
    print("DONE")


if __name__ == "__main__":
    run()
