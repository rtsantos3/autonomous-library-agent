"""
Integration test: run full ingestion pipeline on 20 queued nodes,
then report interconnections materialized.
"""

import sys
from datetime import date
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")
load_dotenv(Path(__file__).resolve().parents[3] / ".env")

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.trellis import find_nodes, get_node
from pipeline.ingestion import ingest_reference_pipeline

DOIS = [
    "10.3390/microorganisms14030540",
    "10.1016/j.carbpol.2025.124723",
    "10.64898/2025.12.10.693402",
    "10.1101/2025.04.23.650130",
    "10.1161/hypertensionaha.125.17950",
    "10.3390/life15071033",
    "10.21203/rs.3.rs-6255159/v1",
    "10.32604/biocell.2026.075338",
    "10.3390/nu18030531",
    "10.18282/po4581",
    "10.1038/s41368-025-00415-2",
    "10.1016/j.phrs.2026.108091",
    "10.1016/j.jare.2025.12.010",
    "10.3389/fsurg.2025.1688387",
    "10.1080/15548627.2025.2568487",
    "10.1038/s12276-025-01518-w",
    "10.1080/1040841x.2025.2532611",
    "10.1016/j.immuni.2025.06.019",
    "10.3892/ijmm.2025.5571",
    "10.1021/acs.jafc.4c11988",
]


def count_queued():
    return len(find_nodes(tag="pipeline:queued"))


def run():
    results = []
    total_linked = 0
    total_stored = 0
    failed = []

    print(f"\n{'='*60}")
    print(f"Pipeline test — {len(DOIS)} papers — {date.today()}")
    print(f"{'='*60}")

    queued_before = count_queued()

    for i, doi in enumerate(DOIS, 1):
        print(f"\n[{i:02d}/{len(DOIS)}] {doi}")
        outcome = ingest_reference_pipeline({"doi": doi})

        if outcome.errors:
            print(f"  FAIL: {outcome.errors}")
            failed.append(doi)
            continue

        created = outcome.upsert.created if outcome.upsert else None
        stored = outcome.citation_store.stored if outcome.citation_store else 0
        linked = outcome.link.linked if outcome.link else 0
        skipped = outcome.link.skipped if outcome.link else 0
        status = outcome.verify.pipeline_status if outcome.verify else "?"
        src = outcome.resolve.source if outcome.resolve else "?"

        total_linked += linked
        total_stored += stored

        print(f"  slug:    {outcome.upsert.slug if outcome.upsert else 'none'}")
        print(f"  created: {created}  source: {src}  status: {status}")
        print(
            f"  citations stored: {stored}  edges linked: {linked}  skipped: {skipped}"
        )

        results.append(
            {
                "doi": doi,
                "slug": outcome.upsert.slug if outcome.upsert else None,
                "created": created,
                "source": src,
                "stored": stored,
                "linked": linked,
                "skipped": skipped,
                "status": status,
            }
        )

    queued_after = count_queued()

    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"  Processed:        {len(results)}/{len(DOIS)}")
    print(f"  Failed:           {len(failed)}")
    print(f"  Created new:      {sum(1 for r in results if r['created'])}")
    print(f"  Updated existing: {sum(1 for r in results if r['created'] is False)}")
    print(f"  Citations stored: {total_stored}")
    print(f"  Edges linked:     {total_linked}")
    print(f"  Queued before:    {queued_before}")
    print(
        f"  Queued after:     {queued_after}  (delta: {queued_after - queued_before}, should be 0 — no stubs)"
    )
    if failed:
        print("\n  Failed DOIs:")
        for d in failed:
            print(f"    {d}")

    # Check cross-links between the 20 papers
    slugs = {r["slug"] for r in results if r["slug"]}
    print(f"\n  Checking cross-links among the {len(slugs)} processed nodes...")
    cross = 0
    cross_failures = 0
    for r in results:
        if not r["slug"]:
            continue
        try:
            node = get_node(r["slug"])
            meta = (node.get("metadata") or {}).get("reference") or {}
            items = (meta.get("outbound_citations") or {}).get("items") or []
            for item in items:
                item_doi = item.get("doi") or ""
                if any(item_doi == rd for rd in DOIS if rd != r["doi"]):
                    cross += 1
                    print(
                        f"    {r['slug']} → cites a peer in our batch (doi: {item_doi})"
                    )
        except Exception as exc:
            cross_failures += 1
            print(
                f"    ERROR checking cross-links for {r['slug']} ({r['doi']}): {exc!r}"
            )

    print(f"\n  Cross-citations within batch: {cross}")
    print(f"  Cross-link check failures: {cross_failures}")
    print(
        f"  No-stub check: queued delta = {queued_after - queued_before} (expected 0)"
    )
    print(f"\n{'='*60}")
    print("DONE")


if __name__ == "__main__":
    run()
