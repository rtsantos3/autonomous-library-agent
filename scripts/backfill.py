#!/usr/bin/env python3
"""Backfill scaffolded Trellis references with enriched tags and metadata."""
from __future__ import annotations

import argparse
import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from pipeline.ingestion import backfill_nodes


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="skip nodes that already have topical tags (faster, but will not backfill citation edges on those nodes)",
    )
    parser.add_argument("--statuses", default="queued,scaffolded,failed")
    parser.add_argument("--chunk-size", type=int, default=100)
    args = parser.parse_args()
    statuses = tuple(status.strip() for status in args.statuses.split(",") if status.strip())

    _outcomes, result = backfill_nodes(
        workers=args.workers,
        only_missing=args.only_missing,
        statuses=statuses,
        chunk_size=args.chunk_size,
    )

    print("Backfill summary")
    print(f"  statuses: {','.join(statuses)}")
    print(f"  candidates: {result.candidates}")
    print(f"  resolvable: {result.resolvable}")
    print(f"  processed: {result.processed}")
    print(f"  edges_linked: {result.edges_linked}")
    print(f"  citations_stored: {result.citations_stored}")
    print(f"  failed: {result.failed}")
    print(f"  needs_review: {result.needs_review}")
    print(f"  skipped_no_doi: {result.skipped_no_doi}")
    print(f"  skipped_already_tagged: {result.skipped_already_tagged}")
    print(f"  errors: {len(result.errors)}")
    if result.errors:
        print()
        print("First errors")
        for error in result.errors[:10]:
            print(f"  - {error}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
