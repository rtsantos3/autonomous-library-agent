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
        "--all",
        action="store_true",
        help="process all nodes for the status, including already-tagged nodes",
    )
    parser.add_argument("--status", default="scaffolded")
    args = parser.parse_args()

    _outcomes, result = backfill_nodes(
        workers=args.workers,
        only_missing=not args.all,
        status=args.status,
    )

    print("Backfill summary")
    print(f"  candidates: {result.candidates}")
    print(f"  resolvable: {result.resolvable}")
    print(f"  backfilled: {result.backfilled}")
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
