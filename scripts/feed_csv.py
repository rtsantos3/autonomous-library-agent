#!/usr/bin/env python3
"""
Feed papers from EndNote CSV into ingest_paper(), one at a time.
Caches DOI/title in memory. Prints progress. Safe to restart.
"""

import csv
import json
import os
import subprocess
import sys
import time
import unicodedata

sys.path.insert(0, os.path.dirname(__file__))
from ingest import ingest_paper  # noqa: E402

CSV = os.path.join(os.path.dirname(__file__), "..", "data", "endnote_papers.csv")
PROGRESS = "/tmp/bulk_progress.json"


def norm(t):
    t = unicodedata.normalize("NFKD", (t or "").lower().strip())
    t = t.encode("ascii", "ignore").decode("ascii")
    t = t.rstrip(".")
    return " ".join(t.split())


# Load CSV, dedup within file
rows = []
seen_d, seen_t = set(), set()
with open(CSV, "r", encoding="utf-8", errors="replace") as f:
    for r in csv.DictReader(f):
        doi = (r.get("doi") or "").strip()
        title = r.get("title", "").strip()
        if not title or not doi.startswith("10."):
            continue
        dl, tl = doi.lower(), norm(title)
        if dl in seen_d or tl in seen_t:
            continue
        seen_d.add(dl)
        seen_t.add(tl)
        rows.append(r)

total = len(rows)
print(f"CSV papers with DOI: {total}")

# Load in-memory caches: scan all existing scaffolded nodes once
doi_cache = {}
title_cache = {}
r = subprocess.run(
    ["trellis", "find", "--tag", "pipeline:scaffolded", "--json", "--limit", "5000"],
    capture_output=True,
    text=True,
    timeout=30,
)
try:
    nodes = json.loads(r.stdout) if r.stdout.strip() else []
except Exception:
    nodes = []
for n in nodes:
    slug = n.get("slug", "")
    uri = n.get("uri", "") or ""
    if uri.startswith("doi:"):
        doi_cache[uri[4:].strip().lower()] = slug
    elif "doi.org/" in uri:
        doi_cache[uri.split("doi.org/")[-1].strip().lower()] = slug
    nt = norm(n.get("title", ""))
    if nt:
        title_cache[nt] = slug

print(f"Caches: DOI={len(doi_cache)} Title={len(title_cache)}")

# Feed
added = skipped = failed = 0
start = time.time()

for i, row in enumerate(rows):
    doi = row.get("doi", "").strip()
    title = row.get("title", "").strip()

    # Fast cache-only dedup
    dl = doi.lower()
    if dl in doi_cache:
        skipped += 1
        continue
    nt = norm(title)
    if nt in title_cache:
        skipped += 1
        continue

    # Not in cache — ingest
    slug = ingest_paper(
        doi=doi,
        title=title,
        abstract=row.get("abstract", "").strip(),
        year=row.get("year", "").strip(),
        venue=row.get("venue", "").strip(),
        authors=row.get("authors", "").strip(),
        source="endnote-csv",
        depth=0,
    )

    if slug:
        added += 1
        doi_cache[dl] = slug
        title_cache[nt] = slug
    else:
        failed += 1

    if (i + 1) % 100 == 0:
        elapsed = time.time() - start
        rate = (i + 1) / elapsed
        eta = (total - i - 1) / rate
        print(
            f"  [{i+1}/{total}] +{added} skip={skipped} fail={failed} | "
            f"{rate:.0f}/s ETA:{eta/60:.0f}m"
        )

elapsed = time.time() - start
print(f"\n=== DONE {elapsed/60:.1f}m ===")
print(f"Added: {added}  Skipped: {skipped}  Failed: {failed}  Total: {total}")
