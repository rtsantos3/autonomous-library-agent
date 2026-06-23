#!/usr/bin/env python3
"""Bulk scaffold EndNote CSV papers into Trellis via ingest.ingest_paper()."""
import csv, sys, os, time, unicodedata

sys.path.insert(0, os.path.dirname(__file__))
from ingest import ingest_paper, load_existing_titles, load_existing_dois, _norm_title

CSV_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'endnote_papers.csv')


def norm_title(t):
    t = unicodedata.normalize('NFKD', (t or '').lower().strip())
    t = t.encode('ascii', 'ignore').decode('ascii')
    t = t.rstrip('.')
    return ' '.join(t.split())


# Load CSV
papers = []
with open(CSV_PATH, 'r', encoding='utf-8', errors='replace') as f:
    for row in csv.DictReader(f):
        papers.append(row)

# Dedup within CSV
seen_dois = set()
seen_titles = set()
unique = []
for row in papers:
    title = row.get('title', '').strip()
    doi = (row.get('doi') or '').strip()
    if not title or not doi.startswith('10.'):
        continue
    dl = doi.lower()
    tl = norm_title(title)
    if dl in seen_dois or tl in seen_titles:
        continue
    seen_dois.add(dl)
    seen_titles.add(tl)
    unique.append(row)

print(f"Papers with DOI: {len(unique)}")

# Pre-load caches (one query each, avoids per-paper CLI calls)
print("Loading caches...")
title_cache = load_existing_titles()
doi_cache = load_existing_dois()
print(f"Title cache: {len(title_cache)} | DOI cache: {len(doi_cache)}")

# Ingest
ok = skip = fail = 0
start = time.time()

for i, row in enumerate(unique):
    slug = ingest_paper(
        doi=row.get('doi', '').strip(),
        title=row.get('title', '').strip(),
        abstract=row.get('abstract', '').strip(),
        year=row.get('year', '').strip(),
        venue=row.get('venue', '').strip(),
        authors=row.get('authors', '').strip(),
        source='endnote-csv',
        depth=0,
        title_cache=title_cache,
        doi_cache=doi_cache,
    )
    if slug is None:
        fail += 1
    else:
        # Update caches so subsequent CSV dupes hit in-memory
        nt = _norm_title(row.get('title', ''))
        if nt:
            title_cache[nt] = slug
        doi_cache[row.get('doi', '').strip().lower()] = slug
        ok += 1

    if (i + 1) % 200 == 0:
        elapsed = time.time() - start
        rate = (i + 1) / elapsed
        eta = (len(unique) - i - 1) / rate
        print(f"  [{i+1}/{len(unique)}] OK:{ok} FAIL:{fail} | {rate:.1f}/s ETA:{eta/60:.1f}m")

elapsed = time.time() - start
print(f"\n=== DONE in {elapsed/60:.1f}m ===")
print(f"Added/Skipped: {ok}  Failed: {fail}  Total: {len(unique)}")
