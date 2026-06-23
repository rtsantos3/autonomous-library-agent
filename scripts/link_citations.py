#!/usr/bin/env python3
"""
Build citation edges between existing Trellis nodes.
For each paper with a DOI, query S2 for references/citations,
then trellis link any matches that already exist in our graph.
No new nodes created.
"""
import json, subprocess, os, sys, time, requests

S2_KEY = os.environ.get('S2_API_KEY', '')
S2_BASE = 'https://api.semanticscholar.org/graph/v1'

def trellis(*args):
    r = subprocess.run(['trellis'] + list(args), capture_output=True, text=True, timeout=15)
    return r

# Pre-load all existing DOIs
print("Loading existing nodes...")
r = trellis('find', '--tag', 'pipeline:scaffolded', '--json', '--limit', '5000')
nodes = json.loads(r.stdout or '[]')
doi_to_slug = {}
for n in nodes:
    uri = n.get('uri', '') or ''
    doi = ''
    if uri.startswith('doi:'):
        doi = uri[4:].strip()
    elif 'doi.org/' in uri:
        doi = uri.split('doi.org/')[-1].strip()
    if doi:
        doi_to_slug[doi.lower()] = n.get('slug', '')

print(f"Loaded {len(doi_to_slug)} nodes with DOIs")

# Track existing links to avoid duplicates
existing_links = set()
r = trellis('find', '--type', 'reference', '--json', '--limit', '5000')
# We'll just try links and let trellis handle dupes

headers = {}
if S2_KEY:
    headers['x-api-key'] = S2_KEY

linked = skipped = errors = 0
total = len(doi_to_slug)
start = time.time()

for i, (doi, slug) in enumerate(doi_to_slug.items()):
    # Get outbound references (papers this paper cites)
    try:
        url = f'{S2_BASE}/paper/DOI:{doi}?fields=references.externalIds'
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            errors += 1
            continue
        data = r.json()
    except Exception as e:
        errors += 1
        continue

    refs = data.get('references') or []
    for ref in refs:
        ext = ref.get('externalIds') or {}
        ref_doi = (ext.get('DOI') or '').lower()
        if not ref_doi or ref_doi not in doi_to_slug:
            continue
        target_slug = doi_to_slug[ref_doi]
        if target_slug == slug:
            continue

        # Link: slug cites target_slug
        r = trellis('link', slug, target_slug, '--relation', 'cites', '--actor-id', 'daedalus')
        if r.returncode == 0:
            linked += 1
        # Skip errors (likely duplicate link)

    # Rate limit: 10 req/s with key, 1 req/s without
    time.sleep(0.11 if S2_KEY else 1.1)

    if (i + 1) % 50 == 0:
        elapsed = time.time() - start
        rate = (i+1) / elapsed
        eta = (total - i - 1) / rate
        print(f"  [{i+1}/{total}] links={linked} errors={errors} | {rate:.1f}/s ETA:{eta/60:.0f}m")

elapsed = time.time() - start
print(f"\n=== DONE {elapsed/60:.1f}m ===")
print(f"Links created: {linked}")
print(f"Errors: {errors}")
print(f"Papers processed: {total}")
