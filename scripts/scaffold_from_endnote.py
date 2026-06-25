#!/usr/bin/env python3
"""
Scaffold Trellis graph from EndNote library + Semantic Scholar citation edges.

Usage:
    python scripts/scaffold_from_endnote.py [--dry-run] [--skip-s2] [--batch SIZE]

Pipeline:
    1. Export EndNote SQLite -> CSV (if needed)
    2. Load CSV, deduplicate internally (DOI then title)
    3. Load ALL existing Trellis nodes into memory index
    4. Add only NEW papers as pipeline:scaffolded (skip duplicates)
    5. Query Semantic Scholar for citation edges between graph papers only
    6. Wire edges via trellis link ... references

No LLM work. No new papers from S2. Only scaffold + edges.
"""

import argparse
import csv
import json
import os
import subprocess
import time
import unicodedata
from collections import defaultdict

# ── Config ──────────────────────────────────────────────────────────────────

ENDNOTE_DB = "data/endnote-extracted/sdb/sdb.eni"
ENDNOTE_CSV = "data/endnote_papers.csv"
TRELLIS_PARENT = "microbiome-research-library"
S2_BASE = "https://api.semanticscholar.org/graph/v1/paper"

# ── Normalization ───────────────────────────────────────────────────────────


def norm_title(title):
    t = unicodedata.normalize("NFKD", (title or "").lower().strip())
    t = t.encode("ascii", "ignore").decode("ascii")
    t = t.rstrip(".")
    return " ".join(t.split())


# ── Trellis helpers ─────────────────────────────────────────────────────────


def trellis_json(*args):
    cmd = ["trellis"] + list(args) + ["--json"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if r.stdout.strip():
            return json.loads(r.stdout, strict=False)
    except Exception:
        pass
    return None


def load_trellis_index():
    """Load all existing nodes into in-memory DOI/title indexes."""
    doi_idx = {}  # doi_lower -> slug
    title_idx = {}  # norm_title -> slug
    count = 0

    for tag in [
        "pipeline:queued",
        "pipeline:scaffolded",
        "pipeline:digested",
        "pipeline:partial",
        "pipeline:needs-review",
        "pipeline:failed",
    ]:
        data = trellis_json("find", "--tag", tag)
        if not isinstance(data, list):
            continue
        for n in data:
            slug = n.get("slug", "")
            # DOI from uri
            uri = n.get("uri") or ""
            if uri.startswith("doi:"):
                doi_idx[uri[4:].lower()] = slug
            # DOI from metadata
            meta = n.get("metadata", {})
            if isinstance(meta, dict):
                ref = meta.get("reference", {})
                if isinstance(ref, dict) and ref.get("doi"):
                    doi_idx[ref["doi"].lower()] = slug
            # Title
            title = n.get("title", "")
            if title:
                title_idx[norm_title(title)] = slug
            count += 1

    print(
        f"    Trellis index: {count} nodes, {len(doi_idx)} DOIs, {len(title_idx)} titles"
    )
    return doi_idx, title_idx


def check_dup(doi, title, doi_idx, title_idx):
    """In-memory dedup. Returns (is_dup, slug)."""
    if doi and doi.strip():
        d = doi.strip().lower()
        if d in doi_idx:
            return True, doi_idx[d]
    if title:
        nt = norm_title(title)
        if nt in title_idx:
            return True, title_idx[nt]
    return False, None


# ── Step 1: Export EndNote SQLite ───────────────────────────────────────────


def export_csv(path):
    print("[1] Exporting EndNote SQLite -> CSV...")
    subprocess.run(
        [
            "sqlite3",
            "-header",
            "-csv",
            ENDNOTE_DB,
            "SELECT id, title, author, year, abstract, url, "
            "electronic_resource_number as doi, secondary_title as journal, "
            "volume, pages, keywords FROM refs WHERE trash_state=0",
        ],
        stdout=open(path, "w"),
        check=True,
    )
    r = subprocess.run(["wc", "-l", path], capture_output=True, text=True)
    print(f"    {r.stdout.strip()}")


# ── Step 2: Load and deduplicate CSV ────────────────────────────────────────


def load_and_dedup(path):
    print("[2] Loading and deduplicating EndNote CSV...")
    papers = []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            papers.append(row)
    print(f"    Raw: {len(papers)} papers")

    # Internal dedup: best copy per DOI, then unique titles for no-DOI
    by_doi = defaultdict(list)
    no_doi = []
    for p in papers:
        doi = p.get("doi", "").strip()
        if doi:
            by_doi[doi.lower()].append(p)
        else:
            no_doi.append(p)

    deduped = []
    for doi_key, group in by_doi.items():
        best = max(
            group,
            key=lambda p: len(p.get("abstract", "") or "")
            + len(p.get("keywords", "") or ""),
        )
        deduped.append(best)

    seen = set()
    for p in no_doi:
        nt = norm_title(p.get("title", ""))
        if nt and nt not in seen:
            seen.add(nt)
            deduped.append(p)

    has_doi = sum(1 for p in deduped if p.get("doi", "").strip())
    print(f"    After internal dedup: {len(deduped)} ({has_doi} with DOIs)")
    return deduped


# ── Step 3: Scaffold to Trellis ─────────────────────────────────────────────


def scaffold(papers, doi_idx, title_idx, dry_run=False):
    print(f"\n[3] Scaffolding {len(papers)} papers into Trellis...")
    stats = {"added": 0, "dup": 0, "fail": 0}
    new_slug_map = {}  # doi -> slug (newly added)
    existing_slug_map = {}  # doi -> slug (already in graph)

    for i, p in enumerate(papers):
        title = (p.get("title") or "").strip()
        doi = (p.get("doi") or "").strip()
        if not title:
            stats["fail"] += 1
            continue

        is_dup, slug = check_dup(doi, title, doi_idx, title_idx)
        if is_dup:
            stats["dup"] += 1
            if doi and slug:
                existing_slug_map[doi.lower()] = slug
            continue

        if dry_run:
            stats["added"] += 1
            if doi:
                new_slug_map[doi.lower()] = f"dry-{i}"
            continue

        # Build metadata
        meta = {"reference": {"schema": "reference-v1", "title": title}}
        if doi:
            meta["reference"]["doi"] = doi
        if p.get("year"):
            meta["reference"]["year"] = p["year"]
        if p.get("journal"):
            meta["reference"]["venue"] = p["journal"]
        if p.get("author"):
            meta["reference"]["citation"] = p["author"]

        tags = ["pipeline:scaffolded", "source:endnote"]
        if p.get("year"):
            tags.append(f"year:{p['year']}")

        title_esc = title.replace("\\", "\\\\").replace('"', '\\"')
        meta_json = json.dumps(meta, ensure_ascii=False).replace("'", "'\\''")

        result = trellis_json(
            "add",
            "reference",
            title_esc,
            "--abstract",
            (p.get("abstract") or "")[:4000],
            "--metadata",
            meta_json,
            "--tags",
            ",".join(tags),
            "--parent",
            TRELLIS_PARENT,
        )

        if isinstance(result, dict) and result.get("ok"):
            slug = result.get("node", {}).get("slug", "")
            stats["added"] += 1
            if doi:
                new_slug_map[doi.lower()] = slug
                doi_idx[doi.lower()] = slug  # update index
            title_idx[norm_title(title)] = slug  # update index
        else:
            stats["fail"] += 1
            if (i + 1) % 100 == 0:
                err = str(result)[:100] if result else "no output"
                print(f"    FAIL at {i}: {err}")

        if (i + 1) % 100 == 0:
            print(
                f"    [{i+1}/{len(papers)}] added={stats['added']} "
                f"dup={stats['dup']} fail={stats['fail']}"
            )

    print("\n    Scaffold results:")
    print(f"      Added:    {stats['added']}")
    print(f"      Existing: {stats['dup']}")
    print(f"      Failed:   {stats['fail']}")

    # Merge slug maps
    slug_map = {}
    slug_map.update(existing_slug_map)
    slug_map.update(new_slug_map)
    return slug_map


# ── Step 4: Derive edges from Semantic Scholar ──────────────────────────────


def derive_edges(slug_map, dry_run=False):
    """Query S2 references for each paper, keep only intra-graph edges."""
    s2_key = os.environ.get("S2_API_KEY")
    delay = 0.1 if s2_key else 1.0
    dois = list(slug_map.keys())
    edges = set()
    no_data = 0

    print(
        f"\n[4] Querying S2 for citation edges ({len(dois)} papers, {delay}s delay)..."
    )
    import urllib.request

    for i, doi in enumerate(dois):
        src = slug_map[doi]
        url = f"{S2_BASE}/DOI:{doi}/references?fields=externalIds&limit=500"
        headers = {}
        if s2_key:
            headers["x-api-key"] = s2_key
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
                for ref in data.get("data") or []:
                    ext = (ref.get("citedPaper") or {}).get("externalIds") or {}
                    rd = ext.get("DOI")
                    if rd and rd.lower() in slug_map and slug_map[rd.lower()] != src:
                        edges.add((src, slug_map[rd.lower()]))
        except Exception:
            no_data += 1

        if (i + 1) % 100 == 0 or i == len(dois) - 1:
            print(f"    [{i+1}/{len(dois)}] edges={len(edges)} miss={no_data}")

        time.sleep(delay)

    print(f"\n    Intra-graph edges: {len(edges)}")
    print(f"    S2 no data: {no_data}")
    return edges


def wire_edges(edges, dry_run=False):
    if dry_run:
        print(f"\n[5] DRY RUN: Would wire {len(edges)} edges")
        for src, tgt in sorted(edges)[:10]:
            print(f"    {src[:40]} -> {tgt[:40]}")
        if len(edges) > 10:
            print(f"    ... and {len(edges)-10} more")
        return

    print(f"\n[5] Wiring {len(edges)} edges...")
    ok = fail = 0
    for src, tgt in edges:
        r = trellis_json("link", src, tgt, "--relation", "references")
        if isinstance(r, dict) and r.get("ok"):
            ok += 1
        else:
            fail += 1
    print(f"    OK: {ok}, Fail: {fail}")


# ── Main ────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-s2", action="store_true")
    parser.add_argument("--export-csv", action="store_true")
    parser.add_argument("--csv", default=ENDNOTE_CSV)
    args = parser.parse_args()

    if args.export_csv:
        export_csv(args.csv)

    # Step 2: Load EndNote
    papers = load_and_dedup(args.csv)

    # Step 3: Load Trellis index + scaffold
    print("\n    Loading Trellis index...")
    doi_idx, title_idx = load_trellis_index()
    slug_map = scaffold(papers, doi_idx, title_idx, dry_run=args.dry_run)

    # Step 4-5: S2 edges
    if not args.skip_s2:
        edges = derive_edges(slug_map, dry_run=args.dry_run)
        wire_edges(edges, dry_run=args.dry_run)
    else:
        print("\n[4] Skipping S2 (--skip-s2)")

    print(f"\nDone. Slugs indexed: {len(slug_map)}")


if __name__ == "__main__":
    main()
