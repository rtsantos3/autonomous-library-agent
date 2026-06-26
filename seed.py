"""
Seed script: loads all papers from the EndNote library into Trellis as pipeline:queued nodes.

Usage:
    python seed.py
    python seed.py --dry-run   # preview without writing to Trellis
"""

import argparse
import json
import sqlite3
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
TRELLIS_BIN = "/home/articulatus/.nvm/versions/node/v22.17.0/bin/trellis"
PARENT_SLUG = "microbiome-research-library"

ENL_DB = PROJECT_ROOT / "data" / "My EndNote Library-9.3.enl"
SDB_DB = PROJECT_ROOT / "data" / "endnote-extracted" / "sdb" / "sdb.eni"
PDF_DIR = PROJECT_ROOT / "data" / "endnote-extracted" / "PDF"


def get_papers():
    """Read all papers from EndNote DB, join with PDF paths from sdb."""
    conn_enl = sqlite3.connect(str(ENL_DB))
    conn_enl.row_factory = sqlite3.Row
    papers = conn_enl.execute(
        "SELECT id, title, author, year, electronic_resource_number AS doi, "
        "accession_number AS pmid, abstract, keywords, secondary_title AS venue "
        "FROM enl_refs WHERE trash_state=0"
    ).fetchall()
    conn_enl.close()

    # Build PDF path lookup from sdb
    pdf_map = {}
    if SDB_DB.exists():
        conn_sdb = sqlite3.connect(str(SDB_DB))
        rows = conn_sdb.execute(
            "SELECT refs_id, file_path FROM file_res WHERE file_path LIKE '%.pdf'"
        ).fetchall()
        conn_sdb.close()
        for refs_id, file_path in rows:
            full_path = PDF_DIR / file_path
            if full_path.exists() and refs_id not in pdf_map:
                pdf_map[refs_id] = str(full_path)

    results = []
    for p in papers:
        record = dict(p)
        record["pdf_path"] = pdf_map.get(record["id"])
        results.append(record)

    return results


def trellis_add(title, doi=None, pmid=None, year=None, pdf_path=None):
    """Add a single pipeline:queued node to Trellis. Returns slug or None."""
    args = [TRELLIS_BIN, "add", "custom", title]

    if doi:
        args += ["--uri", f"https://doi.org/{doi}"]

    tags = ["pipeline:queued"]
    if year:
        tags.append(f"year:{year}")
    if pmid:
        tags.append(f"pmid:{pmid}")
    args += ["--tags", ",".join(tags)]

    if pdf_path:
        args += ["--file", pdf_path]

    args += ["--parent", PARENT_SLUG]
    args += ["--actor-id", "seed-script"]
    args.append("--json")

    result = subprocess.run(args, cwd=str(PROJECT_ROOT), capture_output=True, text=True)
    if result.returncode != 0:
        return None, result.stderr.strip()

    data = json.loads(result.stdout)
    node = data.get("node", data)
    return node.get("slug"), None


def trellis_find_uri(doi):
    """Check if a node with this DOI already exists."""
    uri = f"https://doi.org/{doi}"
    result = subprocess.run(
        [TRELLIS_BIN, "find", "--text", uri, "--json"],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return False
    parsed = json.loads(result.stdout)
    nodes = (
        parsed
        if isinstance(parsed, list)
        else parsed.get("results", parsed.get("nodes", []))
    )
    return any(n.get("uri") == uri for n in nodes)


def main():
    parser = argparse.ArgumentParser(description="Seed Trellis from EndNote library")
    parser.add_argument(
        "--dry-run", action="store_true", help="Preview without writing"
    )
    args = parser.parse_args()

    papers = get_papers()
    print(f"Found {len(papers)} papers in EndNote library")
    print(f"  {sum(1 for p in papers if p['pdf_path'])} have local PDFs")
    print()

    if args.dry_run:
        for p in papers[:10]:
            pdf = "PDF" if p["pdf_path"] else "no PDF"
            doi = p["doi"] or "no DOI"
            print(f"  [{pdf}] {doi} | {p['title'][:70]}")
        print(f"  ... and {len(papers) - 10} more")
        return

    added = 0
    skipped = 0
    failed = 0

    for i, p in enumerate(papers):
        title = p["title"]
        if not title or not title.strip():
            failed += 1
            continue

        # Dedup by DOI
        if p["doi"] and trellis_find_uri(p["doi"]):
            skipped += 1
            if (i + 1) % 100 == 0:
                print(
                    f"  [{i+1}/{len(papers)}] added={added} skipped={skipped} failed={failed}"
                )
            continue

        slug, err = trellis_add(
            title=title.strip(),
            doi=p["doi"] if p["doi"] else None,
            pmid=p["pmid"] if p["pmid"] else None,
            year=p["year"] if p["year"] else None,
            pdf_path=p["pdf_path"],
        )

        if slug:
            added += 1
        else:
            failed += 1
            if err:
                print(f"  FAIL: {title[:50]} — {err[:100]}")

        if (i + 1) % 100 == 0:
            print(
                f"  [{i+1}/{len(papers)}] added={added} skipped={skipped} failed={failed}"
            )

    print(f"\nDone. added={added} skipped={skipped} failed={failed}")


if __name__ == "__main__":
    main()
