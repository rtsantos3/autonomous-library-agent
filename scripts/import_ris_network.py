#!/usr/bin/env python3
"""
Import RIS records into Trellis and optionally expand citations for a seed DOI.

Examples:
    python scripts/import_ris_network.py data/endnote-extracted/PDF
    python scripts/import_ris_network.py data/endnote-extracted/PDF --seed-doi 10.1038/nature25973
    python scripts/import_ris_network.py some_file.ris --seed-title "Environment dominates over host genetics..."

The importer keeps the pipeline additive:
    - RIS records become pipeline:scaffolded nodes
    - citation expansion adds/link queued nodes when a DOI is available
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))

from ingest import ingest_paper, load_existing_dois, load_existing_titles  # noqa: E402

TRELLIS_BIN = "/home/articulatus/.nvm/versions/node/v22.17.0/bin/trellis"
PARENT = "microbiome-research-library"
ACTOR_ID = "daedalus"
S2_BASE = "https://api.semanticscholar.org/graph/v1"
S2_KEY = os.getenv("S2_API_KEY", "")


@dataclass
class RisRecord:
    title: str = ""
    abstract: str = ""
    doi: str = ""
    year: str = ""
    venue: str = ""
    authors: list[str] = None
    keywords: list[str] = None
    url: str = ""

    def __post_init__(self):
        self.authors = self.authors or []
        self.keywords = self.keywords or []


@dataclass
class ImportOutcome:
    slug: Optional[str]
    doi: str
    imported: bool
    linked_edges: int = 0
    skipped_reason: str = ""


def trellis(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [TRELLIS_BIN, *args],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=30,
    )


def trellis_json(*args: str):
    result = trellis(*args, "--json")
    if result.returncode != 0 or not result.stdout.strip():
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def normalize_doi(value: str) -> str:
    if not value:
        return ""
    doi = value.strip()
    doi = re.sub(r"^doi:\s*", "", doi, flags=re.I)
    doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi, flags=re.I)
    doi = doi.strip().rstrip(" .;,")
    return doi.lower()


def normalize_text(value: str) -> str:
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", value.lower().strip())
    text = text.encode("ascii", "ignore").decode("ascii")
    return " ".join(text.split())


def parse_ris_text(text: str) -> list[RisRecord]:
    records: list[RisRecord] = []
    current: dict[str, list[str]] = {}
    current_tag: Optional[str] = None

    def flush() -> None:
        nonlocal current
        if not current:
            return
        record = RisRecord(
            title=first(current, "T1", "TI"),
            abstract=first(current, "AB", "N2"),
            doi=extract_doi(current),
            year=extract_year(current),
            venue=first(current, "JO", "T2", "J2"),
            authors=values(current, "AU"),
            keywords=values(current, "KW"),
            url=first(current, "UR"),
        )
        if record.title:
            records.append(record)
        current = {}

    for raw_line in text.splitlines():
        line = raw_line.rstrip("\n")
        if not line.strip():
            continue
        if line.startswith("ER  -"):
            flush()
            current_tag = None
            continue
        match = re.match(r"^([A-Z0-9]{2})  - (.*)$", line)
        if match:
            tag, value = match.group(1), match.group(2).strip()
            current.setdefault(tag, []).append(value)
            current_tag = tag
            continue
        if current_tag and line[:1].isspace():
            current[current_tag][-1] = f"{current[current_tag][-1]} {line.strip()}".strip()

    flush()
    return records


def parse_ris_file(path: Path) -> list[RisRecord]:
    return parse_ris_text(path.read_text(encoding="utf-8", errors="replace"))


def collect_ris_files(root: Path) -> list[Path]:
    if root.is_file():
        return [root] if root.suffix.lower() == ".ris" else []
    return sorted(p for p in root.rglob("*.ris") if p.is_file())


def first(data: dict[str, list[str]], *keys: str) -> str:
    for key in keys:
        values = data.get(key) or []
        for value in values:
            if value and value.strip():
                return value.strip()
    return ""


def values(data: dict[str, list[str]], *keys: str) -> list[str]:
    out: list[str] = []
    for key in keys:
        for value in data.get(key) or []:
            value = (value or "").strip()
            if value:
                out.append(value)
    return out


def extract_year(data: dict[str, list[str]]) -> str:
    for key in ("PY", "Y1", "DA"):
        raw = first(data, key)
        if not raw:
            continue
        match = re.search(r"(19|20)\d{2}", raw)
        if match:
            return match.group(0)
    return ""


def extract_doi(data: dict[str, list[str]]) -> str:
    candidates = values(data, "DO", "M3", "UR", "N1")
    for candidate in candidates:
        doi = normalize_doi(candidate)
        if doi.startswith("10."):
            return doi
        match = re.search(r"(10\.\S+)", candidate, flags=re.I)
        if match:
            return normalize_doi(match.group(1))
    return ""


def maybe_link(source_slug: str, target_slug: str, relation: str) -> bool:
    result = trellis("link", source_slug, target_slug, "--relation", relation, "--actor-id", ACTOR_ID)
    return result.returncode == 0


def add_title_only_node(
    *,
    title: str,
    abstract: str,
    year: str,
    venue: str,
    authors: list[str],
    doi: str,
    url: str,
    extra_tags: list[str],
    title_cache: dict[str, str],
    dry_run: bool,
) -> Optional[str]:
    if dry_run:
        return f"dry-run:{normalize_text(title)[:32]}"

    tags = ["pipeline:scaffolded", "source:ris", "depth:0", *extra_tags]
    if year:
        tags.append(f"year:{year}")

    metadata_reference = {"schema": "reference-v1", "title": title}
    if doi:
        metadata_reference["doi"] = doi
    if url:
        metadata_reference["url"] = url
    if year:
        metadata_reference["year"] = year
    if venue:
        metadata_reference["venue"] = venue
    if authors:
        metadata_reference["authors"] = authors

    args = ["add", "reference", title]
    if abstract:
        args += ["--abstract", abstract]
    args += [
        "--metadata",
        json.dumps({"reference": metadata_reference}, ensure_ascii=False),
        "--tags",
        ",".join(tags),
        "--parent",
        PARENT,
        "--actor-id",
        ACTOR_ID,
        "--json",
    ]
    result = trellis(*args)
    if result.returncode != 0 or not result.stdout.strip():
        return None
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    node = data.get("node", data)
    slug = node.get("slug")
    if slug:
        title_cache[normalize_text(title)] = slug
    return slug


def resolve_record_identity(record: RisRecord, title_cache: dict[str, str], doi_cache: dict[str, str]) -> tuple[str, Optional[str], str]:
    """
    Normalize the record identity before any Trellis writes.

    Returns:
        (doi, existing_slug, skip_reason)
    """
    doi = normalize_doi(record.doi)
    title = record.title.strip()
    if doi and doi in doi_cache:
        return doi, doi_cache[doi], "duplicate-doi"

    if title:
        existing = title_cache.get(normalize_text(title))
        if existing:
            return doi, existing, "duplicate-title"

    return doi, None, ""


def persist_reference_record(
    record: RisRecord,
    *,
    source: str,
    depth: int,
    title_cache: dict[str, str],
    doi_cache: dict[str, str],
    dry_run: bool,
    doi: str,
) -> Optional[str]:
    if dry_run:
        return f"dry-run:{normalize_text(record.title)[:32]}"

    if doi:
        slug = ingest_paper(
            doi=doi,
            title=record.title,
            abstract=record.abstract or None,
            year=record.year or None,
            venue=record.venue or None,
            authors="; ".join(record.authors) if record.authors else None,
            source=source,
            depth=depth,
            parent=PARENT,
            actor=ACTOR_ID,
            extra_tags=[f"kw:{normalize_text(kw).replace(' ', '-')}" for kw in record.keywords if normalize_text(kw)],
            title_cache=title_cache,
            doi_cache=doi_cache,
        )
    else:
        slug = add_title_only_node(
            title=record.title,
            abstract=record.abstract or "",
            year=record.year or "",
            venue=record.venue or "",
            authors=record.authors,
            doi=doi,
            url=record.url or "",
            extra_tags=[f"kw:{normalize_text(kw).replace(' ', '-')}" for kw in record.keywords if normalize_text(kw)],
            title_cache=title_cache,
            dry_run=dry_run,
        )

    if slug and doi:
        doi_cache[doi] = slug
    if slug:
        title_cache[normalize_text(record.title)] = slug
    return slug


def resolve_node_by_doi(doi: str, doi_cache: dict[str, str]) -> Optional[str]:
    doi = normalize_doi(doi)
    if not doi:
        return None
    if doi in doi_cache:
        return doi_cache[doi]

    uri = f"https://doi.org/{doi}"
    hits = trellis_json("find", "--text", uri)
    if not hits:
        return None
    nodes = hits if isinstance(hits, list) else hits.get("results", hits.get("nodes", []))
    for node in nodes:
        node_uri = (node.get("uri") or "").strip().lower()
        meta = node.get("metadata", {}) if isinstance(node.get("metadata", {}), dict) else {}
        ref = meta.get("reference", {}) if isinstance(meta, dict) else {}
        ref_doi = normalize_doi(str(ref.get("doi", "") or ""))
        ref_url = str(ref.get("url", "") or "").lower()
        if node_uri in {uri.lower(), f"doi:{doi}"} or ref_doi == doi or f"doi.org/{doi}" in ref_url:
            slug = node.get("slug")
            if slug:
                doi_cache[doi] = slug
                return slug
    return None


def resolve_node_by_title(title: str, title_cache: dict[str, str]) -> Optional[str]:
    nt = normalize_text(title)
    if not nt:
        return None
    if nt in title_cache:
        return title_cache[nt]
    hits = trellis_json("find", "--text", title)
    if not hits:
        return None
    nodes = hits if isinstance(hits, list) else hits.get("results", hits.get("nodes", []))
    for node in nodes:
        meta = node.get("metadata", {}) if isinstance(node.get("metadata", {}), dict) else {}
        ref = meta.get("reference", {}) if isinstance(meta, dict) else {}
        if normalize_text(node.get("title", "")) == nt or normalize_text(ref.get("title", "")) == nt:
            slug = node.get("slug")
            if slug:
                title_cache[nt] = slug
                return slug
    return None


def ingest_record(
    record: RisRecord,
    *,
    source: str,
    depth: int,
    title_cache: dict[str, str],
    doi_cache: dict[str, str],
    dry_run: bool = False,
) -> Optional[str]:
    doi, existing_slug, _ = resolve_record_identity(record, title_cache, doi_cache)
    if existing_slug:
        return existing_slug
    return persist_reference_record(
        record,
        source=source,
        depth=depth,
        title_cache=title_cache,
        doi_cache=doi_cache,
        dry_run=dry_run,
        doi=doi,
    )


def fetch_related(doi: str, direction: str) -> list[dict]:
    headers = {"x-api-key": S2_KEY} if S2_KEY else {}
    url = f"{S2_BASE}/paper/DOI:{doi}/{direction}"
    params = {"fields": "title,externalIds,year", "limit": 100}
    response = requests.get(url, headers=headers, params=params, timeout=30)
    if response.status_code != 200:
        return []
    payload = response.json()
    return payload.get("data") or []


def related_entry(item: dict, direction: str) -> tuple[str, str]:
    paper = item.get("citedPaper" if direction == "references" else "citingPaper") or item
    title = paper.get("title", "") or ""
    doi = normalize_doi((paper.get("externalIds") or {}).get("DOI", "") or "")
    return title, doi


def expand_citations(
    seed_slug: str,
    seed_doi: str,
    *,
    max_hops: int,
    dry_run: bool,
    title_cache: dict[str, str],
    doi_cache: dict[str, str],
) -> int:
    if not seed_doi:
        return 0

    queue: list[tuple[str, str, int]] = [(seed_slug, seed_doi, 0)]
    seen = {normalize_doi(seed_doi)}
    linked = 0

    while queue:
        source_slug, source_doi, depth = queue.pop(0)
        if depth >= max_hops:
            continue

        for direction in ("references", "citations"):
            relation_items = fetch_related(source_doi, direction)
            for item in relation_items:
                title, related_doi = related_entry(item, direction)
                target_slug = None

                if related_doi:
                    target_slug = resolve_node_by_doi(related_doi, doi_cache)
                    if not target_slug:
                        record = RisRecord(title=title, doi=related_doi)
                        target_slug = ingest_record(
                            record,
                            source="semantic-scholar",
                            depth=depth + 1,
                            title_cache=title_cache,
                            doi_cache=doi_cache,
                            dry_run=dry_run,
                        )
                    if related_doi not in seen:
                        seen.add(related_doi)
                        if target_slug and depth + 1 < max_hops:
                            queue.append((target_slug, related_doi, depth + 1))
                elif title:
                    target_slug = resolve_node_by_title(title, title_cache)

                if not target_slug or target_slug == source_slug:
                    continue

                if direction == "references":
                    if maybe_link(source_slug, target_slug, "cites"):
                        linked += 1
                else:
                    if maybe_link(target_slug, source_slug, "cites"):
                        linked += 1

    return linked


def orchestrate_reference_ingestion(
    record: RisRecord,
    *,
    source: str,
    depth: int,
    title_cache: dict[str, str],
    doi_cache: dict[str, str],
    dry_run: bool = False,
    max_hops: int = 2,
    expand: bool = True,
) -> ImportOutcome:
    """
    Central ingestion orchestrator.

    Pipeline:
        1. Resolve DOI and dedupe against the current graph.
        2. Persist the reference node.
        3. Fetch and wire citations when the record has a DOI.
    """
    doi, existing_slug, skip_reason = resolve_record_identity(record, title_cache, doi_cache)
    if existing_slug:
        return ImportOutcome(slug=existing_slug, doi=doi, imported=False, skipped_reason=skip_reason)

    slug = persist_reference_record(
        record,
        source=source,
        depth=depth,
        title_cache=title_cache,
        doi_cache=doi_cache,
        dry_run=dry_run,
        doi=doi,
    )
    if not slug:
        return ImportOutcome(slug=None, doi=doi, imported=False, skipped_reason="persist-failed")

    linked_edges = 0
    if expand and doi and not dry_run:
        linked_edges = expand_citations(
            slug,
            doi,
            max_hops=max_hops,
            dry_run=dry_run,
            title_cache=title_cache,
            doi_cache=doi_cache,
        )

    return ImportOutcome(slug=slug, doi=doi, imported=True, linked_edges=linked_edges)


def main() -> int:
    parser = argparse.ArgumentParser(description="Import RIS records into Trellis and expand citations.")
    parser.add_argument("path", nargs="?", default="data/endnote-extracted/PDF", help="RIS file or directory")
    parser.add_argument("--seed-doi", default=None, help="Seed DOI for citation expansion")
    parser.add_argument("--seed-title", default=None, help="Fallback title for seed lookup")
    parser.add_argument("--max-hops", type=int, default=2, help="Citation expansion depth")
    parser.add_argument("--dry-run", action="store_true", help="Parse and report without writing to Trellis")
    args = parser.parse_args()

    root = Path(args.path)
    ris_files = collect_ris_files(root)
    if not ris_files:
        print(f"No RIS files found under {root}")
        return 1

    doi_cache = load_existing_dois()
    title_cache = load_existing_titles()

    imported = 0
    skipped = 0
    failed = 0
    linked = 0

    for path in ris_files:
        try:
            records = parse_ris_file(path)
        except OSError as exc:
            print(f"[warn] {path}: {exc}")
            failed += 1
            continue

        for record in records:
            outcome = orchestrate_reference_ingestion(
                record,
                source="ris",
                depth=0,
                title_cache=title_cache,
                doi_cache=doi_cache,
                dry_run=args.dry_run,
                max_hops=args.max_hops,
                expand=bool(args.seed_doi or args.seed_title),
            )
            if outcome.imported:
                imported += 1
                linked += outcome.linked_edges
            else:
                skipped += 1

    print(f"Imported={imported} Skipped={skipped} Linked={linked} Files={len(ris_files)}")

    if args.dry_run or (not args.seed_doi and not args.seed_title):
        return 0

    seed_doi = normalize_doi(args.seed_doi or "")
    seed_slug = None
    if seed_doi:
        seed_slug = resolve_node_by_doi(seed_doi, doi_cache)
    if not seed_slug and args.seed_title:
        seed_slug = resolve_node_by_title(args.seed_title, title_cache)

    if not seed_slug:
        print("Seed paper not found in Trellis; citation expansion skipped.")
        return 0

    if not seed_doi and args.seed_title:
        # If the seed was found by title, try to recover a DOI from the imported cache.
        seed_doi = next((doi for doi, slug in doi_cache.items() if slug == seed_slug), "")

    linked = expand_citations(
        seed_slug,
        seed_doi,
        max_hops=args.max_hops,
        dry_run=args.dry_run,
        title_cache=title_cache,
        doi_cache=doi_cache,
    )
    print(f"Citation links created: {linked}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
