#!/usr/bin/env python3
"""
Import RIS records into Trellis through the canonical ingestion pipeline.

Examples:
    python scripts/import_ris_network.py data/endnote-extracted/PDF
    python scripts/import_ris_network.py some_file.ris
    python scripts/import_ris_network.py some_file.ris --dry-run

This importer parses RIS files and hands every record to
`pipeline.ingestion.ingest_batch` — the single ingestion pipeline. Each record
becomes a fully enriched, deduped, citation-linked node in one pass:

    - records with a DOI drive the S2 batch prefetch + full enrichment
    - records without a DOI fall back to per-paper title/PMID enrichment
    - citation edges are fetched and linked by the pipeline; no separate
      backfill or expansion step is needed

The file's only unique responsibility is RIS parsing. All persistence,
deduplication, enrichment, and edge linking belong to the pipeline.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.ingestion import ingest_batch  # noqa: E402
from pipeline.trellis import _workspace  # noqa: E402

# ---------------------------------------------------------------------------
# RIS model
# ---------------------------------------------------------------------------


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

    def to_input(self) -> dict:
        """
        Project the parsed record into the pipeline's raw-input dict. Topical
        fields (keywords) are intentionally omitted: the pipeline re-derives
        kw:/mesh:/field: tags from the freshly resolved record, so passing
        source-side keywords would only be discarded downstream.
        """
        record = {"title": self.title}
        if self.doi:
            record["doi"] = self.doi
        if self.abstract:
            record["abstract"] = self.abstract
        if self.year:
            record["year"] = self.year
        if self.venue:
            record["venue"] = self.venue
        if self.authors:
            record["authors"] = self.authors
        return record


# ---------------------------------------------------------------------------
# RIS parsing
# ---------------------------------------------------------------------------


def normalize_doi(value: str) -> str:
    if not value:
        return ""
    doi = value.strip()
    doi = re.sub(r"^doi:\s*", "", doi, flags=re.I)
    doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi, flags=re.I)
    doi = doi.strip().rstrip(" .;,")
    return doi.lower()


def first(data: dict[str, list[str]], *keys: str) -> str:
    for key in keys:
        for value in data.get(key) or []:
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
            current[current_tag][
                -1
            ] = f"{current[current_tag][-1]} {line.strip()}".strip()

    flush()
    return records


def parse_ris_file(path: Path) -> list[RisRecord]:
    return parse_ris_text(path.read_text(encoding="utf-8", errors="replace"))


def collect_ris_files(root: Path) -> list[Path]:
    if root.is_file():
        return [root] if root.suffix.lower() == ".ris" else []
    return sorted(p for p in root.rglob("*.ris") if p.is_file())


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _outcome_status(outcome) -> str:
    if outcome.errors:
        return "error"
    if outcome.verify and outcome.verify.pipeline_status:
        return outcome.verify.pipeline_status
    return "unknown"


def main() -> int:
    # Enrichment (PubMed / S2 / Crossref) reads API keys from .env, matching
    # backfill.py; without this the pipeline runs key-less and gets rate-limited.
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Import RIS records into Trellis via the ingestion pipeline."
    )
    parser.add_argument(
        "path",
        nargs="?",
        default="data/endnote-extracted/PDF",
        help="RIS file or directory",
    )
    parser.add_argument(
        "--workers", type=int, default=8, help="Pipeline worker threads"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and report records without writing to Trellis",
    )
    args = parser.parse_args()

    print(f"workspace : {_workspace()}")
    print(f"path      : {args.path}")

    root = Path(args.path)
    ris_files = collect_ris_files(root)
    if not ris_files:
        print(f"no RIS files found under {root}")
        return 1

    print(f"files     : {len(ris_files)}")

    records: list[RisRecord] = []
    for path in ris_files:
        try:
            records.extend(parse_ris_file(path))
        except OSError as exc:
            print(f"[warn] {path}: {exc}")

    if not records:
        print("no RIS records parsed")
        return 1

    print(f"records   : {len(records)}")
    print()
    for i, record in enumerate(records, 1):
        print(f"[{i}/{len(records)}] {record.title[:70]}")
        print(f"         doi={record.doi or 'none'}  year={record.year or '?'}")

    if args.dry_run:
        print()
        print("=== dry-run: nothing written ===")
        return 0

    print()
    print(f"ingesting {len(records)} records through pipeline...")
    print()

    items = [record.to_input() for record in records]
    outcomes, _metrics = ingest_batch(items, workers=args.workers)

    created = updated = errored = linked = 0
    for i, (record, outcome) in enumerate(zip(records, outcomes), 1):
        status = _outcome_status(outcome)
        edges = outcome.link.linked if outcome.link else 0
        linked += edges
        if outcome.errors:
            errored += 1
            print(f"[{i}/{len(records)}] {record.title[:70]}")
            print(f"         -> error  ({outcome.errors[0]})")
        elif outcome.upsert and outcome.upsert.created:
            created += 1
            print(f"[{i}/{len(records)}] {record.title[:70]}")
            print(
                f"         -> created  slug={outcome.upsert.slug}  "
                f"status={status}  edges={edges}"
            )
        else:
            updated += 1
            slug = outcome.upsert.slug if outcome.upsert else "?"
            print(f"[{i}/{len(records)}] {record.title[:70]}")
            print(f"         -> updated  slug={slug}  status={status}  edges={edges}")

    print()
    print(
        f"=== done: created={created} updated={updated} "
        f"errors={errored} edges_linked={linked} ==="
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
