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
import os
import re
import shutil
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline import trellis  # noqa: E402
from pipeline._http import CROSSREF_LIMITER, http_get  # noqa: E402
from pipeline.ingestion import _classify_failure, ingest_batch  # noqa: E402
from pipeline.ris_sink import RisSink  # noqa: E402
from pipeline.trellis import _norm_title, _workspace  # noqa: E402

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
        Project the parsed record into the pipeline's raw-input dict. RIS KW
        keywords are passed as a fallback floor: enrichment-derived kw:/mesh:/
        field: tags always win, but resolve_identity keeps these source-side
        keywords when enrichment resolves none (e.g. a record not found in
        PubMed/S2), so an enrichment-miss node is not left with no topical tags.
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
        if self.keywords:
            record["keywords"] = self.keywords
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
    return _classify_failure(outcome.errors) if outcome.errors else "digested"


# ---------------------------------------------------------------------------
# Sink helpers: DOI fallback, stub notes, archive
# ---------------------------------------------------------------------------


def crossref_doi_for_title(title: str) -> str:
    """
    Return the DOI of the Crossref record whose title matches exactly (after
    normalization), else "". Trellis requires a locator on every reference,
    so a DOI-less record that enrichment cannot resolve would be refused.
    Exact-match only: a near-miss DOI would attach the wrong paper.
    """
    params = {"query.bibliographic": title, "rows": 5, "select": "DOI,title"}
    if os.getenv("CROSSREF_EMAIL"):
        params["mailto"] = os.environ["CROSSREF_EMAIL"]
    try:
        resp = http_get(
            "https://api.crossref.org/works",
            params=params,
            limiter=CROSSREF_LIMITER,
            timeout=30,
        )
        items = resp.json().get("message", {}).get("items", [])
    except Exception as exc:  # network/JSON failure -> no fallback, not fatal
        print(f"[warn] crossref lookup failed for {title[:60]!r}: {exc}")
        return ""
    want = _norm_title(title)
    for item in items:
        if _norm_title((item.get("title") or [""])[0]) == want:
            return normalize_doi(item.get("DOI", ""))
    return ""


def _note_slug(text: str, words: int) -> str:
    parts = re.sub(r"[^a-z0-9]+", " ", text.lower()).split()
    return "-".join(parts[:words]) or "untitled"


def write_stub_note(record: RisRecord, notes_dir: Path, source: str) -> Optional[Path]:
    """
    Write a stub reference note for a newly created node. Never overwrites an
    existing file: returns None so the caller leaves the node without a file.
    """
    first_author = record.authors[0].split(",")[0] if record.authors else "anon"
    name = (
        f"{_note_slug(first_author, 1)}-{record.year or 'nd'}-"
        f"{_note_slug(record.title, 6)}.md"
    )
    path = notes_dir / name
    if path.exists():
        print(f"[warn] note exists, not overwritten: {path}")
        return None
    locator = (
        f"doi:[{record.doi}](https://doi.org/{record.doi})"
        if record.doi
        else (record.url or "no identifier")
    )
    path.write_text(
        f"# {record.title}\n\n"
        f"**Category:** Reference (RIS import, unfiled)\n"
        f"**Primary source:** {'; '.join(record.authors) or 'unknown'}. "
        f'"{record.title}." *{record.venue or "unknown venue"}* '
        f"({record.year or 'n.d.'}). {locator}\n"
        f"**Accessed:** {date.today()} (RIS import from `{source}`)\n\n"
        f"## Abstract\n\n{record.abstract or '_No abstract in the RIS record._'}\n\n"
        f"## What it does\n\n_To be written._\n\n"
        f"## What it does not establish\n\n_To be written._\n\n"
        f"## Relevance\n\n_Unfiled._\n",
        encoding="utf-8",
    )
    return path


def archive_files(files: list[Path], archive_dir: Path) -> None:
    archive_dir.mkdir(parents=True, exist_ok=True)
    stamp = date.today().isoformat()
    for path in files:
        shutil.move(str(path), archive_dir / f"{stamp}-{path.name}")


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
    parser.add_argument(
        "--parent",
        help="Existing node slug to parent new references under "
        f"(default: TRELLIS_PROJECT_SLUG or {trellis.PROJECT_SLUG})",
    )
    parser.add_argument(
        "--archive",
        type=Path,
        help="Move the processed RIS files here when no record errored "
        "(excluded from collection, so a sink folder can hold its own archive)",
    )
    parser.add_argument(
        "--notes-dir",
        type=Path,
        help="Write a stub Markdown note per newly created reference here and "
        "bind it as the node's file (must be inside the Trellis workspace)",
    )
    parser.add_argument(
        "--no-crossref",
        action="store_true",
        help="Skip the Crossref exact-title DOI lookup for DOI-less records",
    )
    parser.add_argument(
        "--ris-sink",
        nargs="?",
        const="",
        metavar="PATH",
        help="Also append every resolved paper to an RIS file "
        "(no PATH: RIS_SINK_PATH or the default vault references/library.ris)",
    )
    args = parser.parse_args()

    if args.parent:
        os.environ["TRELLIS_PROJECT_SLUG"] = args.parent
    workspace = Path(_workspace()).resolve()
    notes_dir = args.notes_dir.resolve() if args.notes_dir else None
    if notes_dir and workspace not in notes_dir.parents and notes_dir != workspace:
        print(f"--notes-dir {notes_dir} is outside the workspace {workspace}")
        return 2

    print(f"workspace : {workspace}")
    print(f"parent    : {trellis._project_slug()}")
    print(f"path      : {args.path}")

    root = Path(args.path)
    ris_files = collect_ris_files(root)
    if args.archive:
        archive = args.archive.resolve()
        ris_files = [p for p in ris_files if archive not in p.resolve().parents]
    # Never read (or archive) the sink's own output as input — whether the sink
    # comes from --ris-sink or from RIS_SINK_PATH via the intake hook.
    sink = RisSink(args.ris_sink or None) if args.ris_sink is not None else None
    sink_path = (sink or RisSink()).path.resolve()
    ris_files = [p for p in ris_files if p.resolve() != sink_path]
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

    # Collapse duplicate records before ingesting. EndNote RIS exports often
    # repeat the same entry; the pipeline's dedup only sees the graph as it was
    # at batch start, so two identical new records would each create a node.
    # Key on DOI when present, else the title; keep first occurrence.
    seen: set[str] = set()
    unique: list[RisRecord] = []
    for record in records:
        key = record.doi or record.title.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(record)
    dropped = len(records) - len(unique)
    records = unique
    if dropped:
        print(f"deduped   : dropped {dropped} duplicate record(s)")

    print(f"records   : {len(records)}")
    print()
    for i, record in enumerate(records, 1):
        print(f"[{i}/{len(records)}] {record.title[:70]}")
        print(f"         doi={record.doi or 'none'}  year={record.year or '?'}")

    if args.dry_run:
        print()
        print("=== dry-run: nothing written ===")
        return 0

    # Fail fast on a missing parent: add_reference would refuse every new node.
    try:
        trellis.get_node(trellis._project_slug())
    except Exception as exc:
        print(f"parent node {trellis._project_slug()!r} not found: {exc}")
        return 2

    if not args.no_crossref:
        for record in records:
            if not record.doi:  # the pipeline locates new nodes by DOI only
                record.doi = crossref_doi_for_title(record.title)
                if record.doi:
                    print(f"crossref  : {record.title[:60]} -> {record.doi}")

    print()
    print(f"ingesting {len(records)} records through pipeline...")
    print()

    items = [record.to_input() for record in records]
    if sink:
        print(f"ris sink  : {sink.path}")
    outcomes, _metrics = ingest_batch(items, workers=args.workers, ris_sink=sink)

    created = updated = errored = linked = notes = 0
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
            if notes_dir:
                notes_dir.mkdir(parents=True, exist_ok=True)
                note = write_stub_note(record, notes_dir, root.name)
                if note:
                    rel = note.relative_to(workspace).as_posix()
                    trellis.update_node(outcome.upsert.slug, file_path=rel)
                    notes += 1
                    print(f"         -> note     {rel}")
        else:
            updated += 1
            slug = outcome.upsert.slug if outcome.upsert else "?"
            print(f"[{i}/{len(records)}] {record.title[:70]}")
            print(f"         -> updated  slug={slug}  status={status}  edges={edges}")

    print()
    print(
        f"=== done: created={created} updated={updated} "
        f"errors={errored} edges_linked={linked} notes={notes} ==="
    )
    if args.archive:
        if errored:
            print(
                f"archive   : skipped ({errored} error(s)); files left in place to retry"
            )
        else:
            archive_files(ris_files, args.archive)
            print(f"archive   : moved {len(ris_files)} file(s) to {args.archive}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
