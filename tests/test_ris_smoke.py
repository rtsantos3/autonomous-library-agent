#!/usr/bin/env python3
"""
Smoke test for the RIS import path.

This is intentionally small and integration-style:
1. Parse one RIS record from the LAD export.
2. Add it to Trellis with the global `trellis` CLI.
3. Confirm the node was created.
4. Remove it again so the graph stays clean.

Run:
    python tests/test_ris_smoke.py
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RIS_PATH = PROJECT_ROOT / "data" / "endnote-extracted" / "PDF" / "2086963638" / "4690500citation.ris"
TRELLIS = "trellis"
PARENT = "microbiome-research-library"
ACTOR_ID = "daedalus"


@dataclass
class RisRecord:
    title: str
    abstract: str
    doi: str
    year: str
    venue: str
    authors: list[str]
    url: str


def run_trellis(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [TRELLIS, *args],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=30,
    )


def parse_ris(path: Path) -> RisRecord:
    text = path.read_text(encoding="utf-8", errors="replace")

    def first(tag: str) -> str:
        match = re.search(rf"^{tag}  - (.*)$", text, flags=re.M)
        return match.group(1).strip() if match else ""

    def all_vals(tag: str) -> list[str]:
        return [m.group(1).strip() for m in re.finditer(rf"^{tag}  - (.*)$", text, flags=re.M)]

    doi = first("DO") or first("M3") or first("UR")
    doi = re.sub(r"^doi:\s*", "", doi, flags=re.I)
    doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi, flags=re.I).strip().rstrip(" .;,")

    year = ""
    for tag in ("PY", "Y1", "DA"):
        raw = first(tag)
        if raw:
            match = re.search(r"(19|20)\d{2}", raw)
            if match:
                year = match.group(0)
                break

    return RisRecord(
        title=first("T1") or first("TI"),
        abstract=first("AB") or first("N2"),
        doi=doi,
        year=year,
        venue=first("JO") or first("T2") or first("J2"),
        authors=all_vals("AU"),
        url=first("UR"),
    )


def make_metadata(record: RisRecord) -> str:
    return json.dumps(
        {
            "reference": {
                "schema": "reference-v1",
                "title": record.title,
                "doi": record.doi,
                "year": record.year,
                "venue": record.venue,
                "authors": record.authors,
                "url": record.url,
            }
        },
        ensure_ascii=False,
    )


def create_reference(record: RisRecord) -> dict:
    args = [
        "add",
        "reference",
        record.title,
        "--abstract",
        record.abstract,
        "--metadata",
        make_metadata(record),
        "--tags",
        "pipeline:scaffolded,source:ris,depth:0",
        "--parent",
        PARENT,
        "--actor-id",
        ACTOR_ID,
        "--json",
    ]
    result = run_trellis(*args)
    if result.returncode != 0:
        raise RuntimeError(f"trellis add failed:\n{result.stderr.strip()}")
    return json.loads(result.stdout)


def remove_reference(uuid: str) -> None:
    result = run_trellis("rm", "--uuid", uuid, "--force", "--json")
    if result.returncode != 0:
        raise RuntimeError(f"trellis rm failed:\n{result.stderr.strip()}")


def purged_removed_reference(uuid: str) -> None:
    result = run_trellis("purge", "--uuid", uuid, "--force", "--json")
    if result.returncode != 0:
        raise RuntimeError(f"trellis purge failed:\n{result.stderr.strip()}")


def assert_contains_reference(uuid: str, doi: str, title: str) -> None:
    result = run_trellis("get", "--uuid", uuid, "--json")
    if result.returncode != 0:
        raise RuntimeError(f"trellis get failed:\n{result.stderr.strip()}")
    payload = json.loads(result.stdout)
    node = payload.get("node", payload)
    if node.get("id") != uuid:
        raise AssertionError(f"Expected node id {uuid}, got {node.get('id')}")
    if str(node.get("type", "")).lower() != "reference":
        raise AssertionError(f"Expected reference node, got {node.get('type')}")
    if str(node.get("title", "")) != title:
        raise AssertionError("Created node title mismatch")
    uri = (node.get("uri") or "").strip().lower()
    if uri not in {
        f"doi:{doi.lower()}",
        f"https://doi.org/{doi.lower()}",
        f"http://dx.doi.org/{doi.lower()}",
    }:
        raise AssertionError(f"Created node URI mismatch: {uri}")


def main() -> int:
    if not RIS_PATH.exists():
        print(f"Missing RIS file: {RIS_PATH}", file=sys.stderr)
        return 1

    record = parse_ris(RIS_PATH)
    if not record.title or not record.doi:
        print("Smoke test requires a RIS record with title and DOI.", file=sys.stderr)
        return 1

    before = run_trellis("find", "--text", f"https://doi.org/{record.doi}", "--json")
    if before.returncode != 0:
        print(before.stderr.strip(), file=sys.stderr)
        return 1

    created = create_reference(record)
    node = created.get("node", created)
    uuid = node.get("id")
    slug = node.get("slug")
    if not uuid:
        print("Trellis did not return a UUID for the created node.", file=sys.stderr)
        return 1

    try:
        assert_contains_reference(uuid, record.doi, record.title)
        print(f"Created reference: {slug} ({uuid})")
    finally:
        remove_reference(uuid)
        purged_removed_reference(uuid)
        print(f"Removed reference: {slug} ({uuid})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
