from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Optional

import requests

from pipeline._http import S2_LIMITER, http_post
from pipeline.citations import CitationItem


BATCH_URL = "https://api.semanticscholar.org/graph/v1/paper/batch"
S2_BATCH_FIELDS = (
    "paperId,title,abstract,year,venue,authors,externalIds,fieldsOfStudy,"
    "s2FieldsOfStudy,publicationTypes,references.externalIds,references.title,references.year"
)


def _slug(text):
    if text is None:
        return None
    slug = re.sub(r"[^0-9a-z]+", "-", str(text).strip().lower()).strip("-")
    return slug or None


@dataclass
class BatchResolved:
    doi: Optional[str]
    s2_id: Optional[str]
    title: Optional[str]
    abstract: Optional[str]
    pmid: Optional[str]
    year: Optional[str]
    venue: Optional[str]
    authors: list[str] = field(default_factory=list)
    fields_of_study: list[str] = field(default_factory=list)
    publication_types: list[str] = field(default_factory=list)
    citations: list[CitationItem] = field(default_factory=list)


def _extend_unique(values: list[str], incoming) -> list[str]:
    seen = set(values)
    for value in incoming or []:
        if value in (None, "", []):
            continue
        text = str(value).strip()
        if text and text not in seen:
            values.append(text)
            seen.add(text)
    return values


def _normalize_doi(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return str(value).strip().lower() or None


def _citation_from_reference(ref: dict) -> Optional[CitationItem]:
    external_ids = (ref or {}).get("externalIds") or {}
    doi = _normalize_doi(external_ids.get("DOI"))
    pmid = external_ids.get("PubMed")
    if pmid is not None:
        pmid = str(pmid).strip() or None
    title = ((ref or {}).get("title") or "").strip()
    if doi is None and not title:
        return None
    return CitationItem(
        doi=doi,
        pmid=pmid,
        s2_id=None,
        title=title,
        year=(ref or {}).get("year"),
    )


def _build_resolved(entry: dict) -> BatchResolved:
    external_ids = entry.get("externalIds") or {}
    fields_of_study: list[str] = []
    _extend_unique(fields_of_study, entry.get("fieldsOfStudy") or [])
    _extend_unique(
        fields_of_study,
        [item.get("category") for item in entry.get("s2FieldsOfStudy") or [] if isinstance(item, dict)],
    )
    citations = []
    for ref in entry.get("references") or []:
        item = _citation_from_reference(ref)
        if item is not None:
            citations.append(item)
    return BatchResolved(
        doi=_normalize_doi(external_ids.get("DOI")),
        s2_id=entry.get("paperId"),
        title=entry.get("title"),
        abstract=entry.get("abstract"),
        pmid=str(external_ids.get("PubMed")).strip() if external_ids.get("PubMed") is not None else None,
        year=str(entry.get("year")) if entry.get("year") else None,
        venue=entry.get("venue"),
        authors=[a.get("name") for a in entry.get("authors") or [] if a.get("name")],
        fields_of_study=fields_of_study,
        publication_types=_extend_unique([], entry.get("publicationTypes") or []),
        citations=citations,
    )


def batch_resolve(dois: list[str], chunk_size: int = 500) -> dict[str, BatchResolved]:
    # Lazy import avoids a module cycle: ingestion uses this aggregator for batch
    # prefetching, while the aggregator needs ingestion's canonical DOI cleanup.
    from pipeline.ingestion import _bare_doi

    normalized: list[tuple[str, str]] = []
    for doi in dois:
        key = _bare_doi(doi)
        if key:
            normalized.append((key.lower(), key))

    result: dict[str, BatchResolved] = {}
    if not normalized:
        return result

    chunk_size = min(max(int(chunk_size or 500), 1), 500)
    headers = {}
    api_key = os.getenv("S2_API_KEY")
    if api_key:
        headers["x-api-key"] = api_key

    # One gated S2 batch call replaces N resolve+fetch calls, avoiding the
    # roughly 1/s S2 rate gate on per-paper lookups.
    for offset in range(0, len(normalized), chunk_size):
        chunk = normalized[offset : offset + chunk_size]
        chunk_ids = [f"DOI:{doi}" for _key, doi in chunk]
        try:
            resp = http_post(
                BATCH_URL,
                params={"fields": S2_BATCH_FIELDS},
                json_body={"ids": chunk_ids},
                headers=headers,
                limiter=S2_LIMITER,
                timeout=30,
            )
            payload = resp.json()
        except requests.RequestException:
            continue
        except ValueError:
            continue

        for (input_key, _doi), entry in zip(chunk, payload or []):
            if not entry:
                continue
            resolved = _build_resolved(entry)
            result[input_key] = resolved
            if resolved.doi:
                result[resolved.doi.lower()] = resolved

    return result
