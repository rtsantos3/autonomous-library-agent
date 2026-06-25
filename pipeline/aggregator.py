from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Optional

import requests

from pipeline._http import S2_LIMITER, http_post
from pipeline._utils import bare_doi, extend_unique
from pipeline.citations import CitationItem

BATCH_URL = "https://api.semanticscholar.org/graph/v1/paper/batch"
S2_BATCH_FIELDS = (
    "paperId,title,abstract,year,venue,authors,externalIds,fieldsOfStudy,"
    "s2FieldsOfStudy,publicationTypes,references.externalIds,references.title,references.year"
)
logger = logging.getLogger(__name__)


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


def _normalize_doi(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return str(value).strip().lower() or None


def _coerce_year(value) -> Optional[int]:
    if isinstance(value, int):
        return int(value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _citation_from_reference(ref: dict) -> Optional[CitationItem]:
    if not isinstance(ref, dict):
        return None
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
        year=_coerce_year((ref or {}).get("year")),
    )


def _build_resolved(entry: dict) -> BatchResolved:
    external_ids = entry.get("externalIds") or {}
    fields_of_study: list[str] = []
    extend_unique(fields_of_study, entry.get("fieldsOfStudy") or [])
    extend_unique(
        fields_of_study,
        [
            item.get("category")
            for item in entry.get("s2FieldsOfStudy") or []
            if isinstance(item, dict)
        ],
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
        pmid=(
            str(external_ids.get("PubMed")).strip()
            if external_ids.get("PubMed") is not None
            else None
        ),
        year=str(entry.get("year")) if entry.get("year") else None,
        venue=entry.get("venue"),
        authors=[a.get("name") for a in entry.get("authors") or [] if a.get("name")],
        fields_of_study=fields_of_study,
        publication_types=extend_unique([], entry.get("publicationTypes") or []),
        citations=citations,
    )


def _malformed_entry(entry: object) -> bool:
    if not isinstance(entry, dict):
        return True
    authors = entry.get("authors")
    if authors is not None and not isinstance(authors, list):
        return True
    if authors is not None and any(not isinstance(author, dict) for author in authors):
        return True
    references = entry.get("references")
    if references is not None and not isinstance(references, list):
        return True
    if references is not None and any(not isinstance(ref, dict) for ref in references):
        return True
    return False


def batch_resolve(dois: list[str], chunk_size: int = 500) -> dict[str, BatchResolved]:
    normalized: list[tuple[str, str]] = []
    for doi in dois:
        key = bare_doi(doi)
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

    chunks_attempted = 0
    chunks_failed = 0

    # One gated S2 batch call replaces N resolve+fetch calls, avoiding the
    # roughly 1/s S2 rate gate on per-paper lookups.
    for offset in range(0, len(normalized), chunk_size):
        chunks_attempted += 1
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
        except requests.RequestException as exc:
            chunks_failed += 1
            logger.warning(
                "s2 batch_resolve chunk offset %s request failed: %s", offset, exc
            )
            continue
        except ValueError as exc:
            chunks_failed += 1
            logger.warning(
                "s2 batch_resolve chunk offset %s json decode failed: %s", offset, exc
            )
            continue

        if isinstance(payload, list):
            entries = payload
        elif isinstance(payload, dict):
            entries = payload.get("data")
            if not isinstance(entries, list):
                chunks_failed += 1
                logger.warning(
                    "s2 batch_resolve chunk offset %s returned unusable dict payload",
                    offset,
                )
                continue
        else:
            chunks_failed += 1
            logger.warning(
                "s2 batch_resolve chunk offset %s returned unusable payload type %s",
                offset,
                type(payload).__name__,
            )
            continue

        # S2's batch endpoint is normally a top-level list aligned to input ids;
        # wrapped/non-list payload handling is defensive so entries are never mis-associated.
        for (input_key, _doi), entry in zip(chunk, entries or []):
            if not entry:
                continue
            if _malformed_entry(entry):
                continue
            resolved = _build_resolved(entry)
            result[input_key] = resolved
            if resolved.doi:
                result[resolved.doi.lower()] = resolved

    resolved_input_keys = {key for key, _doi in normalized if key in result}
    logger.info(
        "s2 batch_resolve summary: input_dois=%s resolved=%s chunks_attempted=%s chunks_failed=%s",
        len(normalized),
        len(resolved_input_keys),
        chunks_attempted,
        chunks_failed,
    )
    return result
