"""OpenAlex's indexed outgoing references, without abstract or full-text retrieval."""

from __future__ import annotations

import logging
import os
import re
from datetime import date
from urllib.parse import quote

import requests

from pipeline._http import OPENALEX_LIMITER, http_get
from pipeline._utils import bare_doi
from pipeline.citation_types import CitationItem, CitationResult, text, year

logger = logging.getLogger(__name__)

# OpenAlex documents at most 100 OR values and a supported page size of 100:
# https://help.openalex.org/api/filtering/
# https://help.openalex.org/api/paging/
# Each unique-ID batch therefore fits in one page; no cursor loop is needed.
_BATCH_SIZE = 100
_FIELDS = "id,doi,ids,title,publication_year"
_WORK_ID = re.compile(r"(?:https?://openalex\.org/)?(W[0-9]+)/?", re.IGNORECASE)
_PMID = re.compile(
    r"(?:https?://(?:pubmed\.ncbi\.nlm\.nih\.gov/|"
    r"(?:www\.)?ncbi\.nlm\.nih\.gov/pubmed/))?([0-9]+)/?",
    re.IGNORECASE,
)


def _work_id(value) -> str | None:
    match = _WORK_ID.fullmatch(text(value))
    return match.group(1).upper() if match else None


def _pmid(value) -> str | None:
    match = _PMID.fullmatch(text(value))
    return match.group(1) if match else None


def parse(payload, hydration_payloads=()) -> CitationResult:
    """Parse a citing work and optional /works batch responses, retaining misses.

    Only IDs in ``referenced_works`` establish outgoing edges. Missing or invalid
    hydration never removes those edges, and incoming citation counts do not
    establish them. Unhydrated targets are identifier-only, with partial status.
    """
    result = CitationResult("openalex", date.today().isoformat(), [])
    details = result.providers.setdefault(result.source, {})
    references = payload.get("referenced_works") if isinstance(payload, dict) else None
    if not isinstance(references, list):
        details["status"] = "unavailable"
        return result
    if not references:
        details["status"] = "empty"
        return result

    by_id: dict[str, CitationItem] = {}
    invalid_reference = False
    for reference in references:
        identifier = _work_id(reference)
        if identifier is None:
            invalid_reference = True
            continue
        if identifier not in by_id:
            by_id[identifier] = CitationItem(
                None, None, None, "", None, [result.source], openalex_id=identifier
            )
    result.items = list(by_id.values())
    if not result.items:
        details["status"] = "unavailable"
        return result

    hydrated = set()
    for response in hydration_payloads:
        records = response.get("results") if isinstance(response, dict) else None
        if not isinstance(records, list):
            continue
        for record in records:
            if not isinstance(record, dict):
                continue
            identifier = _work_id(record.get("id"))
            if identifier not in by_id:
                continue
            item = by_id[identifier]
            ids = record.get("ids")
            ids = ids if isinstance(ids, dict) else {}
            item.doi = (
                item.doi
                or bare_doi(text(record.get("doi")))
                or bare_doi(text(ids.get("doi")))
            )
            item.pmid = item.pmid or _pmid(ids.get("pmid"))
            item.title = item.title or text(record.get("title"))
            item.year = item.year or year(record.get("publication_year"))
            hydrated.add(identifier)
    details["status"] = (
        "partial" if invalid_reference or len(hydrated) < len(by_id) else "available"
    )
    return result


def retrieve(doi: str) -> CitationResult:
    """Resolve a citing DOI, then hydrate every distinct indexed reference ID."""
    params = {}
    api_key = os.getenv("OPENALEX_API_KEY")
    if api_key:
        params["api_key"] = api_key
    try:
        normalized_doi = bare_doi(text(doi))
        if not normalized_doi:
            raise ValueError("A DOI is required")
        response = http_get(
            "https://api.openalex.org/works/https://doi.org/"
            f"{quote(normalized_doi, safe='/')}",
            params={**params, "select": "id,referenced_works"},
            limiter=OPENALEX_LIMITER,
            timeout=30,
        )
        payload = response.json()
    except (requests.RequestException, ValueError):
        # Exception strings can include request URLs containing the API key.
        logger.warning("OpenAlex citing-work lookup failed")
        return CitationResult(
            "openalex", date.today().isoformat(), [], {"openalex": {"status": "failed"}}
        )

    result = parse(payload)
    if not result.items:
        return result
    identifiers = [item.openalex_id for item in result.items]
    hydration_payloads = []
    for start in range(0, len(identifiers), _BATCH_SIZE):
        batch = identifiers[start : start + _BATCH_SIZE]
        try:
            response = http_get(
                "https://api.openalex.org/works",
                params={
                    **params,
                    "filter": "openalex_id:" + "|".join(batch),
                    "select": _FIELDS,
                    "per_page": _BATCH_SIZE,
                    "page": 1,
                },
                limiter=OPENALEX_LIMITER,
                timeout=30,
            )
            hydration_payloads.append(response.json())
        except (requests.RequestException, ValueError):
            # Keep this batch's IDs and continue hydrating the remaining batches.
            logger.warning("OpenAlex reference hydration batch failed")
    return parse(payload, hydration_payloads)


def _abstract(inverted) -> str | None:
    """Rebuild an abstract from OpenAlex's word -> positions inverted index."""
    if not isinstance(inverted, dict):
        return None
    words = {}
    for word, positions in inverted.items():
        for position in positions if isinstance(positions, list) else []:
            if isinstance(position, int):
                words[position] = word
    return " ".join(words[i] for i in sorted(words)) or None


def parse_metadata(payload) -> dict | None:
    """Normalize an OpenAlex work to resolve_identity fields."""
    if not isinstance(payload, dict) or not text(payload.get("title")):
        return None
    authors = []
    for authorship in payload.get("authorships") or []:
        author = authorship.get("author") if isinstance(authorship, dict) else None
        name = text(author.get("display_name")) if isinstance(author, dict) else ""
        if name:
            authors.append(name)
    location = payload.get("primary_location")
    source = location.get("source") if isinstance(location, dict) else None
    published = year(payload.get("publication_year"))
    return {
        "title": text(payload.get("title")),
        "doi": bare_doi(text(payload.get("doi"))),
        "authors": authors,
        "year": str(published) if published else None,
        "venue": text(source.get("display_name")) if isinstance(source, dict) else None,
        "abstract": _abstract(payload.get("abstract_inverted_index")),
    }


def metadata(doi: str) -> dict | None:
    params = {}
    api_key = os.getenv("OPENALEX_API_KEY")
    if api_key:
        params["api_key"] = api_key
    try:
        normalized_doi = bare_doi(text(doi))
        if not normalized_doi:
            raise ValueError("A DOI is required")
        response = http_get(
            "https://api.openalex.org/works/https://doi.org/"
            f"{quote(normalized_doi, safe='/')}",
            params=params or None,
            limiter=OPENALEX_LIMITER,
            timeout=30,
        )
        return parse_metadata(response.json())
    except (requests.RequestException, ValueError):
        # Exception strings can include request URLs containing the API key.
        logger.warning("OpenAlex metadata lookup failed")
        return None
