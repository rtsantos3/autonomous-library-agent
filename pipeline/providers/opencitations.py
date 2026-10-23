"""Outgoing indexed references from OpenCitations, without metadata hydration."""

from __future__ import annotations

import os
import re
from datetime import date
from urllib.parse import quote

import requests

from pipeline._http import OPENCITATIONS_LIMITER, http_get
from pipeline._utils import bare_doi
from pipeline.citation_types import CitationItem, CitationResult, text

SOURCE = "opencitations"
REFERENCES_URL = "https://api.opencitations.net/index/v2/references/doi:"
_DOI = re.compile(r"10\.[0-9]{4,9}/\S+")
_PMID = re.compile(r"[0-9]+")
_OMID = re.compile(r"br/[0-9]+")


def _identifiers(value) -> dict[str, list[str]]:
    identifiers: dict[str, list[str]] = {"doi": [], "pmid": [], "omid": []}
    for token in text(value).split():
        scheme, separator, identifier = token.partition(":")
        scheme = scheme.lower()
        if not separator or scheme not in identifiers:
            continue
        if scheme == "doi":
            identifier = bare_doi(identifier)
            valid = identifier is not None and _DOI.fullmatch(identifier)
        elif scheme == "pmid":
            valid = _PMID.fullmatch(identifier)
        else:
            identifier = identifier.lower()
            valid = _OMID.fullmatch(identifier)
        if valid and identifier not in identifiers[scheme]:
            identifiers[scheme].append(identifier)
    return identifiers


def _items(value) -> list[CitationItem]:
    identifiers = _identifiers(value)
    if not any(identifiers.values()):
        return []

    def item(doi=None, pmid=None, omid=None):
        return CitationItem(
            doi=doi,
            pmid=pmid,
            s2_id=None,
            title="",
            year=None,
            sources=[SOURCE],
            omid=omid,
        )

    if any(len(values) > 1 for values in identifiers.values()):
        # The index can put a preprint DOI and a publication DOI in one row.
        # Shared secondary IDs would collapse those distinct DOIs in the union.
        # Preserve every identifier independently rather than guessing mappings.
        return [
            item(**{scheme: identifier})
            for scheme, values in identifiers.items()
            for identifier in values
        ]
    return [
        item(**{scheme: values[0] for scheme, values in identifiers.items() if values})
    ]


def parse_references(payload) -> CitationResult:
    """Parse only cited PIDs; creation is the citing work's publication date.

    Conflicting identifiers of the same kind become separate ID-only records.
    Empty arrays establish an empty bibliography; malformed rows never do.
    """
    result = CitationResult(
        SOURCE, date.today().isoformat(), [], {SOURCE: {"status": "unavailable"}}
    )
    if not isinstance(payload, list):
        return result
    if not payload:
        result.providers[SOURCE]["status"] = "empty"
        return result

    malformed = False
    for row in payload:
        items = _items(row.get("cited")) if isinstance(row, dict) else []
        if not items:
            malformed = True
        result.items.extend(items)
    if result.items:
        result.providers[SOURCE]["status"] = "partial" if malformed else "available"
    return result


def retrieve(doi: str) -> CitationResult:
    """Retrieve the outgoing citation index, isolating expected request failures."""
    doi = bare_doi(text(doi))
    if doi is None or not _DOI.fullmatch(doi):
        return parse_references(None)
    token = os.getenv("OPENCITATIONS_ACCESS_TOKEN")
    headers = {"authorization": token} if token else {}
    try:
        response = http_get(
            f"{REFERENCES_URL}{quote(doi, safe='')}",
            headers=headers,
            limiter=OPENCITATIONS_LIMITER,
            timeout=30,
        )
        return parse_references(response.json())
    except (requests.RequestException, ValueError):
        return CitationResult(
            SOURCE, date.today().isoformat(), [], {SOURCE: {"status": "failed"}}
        )
