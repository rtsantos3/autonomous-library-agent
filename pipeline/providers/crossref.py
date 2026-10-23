"""Crossref's deposited outgoing references."""

from __future__ import annotations

import logging
import os
from datetime import date
from urllib.parse import quote

import requests

from pipeline._http import CROSSREF_LIMITER, http_get
from pipeline._utils import bare_doi
from pipeline.citation_types import CitationItem, CitationResult, text, year

logger = logging.getLogger(__name__)


def parse(payload) -> CitationResult:
    result = CitationResult("crossref", date.today().isoformat(), [])
    details = result.providers.setdefault(result.source, {})
    message = payload.get("message") if isinstance(payload, dict) else None
    if not isinstance(message, dict):
        details["status"] = "failed"
        return result
    references = message.get("reference")
    if not isinstance(references, list):
        details["status"] = "unavailable"
        return result
    for entry in references:
        if not isinstance(entry, dict):
            continue
        doi = bare_doi(text(entry.get("DOI")))
        title = text(entry.get("article-title"))
        # Raw bibliography strings are not titles and can produce false graph links.
        if doi or title:
            result.items.append(
                CitationItem(
                    doi, None, None, title, year(entry.get("year")), [result.source]
                )
            )
    details["status"] = (
        "available" if result.items else "empty" if not references else "unavailable"
    )
    return result


def retrieve(doi: str) -> CitationResult:
    email = os.getenv("CROSSREF_EMAIL") or os.getenv("CROSSREF_MAILTO")
    try:
        response = http_get(
            f"https://api.crossref.org/works/{quote(doi, safe='')}",
            params={"mailto": email} if email else None,
            limiter=CROSSREF_LIMITER,
            timeout=30,
        )
        return parse(response.json())
    except (requests.RequestException, ValueError):
        logger.warning("Crossref references doi=%s failed", doi)
        return CitationResult(
            "crossref", date.today().isoformat(), [], {"crossref": {"status": "failed"}}
        )
