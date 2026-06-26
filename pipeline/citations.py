"""
citations.py - Outbound citation retrieval from Semantic Scholar.

LLM-independent. Returns raw structured data from the S2 API only.
No Trellis interaction.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import Optional

import requests

from pipeline._http import S2_LIMITER, http_get

logger = logging.getLogger(__name__)


@dataclass
class CitationItem:
    doi: Optional[str]
    pmid: Optional[str]
    s2_id: Optional[str]
    title: str
    year: Optional[int]


@dataclass
class CitationResult:
    source: str
    retrieved_at: str
    items: list[CitationItem]


def _empty_result(source: str = "semantic-scholar") -> CitationResult:
    return CitationResult(
        source=source, retrieved_at=date.today().isoformat(), items=[]
    )


def _normalize_doi(doi: Optional[str]) -> Optional[str]:
    if not doi:
        return None
    return doi.strip().lower() or None


def fetch_outbound_citations(doi: str) -> CitationResult:
    if not doi:
        return _empty_result()

    url = f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}/references"
    params = {"fields": "title,externalIds,year,paperId", "limit": 100}
    headers = {}
    api_key = os.getenv("S2_API_KEY")
    if api_key:
        headers["x-api-key"] = api_key

    try:
        response = http_get(
            url, params=params, headers=headers, limiter=S2_LIMITER, timeout=30
        )
        payload = response.json()
    except requests.RequestException as exc:
        logger.warning("fetch_outbound_citations doi=%s request failed: %s", doi, exc)
        return _empty_result("semantic-scholar-failed")
    except ValueError as exc:
        logger.warning(
            "fetch_outbound_citations doi=%s json decode failed: %s", doi, exc
        )
        return _empty_result("semantic-scholar-failed")

    if not isinstance(payload, dict):
        return _empty_result("semantic-scholar-failed")
    data = payload.get("data")
    if data is None:
        data = []
    if not isinstance(data, list):
        return _empty_result("semantic-scholar-failed")

    items: list[CitationItem] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        cited = (item or {}).get("citedPaper") or {}
        if not isinstance(cited, dict):
            continue
        external_ids = cited.get("externalIds") or {}
        title = (cited.get("title") or "").strip()
        cited_doi = _normalize_doi(external_ids.get("DOI"))
        pmid = external_ids.get("PubMed")
        if pmid is not None:
            pmid = str(pmid).strip() or None
        if cited_doi is None and not title:
            continue
        items.append(
            CitationItem(
                doi=cited_doi,
                pmid=pmid,
                s2_id=cited.get("paperId"),
                title=title,
                year=cited.get("year"),
            )
        )

    return CitationResult(
        source="semantic-scholar",
        retrieved_at=date.today().isoformat(),
        items=items,
    )
