"""Semantic Scholar's indexed outgoing references."""

from __future__ import annotations

import logging
import os
from datetime import date
from urllib.parse import quote

import requests

from pipeline._http import S2_LIMITER, http_get
from pipeline._utils import bare_doi
from pipeline.citation_types import CitationItem, CitationResult, text, year

logger = logging.getLogger(__name__)


def parse(payload) -> CitationResult:
    result = CitationResult("semantic-scholar", date.today().isoformat(), [])
    details = result.providers.setdefault(result.source, {})
    if not isinstance(payload, dict):
        details["status"] = "failed"
        return result
    info = payload.get("citingPaperInfo") or {}
    pdf = info.get("openAccessPdf") or {} if isinstance(info, dict) else {}
    notice = text(pdf.get("disclaimer")) if isinstance(pdf, dict) else ""
    if notice:
        details["notice"] = notice
    data = payload.get("data")
    if not isinstance(data, list):
        details["status"] = (
            "withheld"
            if "references" in notice and "elided" in notice
            else "unavailable"
        )
        return result
    for entry in data:
        cited = entry.get("citedPaper") if isinstance(entry, dict) else None
        if not isinstance(cited, dict):
            continue
        external = cited.get("externalIds") or {}
        if not isinstance(external, dict):
            continue
        doi = bare_doi(text(external.get("DOI")))
        pmid = str(external["PubMed"]).strip() if external.get("PubMed") else None
        s2_id = text(cited.get("paperId")) or None
        title = text(cited.get("title"))
        if doi or pmid or s2_id or title:
            result.items.append(
                CitationItem(
                    doi, pmid, s2_id, title, year(cited.get("year")), [result.source]
                )
            )
    details["status"] = (
        "available" if result.items else "empty" if not data else "unavailable"
    )
    return result


def retrieve(doi: str) -> CitationResult:
    items: list[CitationItem] = []
    offset = 0
    headers = {"x-api-key": os.environ["S2_API_KEY"]} if os.getenv("S2_API_KEY") else {}
    while True:
        try:
            response = http_get(
                "https://api.semanticscholar.org/graph/v1/paper/"
                f"DOI:{quote(doi, safe='')}/references",
                params={
                    "fields": "title,externalIds,year,paperId",
                    "limit": 100,
                    "offset": offset,
                },
                headers=headers,
                limiter=S2_LIMITER,
                timeout=30,
            )
            payload = response.json()
        except (requests.RequestException, ValueError):
            logger.warning("Semantic Scholar references doi=%s failed", doi)
            return CitationResult(
                "semantic-scholar",
                date.today().isoformat(),
                items,
                {"semantic-scholar": {"status": "partial" if items else "failed"}},
            )
        result = parse(payload)
        items.extend(result.items)
        result.items = items
        status = result.providers[result.source]["status"]
        if status != "available":
            if items:
                result.providers[result.source]["status"] = "partial"
            return result
        next_offset = payload.get("next")
        if next_offset is None:
            return result
        if not isinstance(next_offset, int) or next_offset <= offset:
            result.providers[result.source]["status"] = "partial"
            return result
        offset = next_offset
