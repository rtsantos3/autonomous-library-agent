"""Collect and deduplicate outbound references without mutating Trellis."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field, replace
from datetime import date
from typing import Optional
from urllib.parse import quote

import requests

from pipeline._http import CROSSREF_LIMITER, S2_LIMITER, http_get
from pipeline._utils import bare_doi, extend_unique

logger = logging.getLogger(__name__)


@dataclass
class CitationItem:
    doi: Optional[str]
    pmid: Optional[str]
    s2_id: Optional[str]
    title: str
    year: Optional[int]
    sources: list[str] = field(default_factory=list)


@dataclass
class CitationResult:
    source: str
    retrieved_at: str
    items: list[CitationItem]
    providers: dict[str, dict] = field(default_factory=dict)


def _text(value) -> str:
    return value.strip() if isinstance(value, str) else ""


def _year(value) -> Optional[int]:
    if isinstance(value, (int, str)) and str(value).isdigit():
        return int(value)
    return None


def _parse_s2(payload) -> CitationResult:
    result = CitationResult("semantic-scholar", date.today().isoformat(), [])
    details = result.providers.setdefault(result.source, {})
    if not isinstance(payload, dict):
        details["status"] = "failed"
        return result
    info = payload.get("citingPaperInfo") or {}
    pdf = info.get("openAccessPdf") or {} if isinstance(info, dict) else {}
    notice = _text(pdf.get("disclaimer")) if isinstance(pdf, dict) else ""
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
        doi = bare_doi(_text(external.get("DOI")))
        pmid = str(external["PubMed"]).strip() if external.get("PubMed") else None
        s2_id = _text(cited.get("paperId")) or None
        title = _text(cited.get("title"))
        if doi or pmid or s2_id or title:
            result.items.append(
                CitationItem(
                    doi, pmid, s2_id, title, _year(cited.get("year")), [result.source]
                )
            )
    details["status"] = (
        "available" if result.items else "empty" if not data else "unavailable"
    )
    return result


def _fetch_s2(doi: str) -> CitationResult:
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
        except (requests.RequestException, ValueError) as exc:
            logger.warning("Semantic Scholar references doi=%s failed: %s", doi, exc)
            return CitationResult(
                "semantic-scholar",
                date.today().isoformat(),
                items,
                {"semantic-scholar": {"status": "partial" if items else "failed"}},
            )
        result = _parse_s2(payload)
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


def _parse_crossref(payload) -> CitationResult:
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
        doi = bare_doi(_text(entry.get("DOI")))
        title = _text(entry.get("article-title"))
        # Unstructured bibliography strings are not titles. Passing them to the
        # existing title matcher can attach a citation to the wrong paper.
        if doi or title:
            result.items.append(
                CitationItem(
                    doi, None, None, title, _year(entry.get("year")), [result.source]
                )
            )
    details["status"] = (
        "available" if result.items else "empty" if not references else "unavailable"
    )
    return result


def _fetch_crossref(doi: str) -> CitationResult:
    try:
        response = http_get(
            f"https://api.crossref.org/works/{quote(doi, safe='')}",
            params=(
                {"mailto": os.getenv("CROSSREF_MAILTO")}
                if os.getenv("CROSSREF_MAILTO")
                else None
            ),
            limiter=CROSSREF_LIMITER,
            timeout=30,
        )
        return _parse_crossref(response.json())
    except (requests.RequestException, ValueError) as exc:
        logger.warning("Crossref references doi=%s failed: %s", doi, exc)
        return CitationResult(
            "crossref", date.today().isoformat(), [], {"crossref": {"status": "failed"}}
        )


def merge_citations(*results: CitationResult) -> CitationResult:
    """Union references by identifier; use only unambiguous exact-title matches."""
    items: list[CitationItem] = []
    parents: list[int] = []

    def root(idx: int) -> int:
        while parents[idx] != idx:
            parents[idx] = parents[parents[idx]]
            idx = parents[idx]
        return idx

    def enrich(target: CitationItem, incoming: CitationItem) -> None:
        for key in ("doi", "pmid", "s2_id", "title", "year"):
            if not getattr(target, key):
                setattr(target, key, getattr(incoming, key))
        extend_unique(target.sources, incoming.sources)

    by_id: dict[tuple[str, str], int] = {}
    by_title: dict[str, list[int]] = {}
    providers: dict[str, dict] = {}
    sources: list[str] = []
    for result in results:
        providers.update(result.providers)
        for original in result.items:
            item = replace(
                original,
                doi=bare_doi(original.doi),
                sources=list(original.sources or result.source.split("+")),
            )
            if not any((item.doi, item.pmid, item.s2_id, item.title)):
                continue
            extend_unique(sources, item.sources)
            keys = [
                (key, value)
                for key in ("doi", "pmid", "s2_id")
                if (value := getattr(item, key))
            ]
            matches = {root(by_id[key]) for key in keys if key in by_id}
            title = " ".join(item.title.casefold().split())
            if not matches and title:
                matches = {
                    root(idx)
                    for idx in by_title.get(title, [])
                    if not (item.doi and items[root(idx)].doi)
                    and all(
                        not getattr(item, key)
                        or not getattr(items[root(idx)], key)
                        or getattr(item, key) == getattr(items[root(idx)], key)
                        for key in ("pmid", "s2_id")
                    )
                }
                if len(matches) != 1:
                    matches.clear()
            dois = {items[idx].doi for idx in matches if items[idx].doi}
            if item.doi:
                dois.add(item.doi)
            if len(dois) > 1:
                matches.clear()
            if not matches:
                idx = len(items)
                parents.append(idx)
                items.append(item)
            else:
                idx = min(matches)
                for other in sorted(matches - {idx}):
                    enrich(items[idx], items[other])
                    parents[other] = idx
                enrich(items[idx], item)
            for key in keys:
                by_id[key] = idx
            if title and idx not in by_title.setdefault(title, []):
                by_title[title].append(idx)
    return CitationResult(
        "+".join(sources) or "+".join(providers),
        date.today().isoformat(),
        [item for idx, item in enumerate(items) if root(idx) == idx],
        providers,
    )


def fetch_outbound_citations(
    doi: str,
    prefetched: Optional[list[CitationItem]] = None,
) -> CitationResult:
    doi = bare_doi(doi)
    if not doi:
        return CitationResult(
            "semantic-scholar+crossref",
            date.today().isoformat(),
            [],
            {
                source: {"status": "unavailable"}
                for source in ("semantic-scholar", "crossref")
            },
        )
    if prefetched:
        s2 = CitationResult(
            "semantic-scholar",
            date.today().isoformat(),
            prefetched,
            {"semantic-scholar": {"status": "available"}},
        )
    else:
        s2 = _fetch_s2(doi)
    return merge_citations(s2, _fetch_crossref(doi))
