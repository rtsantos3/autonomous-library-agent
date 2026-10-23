"""Retrieve outbound bibliography metadata from Europe PMC's reference index."""

from __future__ import annotations

from datetime import date
from urllib.parse import quote

import requests

from pipeline._http import EUROPE_PMC_LIMITER, http_get
from pipeline._utils import bare_doi
from pipeline.citation_types import CitationItem, CitationResult, text, year

SOURCE = "europe-pmc"
_BASE_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest"
_PAGE_SIZE = 1000
_MAX_PAGES = 1000
_SOURCES = {"AGR", "CBA", "CTX", "ETH", "HIR", "MED", "PAT", "PMC", "PPR"}


def _result(status: str, items: list[CitationItem] | None = None) -> CitationResult:
    return CitationResult(
        SOURCE, date.today().isoformat(), items or [], {SOURCE: {"status": status}}
    )


def _get_json(url: str, params: dict):
    response = http_get(url, params=params, limiter=EUROPE_PMC_LIMITER, timeout=30)
    try:
        return response.json()
    except ValueError as exc:
        raise requests.RequestException("Invalid Europe PMC JSON response") from exc


def _count(payload: dict) -> int | None:
    count = payload.get("hitCount")
    if count is None and "hitCount" not in payload:
        return None
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise ValueError("Invalid Europe PMC hitCount")
    return count


def _identifier(value) -> str:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return str(value)
    return text(value)


def select_record(payload, doi: str) -> dict | None:
    """Prefer PubMed among exact DOI hits that do not report missing references."""
    target = bare_doi(text(doi))
    results = payload.get("resultList") if isinstance(payload, dict) else None
    records = results.get("result") if isinstance(results, dict) else None
    if not target or not isinstance(records, list):
        return None
    selected = None
    for record in records:
        if not isinstance(record, dict):
            continue
        if bare_doi(text(record.get("doi"))) != target:
            continue
        if text(record.get("hasReferences")).upper() == "N":
            continue
        source = text(record.get("source")).upper()
        identifier = _identifier(record.get("id"))
        if source not in _SOURCES or not identifier:
            continue
        if source == "MED" and not (identifier.isascii() and identifier.isdigit()):
            continue
        if source == "PMC" and not (
            identifier.startswith("PMC")
            and identifier[3:].isascii()
            and identifier[3:].isdigit()
        ):
            continue
        if selected is None or source == "MED":
            selected = {**record, "source": source, "id": identifier}
        if source == "MED":
            break
    return selected


def _references(payload) -> list:
    if not isinstance(payload, dict):
        raise ValueError("Invalid Europe PMC reference response")
    count = _count(payload)
    if "referenceList" not in payload and count == 0:
        # The live API omits referenceList for a zero-hit reference response.
        return []
    references = payload.get("referenceList")
    entries = references.get("reference") if isinstance(references, dict) else None
    if not isinstance(entries, list):
        raise ValueError("Missing Europe PMC reference list")
    if count == 0 and entries:
        raise ValueError("Contradictory Europe PMC reference count")
    return entries


def parse_references(payload) -> CitationResult:
    """Parse cited records, never the requesting article or incoming citations."""
    try:
        entries = _references(payload)
    except ValueError:
        return _result("unavailable")
    items = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        doi = bare_doi(text(entry.get("doi")))
        identifier = _identifier(entry.get("id"))
        pmid = (
            identifier
            if text(entry.get("source")).upper() == "MED"
            and identifier.isascii()
            and identifier.isdigit()
            else None
        )
        title = text(entry.get("title"))
        if doi or pmid or title:
            items.append(
                CitationItem(
                    doi, pmid, None, title, year(entry.get("pubYear")), [SOURCE]
                )
            )
    count = _count(payload)
    status = "available" if items else "empty" if not entries else "unavailable"
    if not entries and count:
        status = "unavailable"
    return _result(status, items)


def next_reference_page(
    payload, page: int, records_seen: int, page_size: int = _PAGE_SIZE
) -> int | None:
    """Apply one-based reference pagination; reject premature empty pages."""
    entries = _references(payload)
    request = payload.get("request")
    if isinstance(request, dict) and "offSet" in request:
        offset = request["offSet"]
        if isinstance(offset, bool) or not isinstance(offset, int) or offset != page:
            raise ValueError("Europe PMC returned the wrong reference page")
    count = _count(payload)
    if count is not None:
        if records_seen > count:
            raise ValueError("Europe PMC reference count decreased")
        if records_seen == count:
            return None
        if not entries:
            raise ValueError("Europe PMC reference list ended before hitCount")
        return page + 1
    return page + 1 if len(entries) >= page_size else None


def retrieve(doi: str) -> CitationResult:
    """Resolve an exact DOI, then collect every indexed outbound reference page.

    Both endpoints return metadata only. Pagination is bounded at 1,000 pages;
    malformed/repeated pages and later request failures retain earlier records.
    """
    doi = bare_doi(text(doi))
    if not doi:
        return _result("unavailable")
    items: list[CitationItem] = []
    try:
        cursor = "*"
        seen_cursors = set()
        selected = None
        search_seen = 0
        for _ in range(_MAX_PAGES):
            payload = _get_json(
                f"{_BASE_URL}/search",
                {
                    "query": f'DOI:"{doi}"',
                    "format": "json",
                    "pageSize": _PAGE_SIZE,
                    "cursorMark": cursor,
                },
            )
            candidate = select_record(payload, doi)
            if candidate is not None:
                selected = candidate
                if candidate["source"] == "MED":
                    break
            results = payload.get("resultList") if isinstance(payload, dict) else None
            records = results.get("result") if isinstance(results, dict) else None
            if not isinstance(records, list):
                return _result("unavailable")
            count = _count(payload)
            search_seen += len(records)
            if count is not None and search_seen >= count:
                break
            next_cursor = text(payload.get("nextCursorMark"))
            if not next_cursor:
                if count is not None and search_seen < count:
                    return _result("unavailable")
                break
            if not records or next_cursor == cursor or next_cursor in seen_cursors:
                return _result("unavailable")
            seen_cursors.add(cursor)
            cursor = next_cursor
        else:
            return _result("unavailable")
        if selected is None:
            return _result("unavailable")

        records_seen = 0
        incomplete = False
        previous_entries = None
        source = quote(selected["source"], safe="")
        identifier = quote(selected["id"], safe="")
        for page in range(1, _MAX_PAGES + 1):
            payload = _get_json(
                f"{_BASE_URL}/{source}/{identifier}/references",
                {"format": "json", "page": page, "pageSize": _PAGE_SIZE},
            )
            parsed = parse_references(payload)
            entries = _references(payload)
            if entries and entries == previous_entries:
                return _result("partial" if items else "unavailable", items)
            previous_entries = entries
            next_page = next_reference_page(payload, page, records_seen + len(entries))
            items.extend(parsed.items)
            records_seen += len(entries)
            status = parsed.providers[SOURCE]["status"]
            incomplete = incomplete or status == "unavailable"
            if next_page is None:
                if items:
                    return _result("partial" if incomplete else "available", items)
                return _result("unavailable" if incomplete else "empty")
        return _result("partial" if items else "unavailable", items)
    except requests.RequestException:
        return _result("partial" if items else "failed", items)
    except ValueError:
        # Parsed content can be malformed even when the request succeeded.
        return _result("partial" if items else "unavailable", items)
