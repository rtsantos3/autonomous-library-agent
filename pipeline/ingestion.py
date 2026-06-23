"""
ingestion.py - Single-reference ingestion pipeline orchestrator.

Phases:
  1. parse_input        - normalize raw input dict
  2. resolve_identity   - fill missing metadata from PubMed / S2
  3. find_existing      - dedup check in Trellis
  4. upsert_node        - create or merge-update reference node
  5. fetch_citations    - retrieve outbound citations from S2
  6. store_citations    - read-merge-write citation metadata onto node
  7. link_citations     - materialize edges for already-present targets
  8. verify_outcome     - confirm Trellis state post-run

LLM-independent. No description field written. No stub nodes created.
"""
from __future__ import annotations

import dataclasses
import json
import os
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Optional

import requests

from pipeline.citations import CitationResult, fetch_outbound_citations
from pipeline import trellis


@dataclass
class ParseResult:
    title: Optional[str]
    doi: Optional[str]
    pmid: Optional[str]
    abstract: Optional[str]
    authors: list
    year: Optional[str]
    venue: Optional[str]


@dataclass
class ResolveResult:
    title: str
    doi: Optional[str]
    pmid: Optional[str]
    s2_id: Optional[str]
    abstract: Optional[str]
    authors: list
    year: Optional[str]
    venue: Optional[str]
    alt_dois: list[str]
    source: str


@dataclass
class DedupResult:
    existing_node: Optional[dict]
    match_reason: Optional[str]


@dataclass
class UpsertResult:
    slug: str
    created: bool


@dataclass
class CitationStoreResult:
    stored: int


@dataclass
class LinkResult:
    linked: int
    skipped: int


@dataclass
class VerifyResult:
    node_exists: bool
    has_citation_metadata: bool
    pipeline_status: Optional[str]
    edge_count: int


@dataclass
class IngestionOutcome:
    parse: Optional[ParseResult] = None
    resolve: Optional[ResolveResult] = None
    dedup: Optional[DedupResult] = None
    upsert: Optional[UpsertResult] = None
    citation_store: Optional[CitationStoreResult] = None
    link: Optional[LinkResult] = None
    verify: Optional[VerifyResult] = None
    errors: list[str] = field(default_factory=list)


def _blank_to_none(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return value


def _bare_doi(value: Optional[str]) -> Optional[str]:
    value = _blank_to_none(value)
    if not value:
        return None
    lower = value.lower()
    for prefix in ("doi:", "https://doi.org/", "http://dx.doi.org/"):
        if lower.startswith(prefix):
            value = value[len(prefix):]
            break
    return value.strip().lower() or None


def _canonical_doi_uri(doi: Optional[str]) -> Optional[str]:
    return f"https://doi.org/{doi}" if doi else None


def _node_slug(node: dict) -> Optional[str]:
    return node.get("slug") or node.get("id") or node.get("uuid")


def _merge_missing(existing: dict, incoming: dict) -> dict:
    merged = dict(existing or {})
    for key, value in incoming.items():
        if value in (None, "", []):
            continue
        if merged.get(key) in (None, "", []):
            merged[key] = value
    return merged


def _make_tags(resolved: ResolveResult, existing_tags: Optional[list] = None) -> list[str]:
    tags = [t for t in (existing_tags or []) if not str(t).startswith("pipeline:")]
    tags.append("pipeline:scaffolded")
    if resolved.s2_id:
        tags.append(f"s2id:{resolved.s2_id}")
    if resolved.pmid:
        tags.append(f"pmid:{resolved.pmid}")
    if resolved.year:
        tags.append(f"year:{resolved.year}")
    return list(dict.fromkeys(tags))


def _reference_metadata(resolved: ResolveResult) -> dict:
    return {
        "schema": "reference-v1",
        "doi": resolved.doi,
        "pmid": resolved.pmid,
        "s2_id": resolved.s2_id,
        "year": resolved.year,
        "venue": resolved.venue,
        "authors": resolved.authors,
        "alt_dois": resolved.alt_dois,
    }


def _citation_string(resolved: ResolveResult) -> str:
    authors = "; ".join(resolved.authors or [])
    year = resolved.year or "n.d."
    parts = []
    if authors:
        parts.append(f"{authors} ({year}).")
    else:
        parts.append(f"({year}).")
    parts.append(resolved.title)
    if resolved.venue:
        parts.append(resolved.venue)
    return " ".join(parts)


def parse_input(raw: dict) -> ParseResult:
    title = _blank_to_none(raw.get("title"))
    doi = _bare_doi(raw.get("doi"))
    pmid = _blank_to_none(raw.get("pmid"))
    abstract = _blank_to_none(raw.get("abstract"))
    authors = raw.get("authors") or []
    year = _blank_to_none(raw.get("year"))
    venue = _blank_to_none(raw.get("venue"))

    if isinstance(authors, str):
        authors = [a.strip() for a in authors.split(";") if a.strip()]

    if not doi and not pmid and not (title and len(title.strip()) >= 10):
        raise ValueError("Provide a DOI, PMID, or title of at least 10 characters")

    return ParseResult(
        title=title,
        doi=doi,
        pmid=pmid,
        abstract=abstract,
        authors=authors,
        year=str(year) if year is not None else None,
        venue=venue,
    )


def _pubmed_search(parsed: ParseResult) -> Optional[str]:
    term = parsed.doi or parsed.pmid or parsed.title
    if not term:
        return None
    params = {"db": "pubmed", "term": term, "retmode": "json", "retmax": 1}
    api_key = os.getenv("NCBI_API_KEY")
    if api_key:
        params["api_key"] = api_key
    response = requests.get(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    ids = response.json().get("esearchresult", {}).get("idlist", [])
    return ids[0] if ids else None


def _pubmed_summary(pmid: str) -> dict:
    params = {"db": "pubmed", "id": pmid, "retmode": "json"}
    api_key = os.getenv("NCBI_API_KEY")
    if api_key:
        params["api_key"] = api_key
    response = requests.get(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi",
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json().get("result", {})
    return payload.get(pmid, {})


def _pubmed_abstract(pmid: str) -> Optional[str]:
    params = {"db": "pubmed", "id": pmid, "rettype": "abstract", "retmode": "text"}
    api_key = os.getenv("NCBI_API_KEY")
    if api_key:
        params["api_key"] = api_key
    response = requests.get(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi",
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    text = response.text.strip()
    return text or None


def _fill_from_pubmed(parsed: ParseResult, fields: dict) -> str:
    try:
        pmid = parsed.pmid or _pubmed_search(parsed)
        if not pmid:
            return fields["source"]
        summary = _pubmed_summary(pmid)
        article_ids = summary.get("articleids") or []
        doi = None
        for article_id in article_ids:
            if article_id.get("idtype") == "doi":
                doi = _bare_doi(article_id.get("value"))
                break
        authors = [a.get("name") for a in summary.get("authors") or [] if a.get("name")]
        pub_year = (summary.get("pubdate") or "").split(" ")[0] or None
        if not (pub_year and pub_year.isdigit() and len(pub_year) == 4):
            pub_year = None
        fields.update(
            _merge_missing(
                fields,
                {
                    "title": summary.get("title"),
                    "doi": doi,
                    "pmid": pmid,
                    "authors": authors,
                    "year": pub_year,
                    "venue": summary.get("fulljournalname") or summary.get("source"),
                },
            )
        )
        if not fields.get("abstract"):
            fields["abstract"] = _pubmed_abstract(pmid)
        return "pubmed"
    except requests.RequestException:
        return fields["source"]


def _fill_from_s2(fields: dict) -> str:
    doi = fields.get("doi")
    if not doi:
        return fields["source"]
    headers = {}
    api_key = os.getenv("S2_API_KEY")
    if api_key:
        headers["x-api-key"] = api_key
    try:
        response = requests.get(
            f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}",
            params={"fields": "paperId,title,abstract,authors,year,venue,externalIds"},
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException:
        return fields["source"]
    except ValueError:
        return fields["source"]

    external_ids = payload.get("externalIds") or {}
    authors = [a.get("name") for a in payload.get("authors") or [] if a.get("name")]
    fields.update(
        _merge_missing(
            fields,
            {
                "title": payload.get("title"),
                "doi": _bare_doi(external_ids.get("DOI")),
                "pmid": external_ids.get("PubMed"),
                "s2_id": payload.get("paperId"),
                "abstract": payload.get("abstract"),
                "authors": authors,
                "year": str(payload.get("year")) if payload.get("year") else None,
                "venue": payload.get("venue"),
            },
        )
    )
    return "semantic-scholar" if fields["source"] == "input-only" else fields["source"]


def resolve_identity(parsed: ParseResult) -> ResolveResult:
    fields = {
        "title": parsed.title,
        "doi": parsed.doi,
        "pmid": parsed.pmid,
        "s2_id": None,
        "abstract": parsed.abstract,
        "authors": parsed.authors,
        "year": parsed.year,
        "venue": parsed.venue,
        "alt_dois": [],
        "source": "input-only",
    }
    has_sufficient_title_only_metadata = (
        parsed.title
        and not parsed.doi
        and not parsed.pmid
        and parsed.abstract
        and parsed.authors
        and parsed.year
        and parsed.venue
    )
    if not has_sufficient_title_only_metadata:
        fields["source"] = _fill_from_pubmed(parsed, fields)
        fields["source"] = _fill_from_s2(fields)

    title = _blank_to_none(fields.get("title"))
    if not title:
        raise ValueError("Could not resolve a title for the reference")

    return ResolveResult(
        title=title,
        doi=_bare_doi(fields.get("doi")),
        pmid=_blank_to_none(fields.get("pmid")),
        s2_id=_blank_to_none(fields.get("s2_id")),
        abstract=_blank_to_none(fields.get("abstract")),
        authors=fields.get("authors") or [],
        year=str(fields["year"]) if fields.get("year") is not None else None,
        venue=_blank_to_none(fields.get("venue")),
        alt_dois=fields.get("alt_dois") or [],
        source=fields["source"],
    )


def find_existing(resolved: ResolveResult) -> DedupResult:
    if resolved.s2_id:
        node = trellis.find_by_s2id(resolved.s2_id)
        if node:
            return DedupResult(node, "s2_id")
    if resolved.doi:
        node = trellis.find_by_doi(resolved.doi)
        if node:
            return DedupResult(node, "doi")
    if resolved.pmid:
        node = trellis.find_by_pmid(resolved.pmid)
        if node:
            return DedupResult(node, "pmid")
    if resolved.title:
        node = trellis.find_by_title(resolved.title)
        if node:
            return DedupResult(node, "title")
    return DedupResult(None, None)


def upsert_node(resolved: ResolveResult, dedup: DedupResult) -> UpsertResult:
    metadata = {"reference": _reference_metadata(resolved)}
    tags = _make_tags(resolved)
    citation = _citation_string(resolved)
    today = date.today().isoformat()

    if dedup.existing_node is None:
        node = trellis.add_reference(
            resolved.title,
            uri=_canonical_doi_uri(resolved.doi),
            abstract=resolved.abstract,
            citation=citation,
            metadata=metadata,
            tags=tags,
        )
        slug = _node_slug(node)
        if not slug:
            raise RuntimeError("Trellis add did not return a slug")
        trellis.annotate_node(slug, f"[{today}] Created via ingestion pipeline; source: {resolved.source}")
        return UpsertResult(slug=slug, created=True)

    slug = _node_slug(dedup.existing_node)
    if not slug:
        raise RuntimeError("Existing Trellis node has no slug")
    node = trellis.get_node(slug)
    current_meta = dict(node.get("metadata") or {})
    current_ref = dict(current_meta.get("reference") or {})
    current_meta["reference"] = _merge_missing(current_ref, metadata["reference"])
    trellis.update_node(
        slug,
        metadata=current_meta,
        tags=_make_tags(resolved, node.get("tags") or []),
        citation=citation,
    )
    trellis.annotate_node(
        slug,
        f"[{today}] Dedup match on {dedup.match_reason}; merged metadata; source: {resolved.source}",
    )
    return UpsertResult(slug=slug, created=False)


def store_citations(slug: str, citations: CitationResult) -> CitationStoreResult:
    node = trellis.get_node(slug)
    current_meta = dict(node.get("metadata") or {})
    ref = dict(current_meta.get("reference") or {})
    ref["outbound_citations"] = {
        "source": citations.source,
        "retrieved_at": citations.retrieved_at,
        "items": [asdict(i) for i in citations.items],
    }
    current_meta["reference"] = ref
    trellis.update_node(slug, metadata=current_meta)
    return CitationStoreResult(stored=len(citations.items))


def link_citations(slug: str, citations: CitationResult) -> LinkResult:
    linked = 0
    skipped = 0
    for item in citations.items:
        target = trellis.dedup_check(s2id=item.s2_id, doi=item.doi, pmid=item.pmid, title=item.title)
        if not target:
            skipped += 1
            continue
        target_slug = _node_slug(target)
        if not target_slug:
            skipped += 1
            continue
        result = trellis.link_nodes(slug, target_slug, "references")
        if result.get("ok"):
            linked += 1
        else:
            skipped += 1
    return LinkResult(linked=linked, skipped=skipped)


def verify_outcome(slug: str) -> VerifyResult:
    try:
        node = trellis.get_node(slug)
        ref = ((node.get("metadata") or {}).get("reference") or {})
        items = (ref.get("outbound_citations") or {}).get("items")
        pipeline_status = None
        for tag in node.get("tags") or []:
            if str(tag).startswith("pipeline:"):
                pipeline_status = str(tag).split(":", 1)[1]
                break
        edge_count = 0
        for candidate in trellis.grep_nodes(slug):
            if candidate.get("relation") == "references" or candidate.get("type") == "references":
                edge_count += 1
        return VerifyResult(
            node_exists=True,
            has_citation_metadata=isinstance(items, list),
            pipeline_status=pipeline_status,
            edge_count=edge_count,
        )
    except Exception:
        return VerifyResult(
            node_exists=False,
            has_citation_metadata=False,
            pipeline_status=None,
            edge_count=0,
        )


def ingest_reference_pipeline(raw: dict) -> IngestionOutcome:
    outcome = IngestionOutcome()
    try:
        outcome.parse = parse_input(raw)
        outcome.resolve = resolve_identity(outcome.parse)
        outcome.dedup = find_existing(outcome.resolve)
        outcome.upsert = upsert_node(outcome.resolve, outcome.dedup)
        citations = fetch_outbound_citations(outcome.resolve.doi or "")
        outcome.citation_store = store_citations(outcome.upsert.slug, citations)
        outcome.link = link_citations(outcome.upsert.slug, citations)
        outcome.verify = verify_outcome(outcome.upsert.slug)
    except (ValueError, RuntimeError) as e:
        outcome.errors.append(str(e))
    return outcome


if __name__ == "__main__":
    import argparse

    from dotenv import load_dotenv

    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--doi")
    parser.add_argument("--pmid")
    parser.add_argument("--title")
    args = parser.parse_args()
    raw = {k: v for k, v in vars(args).items() if v}
    if not raw:
        parser.error("Provide at least one of --doi, --pmid, --title")
    outcome = ingest_reference_pipeline(raw)
    print(json.dumps(dataclasses.asdict(outcome), indent=2, default=str))
