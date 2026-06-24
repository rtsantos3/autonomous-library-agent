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
import concurrent.futures
import json
import logging
import os
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Optional, TYPE_CHECKING
import xml.etree.ElementTree as ET

import requests

from pipeline._utils import bare_doi, extend_unique, slugify
from pipeline.citations import CitationResult, fetch_outbound_citations
from pipeline._http import http_get, NCBI_LIMITER, S2_LIMITER, CROSSREF_LIMITER
from pipeline import trellis

if TYPE_CHECKING:
    from pipeline.aggregator import BatchResolved

logger = logging.getLogger(__name__)


@dataclass
class PhaseMetrics:
    name: str
    wall_seconds: float
    per_item_seconds: float
    items: int


@dataclass
class BatchMetrics:
    phases: list[PhaseMetrics]
    total_seconds: float
    workers: int
    node_count_at_index: int


@dataclass
class BackfillResult:
    candidates: int
    resolvable: int
    processed: int
    skipped_no_doi: int
    skipped_already_tagged: int
    edges_linked: int = 0
    citations_stored: int = 0
    errors: list[str] = field(default_factory=list)


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
    fields_of_study: list[str] = field(default_factory=list)
    publication_types: list[str] = field(default_factory=list)
    mesh_terms: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    mesh_major: list[str] = field(default_factory=list)
    mesh_qualifiers: list[str] = field(default_factory=list)


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


def _prefer_canonical_doi(fields: dict, candidate_doi: Optional[str]) -> None:
    # S2 normalizes to the registered/canonical DOI. When it differs from the
    # input DOI, promote the canonical one and retain the original as an alt so
    # dedup still matches records ingested under the older identifier. Unlike
    # _merge_missing this intentionally overwrites an already-populated DOI.
    canonical = bare_doi(candidate_doi)
    if not canonical or canonical == fields.get("doi"):
        return
    if fields.get("doi"):
        extend_unique(fields.setdefault("alt_dois", []), [fields["doi"]])
    fields["doi"] = canonical


def _make_tags(resolved: ResolveResult, existing_tags: Optional[list] = None) -> list[str]:
    tags = [t for t in (existing_tags or []) if not str(t).startswith("pipeline:")]
    tags.append("pipeline:scaffolded")
    if resolved.s2_id:
        tags.append(f"s2id:{resolved.s2_id}")
    if resolved.pmid:
        tags.append(f"pmid:{resolved.pmid}")
    if resolved.year:
        tags.append(f"year:{resolved.year}")
    for value in resolved.fields_of_study:
        slug = slugify(value)
        if slug:
            tags.append(f"field:{slug}")
    for value in resolved.publication_types:
        slug = slugify(value)
        if slug:
            tags.append(f"type:{slug}")
    for value in resolved.mesh_terms:
        slug = slugify(value)
        if slug:
            tags.append(f"mesh:{slug}")
    for value in resolved.keywords:
        slug = slugify(value)
        if slug:
            tags.append(f"kw:{slug}")
    for value in resolved.mesh_major:
        slug = slugify(value)
        if slug:
            tags.append(f"mesh-major:{slug}")
    for value in resolved.mesh_qualifiers:
        slug = slugify(value)
        if slug:
            tags.append(f"mesh-q:{slug}")
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
    parts = []
    # Omit the year segment entirely when the year is unknown rather than
    # emitting a placeholder like "(n.d.)".
    if resolved.year and authors:
        parts.append(f"{authors} ({resolved.year}).")
    elif resolved.year:
        parts.append(f"({resolved.year}).")
    elif authors:
        parts.append(f"{authors}.")
    parts.append(resolved.title)
    if resolved.venue:
        parts.append(resolved.venue)
    return " ".join(parts)


def parse_input(raw: dict) -> ParseResult:
    title = _blank_to_none(raw.get("title"))
    doi = bare_doi(raw.get("doi"))
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
    response = http_get(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
        params=params,
        limiter=NCBI_LIMITER,
        timeout=30,
    )
    ids = response.json().get("esearchresult", {}).get("idlist", [])
    return ids[0] if ids else None


def _element_text(element: Optional[ET.Element]) -> Optional[str]:
    if element is None:
        return None
    text = " ".join("".join(element.itertext()).split())
    return text or None


def _pubmed_fetch(pmid: str) -> dict:
    params = {"db": "pubmed", "id": pmid, "rettype": "abstract", "retmode": "xml"}
    api_key = os.getenv("NCBI_API_KEY")
    if api_key:
        params["api_key"] = api_key
    response = http_get(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi",
        params=params,
        limiter=NCBI_LIMITER,
        timeout=30,
    )
    root = ET.fromstring(response.text)

    abstract_parts = [
        text for text in (_element_text(element) for element in root.findall(".//AbstractText")) if text
    ]
    authors = []
    for author in root.findall(".//Author"):
        last_name = _element_text(author.find("LastName"))
        fore_name = _element_text(author.find("ForeName")) or _element_text(author.find("Initials"))
        if last_name and fore_name:
            authors.append(f"{last_name} {fore_name}")
        elif last_name:
            authors.append(last_name)

    year = _element_text(root.find(".//PubDate/Year"))
    if not year:
        medline_date = _element_text(root.find(".//PubDate/MedlineDate"))
        year = medline_date[:4] if medline_date else None
    if not (year and year.isdigit() and len(year) == 4):
        year = None

    doi = None
    for article_id in root.findall(".//ArticleId"):
        if (article_id.get("IdType") or "").lower() == "doi":
            doi = bare_doi(_element_text(article_id))
            break

    fetched_pmid = None
    for pmid_element in root.findall(".//PMID"):
        if pmid_element.get("Version") == "1":
            fetched_pmid = _element_text(pmid_element)
            break
    if not fetched_pmid:
        fetched_pmid = _element_text(root.find(".//PMID"))

    mesh = [
        text
        for text in (_element_text(element) for element in root.findall(".//MeshHeading/DescriptorName"))
        if text
    ]
    keywords = [
        text
        for text in (_element_text(element) for element in root.findall(".//KeywordList/Keyword"))
        if text
    ]
    mesh_major = []
    # PubMed marks major topics on either the descriptor itself or a qualifier
    # inside the same MeshHeading, so inspect each heading instead of only the
    # flat DescriptorName path used for the broader mesh axis.
    for heading in root.findall(".//MeshHeading"):
        descriptor = heading.find("DescriptorName")
        descriptor_text = _element_text(descriptor)
        descriptor_major = descriptor is not None and descriptor.get("MajorTopicYN") == "Y"
        qualifier_major = any(
            qualifier.get("MajorTopicYN") == "Y" for qualifier in heading.findall("QualifierName")
        )
        if descriptor_text and (descriptor_major or qualifier_major):
            mesh_major.append(descriptor_text)
    mesh_qualifiers = [
        text
        for text in (_element_text(element) for element in root.findall(".//MeshHeading/QualifierName"))
        if text
    ]
    publication_types = [
        text
        for text in (_element_text(element) for element in root.findall(".//PublicationType"))
        if text
    ]

    return {
        "title": _element_text(root.find(".//ArticleTitle")),
        "abstract": " ".join(abstract_parts) or None,
        "authors": authors,
        "year": year,
        "venue": _element_text(root.find(".//Journal/Title")) or _element_text(root.find(".//ISOAbbreviation")),
        "doi": doi,
        "pmid": fetched_pmid or pmid,
        "mesh": mesh,
        "keywords": keywords,
        "mesh_major": extend_unique([], mesh_major),
        "mesh_qualifiers": extend_unique([], mesh_qualifiers),
        "publication_types": extend_unique([], publication_types),
    }


def _fill_from_pubmed(parsed: ParseResult, fields: dict) -> str:
    try:
        pmid = parsed.pmid or _pubmed_search(parsed)
        if not pmid:
            return fields["source"]
        fetched = _pubmed_fetch(pmid)
        fields.update(
            _merge_missing(
                fields,
                {
                    "title": fetched.get("title"),
                    "doi": fetched.get("doi"),
                    "pmid": fetched.get("pmid") or pmid,
                    "abstract": fetched.get("abstract"),
                    "authors": fetched.get("authors") or [],
                    "year": fetched.get("year"),
                    "venue": fetched.get("venue"),
                },
            )
        )
        extend_unique(fields.setdefault("mesh_terms", []), fetched.get("mesh") or [])
        extend_unique(fields.setdefault("keywords", []), fetched.get("keywords") or [])
        extend_unique(fields.setdefault("mesh_major", []), fetched.get("mesh_major") or [])
        extend_unique(fields.setdefault("mesh_qualifiers", []), fetched.get("mesh_qualifiers") or [])
        extend_unique(fields.setdefault("publication_types", []), fetched.get("publication_types") or [])
        return "pubmed"
    except (requests.RequestException, ET.ParseError) as exc:
        identifier = parsed.pmid or parsed.doi or parsed.title
        logger.warning("_fill_from_pubmed identifier=%r failed: %s", identifier, exc)
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
        response = http_get(
            f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}",
            params={
                "fields": (
                    "paperId,title,abstract,authors,year,venue,externalIds,"
                    "fieldsOfStudy,s2FieldsOfStudy,publicationTypes"
                )
            },
            headers=headers,
            limiter=S2_LIMITER,
            timeout=30,
        )
        payload = response.json()
    except requests.RequestException:
        return fields["source"]
    except ValueError:
        return fields["source"]

    if not isinstance(payload, dict):
        return fields["source"]

    external_ids = payload.get("externalIds") or {}
    authors = [a.get("name") for a in payload.get("authors") or [] if a.get("name")]
    fields.update(
        _merge_missing(
            fields,
            {
                "title": payload.get("title"),
                "doi": bare_doi(external_ids.get("DOI")),
                "pmid": external_ids.get("PubMed"),
                "s2_id": payload.get("paperId"),
                "abstract": payload.get("abstract"),
                "authors": authors,
                "year": str(payload.get("year")) if payload.get("year") else None,
                "venue": payload.get("venue"),
            },
        )
    )
    _prefer_canonical_doi(fields, external_ids.get("DOI"))
    extend_unique(fields.setdefault("fields_of_study", []), payload.get("fieldsOfStudy") or [])
    extend_unique(
        fields.setdefault("fields_of_study", []),
        [item.get("category") for item in payload.get("s2FieldsOfStudy") or [] if isinstance(item, dict)],
    )
    extend_unique(fields.setdefault("publication_types", []), payload.get("publicationTypes") or [])
    return "semantic-scholar" if fields["source"] == "input-only" else fields["source"]


def _crossref_year(msg: dict) -> Optional[str]:
    for key in ("published", "published-print", "published-online"):
        parts = ((msg.get(key) or {}).get("date-parts") or [[]])
        if not parts or not parts[0]:
            continue
        year = parts[0][0]
        if isinstance(year, int) and 1000 <= year <= 9999:
            return str(year)
        if isinstance(year, str) and year.isdigit() and len(year) == 4:
            return year
    return None


def _fill_from_crossref(fields: dict) -> str:
    doi = fields.get("doi")
    if not doi:
        return fields["source"]
    email = os.getenv("CROSSREF_EMAIL")
    params = {"mailto": email} if email else {}
    try:
        resp = http_get(
            f"https://api.crossref.org/works/{doi}",
            params=params,
            limiter=CROSSREF_LIMITER,
            timeout=30,
        )
        msg = resp.json().get("message", {})
    except (requests.RequestException, ValueError) as exc:
        logger.warning("_fill_from_crossref doi=%r failed: %s", doi, exc)
        return fields["source"]

    titles = msg.get("title") or []
    venues = msg.get("container-title") or []
    authors = []
    for author in msg.get("author") or []:
        given = _blank_to_none(author.get("given"))
        family = _blank_to_none(author.get("family"))
        if given and family:
            authors.append(f"{given} {family}")
        elif family:
            authors.append(family)

    fields.update(
        _merge_missing(
            fields,
            {
                "title": titles[0] if titles else None,
                "doi": bare_doi(msg.get("DOI")),
                "authors": authors,
                "year": _crossref_year(msg),
                "venue": venues[0] if venues else None,
                "abstract": msg.get("abstract") or None,
            },
        )
    )
    extend_unique(fields.setdefault("keywords", []), msg.get("subject") or [])
    return "crossref" if fields["source"] == "input-only" else fields["source"]


def resolve_identity(parsed: ParseResult, prefetched: "Optional[BatchResolved]" = None) -> ResolveResult:
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
        "fields_of_study": [],
        "publication_types": [],
        "mesh_terms": [],
        "keywords": [],
        "mesh_major": [],
        "mesh_qualifiers": [],
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

    if prefetched is not None:
        fields.update(
            _merge_missing(
                fields,
                {
                    "title": prefetched.title,
                    "doi": prefetched.doi,
                    "pmid": prefetched.pmid,
                    "s2_id": prefetched.s2_id,
                    "abstract": prefetched.abstract,
                    "authors": prefetched.authors,
                    "year": prefetched.year,
                    "venue": prefetched.venue,
                },
            )
        )
        _prefer_canonical_doi(fields, prefetched.doi)
        extend_unique(fields.setdefault("fields_of_study", []), prefetched.fields_of_study)
        extend_unique(fields.setdefault("publication_types", []), prefetched.publication_types)
        if any(
            [
                prefetched.title,
                prefetched.doi,
                prefetched.pmid,
                prefetched.s2_id,
                prefetched.abstract,
            ]
        ):
            fields["source"] = "s2-batch"

        pmid = prefetched.pmid or fields.get("pmid")
        if pmid:
            try:
                fetched = _pubmed_fetch(pmid)
                fields.update(
                    _merge_missing(
                        fields,
                        {
                            "abstract": fetched.get("abstract"),
                            "pmid": fetched.get("pmid") or pmid,
                        },
                    )
                )
                extend_unique(fields.setdefault("mesh_terms", []), fetched.get("mesh") or [])
                extend_unique(fields.setdefault("keywords", []), fetched.get("keywords") or [])
                extend_unique(fields.setdefault("mesh_major", []), fetched.get("mesh_major") or [])
                extend_unique(fields.setdefault("mesh_qualifiers", []), fetched.get("mesh_qualifiers") or [])
                extend_unique(fields.setdefault("publication_types", []), fetched.get("publication_types") or [])
            except (requests.RequestException, ET.ParseError) as exc:
                logger.warning("resolve_identity prefetched pubmed pmid=%r failed: %s", pmid, exc)
                pass

    needs_enrichment = any(
        [
            not _blank_to_none(fields.get("s2_id")),
            not fields.get("authors"),
            not fields.get("year"),
            not _blank_to_none(fields.get("venue")),
            not _blank_to_none(fields.get("abstract")),
        ]
    )
    if not has_sufficient_title_only_metadata and (
        prefetched is None or not _blank_to_none(fields.get("title")) or needs_enrichment
    ):
        fields["source"] = _fill_from_pubmed(parsed, fields)
        fields["source"] = _fill_from_s2(fields)
        if not _blank_to_none(fields.get("title")):
            fields["source"] = _fill_from_crossref(fields)

    title = _blank_to_none(fields.get("title"))
    if not title:
        raise ValueError("Could not resolve a title for the reference")

    return ResolveResult(
        title=title,
        doi=bare_doi(fields.get("doi")),
        pmid=_blank_to_none(fields.get("pmid")),
        s2_id=_blank_to_none(fields.get("s2_id")),
        abstract=_blank_to_none(fields.get("abstract")),
        authors=fields.get("authors") or [],
        year=str(fields["year"]) if fields.get("year") is not None else None,
        venue=_blank_to_none(fields.get("venue")),
        alt_dois=fields.get("alt_dois") or [],
        source=fields["source"],
        fields_of_study=fields.get("fields_of_study") or [],
        publication_types=fields.get("publication_types") or [],
        mesh_terms=fields.get("mesh_terms") or [],
        keywords=fields.get("keywords") or [],
        mesh_major=fields.get("mesh_major") or [],
        mesh_qualifiers=fields.get("mesh_qualifiers") or [],
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


def find_existing_indexed(resolved: ResolveResult, index: dict) -> DedupResult:
    if resolved.s2_id:
        node = trellis.dedup_check_indexed(index, s2id=resolved.s2_id)
        if node:
            return DedupResult(node, "s2_id")
    if resolved.doi:
        node = trellis.dedup_check_indexed(index, doi=resolved.doi)
        if node:
            return DedupResult(node, "doi")
    if resolved.pmid:
        node = trellis.dedup_check_indexed(index, pmid=resolved.pmid)
        if node:
            return DedupResult(node, "pmid")
    if resolved.title:
        node = trellis.dedup_check_indexed(index, title=resolved.title)
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

    slug_or_id = dedup.existing_node.get("id") or dedup.existing_node.get("uuid") or dedup.existing_node.get("slug")
    if not slug_or_id:
        raise RuntimeError("Existing Trellis node has no slug or UUID")
    node = dedup.existing_node
    if "tags" not in node:
        node = trellis.get_node(slug_or_id)
    current_meta = dict(node.get("metadata") or {})
    current_ref = dict(current_meta.get("reference") or {})
    current_meta["reference"] = _merge_missing(current_ref, metadata["reference"])
    trellis.update_node(
        slug_or_id,
        metadata=current_meta,
        tags=_make_tags(resolved, node.get("tags") or []),
        citation=citation,
    )
    trellis.annotate_node(
        slug_or_id,
        f"[{today}] Dedup match on {dedup.match_reason}; merged metadata; source: {resolved.source}",
    )
    return UpsertResult(slug=slug_or_id, created=False)


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


def link_citations(slug: str, citations: CitationResult, index: dict = None) -> LinkResult:
    linked = 0
    skipped = 0
    for item in citations.items:
        if index is not None:
            target = trellis.dedup_check_indexed(
                index,
                s2id=item.s2_id,
                doi=item.doi,
                pmid=item.pmid,
                title=item.title,
            )
        else:
            target = trellis.dedup_check(
                s2id=item.s2_id,
                doi=item.doi,
                pmid=item.pmid,
                title=item.title,
            )
        if not target:
            skipped += 1
            continue
        # Prefer stable identifiers to avoid ambiguous-slug errors in trellis link
        target_slug = target.get("id") or target.get("uuid") or _node_slug(target)
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
    except Exception as exc:
        logger.warning("verify_outcome slug=%r failed: %s", slug, exc)
        return VerifyResult(
            node_exists=False,
            has_citation_metadata=False,
            pipeline_status=None,
            edge_count=0,
        )


def ingest_reference_pipeline(raw: dict, prefetched=None) -> IngestionOutcome:
    outcome = IngestionOutcome()
    try:
        outcome.parse = parse_input(raw)
        outcome.resolve = resolve_identity(outcome.parse, prefetched=prefetched)
        outcome.dedup = find_existing(outcome.resolve)
        outcome.upsert = upsert_node(outcome.resolve, outcome.dedup)
        if prefetched is not None and prefetched.citations:
            citations = CitationResult(
                source="s2-batch",
                retrieved_at=date.today().isoformat(),
                items=prefetched.citations,
            )
        else:
            citations = fetch_outbound_citations(outcome.resolve.doi or "")
        outcome.citation_store = store_citations(outcome.upsert.slug, citations)
        index = trellis.build_node_index()
        outcome.link = link_citations(outcome.upsert.slug, citations, index=index)
        outcome.verify = verify_outcome(outcome.upsert.slug)
    except (ValueError, RuntimeError) as e:
        outcome.errors.append(str(e))
    return outcome


def format_metrics_table(metrics: BatchMetrics) -> str:
    lines = [
        "Batch metrics",
        f"  total: {metrics.total_seconds:.2f}s | workers: {metrics.workers} | nodes at index: {metrics.node_count_at_index}",
        "",
        f"  {'phase':<36} {'items':>7} {'wall':>10} {'per item':>10}",
        f"  {'-' * 36} {'-' * 7} {'-' * 10} {'-' * 10}",
    ]
    for phase in metrics.phases:
        lines.append(
            f"  {phase.name:<36} {phase.items:>7} {phase.wall_seconds:>9.2f}s {phase.per_item_seconds:>9.2f}s"
        )
    return "\n".join(lines)


def resolve_and_upsert(
    item: tuple[int, str],
    outcomes: list[IngestionOutcome],
    prefetched_for_doi,
    resolve_timings: list[float],
    upsert_timings: list[float],
    timing_lock: threading.Lock,
    index: dict,
) -> Optional[tuple[int, str, str]]:
    idx, doi = item
    outcome = outcomes[idx]
    try:
        outcome.parse = parse_input({"doi": doi})
        t0 = time.perf_counter()
        outcome.resolve = resolve_identity(outcome.parse, prefetched=prefetched_for_doi(doi))
        resolve_elapsed = time.perf_counter() - t0
        outcome.dedup = find_existing_indexed(outcome.resolve, index)
        t0 = time.perf_counter()
        outcome.upsert = upsert_node(outcome.resolve, outcome.dedup)
        upsert_elapsed = time.perf_counter() - t0
        with timing_lock:
            resolve_timings.append(resolve_elapsed)
            upsert_timings.append(upsert_elapsed)
        citation_doi = outcome.resolve.doi or doi
        return idx, citation_doi, outcome.upsert.slug
    except Exception as e:
        outcome.errors.append(str(e))
        return None


def fetch_and_store(
    item: tuple[int, str, str],
    outcomes: list[IngestionOutcome],
    prefetched_for_doi,
    dois: list[str],
) -> Optional[tuple[int, str, CitationResult]]:
    idx, doi, slug = item
    outcome = outcomes[idx]
    try:
        prefetched = prefetched_for_doi(dois[idx])
        if prefetched is not None and prefetched.citations:
            citations = CitationResult(
                source="s2-batch",
                retrieved_at=date.today().isoformat(),
                items=prefetched.citations,
            )
        else:
            citations = fetch_outbound_citations(doi)
        outcome.citation_store = store_citations(slug, citations)
        return idx, slug, citations
    except Exception as e:
        outcome.errors.append(str(e))
        return None


def link_stored(
    item: tuple[int, str, CitationResult],
    outcomes: list[IngestionOutcome],
    index: dict,
) -> Optional[int]:
    idx, slug, citations = item
    outcome = outcomes[idx]
    try:
        outcome.link = link_citations(slug, citations, index=index)
        return idx
    except Exception as e:
        outcome.errors.append(str(e))
        return None


def verify_upserted(
    item: tuple[int, str, str],
    outcomes: list[IngestionOutcome],
) -> None:
    idx, _doi, slug = item
    outcome = outcomes[idx]
    try:
        outcome.verify = verify_outcome(slug)
    except Exception as e:
        outcome.errors.append(str(e))


_TOPICAL_TAG_PREFIXES = ("mesh:", "field:", "type:")


def _has_topical_backfill_tags(node: dict) -> bool:
    return any(str(tag).startswith(_TOPICAL_TAG_PREFIXES) for tag in node.get("tags") or [])


def _doi_from_node(node: dict) -> Optional[str]:
    doi = bare_doi(node.get("uri"))
    if doi:
        return doi
    reference = (node.get("metadata") or {}).get("reference", {})
    if isinstance(reference, dict):
        doi = bare_doi(reference.get("doi"))
        if doi:
            return doi
    for tag in node.get("tags") or []:
        tag = str(tag)
        if tag.startswith("doi:"):
            doi = bare_doi(tag)
            if doi:
                return doi
    return None


def backfill_nodes(
    workers: int = 8,
    only_missing: bool = False,
    status: str = "scaffolded",
) -> tuple[list[IngestionOutcome], BackfillResult]:
    """
    Full re-scan of existing entries through the SAME pipeline used for new
    instantiation. Each existing node already carries its DOI, so feeding those
    DOIs through ingest_batch updates the nodes in place via upsert_node's
    dedup-merge path (an upsert: existing -> update, missing -> insert; no
    duplicates) while building the citation tags AND edges that older scaffold
    runs never produced.

    only_missing=True is an optional optimization that skips nodes already
    carrying topical tags; the default (False) reprocesses every entry, which is
    what actually backfills citation edges (a node can have tags but no edges).
    """
    candidates = trellis.get_by_pipeline_status(status)
    result = BackfillResult(
        candidates=len(candidates),
        resolvable=0,
        processed=0,
        skipped_no_doi=0,
        skipped_already_tagged=0,
    )

    dois: list[str] = []
    for node in candidates:
        if only_missing and _has_topical_backfill_tags(node):
            result.skipped_already_tagged += 1
            continue
        doi = _doi_from_node(node)
        if not doi:
            result.skipped_no_doi += 1
            continue
        dois.append(doi)

    result.resolvable = len(dois)
    if not dois:
        return [], result

    # Reuse the instantiation pipeline verbatim: resolve -> dedup-merge upsert ->
    # fetch citations -> link edges -> verify. Existing nodes update in place.
    outcomes, _metrics = ingest_batch(dois, workers=workers)
    for outcome in outcomes:
        if outcome.errors:
            result.errors.extend(outcome.errors)
            continue
        result.processed += 1
        if outcome.link is not None:
            result.edges_linked += outcome.link.linked
        if outcome.citation_store is not None:
            result.citations_stored += outcome.citation_store.stored
    return outcomes, result


def ingest_batch(dois: list[str], workers: int = 8) -> tuple[list[IngestionOutcome], BatchMetrics]:
    batch_t0 = time.perf_counter()
    phases: list[PhaseMetrics] = []

    def add_phase(name: str, wall_seconds: float, items: int) -> None:
        per_item_seconds = wall_seconds / items if items else 0.0
        phases.append(
            PhaseMetrics(
                name=name,
                wall_seconds=wall_seconds,
                per_item_seconds=per_item_seconds,
                items=items,
            )
        )

    from pipeline.aggregator import batch_resolve

    t0 = time.perf_counter()
    resolved_map = batch_resolve(dois)
    elapsed = time.perf_counter() - t0
    add_phase("phase0 batch resolve", elapsed, len(dois))

    def prefetched_for_doi(doi: str):
        key = bare_doi(doi)
        return resolved_map.get(key.lower()) if key else None

    t0 = time.perf_counter()
    index = trellis.build_node_index()
    elapsed = time.perf_counter() - t0
    node_count_at_index = len(index)
    add_phase("index build", elapsed, node_count_at_index)

    outcomes = [IngestionOutcome() for _ in dois]
    resolve_timings: list[float] = []
    upsert_timings: list[float] = []
    timing_lock = threading.Lock()

    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        phase1_results = list(
            executor.map(
                lambda item: resolve_and_upsert(
                    item,
                    outcomes,
                    prefetched_for_doi,
                    resolve_timings,
                    upsert_timings,
                    timing_lock,
                    index,
                ),
                enumerate(dois),
            )
        )
    elapsed = time.perf_counter() - t0
    add_phase("phase1 resolve+upsert", elapsed, len(dois))
    add_phase("phase1 resolve_identity aggregate", sum(resolve_timings), len(resolve_timings))
    add_phase("phase1 upsert_node aggregate", sum(upsert_timings), len(upsert_timings))

    upserted = [result for result in phase1_results if result is not None]
    t0 = time.perf_counter()
    index = trellis.build_node_index()
    elapsed = time.perf_counter() - t0
    add_phase("index rebuild", elapsed, len(index))

    t0 = time.perf_counter()
    for idx, citation_doi, slug in upserted:
        outcome = outcomes[idx]
        try:
            resolved = outcome.resolve
            trellis.reverse_materialize(
                slug,
                doi=(resolved.doi if resolved else None) or citation_doi,
                index=index,
            )
        except Exception as e:
            outcome.errors.append(str(e))
    elapsed = time.perf_counter() - t0
    add_phase("reverse materialize", elapsed, len(upserted))

    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        phase2_results = list(
            executor.map(
                lambda item: fetch_and_store(item, outcomes, prefetched_for_doi, dois),
                upserted,
            )
        )
    elapsed = time.perf_counter() - t0
    add_phase("phase2 fetch+store", elapsed, len(upserted))

    stored = [result for result in phase2_results if result is not None]

    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        list(executor.map(lambda item: link_stored(item, outcomes, index), stored))
    elapsed = time.perf_counter() - t0
    add_phase("phase3 link", elapsed, len(stored))

    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        list(executor.map(lambda item: verify_upserted(item, outcomes), upserted))
    elapsed = time.perf_counter() - t0
    add_phase("phase4 verify", elapsed, len(upserted))

    metrics = BatchMetrics(
        phases=phases,
        total_seconds=time.perf_counter() - batch_t0,
        workers=workers,
        node_count_at_index=node_count_at_index,
    )
    return outcomes, metrics


if __name__ == "__main__":
    import argparse

    from dotenv import load_dotenv

    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--doi")
    parser.add_argument("--pmid")
    parser.add_argument("--title")
    parser.add_argument("--batch-file", help="JSON file with list of DOIs to process in batch")
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    if args.batch_file:
        with open(args.batch_file, "r", encoding="utf-8") as f:
            dois = json.load(f)
        if not isinstance(dois, list) or not all(isinstance(doi, str) for doi in dois):
            parser.error("--batch-file must contain a JSON list of DOI strings")
        outcomes, metrics = ingest_batch(dois, workers=args.workers)
        summary = {
            "total": len(outcomes),
            "succeeded": sum(1 for outcome in outcomes if not outcome.errors),
            "failed": sum(1 for outcome in outcomes if outcome.errors),
            "created": sum(1 for outcome in outcomes if outcome.upsert and outcome.upsert.created),
            "updated": sum(1 for outcome in outcomes if outcome.upsert and not outcome.upsert.created),
        }
        print(
            json.dumps(
                {
                    "summary": summary,
                    "metrics": dataclasses.asdict(metrics),
                    "outcomes": [dataclasses.asdict(outcome) for outcome in outcomes],
                },
                indent=2,
                default=str,
            )
        )
        print()
        print(format_metrics_table(metrics))
        raise SystemExit(0)
    raw = {k: v for k, v in {"doi": args.doi, "pmid": args.pmid, "title": args.title}.items() if v}
    if not raw:
        parser.error("Provide at least one of --doi, --pmid, --title")
    outcome = ingest_reference_pipeline(raw)
    print(json.dumps(dataclasses.asdict(outcome), indent=2, default=str))
