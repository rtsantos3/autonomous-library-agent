"""Collect and deduplicate outbound references without mutating Trellis."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import replace
from datetime import date
from typing import Optional

from pipeline._utils import bare_doi, extend_unique
from pipeline.citation_types import CitationItem, CitationResult
from pipeline.providers import (
    crossref,
    europe_pmc,
    openalex,
    opencitations,
    semantic_scholar,
)

PROVIDERS = (
    ("semantic-scholar", semantic_scholar.retrieve),
    ("crossref", crossref.retrieve),
    ("europe-pmc", europe_pmc.retrieve),
    ("openalex", openalex.retrieve),
    ("opencitations", opencitations.retrieve),
)
IDENTIFIERS = ("doi", "pmid", "s2_id", "openalex_id", "omid")


def ambiguous_citation_keys(items: Iterable[CitationItem]) -> set[tuple[str, str]]:
    """Resolve DOI conflicts through identifier-poor bridges before merging."""
    records = list(items)
    direct: dict[tuple[str, str], set[str]] = {}
    parents: dict[tuple[str, str], tuple[str, str]] = {}

    def keys(item):
        for field in (*IDENTIFIERS[1:], "title"):
            value = getattr(item, field)
            if field == "title":
                value = " ".join(value.casefold().split())
            if value:
                yield field, value

    def root(key):
        parents.setdefault(key, key)
        while parents[key] != key:
            parents[key] = parents[parents[key]]
            key = parents[key]
        return key

    for item in records:
        doi = bare_doi(item.doi)
        if doi:
            for key in keys(item):
                direct.setdefault(key, set()).add(doi)

    for item in records:
        if bare_doi(item.doi):
            continue
        group = [key for key in keys(item) if key[0] != "title"]
        if group:
            anchor = root(group[0])
            for key in group[1:]:
                parents[root(key)] = anchor

    anchored_dois: dict[tuple[str, str], set[str]] = {}
    for key, dois in direct.items():
        if key[0] != "title":
            anchored_dois.setdefault(root(key), set()).update(dois)

    # Stable-ID anchors only supply title evidence. Titles can connect unanchored
    # groups, but a shared generic title cannot contaminate anchored identities.
    for item in records:
        if bare_doi(item.doi) or not item.title:
            continue
        title_key = ("title", " ".join(item.title.casefold().split()))
        group = [key for key in keys(item) if key[0] != "title"]
        if not group:
            continue
        anchor = root(group[0])
        known = anchored_dois.get(anchor)
        if known:
            direct.setdefault(title_key, set()).update(known)
        else:
            parents[root(title_key)] = anchor

    component_dois: dict[tuple[str, str], set[str]] = {}
    for key, dois in direct.items():
        component_dois.setdefault(root(key), set()).update(dois)
    return {key for key in parents if len(component_dois.get(root(key), ())) > 1}


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
        for key in (*IDENTIFIERS, "title", "year"):
            if not getattr(target, key):
                setattr(target, key, getattr(incoming, key))
        extend_unique(target.sources, incoming.sources)

    by_id: dict[tuple[str, str], set[int]] = {}
    by_title: dict[str, list[int]] = {}
    # Decide ambiguity before assigning identifier-poor records to DOI groups.
    ambiguous = ambiguous_citation_keys(
        item for result in results for item in result.items
    )
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
            if not any(getattr(item, key) for key in (*IDENTIFIERS, "title")):
                continue
            extend_unique(sources, item.sources)
            keys = [
                (key, value) for key in IDENTIFIERS if (value := getattr(item, key))
            ]
            matches = {
                root(idx)
                for key in keys
                for idx in by_id.get(key, ())
                if key not in ambiguous or (not item.doi and not items[root(idx)].doi)
            }
            title = " ".join(item.title.casefold().split())
            if title and ("title", title) not in ambiguous:
                title_matches = {
                    root(idx)
                    for idx in by_title.get(title, [])
                    if not (item.doi and items[root(idx)].doi)
                    and all(
                        not getattr(item, key)
                        or not getattr(items[root(idx)], key)
                        or getattr(item, key) == getattr(items[root(idx)], key)
                        for key in IDENTIFIERS[1:]
                    )
                }
                if len(title_matches) == 1:
                    matches.update(title_matches)
            if item.doi:
                # A preprint and journal article can share a PMID. Conflicting
                # secondary matches must not invalidate an exact DOI match.
                matches = {
                    idx
                    for idx in matches
                    if not items[idx].doi or items[idx].doi == item.doi
                }
            elif len({items[idx].doi for idx in matches if items[idx].doi}) > 1:
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
                by_id.setdefault(key, set()).add(idx)
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
            "+".join(name for name, _retrieve in PROVIDERS),
            date.today().isoformat(),
            [],
            {name: {"status": "unavailable"} for name, _retrieve in PROVIDERS},
        )
    results = []
    for name, retrieve in PROVIDERS:
        if name == "semantic-scholar" and prefetched:
            results.append(
                CitationResult(
                    name,
                    date.today().isoformat(),
                    prefetched,
                    {name: {"status": "available"}},
                )
            )
        else:
            results.append(retrieve(doi))
    return merge_citations(*results)
