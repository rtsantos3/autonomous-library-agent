import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.citations import (  # noqa: E402
    CitationItem,
    CitationResult,
    _parse_crossref,
    _parse_s2,
    merge_citations,
)


def result(source, *items):
    return CitationResult(source, "2026-10-22", list(items))


def test_union_keeps_provider_exclusive_references_and_merges_doi_aliases():
    s2 = result(
        "semantic-scholar",
        CitationItem("https://doi.org/10.1234/ABC", "123", "s2-a", "Shared", 2020),
        CitationItem("10.1234/s2-only", None, None, "S2 only", 2021),
    )
    crossref = result(
        "crossref",
        CitationItem("DOI:10.1234/abc", None, None, "Shared title variant", None),
        CitationItem("10.1234/cr-only", None, None, "Crossref only", 2022),
        CitationItem("10.1234/cr-only", None, None, "Crossref only", 2022),
    )
    union = merge_citations(s2, crossref)
    assert {item.doi for item in union.items} == {
        "10.1234/abc",
        "10.1234/s2-only",
        "10.1234/cr-only",
    }
    shared = next(item for item in union.items if item.doi == "10.1234/abc")
    assert (shared.pmid, shared.s2_id, shared.year) == ("123", "s2-a", 2020)
    assert shared.sources == ["semantic-scholar", "crossref"]
    assert s2.items[0].sources == []
    assert s2.items[0].doi == "https://doi.org/10.1234/ABC"
    assert merge_citations(union, s2, crossref).items == union.items


@pytest.mark.parametrize("identifier", ["pmid", "s2_id"])
def test_shared_identifier_enriches_missing_doi(identifier):
    first = CitationItem(None, None, None, "Original title", None)
    second = CitationItem("10.1234/target", None, None, "Corrected title", 2023)
    setattr(first, identifier, "123")
    setattr(second, identifier, "123")
    union = merge_citations(
        result("semantic-scholar", first), result("crossref", second)
    )
    assert [(item.doi, item.year) for item in union.items] == [("10.1234/target", 2023)]
    assert union.items[0].sources == ["semantic-scholar", "crossref"]


def test_bridge_reference_collapses_previously_separate_identifier_groups():
    union = merge_citations(
        result(
            "semantic-scholar",
            CitationItem("10.1234/a", None, None, "First title", None),
            CitationItem(None, "123", None, "Second title", None),
            CitationItem("10.1234/a", "123", "s2-a", "Bridge", 2020),
        )
    )
    assert [(item.doi, item.pmid, item.s2_id) for item in union.items] == [
        ("10.1234/a", "123", "s2-a")
    ]


def test_title_match_enriches_identifier_poor_reference_without_fuzzy_matching():
    union = merge_citations(
        result(
            "semantic-scholar",
            CitationItem(None, None, "s2-id", " A Shared TITLE ", 2020),
        ),
        result(
            "crossref", CitationItem("10.1234/a", None, None, "A shared title", 2020)
        ),
    )
    assert [(item.doi, item.s2_id) for item in union.items] == [("10.1234/a", "s2-id")]


def test_identical_titles_do_not_merge_conflicting_dois_or_ambiguous_targets():
    union = merge_citations(
        result(
            "crossref",
            CitationItem("10.1234/a", None, None, "Editorial", 2020),
            CitationItem("10.1234/b", None, None, "Editorial", 2020),
        ),
        result("semantic-scholar", CitationItem(None, None, None, "Editorial", 2020)),
    )
    assert [item.doi for item in union.items] == ["10.1234/a", "10.1234/b", None]


def test_crossref_uses_deposited_entries_not_reference_count_or_unstructured_titles():
    parsed = _parse_crossref(
        {
            "message": {
                "reference-count": 999,
                "reference": [
                    {
                        "DOI": "https://doi.org/10.1234/ABC",
                        "article-title": "A title",
                        "year": "2021",
                    },
                    {"DOI": "10.1234/doi-only"},
                    {"article-title": "A title-only reference", "year": "unknown"},
                    {"unstructured": "Author. Not an article title. Journal (2020)."},
                    None,
                ],
            }
        }
    )
    assert [(item.doi, item.title, item.year) for item in parsed.items] == [
        ("10.1234/abc", "A title", 2021),
        ("10.1234/doi-only", "", None),
        (None, "A title-only reference", None),
    ]
    assert all(item.sources == ["crossref"] for item in parsed.items)
    assert parsed.providers["crossref"]["status"] == "available"


def test_publisher_withheld_references_remain_distinct_from_empty_bibliography():
    notice = "Notice: The following paper fields have been elided by the publisher: {'references'}."
    withheld = _parse_s2(
        {"data": None, "citingPaperInfo": {"openAccessPdf": {"disclaimer": notice}}}
    )
    empty = _parse_s2({"data": []})
    assert withheld.providers["semantic-scholar"] == {
        "status": "withheld",
        "notice": notice,
    }
    assert empty.providers["semantic-scholar"]["status"] == "empty"
    union = merge_citations(
        withheld, _parse_crossref({"message": {"reference": [{"DOI": "10.1234/a"}]}})
    )
    assert [item.doi for item in union.items] == ["10.1234/a"]
    assert union.items[0].sources == ["crossref"]
    assert union.providers["semantic-scholar"]["status"] == "withheld"


@pytest.mark.parametrize(
    "payload, status",
    [
        ({"message": {"reference-count": 20}}, "unavailable"),
        ({"message": {"reference": []}}, "empty"),
        ({"message": {"reference": [{"unstructured": "Unknown"}]}}, "unavailable"),
        ([], "failed"),
    ],
)
def test_missing_crossref_bibliography_does_not_claim_available(payload, status):
    parsed = _parse_crossref(payload)
    assert parsed.items == []
    assert parsed.providers["crossref"]["status"] == status


def test_s2_retains_identifier_only_references_and_normalizes_identifiers():
    parsed = _parse_s2(
        {
            "data": [
                {
                    "citedPaper": {
                        "paperId": "s2-a",
                        "externalIds": {"DOI": " 10.1234/ABC ", "PubMed": 123},
                        "title": " A title ",
                        "year": 2020,
                    }
                },
                {"citedPaper": {"externalIds": {"PubMed": "456"}}},
                {"citedPaper": {"paperId": "s2-only"}},
                {"citedPaper": {}},
                None,
            ]
        }
    )
    assert [(item.doi, item.pmid, item.s2_id, item.title) for item in parsed.items] == [
        ("10.1234/abc", "123", "s2-a", "A title"),
        (None, "456", None, ""),
        (None, None, "s2-only", ""),
    ]
