import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.citation_types import CitationItem, CitationResult  # noqa: E402
from pipeline.citations import merge_citations  # noqa: E402
from pipeline.providers import crossref, semantic_scholar  # noqa: E402


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


@pytest.mark.parametrize("identifier", ["pmid", "s2_id", "openalex_id", "omid"])
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
    parsed = crossref.parse(
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
    withheld = semantic_scholar.parse(
        {"data": None, "citingPaperInfo": {"openAccessPdf": {"disclaimer": notice}}}
    )
    empty = semantic_scholar.parse({"data": []})
    assert withheld.providers["semantic-scholar"] == {
        "status": "withheld",
        "notice": notice,
    }
    assert empty.providers["semantic-scholar"]["status"] == "empty"
    union = merge_citations(
        withheld, crossref.parse({"message": {"reference": [{"DOI": "10.1234/a"}]}})
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
    parsed = crossref.parse(payload)
    assert parsed.items == []
    assert parsed.providers["crossref"]["status"] == status


def test_s2_retains_identifier_only_references_and_normalizes_identifiers():
    parsed = semantic_scholar.parse(
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


def test_index_only_reference_is_enriched_without_losing_provider_provenance():
    index_only = CitationItem(None, None, None, "", None, openalex_id="W123")
    doi_bridge = CitationItem(
        "10.1234/target", None, None, "Target", 2020, openalex_id="W123"
    )
    oc_bridge = CitationItem("10.1234/target", "456", None, "", None, omid="br/789")
    omid_only = CitationItem(None, None, None, "", None, omid="br/789")
    union = merge_citations(
        result("openalex", index_only),
        result("crossref", doi_bridge),
        result("opencitations", oc_bridge),
        result("europe-pmc", omid_only),
        result(
            "semantic-scholar",
            CitationItem(None, "456", "s2-target", "Target", 2020),
        ),
    )
    assert [
        (item.doi, item.pmid, item.s2_id, item.openalex_id, item.omid)
        for item in union.items
    ] == [("10.1234/target", "456", "s2-target", "W123", "br/789")]
    assert set(union.items[0].sources) == {
        "semantic-scholar",
        "crossref",
        "europe-pmc",
        "openalex",
        "opencitations",
    }


@pytest.mark.parametrize("identifier", ["openalex_id", "omid"])
def test_matching_index_id_does_not_merge_conflicting_dois(identifier):
    first = CitationItem("10.1234/a", None, None, "Title", None)
    second = CitationItem("10.1234/b", None, None, "Title", None)
    setattr(first, identifier, "123")
    setattr(second, identifier, "123")
    union = merge_citations(result("openalex", first, second))
    assert [item.doi for item in union.items] == ["10.1234/a", "10.1234/b"]


@pytest.mark.parametrize("identifier", ["openalex_id", "omid"])
def test_title_match_does_not_override_conflicting_index_ids(identifier):
    first = CitationItem(None, None, None, "Shared title", None)
    second = CitationItem(None, None, None, "Shared title", None)
    setattr(first, identifier, "123")
    setattr(second, identifier, "456")
    union = merge_citations(result("openalex", first, second))
    assert [getattr(item, identifier) for item in union.items] == ["123", "456"]


def test_same_doi_merges_despite_a_pmid_shared_with_a_distinct_preprint():
    union = merge_citations(
        result(
            "semantic-scholar",
            CitationItem(
                "10.1101/2020.11.19.388223",
                "33944776",
                "s2-preprint",
                "bioBakery 3",
                2020,
            ),
        ),
        result(
            "crossref",
            CitationItem("10.7554/elife.65088", None, None, "", 2021),
        ),
        result(
            "europe-pmc",
            CitationItem(None, "33944776", None, "bioBakery 3", 2021),
        ),
        result(
            "openalex",
            CitationItem(
                "10.7554/elife.65088",
                "33944776",
                None,
                "bioBakery 3",
                2021,
                openalex_id="W3159504416",
            ),
        ),
        result(
            "opencitations",
            CitationItem(
                "10.7554/elife.65088", "33944776", None, "", None, omid="br/0640841103"
            ),
        ),
    )
    assert [item.doi for item in union.items] == [
        "10.1101/2020.11.19.388223",
        "10.7554/elife.65088",
        None,
    ]
    assert union.items[1].sources == ["crossref", "openalex", "opencitations"]
    assert (union.items[1].openalex_id, union.items[1].omid) == (
        "W3159504416",
        "br/0640841103",
    )
    assert (union.items[2].pmid, union.items[2].sources) == (
        "33944776",
        ["europe-pmc"],
    )


@pytest.mark.parametrize(
    "order",
    [(0, 1, 2), (0, 2, 1), (1, 0, 2), (1, 2, 0), (2, 0, 1), (2, 1, 0)],
)
@pytest.mark.parametrize("transitive", [False, True])
def test_secondary_identifier_ambiguity_is_independent_of_provider_order(
    order, transitive
):
    second_pmid = None if transitive else "123"
    second_s2 = "s2-b" if transitive else None
    records = [
        result("crossref", CitationItem("10.1234/a", "123", None, "", None)),
        result("openalex", CitationItem("10.1234/b", second_pmid, second_s2, "", None)),
        result("europe-pmc", CitationItem(None, "123", second_s2, "", None)),
    ]
    union = merge_citations(*(records[idx] for idx in order))
    assert {
        (item.doi, item.pmid, item.s2_id, tuple(item.sources)) for item in union.items
    } == {
        ("10.1234/a", "123", None, ("crossref",)),
        ("10.1234/b", second_pmid, second_s2, ("openalex",)),
        (None, "123", second_s2, ("europe-pmc",)),
    }


def test_generic_doi_less_titles_do_not_make_unique_identifiers_ambiguous():
    from itertools import permutations

    records = [
        result("crossref", CitationItem("10.1234/a", "123", None, "Article A", None)),
        result("crossref", CitationItem("10.1234/b", None, "s2-b", "Article B", None)),
        result("europe-pmc", CitationItem(None, "123", None, "Editorial", None)),
        result("openalex", CitationItem(None, None, "s2-b", "Editorial", None)),
    ]
    for order in permutations(records):
        union = merge_citations(*order)
        assert {
            (item.doi, item.pmid, item.s2_id, frozenset(item.sources))
            for item in union.items
        } == {
            ("10.1234/a", "123", None, frozenset({"crossref", "europe-pmc"})),
            ("10.1234/b", None, "s2-b", frozenset({"crossref", "openalex"})),
        }


def test_unambiguous_title_can_bridge_an_existing_identifier_only_group():
    from itertools import permutations

    records = [
        result("crossref", CitationItem("10.1234/a", None, None, "Article A", None)),
        result("europe-pmc", CitationItem(None, "123", None, "Article A", None)),
        result("europe-pmc", CitationItem(None, "123", None, "Alternate title", None)),
    ]
    for order in permutations(records):
        union = merge_citations(*order)
        assert {
            (item.doi, item.pmid, frozenset(item.sources)) for item in union.items
        } == {("10.1234/a", "123", frozenset({"crossref", "europe-pmc"}))}


def test_unanchored_title_bridges_do_not_choose_between_distinct_dois():
    from itertools import permutations

    records = [
        result("crossref", CitationItem("10.1234/a", None, None, "Title X", None)),
        result("crossref", CitationItem("10.1234/b", None, None, "Title Z", None)),
        result("europe-pmc", CitationItem(None, "123", None, "Title X", None)),
        result("europe-pmc", CitationItem(None, "123", None, "Title Y", None)),
        result("openalex", CitationItem(None, None, "s2-b", "Title Y", None)),
        result("openalex", CitationItem(None, None, "s2-b", "Title Z", None)),
    ]
    for order in permutations(records):
        union = merge_citations(*order)
        assert {
            (item.doi, item.pmid, item.s2_id, frozenset(item.sources))
            for item in union.items
        } == {
            ("10.1234/a", None, None, frozenset({"crossref"})),
            ("10.1234/b", None, None, frozenset({"crossref"})),
            (None, "123", None, frozenset({"europe-pmc"})),
            (None, None, "s2-b", frozenset({"openalex"})),
        }
