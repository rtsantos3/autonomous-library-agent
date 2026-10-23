import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.providers.openalex import parse  # noqa: E402


def test_identifiers_and_metadata_are_normalized_without_losing_reference_order():
    result = parse(
        {"referenced_works": ["https://openalex.org/W2", "w1", "W2"]},
        [
            {
                "results": [
                    {
                        "id": "https://openalex.org/W1",
                        "doi": " https://doi.org/10.1234/ABC ",
                        "ids": {"pmid": "https://pubmed.ncbi.nlm.nih.gov/12345/"},
                        "title": " A cited title ",
                        "publication_year": "2021",
                    },
                    {
                        "id": "https://openalex.org/w2",
                        "doi": None,
                        "ids": {
                            "doi": "DOI:10.1234/SECOND",
                            "pmid": "http://www.ncbi.nlm.nih.gov/pubmed/6789",
                        },
                        "title": None,
                        "publication_year": None,
                    },
                ]
            }
        ],
    )
    assert [
        (item.openalex_id, item.doi, item.pmid, item.title, item.year)
        for item in result.items
    ] == [
        ("W2", "10.1234/second", "6789", "", None),
        ("W1", "10.1234/abc", "12345", "A cited title", 2021),
    ]
    assert all(item.sources == ["openalex"] for item in result.items)
    assert all(item.s2_id is None and item.omid is None for item in result.items)
    assert result.providers["openalex"]["status"] == "available"


@pytest.mark.parametrize(
    "payload",
    [None, [], {}, {"referenced_works": None}, {"referenced_works": "W1"}],
)
def test_missing_or_malformed_bibliography_is_unavailable_not_empty(payload):
    result = parse(payload)
    assert result.items == []
    assert result.providers["openalex"]["status"] == "unavailable"


def test_empty_bibliography_is_genuinely_empty_regardless_of_incoming_counts():
    result = parse(
        {"referenced_works": [], "cited_by_count": 900, "related_works": ["W1"]},
        [{"results": [{"id": "W1", "title": "Not an outgoing reference"}]}],
    )
    assert result.items == []
    assert result.providers["openalex"]["status"] == "empty"


@pytest.mark.parametrize(
    "identifier",
    [
        None,
        123,
        {},
        {"id": "W1"},
        "",
        "W",
        "A123",
        "W-1",
        "W１２３",
        "https://example.org/W1",
        "https://openalex.org/W1/other",
        "https://openalex.org/W1?query=1",
    ],
)
def test_invalid_reference_ids_never_create_edges(identifier):
    result = parse({"referenced_works": [identifier]})
    assert result.items == []
    assert result.providers["openalex"]["status"] == "unavailable"


def test_invalid_reference_among_valid_targets_marks_retained_result_partial():
    result = parse(
        {"referenced_works": ["W1", "A1", None]},
        [{"results": [{"id": "W1", "title": "Known target"}]}],
    )
    assert [item.openalex_id for item in result.items] == ["W1"]
    assert result.items[0].title == "Known target"
    assert result.providers["openalex"]["status"] == "partial"


def test_unrelated_hydration_and_related_or_incoming_records_are_excluded():
    result = parse(
        {
            "id": "W999",
            "referenced_works": ["W1"],
            "related_works": ["W2"],
            "cited_by_count": 500,
        },
        [
            {
                "results": [
                    {"id": "W2", "doi": "10.1234/unrelated", "title": "Related"},
                    {"id": "W999", "title": "The citing work"},
                    {"id": "W1", "title": "The cited work"},
                ]
            }
        ],
    )
    assert [(item.openalex_id, item.title) for item in result.items] == [
        ("W1", "The cited work")
    ]
    assert result.providers["openalex"]["status"] == "available"


def test_incomplete_hydration_retains_all_known_targets_as_identifier_only():
    result = parse(
        {"referenced_works": ["W1", "W2", "W3", "https://openalex.org/W2"]},
        [
            {"results": [{"id": "W1", "title": "Hydrated first"}]},
            None,
            {"results": [{"id": "W3", "doi": "10.1234/third"}]},
        ],
    )
    assert [item.openalex_id for item in result.items] == ["W1", "W2", "W3"]
    missing = result.items[1]
    assert (missing.doi, missing.pmid, missing.title, missing.year) == (
        None,
        None,
        "",
        None,
    )
    assert result.items[0].title == "Hydrated first"
    assert result.items[2].doi == "10.1234/third"
    assert result.providers["openalex"]["status"] == "partial"


@pytest.mark.parametrize(
    "hydration",
    [[], [None], [{}], [{"results": None}], [{"results": [None, "W1", {}]}]],
)
def test_missing_or_malformed_hydration_never_discards_indexed_ids(hydration):
    result = parse({"referenced_works": ["W1", "W2"]}, hydration)
    assert [item.openalex_id for item in result.items] == ["W1", "W2"]
    assert result.providers["openalex"]["status"] == "partial"


def test_identifier_only_hydrated_record_is_usable_without_a_fabricated_title():
    result = parse(
        {"referenced_works": ["W1"]},
        [
            {
                "results": [
                    {
                        "id": "W1",
                        "doi": {"value": "10.1234/not-a-string"},
                        "ids": [],
                        "title": ["Not a title"],
                        "publication_year": {"year": 2020},
                        "reference": "Author. Raw bibliography text (2020).",
                    }
                ]
            }
        ],
    )
    item = result.items[0]
    assert (item.openalex_id, item.doi, item.pmid, item.title, item.year) == (
        "W1",
        None,
        None,
        "",
        None,
    )
    assert result.providers["openalex"]["status"] == "available"


@pytest.mark.parametrize(
    "pmid",
    ["https://example.org/123", "https://pubmed.ncbi.nlm.nih.gov/ABC", 123, [], None],
)
def test_invalid_pmid_values_are_not_reinterpreted_as_identifiers(pmid):
    result = parse(
        {"referenced_works": ["W1"]},
        [{"results": [{"id": "W1", "ids": {"pmid": pmid}}]}],
    )
    assert result.items[0].openalex_id == "W1"
    assert result.items[0].pmid is None
