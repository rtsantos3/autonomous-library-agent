import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.providers.opencitations import parse_references  # noqa: E402


def test_outgoing_cited_identifiers_not_citing_or_creation_metadata():
    result = parse_references(
        [
            {
                "citing": "doi:10.7717/peerj-cs.421 pmid:33817056 omid:br/06101801781",
                "cited": "omid:br/06180334099 DOI:10.1108/JD-12-2013-0166 pmid:12345",
                "creation": "2021-03-10",
                "title": "This is not target metadata",
                "timespan": "P6Y0M1D",
            }
        ]
    )
    assert result.source == "opencitations"
    assert result.providers == {"opencitations": {"status": "available"}}
    assert len(result.items) == 1
    item = result.items[0]
    assert (item.doi, item.pmid, item.omid) == (
        "10.1108/jd-12-2013-0166",
        "12345",
        "br/06180334099",
    )
    assert (item.title, item.year, item.s2_id, item.openalex_id) == (
        "",
        None,
        None,
        None,
    )
    assert item.sources == ["opencitations"]


def test_identifier_only_references_survive_without_doi_or_metadata():
    result = parse_references(
        [
            {"cited": "pmid:33817056"},
            {"cited": "omid:br/06180334099"},
            {"cited": "doi:https://doi.org/10.1234/DOI-Only"},
        ]
    )
    assert [(item.doi, item.pmid, item.omid) for item in result.items] == [
        (None, "33817056", None),
        (None, None, "br/06180334099"),
        ("10.1234/doi-only", None, None),
    ]
    assert all(item.title == "" and item.year is None for item in result.items)
    assert result.providers["opencitations"]["status"] == "available"


def test_distinct_dois_are_not_conflated_through_shared_secondary_identifiers():
    result = parse_references(
        [
            {
                "cited": "omid:br/06101802023 doi:10.7717/peerj-cs.163 "
                "pmid:33816816 doi:10.7287/peerj.preprints.26727v1"
            }
        ]
    )
    assert [(item.doi, item.pmid, item.omid) for item in result.items] == [
        ("10.7717/peerj-cs.163", None, None),
        ("10.7287/peerj.preprints.26727v1", None, None),
        (None, "33816816", None),
        (None, None, "br/06101802023"),
    ]
    assert result.providers["opencitations"]["status"] == "available"


@pytest.mark.parametrize(
    "cited,expected",
    [
        (
            "doi:10.1234/ABC DOI:10.1234/abc pmid:123 pmid:123 omid:br/456",
            [("10.1234/abc", "123", "br/456")],
        ),
        (
            "pmid:123 pmid:456 omid:br/789",
            [(None, "123", None), (None, "456", None), (None, None, "br/789")],
        ),
        (
            "omid:br/123 omid:br/456 doi:10.1234/target",
            [
                ("10.1234/target", None, None),
                (None, None, "br/123"),
                (None, None, "br/456"),
            ],
        ),
    ],
)
def test_multiple_identifier_values_preserve_only_unambiguous_associations(
    cited, expected
):
    result = parse_references([{"cited": cited}])
    assert [(item.doi, item.pmid, item.omid) for item in result.items] == expected


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        {"error": "Backend unavailable"},
        "[]",
        0,
        [None],
        ["doi:10.1234/not-a-row"],
        [{}],
        [{"citing": "doi:10.1234/citing-only"}],
        [{"cited": None}],
        [{"cited": ["doi:10.1234/wrong-type"]}],
        [{"cited": {"doi": "10.1234/wrong-type"}}],
        [{"cited": ""}],
        [{"cited": "unstructured reference text"}],
        [{"cited": "doi:not-a-doi pmid:abc omid:br/not-digits"}],
        [{"cited": "pmid:１２３ omid:ar/123 doi:10.1/not-a-registrant"}],
    ],
)
def test_invalid_payloads_are_unavailable_not_empty(payload):
    result = parse_references(payload)
    assert result.items == []
    assert result.providers == {"opencitations": {"status": "unavailable"}}


def test_empty_array_is_a_genuinely_empty_bibliography():
    result = parse_references([])
    assert result.items == []
    assert result.providers == {"opencitations": {"status": "empty"}}


def test_malformed_rows_do_not_discard_valid_references_or_claim_completeness():
    result = parse_references(
        [None, {"cited": "doi:10.1234/target"}, {"cited": 123}, {"citing": "pmid:456"}]
    )
    assert [item.doi for item in result.items] == ["10.1234/target"]
    assert result.providers == {"opencitations": {"status": "partial"}}


def test_unknown_or_invalid_identifier_tokens_do_not_invent_target_metadata():
    result = parse_references(
        [{"cited": "isbn:9781234567890 nonsense pmid:invalid omid:br/123"}]
    )
    assert len(result.items) == 1
    item = result.items[0]
    assert (item.doi, item.pmid, item.omid, item.title, item.year) == (
        None,
        None,
        "br/123",
        "",
        None,
    )
