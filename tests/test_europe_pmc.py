import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.providers.europe_pmc import (  # noqa: E402
    next_reference_page,
    parse_references,
    select_record,
)


def test_reference_parser_keeps_med_identifiers_but_never_uses_pmc_ids_as_pmids():
    parsed = parse_references(
        {
            "hitCount": 5,
            "referenceList": {
                "reference": [
                    {"source": "MED", "id": "21574616", "pubYear": 2011},
                    {"source": "PMC", "id": "PMC4221854", "title": "PMC record"},
                    {"source": "PMC", "id": "4221854", "title": "Numeric PMC record"},
                    {"source": "MED", "id": "PMC123", "title": "Invalid MED id"},
                    {"source": "MED", "id": True, "title": "Boolean id"},
                ]
            },
        }
    )
    assert [item.pmid for item in parsed.items] == ["21574616", None, None, None, None]
    assert (parsed.items[0].title, parsed.items[0].year) == ("", 2011)
    assert parsed.providers["europe-pmc"]["status"] == "available"
    assert all(item.sources == ["europe-pmc"] for item in parsed.items)


def test_reference_parser_normalizes_doi_and_keeps_structured_title_only_records():
    parsed = parse_references(
        {
            "hitCount": 6,
            "referenceList": {
                "reference": [
                    {"doi": " https://doi.org/10.1234/ABC ", "pubYear": "2020"},
                    {"doi": "DOI:10.1234/DEF", "title": " Structured title "},
                    {"title": "Title without identifiers", "pubYear": "unknown"},
                    {"citation": "Author. A raw reference. Journal (2020)."},
                    {"title": ["Not a string"], "doi": {"value": "10.1234/bad"}},
                    None,
                ]
            },
        }
    )
    assert [(item.doi, item.title, item.year) for item in parsed.items] == [
        ("10.1234/abc", "", 2020),
        ("10.1234/def", "Structured title", None),
        (None, "Title without identifiers", None),
    ]


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {},
        {"hitCount": 3},
        {"referenceList": {}},
        {"referenceList": {"reference": None}},
        {"referenceList": {"reference": "not a list"}},
        {"hitCount": True},
        {"hitCount": -1},
        {"hitCount": "0"},
        {"hitCount": 0, "referenceList": None},
        {"hitCount": 1, "referenceList": {"reference": []}},
        {"hitCount": 0, "referenceList": {"reference": [{"title": "Contradiction"}]}},
        {"hitCount": 1, "referenceList": {"reference": [{"unstructured": "Raw text"}]}},
    ],
)
def test_missing_malformed_and_unusable_bibliographies_are_not_empty(payload):
    parsed = parse_references(payload)
    assert parsed.items == []
    assert parsed.providers["europe-pmc"]["status"] == "unavailable"


@pytest.mark.parametrize(
    "payload",
    [
        {"hitCount": 0},
        {"hitCount": 0, "referenceList": {"reference": []}},
        {"referenceList": {"reference": []}},
    ],
)
def test_actual_zero_reference_responses_are_empty(payload):
    parsed = parse_references(payload)
    assert parsed.items == []
    assert parsed.providers["europe-pmc"]["status"] == "empty"


def test_outbound_parser_uses_reference_list_not_requesting_article_or_citations():
    parsed = parse_references(
        {
            "request": {"source": "MED", "id": "23903748"},
            "hitCount": 1,
            "referenceList": {"reference": [{"source": "MED", "id": "21574616"}]},
            "citationList": {"citation": [{"source": "MED", "id": "99999999"}]},
            "resultList": {"result": [{"doi": "10.1038/nature12373"}]},
        }
    )
    assert [(item.pmid, item.doi) for item in parsed.items] == [("21574616", None)]
    assert parse_references({"citationList": {"citation": []}}).providers == {
        "europe-pmc": {"status": "unavailable"}
    }


def test_lookup_requires_exact_doi_source_and_identifier_and_prefers_med():
    selected = select_record(
        {
            "resultList": {
                "result": [
                    {"doi": "10.1234/another", "source": "MED", "id": "111"},
                    {"doi": "10.1234/target", "source": "PMC", "id": "PMC222"},
                    {"doi": "10.1234/target", "source": "MED", "id": "PMC333"},
                    {"doi": "10.1234/target", "source": "MED"},
                    {"doi": "10.1234/target", "source": "UNKNOWN", "id": "444"},
                    {"doi": "DOI:10.1234/TARGET", "source": "MED", "id": "555"},
                ]
            }
        },
        "https://doi.org/10.1234/target",
    )
    assert (selected["source"], selected["id"]) == ("MED", "555")


def test_lookup_preserves_pmc_prefix_without_fabricating_a_med_record():
    record = {"doi": "10.1234/target", "source": "PMC", "id": "PMC222"}
    assert (
        select_record({"resultList": {"result": [record]}}, "10.1234/target") == record
    )
    assert select_record({"resultList": {"result": [record]}}, "10.1234/other") is None
    assert select_record({"resultList": {"result": []}}, "10.1234/target") is None
    assert select_record(None, "10.1234/target") is None


def test_reference_pagination_uses_hit_count_even_when_page_is_short():
    first = {
        "hitCount": 3,
        "request": {"offSet": 1},
        "referenceList": {"reference": [{"source": "MED", "id": "1"}]},
    }
    last = {
        "hitCount": 3,
        "request": {"offSet": 2},
        "referenceList": {
            "reference": [{"source": "MED", "id": "2"}, {"title": "Last"}]
        },
    }
    assert next_reference_page(first, page=1, records_seen=1) == 2
    assert next_reference_page(last, page=2, records_seen=3) is None
    assert next_reference_page({"hitCount": 0}, page=1, records_seen=0) is None


def test_reference_pagination_without_count_uses_page_size():
    payload = {"referenceList": {"reference": [{"title": "One"}]}}
    assert next_reference_page(payload, page=1, records_seen=1, page_size=1) == 2
    assert next_reference_page(payload, page=1, records_seen=1, page_size=2) is None


@pytest.mark.parametrize(
    "payload,page,seen",
    [
        ({"hitCount": 2, "referenceList": {"reference": []}}, 2, 1),
        ({"hitCount": 1, "referenceList": {"reference": [{"title": "One"}]}}, 2, 2),
        (
            {
                "hitCount": 1,
                "request": {"offSet": 1},
                "referenceList": {"reference": []},
            },
            2,
            1,
        ),
    ],
)
def test_reference_pagination_rejects_truncation_counts_and_repeated_page_offsets(
    payload, page, seen
):
    with pytest.raises(ValueError):
        next_reference_page(payload, page=page, records_seen=seen)


@pytest.mark.parametrize("med_first", [True, False])
def test_lookup_does_not_prefer_med_without_references_over_available_pmc(med_first):
    med = {
        "doi": "10.1234/target",
        "source": "MED",
        "id": "123",
        "hasReferences": "N",
    }
    pmc = {
        "doi": "10.1234/target",
        "source": "PMC",
        "id": "PMC456",
        "hasReferences": "Y",
    }
    records = [med, pmc] if med_first else [pmc, med]
    selected = select_record({"resultList": {"result": records}}, "10.1234/target")
    assert (selected["source"], selected["id"]) == ("PMC", "PMC456")
