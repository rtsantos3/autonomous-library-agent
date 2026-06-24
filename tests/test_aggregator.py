import logging
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import aggregator
from pipeline.aggregator import (
    BATCH_URL,
    S2_BATCH_FIELDS,
    _build_resolved,
    _citation_from_reference,
    _coerce_year,
    batch_resolve,
)


class Response:
    def __init__(self, payload=None, json_error=None):
        self.payload = payload if payload is not None else {}
        self.json_error = json_error

    def json(self):
        if self.json_error is not None:
            raise self.json_error
        return self.payload


def s2_entry(
    doi="10.1/Returned",
    title="Resolved title",
    paper_id="s2-1",
    references=None,
    **overrides,
):
    data = {
        "paperId": paper_id,
        "title": title,
        "abstract": "Abstract",
        "year": 2024,
        "venue": "Journal",
        "authors": [{"name": "Author A"}, {"name": "Author B"}],
        "externalIds": {"DOI": doi, "PubMed": "123"},
        "fieldsOfStudy": ["Biology"],
        "s2FieldsOfStudy": [{"category": "Medicine", "source": "s2"}],
        "publicationTypes": ["Review"],
        "references": [] if references is None else references,
    }
    data.update(overrides)
    return data


def test_batch_resolve_list_payload_maps_input_and_returned_doi_skips_none():
    payload = [s2_entry(doi="10.1/Returned"), None]

    with patch("pipeline.aggregator.http_post", return_value=Response(payload)) as post:
        resolved_map = batch_resolve(["10.1/Input", "10.missing/y"])

    post.assert_called_once_with(
        BATCH_URL,
        params={"fields": S2_BATCH_FIELDS},
        json_body={"ids": ["DOI:10.1/input", "DOI:10.missing/y"]},
        headers={},
        limiter=aggregator.S2_LIMITER,
        timeout=30,
    )
    assert set(resolved_map) == {"10.1/input", "10.1/returned"}
    assert resolved_map["10.1/input"] is resolved_map["10.1/returned"]
    assert resolved_map["10.1/input"].doi == "10.1/returned"
    assert "10.missing/y" not in resolved_map


def test_batch_resolve_dict_data_payload_resolves():
    with patch("pipeline.aggregator.http_post", return_value=Response({"data": [s2_entry()]})):
        resolved_map = batch_resolve(["https://doi.org/10.1/input"])

    assert resolved_map["10.1/input"].title == "Resolved title"
    assert resolved_map["10.1/returned"].s2_id == "s2-1"


def test_batch_resolve_unusable_dict_payload_skips_chunk_and_logs_warning(caplog):
    caplog.set_level(logging.WARNING, logger="pipeline.aggregator")

    with patch("pipeline.aggregator.http_post", return_value=Response({"error": "no data"})):
        resolved_map = batch_resolve(["10.1/input"])

    assert resolved_map == {}
    assert "returned unusable dict payload" in caplog.text


@pytest.mark.parametrize("payload", [{"data": None}, {"data": {}}, {"data": "bad"}])
def test_batch_resolve_dict_payload_missing_list_data_skips_chunk(payload):
    with patch("pipeline.aggregator.http_post", return_value=Response(payload)):
        assert batch_resolve(["10.1/input"]) == {}


@pytest.mark.parametrize("entry", [[], "bad entry", {"authors": ["bad author"]}, {"references": ["bad ref"]}])
def test_batch_resolve_malformed_entries_are_skipped_gracefully(entry):
    with patch("pipeline.aggregator.http_post", return_value=Response([entry])):
        assert batch_resolve(["10.1/input"]) == {}


@pytest.mark.parametrize("payload", ["bad", 123, object()])
def test_batch_resolve_non_list_non_dict_payload_skips_chunk(payload):
    with patch("pipeline.aggregator.http_post", return_value=Response(payload)):
        assert batch_resolve(["10.1/input"]) == {}


def test_batch_resolve_request_and_json_errors_skip_chunks_and_continue(caplog):
    caplog.set_level(logging.WARNING, logger="pipeline.aggregator")
    responses = [
        requests.RequestException("network down"),
        Response(json_error=ValueError("not json")),
        Response([s2_entry(doi="10.3/good")]),
    ]

    def fake_post(*args, **kwargs):
        item = responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    with patch("pipeline.aggregator.http_post", side_effect=fake_post) as post:
        resolved_map = batch_resolve(["10.1/bad", "10.2/bad", "10.3/input"], chunk_size=1)

    assert post.call_count == 3
    assert set(resolved_map) == {"10.3/input", "10.3/good"}
    assert "request failed" in caplog.text
    assert "json decode failed" in caplog.text


def test_batch_resolve_chunks_multiple_calls_and_clamps_large_chunk_size():
    payloads = [
        Response([s2_entry(doi="10.1/a")]),
        Response([s2_entry(doi="10.1/b")]),
        Response([s2_entry(doi="10.1/c")]),
    ]
    with patch("pipeline.aggregator.http_post", side_effect=payloads) as post:
        resolved_map = batch_resolve(["10.1/a", "10.1/b", "10.1/c"], chunk_size=1)

    assert post.call_count == 3
    assert {"10.1/a", "10.1/b", "10.1/c"} <= set(resolved_map)

    ids = [f"10.500/{i}" for i in range(501)]
    with patch("pipeline.aggregator.http_post", return_value=Response([])) as post:
        batch_resolve(ids, chunk_size=999)

    assert post.call_count == 2
    first_body = post.call_args_list[0].kwargs["json_body"]
    second_body = post.call_args_list[1].kwargs["json_body"]
    assert len(first_body["ids"]) == 500
    assert len(second_body["ids"]) == 1


def test_batch_resolve_zero_chunk_size_uses_current_default_behavior():
    with patch("pipeline.aggregator.http_post", return_value=Response([])) as post:
        batch_resolve(["10.1/a", "10.1/b"], chunk_size=0)

    assert post.call_count == 1
    assert len(post.call_args.kwargs["json_body"]["ids"]) == 2


def test_batch_resolve_duplicate_input_dois_do_not_crash():
    payload = [
        s2_entry(doi="10.1/Dupe", title="First", paper_id="s2-first"),
        s2_entry(doi="10.1/Dupe", title="Second", paper_id="s2-second"),
    ]

    with patch("pipeline.aggregator.http_post", return_value=Response(payload)):
        resolved_map = batch_resolve(["10.1/dupe", "https://doi.org/10.1/DUPE"])

    assert set(resolved_map) == {"10.1/dupe"}
    assert resolved_map["10.1/dupe"].s2_id == "s2-second"


def test_batch_resolve_canonical_alias_maps_input_and_returned_canonical_doi():
    with patch("pipeline.aggregator.http_post", return_value=Response([s2_entry(doi="10.1/Canonical")])):
        resolved_map = batch_resolve(["10.1/input"])

    assert resolved_map["10.1/input"] is resolved_map["10.1/canonical"]


def test_citation_from_reference_missing_doi_and_title_returns_none():
    assert _citation_from_reference({"externalIds": {"PubMed": "123"}, "title": "   "}) is None
    assert _citation_from_reference({}) is None
    assert _citation_from_reference(None) is None


def test_citation_from_reference_title_only_and_pmid_normalization():
    citation = _citation_from_reference(
        {"externalIds": {"PubMed": 456}, "title": "  Title only  ", "year": "2020"}
    )

    assert citation.doi is None
    assert citation.pmid == "456"
    assert citation.title == "Title only"
    assert citation.year == 2020


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (2024, 2024),
        ("2024", 2024),
        ("20a4", None),
        (None, None),
    ],
)
def test_coerce_year(value, expected):
    assert _coerce_year(value) == expected


def test_build_resolved_merges_topical_metadata_authors_types_and_citations():
    resolved = _build_resolved(
        s2_entry(
            doi="10.1/Main",
            externalIds={"DOI": "10.1/Main", "PubMed": " 123 "},
            fieldsOfStudy=["Biology", "Biology"],
            s2FieldsOfStudy=[
                {"category": "Medicine"},
                {"category": "Biology"},
                {"category": "Computer Science"},
                {"not_category": "ignored"},
            ],
            authors=[{"name": "Author A"}, {"name": ""}, {}, {"name": "Author B"}],
            publicationTypes=["Review", "Review", "Journal Article"],
            references=[
                {
                    "externalIds": {"DOI": "10.2/Ref", "PubMed": " 456 "},
                    "title": "Reference",
                    "year": "2021",
                },
                {"externalIds": {}, "title": "", "year": 2020},
                {"externalIds": {}, "title": "Title-only reference", "year": "not-year"},
            ],
        )
    )

    assert resolved.doi == "10.1/main"
    assert resolved.pmid == "123"
    assert resolved.authors == ["Author A", "Author B"]
    assert resolved.fields_of_study == ["Biology", "Medicine", "Computer Science"]
    assert resolved.publication_types == ["Review", "Journal Article"]
    assert len(resolved.citations) == 2
    assert resolved.citations[0].doi == "10.2/ref"
    assert resolved.citations[0].pmid == "456"
    assert resolved.citations[0].year == 2021
    assert resolved.citations[1].title == "Title-only reference"
    assert resolved.citations[1].year is None
