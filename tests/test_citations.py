import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.citations import (  # noqa: E402
    CitationItem,
    CitationResult,
    fetch_outbound_citations,
)


class Response:
    def __init__(self, payload=None, text="", status_code=200):
        self.payload = payload if payload is not None else {}
        self.text = text
        self.status_code = status_code

    def json(self):
        return self.payload


class JsonErrorResponse(Response):
    def json(self):
        raise ValueError("bad json")


def test_fetch_outbound_citations_empty_doi_returns_empty_result():
    result = fetch_outbound_citations("")

    assert result == CitationResult(
        source="semantic-scholar",
        retrieved_at=date.today().isoformat(),
        items=[],
    )


def test_fetch_outbound_citations_http_error_returns_failed_source():
    with patch(
        "pipeline.citations.http_get", side_effect=requests.RequestException("boom")
    ):
        result = fetch_outbound_citations("10.1/x")

    assert result == CitationResult(
        source="semantic-scholar-failed",
        retrieved_at=date.today().isoformat(),
        items=[],
    )


def test_fetch_outbound_citations_json_error_returns_failed_source():
    with patch("pipeline.citations.http_get", return_value=JsonErrorResponse()):
        result = fetch_outbound_citations("10.1/x")

    assert result == CitationResult(
        source="semantic-scholar-failed",
        retrieved_at=date.today().isoformat(),
        items=[],
    )


@patch("pipeline.citations.http_get")
@pytest.mark.parametrize("payload", [[], "not a dict"])
def test_fetch_outbound_citations_malformed_top_level_payload_returns_empty(
    http_get, payload
):
    http_get.return_value = Response(payload)

    result = fetch_outbound_citations("10.1/x")

    assert result.items == []
    assert result.source in {"semantic-scholar", "semantic-scholar-failed"}


@patch("pipeline.citations.http_get")
@pytest.mark.parametrize(
    "payload", [{"other": []}, {"data": "not a list"}, {"data": {"bad": "shape"}}]
)
def test_fetch_outbound_citations_missing_or_non_list_data_returns_empty(
    http_get, payload
):
    http_get.return_value = Response(payload)

    result = fetch_outbound_citations("10.1/x")

    assert result.items == []
    assert result.source in {"semantic-scholar", "semantic-scholar-failed"}


def test_fetch_outbound_citations_item_missing_cited_paper_returns_empty():
    with patch(
        "pipeline.citations.http_get",
        return_value=Response({"data": [{"notCitedPaper": {}}]}),
    ):
        result = fetch_outbound_citations("10.1/x")

    assert result == CitationResult(
        source="semantic-scholar",
        retrieved_at=date.today().isoformat(),
        items=[],
    )


def test_fetch_outbound_citations_parses_reference_payload():
    payload = {
        "data": [
            {
                "citedPaper": {
                    "paperId": "s2-1",
                    "title": " Cited paper ",
                    "year": 2020,
                    "externalIds": {"DOI": " 10.2/ABC ", "PubMed": 12345},
                }
            },
            {
                "citedPaper": {
                    "paperId": "s2-2",
                    "title": "",
                    "year": 2021,
                    "externalIds": {"PubMed": " 67890 "},
                }
            },
            {
                "citedPaper": {
                    "paperId": "s2-3",
                    "title": "Title-only paper",
                    "year": None,
                    "externalIds": {"PubMed": "   "},
                }
            },
            {
                "citedPaper": {
                    "paperId": "s2-skip",
                    "title": "   ",
                    "year": 2022,
                    "externalIds": {},
                }
            },
            None,
        ]
    }

    with patch("pipeline.citations.http_get", return_value=Response(payload)) as get:
        result = fetch_outbound_citations("10.1/x")

    get.assert_called_once()
    assert result == CitationResult(
        source="semantic-scholar",
        retrieved_at=date.today().isoformat(),
        items=[
            CitationItem(
                doi="10.2/abc",
                pmid="12345",
                s2_id="s2-1",
                title="Cited paper",
                year=2020,
            ),
            CitationItem(
                doi=None,
                pmid=None,
                s2_id="s2-3",
                title="Title-only paper",
                year=None,
            ),
        ],
    )
