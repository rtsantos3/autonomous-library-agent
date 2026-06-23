import sys
from pathlib import Path
from unittest.mock import patch

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import ingestion


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


def fields(**overrides):
    data = {
        "title": None,
        "doi": "10.1/input",
        "pmid": None,
        "s2_id": None,
        "abstract": None,
        "authors": [],
        "year": None,
        "venue": None,
        "alt_dois": [],
        "source": "input-only",
        "keywords": [],
    }
    data.update(overrides)
    return data


def test_prefer_canonical_doi_promotes_candidate_and_retains_old_doi():
    data = fields(doi="10.1/input")

    ingestion._prefer_canonical_doi(data, "https://doi.org/10.1/canonical")

    assert data["doi"] == "10.1/canonical"
    assert data["alt_dois"] == ["10.1/input"]


def test_prefer_canonical_doi_equal_candidate_does_not_change_fields():
    data = fields(doi="10.1/input")

    ingestion._prefer_canonical_doi(data, "10.1/input")

    assert data["doi"] == "10.1/input"
    assert data["alt_dois"] == []


def test_prefer_canonical_doi_empty_candidate_does_not_change_fields():
    for candidate in ("", None):
        data = fields(doi="10.1/input")

        ingestion._prefer_canonical_doi(data, candidate)

        assert data["doi"] == "10.1/input"
        assert data["alt_dois"] == []


def test_prefer_canonical_doi_initially_empty_sets_doi_without_alt_doi():
    data = fields(doi=None)

    ingestion._prefer_canonical_doi(data, "10.1/canonical")

    assert data["doi"] == "10.1/canonical"
    assert data["alt_dois"] == []


def test_fill_from_crossref_fills_missing_fields_and_extends_keywords():
    payload = {
        "message": {
            "title": ["Crossref title"],
            "DOI": "https://doi.org/10.1/crossref",
            "author": [
                {"given": "Ada", "family": "Lovelace"},
                {"family": "Darwin"},
                {"given": "Ignored"},
            ],
            "published": {"date-parts": [[2024, 6, 1]]},
            "container-title": ["Crossref Journal"],
            "abstract": "Crossref abstract",
            "subject": ["Microbiome", "Metagenomics", "Microbiome"],
        }
    }
    data = fields(doi="10.1/input", keywords=["Existing"])

    with patch("pipeline.ingestion.http_get", return_value=Response(payload)) as get:
        source = ingestion._fill_from_crossref(data)

    get.assert_called_once()
    assert source == "crossref"
    assert data["title"] == "Crossref title"
    assert data["doi"] == "10.1/input"
    assert data["authors"] == ["Ada Lovelace", "Darwin"]
    assert data["year"] == "2024"
    assert data["venue"] == "Crossref Journal"
    assert data["abstract"] == "Crossref abstract"
    assert data["keywords"] == ["Existing", "Microbiome", "Metagenomics"]


def test_fill_from_crossref_request_failure_returns_prior_source_unchanged():
    data = fields(source="semantic-scholar")
    before = dict(data)

    with patch("pipeline.ingestion.http_get", side_effect=requests.RequestException("boom")):
        source = ingestion._fill_from_crossref(data)

    assert source == "semantic-scholar"
    assert data == before


def test_fill_from_crossref_json_failure_returns_prior_source_unchanged():
    data = fields(source="pubmed")
    before = dict(data)

    with patch("pipeline.ingestion.http_get", return_value=JsonErrorResponse()):
        source = ingestion._fill_from_crossref(data)

    assert source == "pubmed"
    assert data == before


def test_crossref_year_uses_published_first():
    assert ingestion._crossref_year({"published": {"date-parts": [[2025, 1, 2]]}}) == "2025"


def test_crossref_year_uses_published_print_when_published_missing():
    assert ingestion._crossref_year({"published-print": {"date-parts": [["2024"]]}}) == "2024"


def test_crossref_year_uses_published_online_when_other_dates_missing():
    msg = {"published": {"date-parts": [[]]}, "published-online": {"date-parts": [[2023]]}}

    assert ingestion._crossref_year(msg) == "2023"


def test_crossref_year_returns_none_for_missing_or_invalid_year():
    assert ingestion._crossref_year({}) is None
    assert ingestion._crossref_year({"published": {"date-parts": [["24"]]}}) is None
    assert ingestion._crossref_year({"published": {"date-parts": [[999]]}}) is None
