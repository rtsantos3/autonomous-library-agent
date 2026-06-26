import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from unittest.mock import patch

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import ingestion  # noqa: E402
from pipeline.aggregator import BatchResolved  # noqa: E402


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

    with patch(
        "pipeline.ingestion.http_get", side_effect=requests.RequestException("boom")
    ):
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


def test_fill_from_crossref_missing_message_does_not_crash():
    data = fields(source="input-only")

    with patch("pipeline.ingestion.http_get", return_value=Response({"status": "ok"})):
        source = ingestion._fill_from_crossref(data)

    assert source == "crossref"
    assert data["title"] is None
    assert data["doi"] == "10.1/input"
    assert data["keywords"] == []


def test_crossref_year_uses_published_first():
    assert (
        ingestion._crossref_year({"published": {"date-parts": [[2025, 1, 2]]}})
        == "2025"
    )


def test_crossref_year_uses_published_print_when_published_missing():
    assert (
        ingestion._crossref_year({"published-print": {"date-parts": [["2024"]]}})
        == "2024"
    )


def test_crossref_year_uses_published_online_when_other_dates_missing():
    msg = {
        "published": {"date-parts": [[]]},
        "published-online": {"date-parts": [[2023]]},
    }

    assert ingestion._crossref_year(msg) == "2023"


def test_crossref_year_returns_none_for_missing_or_invalid_year():
    assert ingestion._crossref_year({}) is None
    assert ingestion._crossref_year({"published": {"date-parts": [["24"]]}}) is None
    assert ingestion._crossref_year({"published": {"date-parts": [[999]]}}) is None


@pytest.mark.parametrize("payload", [[], "not a dict"])
def test_fill_from_s2_malformed_payload_returns_prior_source(payload):
    data = fields(source="pubmed", title="Existing title")
    before = dict(data)

    with patch("pipeline.ingestion.http_get", return_value=Response(payload)):
        source = ingestion._fill_from_s2(data)

    assert source == "pubmed"
    assert data == before


def test_fill_from_s2_missing_external_ids_still_fills_available_fields():
    data = fields(source="input-only")
    payload = {
        "paperId": "s2-1",
        "title": "S2 title",
        "abstract": "S2 abstract",
        "authors": [{"name": "Author A"}, {}],
        "year": 2024,
        "venue": "S2 Journal",
        "fieldsOfStudy": ["Biology"],
        "s2FieldsOfStudy": [{"category": "Medicine"}],
        "publicationTypes": ["Journal Article"],
    }

    with patch("pipeline.ingestion.http_get", return_value=Response(payload)):
        source = ingestion._fill_from_s2(data)

    assert source == "semantic-scholar"
    assert data["title"] == "S2 title"
    assert data["doi"] == "10.1/input"
    assert data["pmid"] is None
    assert data["s2_id"] == "s2-1"
    assert data["authors"] == ["Author A"]
    assert data["year"] == "2024"
    assert data["venue"] == "S2 Journal"
    assert data["fields_of_study"] == ["Biology", "Medicine"]
    assert data["publication_types"] == ["Journal Article"]


def test_fill_from_pubmed_malformed_xml_returns_prior_source_unchanged():
    parsed = ingestion.ParseResult(
        title=None,
        doi=None,
        pmid="123",
        abstract=None,
        authors=[],
        year=None,
        venue=None,
    )
    data = fields(doi=None, source="semantic-scholar")
    before = dict(data)

    with patch(
        "pipeline.ingestion.http_get", return_value=Response(text="<PubmedArticleSet>")
    ):
        source = ingestion._fill_from_pubmed(parsed, data)

    assert source == "semantic-scholar"
    assert data == before


def test_pubmed_fetch_missing_title_and_medline_date_year_fallback():
    xml = """
    <PubmedArticleSet>
      <PubmedArticle>
        <MedlineCitation>
          <PMID Version="1">123</PMID>
          <Article>
            <Journal>
              <Title>Journal Name</Title>
            </Journal>
            <Abstract>
              <AbstractText>Abstract text.</AbstractText>
            </Abstract>
            <AuthorList>
              <Author><LastName>Smith</LastName><ForeName>Ada</ForeName></Author>
            </AuthorList>
          </Article>
          <DateCompleted>
            <Year>1999</Year>
          </DateCompleted>
        </MedlineCitation>
        <PubmedData>
          <ArticleIdList>
            <ArticleId IdType="doi">10.1/PubMed</ArticleId>
          </ArticleIdList>
          <History>
            <PubMedPubDate PubStatus="pubmed">
              <Year>1999</Year>
            </PubMedPubDate>
          </History>
        </PubmedData>
      </PubmedArticle>
      <PubDate>
        <MedlineDate>2020 Jan-Feb</MedlineDate>
      </PubDate>
    </PubmedArticleSet>
    """

    with patch("pipeline.ingestion.http_get", return_value=Response(text=xml)):
        fetched = ingestion._pubmed_fetch("123")

    assert fetched["title"] is None
    assert fetched["year"] == "2020"
    assert fetched["pmid"] == "123"
    assert fetched["doi"] == "10.1/pubmed"
    assert fetched["abstract"] == "Abstract text."
    assert fetched["authors"] == ["Smith Ada"]


def test_resolve_identity_prefetched_pubmed_failure_preserves_s2_batch_source():
    parsed = ingestion.ParseResult(
        title=None,
        doi="10.1/input",
        pmid=None,
        abstract=None,
        authors=[],
        year=None,
        venue=None,
    )
    prefetched = BatchResolved(
        doi="10.1/canonical",
        s2_id="s2-1",
        title="Prefetched title",
        abstract="Prefetched abstract",
        pmid="123",
        year="2024",
        venue="Prefetched Journal",
        authors=["Author A"],
        fields_of_study=["Biology"],
        publication_types=["Journal Article"],
        citations=[],
    )

    with patch(
        "pipeline.ingestion._pubmed_fetch", side_effect=ET.ParseError("bad xml")
    ):
        result = ingestion.resolve_identity(parsed, prefetched=prefetched)

    assert result.source == "s2-batch"
    assert result.title == "Prefetched title"
    assert result.doi == "10.1/canonical"
    assert result.pmid == "123"
    assert result.s2_id == "s2-1"
    assert result.abstract == "Prefetched abstract"
    assert result.authors == ["Author A"]
    assert result.year == "2024"
    assert result.venue == "Prefetched Journal"
