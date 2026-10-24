"""Title fallbacks after Crossref: DataCite and OpenAlex metadata providers."""

import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import ingestion  # noqa: E402
from pipeline.providers import datacite, openalex  # noqa: E402


def test_datacite_metadata_is_normalized():
    found = datacite.parse_metadata(
        {
            "data": {
                "attributes": {
                    "doi": "10.18130/V3P499",
                    "titles": [{"title": " A Thesis Title "}],
                    "creators": [
                        {"givenName": "John", "familyName": "Knight"},
                        {"name": "Leveson, Nancy"},
                    ],
                    "publicationYear": 1985,
                    "publisher": "University of Virginia",
                    "container": {},
                    "descriptions": [
                        {"descriptionType": "Other", "description": "not this"},
                        {"descriptionType": "Abstract", "description": "The abstract."},
                    ],
                }
            }
        }
    )
    assert found == {
        "title": "A Thesis Title",
        "doi": "10.18130/v3p499",
        "authors": ["John Knight", "Leveson, Nancy"],
        "year": "1985",
        "venue": "University of Virginia",
        "abstract": "The abstract.",
    }
    assert datacite.parse_metadata({"errors": [{"status": "404"}]}) is None


def test_openalex_metadata_rebuilds_abstract_and_needs_a_title():
    found = openalex.parse_metadata(
        {
            "title": "Counting Cycles with AI",
            "doi": "https://doi.org/10.48550/arxiv.2505.17964",
            "publication_year": 2025,
            "authorships": [{"author": {"display_name": "Jiashun Jin"}}],
            "primary_location": {"source": {"display_name": "arXiv"}},
            "abstract_inverted_index": {"Cycle": [0], "counts": [1], "matter": [2]},
        }
    )
    assert found["title"] == "Counting Cycles with AI"
    assert found["doi"] == "10.48550/arxiv.2505.17964"
    assert found["authors"] == ["Jiashun Jin"]
    assert found["year"] == "2025"
    assert found["venue"] == "arXiv"
    assert found["abstract"] == "Cycle counts matter"
    assert openalex.parse_metadata({"title": None}) is None


def _fields(doi="10.48550/arxiv.1", title=None):
    return {"doi": doi, "title": title, "authors": [], "source": "input-only"}


def test_fallbacks_try_in_order_and_stop_at_the_first_title():
    calls = []

    def empty(doi):
        calls.append("first")
        return None

    def titled(doi):
        calls.append("second")
        return {"title": "Found Title", "doi": "10.9/other", "year": "2025"}

    def never(doi):
        calls.append("third")
        return {"title": "Too Late"}

    chain = (("first", empty), ("second", titled), ("third", never))
    fields = _fields()
    with patch.object(ingestion, "METADATA_FALLBACKS", chain):
        source = ingestion._fill_from_fallbacks(fields)
    assert calls == ["first", "second"]
    assert fields["title"] == "Found Title"
    assert fields["doi"] == "10.48550/arxiv.1"  # requested DOI kept
    assert source == "second"


def test_a_failing_fallback_does_not_stop_the_chain():
    def broken(doi):
        raise RuntimeError("provider down")

    chain = (("broken", broken), ("ok", lambda doi: {"title": "Recovered"}))
    fields = _fields()
    with patch.object(ingestion, "METADATA_FALLBACKS", chain):
        assert ingestion._fill_from_fallbacks(fields) == "ok"
    assert fields["title"] == "Recovered"


def test_fallbacks_are_skipped_without_a_doi():
    def must_not_run(doi):
        raise AssertionError("called without a DOI")

    fields = _fields(doi=None)
    with patch.object(ingestion, "METADATA_FALLBACKS", (("x", must_not_run),)):
        assert ingestion._fill_from_fallbacks(fields) == "input-only"


def test_doi_only_record_resolves_title_when_crossref_has_none():
    # The vault failure: a DataCite DOI unknown to PubMed, S2, and Crossref.
    parsed = ingestion.parse_input({"doi": "10.18130/v3p499"})
    chain = (("datacite", lambda doi: {"title": "An Experimental Evaluation"}),)
    with patch(
        "pipeline.ingestion._fill_from_pubmed",
        side_effect=lambda parsed, fields: fields["source"],
    ), patch(
        "pipeline.ingestion._fill_from_s2", side_effect=lambda fields: fields["source"]
    ), patch(
        "pipeline.ingestion._fill_from_crossref",
        side_effect=lambda fields: fields["source"],
    ), patch.object(
        ingestion, "METADATA_FALLBACKS", chain
    ):
        result = ingestion.resolve_identity(parsed)
    assert result.title == "An Experimental Evaluation"
    assert result.doi == "10.18130/v3p499"
    assert result.source == "datacite"
