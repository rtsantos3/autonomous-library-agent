import subprocess
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import ingestion
from pipeline.aggregator import BatchResolved
from pipeline.citations import CitationItem, CitationResult, fetch_outbound_citations
from pipeline.ingestion import (
    DedupResult,
    ParseResult,
    ResolveResult,
    UpsertResult,
)


class Response:
    def __init__(self, payload=None, text="", status_code=200):
        self.payload = payload if payload is not None else {}
        self.text = text
        self.status_code = status_code

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError("http error")


def resolved(**overrides):
    data = {
        "title": "A microbiome paper",
        "doi": "10.1/x",
        "pmid": "123",
        "s2_id": "s2-1",
        "abstract": "Abstract",
        "authors": ["Author A", "Author B"],
        "year": "2024",
        "venue": "Journal",
        "alt_dois": [],
        "source": "input-only",
    }
    data.update(overrides)
    return ResolveResult(**data)


def citation_result(items=None):
    return CitationResult(
        source="semantic-scholar",
        retrieved_at="2026-06-23",
        items=[] if items is None else items,
    )


def pubmed_xml(
    pmid="123",
    title="Resolved title",
    abstract="Resolved abstract",
    doi="10.1/x",
    authors=None,
    year="2024",
    journal="Journal",
):
    authors = ["Author A"] if authors is None else authors
    author_xml = ""
    for author in authors:
        parts = author.split(" ", 1)
        last_name = parts[0]
        fore_name = parts[1] if len(parts) > 1 else ""
        author_xml += f"<Author><LastName>{last_name}</LastName><ForeName>{fore_name}</ForeName></Author>"
    return f"""
    <PubmedArticleSet>
      <PubmedArticle>
        <MedlineCitation>
          <PMID Version="1">{pmid}</PMID>
          <Article>
            <Journal>
              <Title>{journal}</Title>
              <ISOAbbreviation>{journal}</ISOAbbreviation>
              <JournalIssue><PubDate><Year>{year}</Year></PubDate></JournalIssue>
            </Journal>
            <ArticleTitle>{title}</ArticleTitle>
            <Abstract><AbstractText>{abstract}</AbstractText></Abstract>
            <AuthorList>{author_xml}</AuthorList>
          </Article>
        </MedlineCitation>
        <PubmedData>
          <ArticleIdList><ArticleId IdType="doi">{doi}</ArticleId></ArticleIdList>
        </PubmedData>
      </PubmedArticle>
    </PubmedArticleSet>
    """


@pytest.fixture
def live_trellis():
    try:
        subprocess.run(["trellis", "--help"], capture_output=True, text=True, check=True)
    except (FileNotFoundError, subprocess.CalledProcessError):
        pytest.skip("trellis not available")

    created_slugs = []

    def register(slug):
        if slug:
            created_slugs.append(slug)
        return slug

    yield register

    for slug in created_slugs:
        subprocess.run(["trellis", "rm", slug, "--json"], capture_output=True, text=True)


class TestIngestionUnit:
    # parse_input
    def test_parse_doi_normalized(self):
        assert ingestion.parse_input({"doi": "https://doi.org/10.1/x"}).doi == "10.1/x"

    def test_parse_doi_prefix_stripped(self):
        assert ingestion.parse_input({"doi": "doi:10.1/x"}).doi == "10.1/x"

    def test_parse_dx_doi_prefix_stripped(self):
        assert ingestion.parse_input({"doi": "http://dx.doi.org/10.1/x"}).doi == "10.1/x"

    def test_parse_pmid_only(self):
        assert ingestion.parse_input({"pmid": "123"}).pmid == "123"

    def test_parse_no_identifier_raises(self):
        with pytest.raises(ValueError):
            ingestion.parse_input({"title": "hi"})

    def test_parse_title_only_sufficient(self):
        assert ingestion.parse_input({"title": "A long enough title"}).title == "A long enough title"

    def test_parse_all_none_fields_raises(self):
        raw = {
            "title": None,
            "doi": None,
            "pmid": None,
            "abstract": None,
            "authors": None,
            "year": None,
            "venue": None,
        }
        with pytest.raises(ValueError):
            ingestion.parse_input(raw)

    def test_parse_whitespace_only_title_raises(self):
        with pytest.raises(ValueError):
            ingestion.parse_input({"title": "       "})

    def test_parse_title_exactly_ten_chars_accepted(self):
        assert ingestion.parse_input({"title": "1234567890"}).title == "1234567890"

    def test_parse_title_nine_chars_raises(self):
        with pytest.raises(ValueError):
            ingestion.parse_input({"title": "123456789"})

    def test__make_tags_emits_topical_tags(self):
        tags = ingestion._make_tags(
            resolved(
                fields_of_study=["Biology"],
                publication_types=["Review"],
                mesh_terms=["Gastrointestinal Microbiome"],
                keywords=["16S rRNA"],
            )
        )
        assert "field:biology" in tags
        assert "type:review" in tags
        assert "mesh:gastrointestinal-microbiome" in tags
        assert "kw:16s-rrna" in tags

    # resolve_identity
    def test_resolve_doi_input_attempts_s2_when_title_present(self):
        parsed = ParseResult("Title", "10.1/x", None, None, ["Author"], None, None)
        s2_payload = {
            "paperId": "s2-title-doi",
            "externalIds": {"DOI": "10.1/x"},
        }
        with patch("pipeline.ingestion._fill_from_pubmed", side_effect=lambda parsed, fields: fields["source"]) as pubmed, patch(
            "pipeline.ingestion.requests.get", return_value=Response(s2_payload)
        ) as get:
            result = ingestion.resolve_identity(parsed)
        pubmed.assert_called_once()
        get.assert_called_once()
        assert result.source == "semantic-scholar"
        assert result.title == "Title"
        assert result.s2_id == "s2-title-doi"

    def test_resolve_calls_pubmed_on_missing_title(self):
        parsed = ParseResult(None, "10.1/x", None, None, [], None, None)
        responses = [
            Response({"esearchresult": {"idlist": ["123"]}}),
            Response(text=pubmed_xml()),
            Response({"paperId": "s2", "externalIds": {"DOI": "10.1/x"}}),
        ]
        with patch("pipeline.ingestion.requests.get", side_effect=responses) as get:
            result = ingestion.resolve_identity(parsed)
        assert result.title == "Resolved title"
        assert result.source == "pubmed"
        assert get.call_count == 3

    def test_resolve_s2_hit_path_no_pubmed_call(self):
        parsed = ParseResult(None, "10.1/x", None, None, [], None, None)
        s2_payload = {
            "paperId": "s2-abc",
            "title": "Resolved from Semantic Scholar",
            "abstract": "S2 abstract",
            "authors": [{"name": "Author A"}, {"name": "Author B"}],
            "year": 2024,
            "venue": "S2 Venue",
            "externalIds": {"DOI": "10.1/x", "PubMed": "123"},
        }
        with patch("pipeline.ingestion._fill_from_pubmed", side_effect=lambda parsed, fields: fields["source"]) as pubmed, patch(
            "pipeline.ingestion.requests.get", return_value=Response(s2_payload)
        ) as get:
            result = ingestion.resolve_identity(parsed)
        pubmed.assert_called_once()
        get.assert_called_once()
        assert result.source == "semantic-scholar"
        assert result.title == "Resolved from Semantic Scholar"
        assert result.pmid == "123"
        assert result.s2_id == "s2-abc"

    def test_resolve_raises_on_no_title(self):
        parsed = ParseResult(None, "10.1/x", None, None, [], None, None)
        responses = [
            Response({"esearchresult": {"idlist": []}}),
            Response({"externalIds": {"DOI": "10.1/x"}}),
            Response({"message": {}}),
        ]
        with patch("pipeline.ingestion.requests.get", side_effect=responses):
            with pytest.raises(ValueError):
                ingestion.resolve_identity(parsed)

    def test_resolve_pubmed_and_s2_empty_responses_raise(self):
        parsed = ParseResult(None, "10.1/x", None, None, [], None, None)
        responses = [
            Response({"esearchresult": {"idlist": []}}),
            Response({}),
            Response({"message": {}}),
        ]
        with patch("pipeline.ingestion.requests.get", side_effect=responses):
            with pytest.raises(ValueError):
                ingestion.resolve_identity(parsed)

    def test_resolve_complete_doi_input_still_attempts_s2_for_s2id(self):
        parsed = ParseResult("Already complete title", "10.1/x", None, None, ["Author A"], "2024", "Journal")
        s2_payload = {
            "paperId": "s2-complete-doi",
            "externalIds": {"DOI": "10.1/x"},
        }
        with patch("pipeline.ingestion._fill_from_pubmed", side_effect=lambda parsed, fields: fields["source"]) as pubmed, patch(
            "pipeline.ingestion.requests.get", return_value=Response(s2_payload)
        ) as get:
            result = ingestion.resolve_identity(parsed)
        pubmed.assert_called_once()
        get.assert_called_once()
        assert result.source == "semantic-scholar"
        assert result.authors == ["Author A"]
        assert result.s2_id == "s2-complete-doi"

    def test_resolve_with_prefetched_seeds_from_batch_and_skips_esearch(self):
        parsed = ParseResult(None, "10.1/x", None, None, [], None, None)
        prefetched = BatchResolved(
            doi="10.1/x",
            s2_id="s2-batch-1",
            title="Batch title",
            abstract="Batch abstract",
            pmid="123",
            year="2024",
            venue="Batch Journal",
            authors=["Author A"],
            fields_of_study=["Biology"],
            publication_types=["Review"],
            citations=[CitationItem("10.2/y", None, None, "Target", 2020)],
        )
        with patch(
            "pipeline.ingestion._pubmed_fetch",
            return_value={"abstract": None, "pmid": "123", "mesh": ["X"], "keywords": []},
        ), patch("pipeline.ingestion._pubmed_search") as search:
            result = ingestion.resolve_identity(parsed, prefetched=prefetched)
        search.assert_not_called()
        assert result.title == "Batch title"
        assert result.source == "s2-batch"
        assert result.s2_id == "s2-batch-1"
        assert result.mesh_terms == ["X"]

    def test_batch_resolve_builds_map(self):
        from pipeline.aggregator import batch_resolve

        payload = [
            {
                "paperId": "s2-1",
                "title": "Batch title",
                "abstract": "Abstract",
                "year": 2024,
                "venue": "Journal",
                "authors": [{"name": "Author A"}],
                "externalIds": {"DOI": "10.1/X", "PubMed": "123"},
                "fieldsOfStudy": ["Biology"],
                "s2FieldsOfStudy": [{"category": "Medicine", "source": "s2"}],
                "publicationTypes": ["Review"],
                "references": [
                    {
                        "externalIds": {"DOI": "10.2/Y", "PubMed": "456"},
                        "title": "Reference title",
                        "year": 2020,
                    },
                    {"externalIds": {}, "title": "", "year": 2021},
                ],
            },
            None,
        ]
        with patch("pipeline.aggregator.http_post", return_value=Response(payload)) as post:
            resolved_map = batch_resolve(["10.1/x", "10.missing/y"])
        post.assert_called_once()
        assert "10.1/x" in resolved_map
        assert "10.missing/y" not in resolved_map
        result = resolved_map["10.1/x"]
        assert result.s2_id == "s2-1"
        assert result.fields_of_study == ["Biology", "Medicine"]
        assert result.publication_types == ["Review"]
        assert len(result.citations) == 1
        assert result.citations[0].doi == "10.2/y"
        assert result.citations[0].pmid == "456"

    @pytest.mark.xfail(
        reason="resolve_identity currently keeps the input DOI because _merge_missing does not overwrite populated fields",
        strict=True,
    )
    def test_resolve_s2_canonical_doi_replaces_input_doi(self):
        parsed = ParseResult(None, "10.1/input", None, None, [], None, None)
        payload = {
            "paperId": "s2-canon",
            "title": "Canonical DOI paper",
            "externalIds": {"DOI": "10.1/canonical"},
        }
        with patch("pipeline.ingestion._fill_from_pubmed", side_effect=lambda parsed, fields: fields["source"]), patch(
            "pipeline.ingestion.requests.get", return_value=Response(payload)
        ):
            result = ingestion.resolve_identity(parsed)
        assert result.doi == "10.1/canonical"

    # find_existing
    def test_find_existing_by_s2id(self):
        with patch("pipeline.ingestion.trellis.find_by_s2id", return_value={"slug": "existing", "tags": ["s2id:s2-1"]}):
            result = ingestion.find_existing(resolved())
        assert result.match_reason == "s2_id"

    def test_find_existing_by_doi(self):
        with patch("pipeline.ingestion.trellis.find_by_s2id", return_value=None), patch(
            "pipeline.ingestion.trellis.find_by_doi", return_value={"slug": "existing", "uri": "https://doi.org/10.1/x"}
        ):
            result = ingestion.find_existing(resolved(pmid=None))
        assert result.match_reason == "doi"

    def test_find_existing_by_pmid_tag(self):
        with patch("pipeline.ingestion.trellis.find_by_s2id", return_value=None), patch(
            "pipeline.ingestion.trellis.find_by_doi", return_value=None
        ), patch("pipeline.ingestion.trellis.find_by_pmid", return_value={"slug": "pmid-node", "tags": ["pmid:123"]}):
            result = ingestion.find_existing(resolved())
        assert result.match_reason == "pmid"
        assert result.existing_node["slug"] == "pmid-node"

    def test_find_existing_by_normalized_unicode_title(self):
        stored = {"slug": "title-node", "title": "Microbiome cafe study"}
        with patch("pipeline.ingestion.trellis.find_by_s2id", return_value=None), patch(
            "pipeline.ingestion.trellis.find_by_doi", return_value=None
        ), patch("pipeline.ingestion.trellis.find_by_pmid", return_value=None), patch(
            "pipeline.ingestion.trellis.find_by_title", return_value=stored
        ):
            result = ingestion.find_existing(resolved(doi=None, pmid=None, s2_id=None, title="Microbiome cafe study."))
        assert result.match_reason == "title"
        assert result.existing_node == stored

    def test_find_existing_node_with_doi_uri_prefix_matches_doi(self):
        node = {"slug": "doi-node", "uri": "doi:10.1/x"}
        with patch("pipeline.ingestion.trellis.find_by_s2id", return_value=None), patch(
            "pipeline.ingestion.trellis.find_by_doi", return_value=node
        ):
            result = ingestion.find_existing(resolved(pmid=None))
        assert result.match_reason == "doi"
        assert result.existing_node == node

    def test_find_existing_s2_empty_falls_through_to_doi(self):
        node = {"slug": "doi-node", "uri": "https://doi.org/10.1/x"}
        with patch("pipeline.ingestion.trellis.find_by_s2id", return_value=None) as s2, patch(
            "pipeline.ingestion.trellis.find_by_doi", return_value=node
        ) as doi:
            result = ingestion.find_existing(resolved())
        s2.assert_called_once_with("s2-1")
        doi.assert_called_once_with("10.1/x")
        assert result.match_reason == "doi"

    def test_find_existing_none(self):
        with patch("pipeline.ingestion.trellis.find_by_s2id", return_value=None), patch(
            "pipeline.ingestion.trellis.find_by_doi", return_value=None
        ), patch("pipeline.ingestion.trellis.find_by_pmid", return_value=None), patch(
            "pipeline.ingestion.trellis.find_by_title", return_value=None
        ):
            result = ingestion.find_existing(resolved())
        assert result.existing_node is None

    # upsert_node
    def test_upsert_creates_new_node(self):
        with patch("pipeline.ingestion.trellis.add_reference", return_value={"slug": "new"}), patch(
            "pipeline.ingestion.trellis.annotate_node"
        ):
            result = ingestion.upsert_node(resolved(), DedupResult(None, None))
        assert result == UpsertResult(slug="new", created=True)

    def test_upsert_add_reference_runtime_error_reraises(self):
        with patch("pipeline.ingestion.trellis.add_reference", side_effect=RuntimeError("trellis failed")):
            with pytest.raises(RuntimeError, match="trellis failed"):
                ingestion.upsert_node(resolved(), DedupResult(None, None))

    def test_upsert_no_doi_omits_uri_and_reference_doi_remains_none(self):
        with patch("pipeline.ingestion.trellis.add_reference", return_value={"slug": "new"}) as add, patch(
            "pipeline.ingestion.trellis.annotate_node"
        ):
            ingestion.upsert_node(resolved(doi=None), DedupResult(None, None))
        assert add.call_args.kwargs["uri"] is None
        assert add.call_args.kwargs["metadata"]["reference"]["doi"] is None

    def test_upsert_updates_existing_node(self):
        existing = {"slug": "old", "metadata": {}, "tags": []}
        with patch("pipeline.ingestion.trellis.get_node") as get_node, patch(
            "pipeline.ingestion.trellis.update_node", return_value={"slug": "old"}
        ) as update, patch(
            "pipeline.ingestion.trellis.annotate_node"
        ):
            result = ingestion.upsert_node(resolved(), DedupResult(existing, "doi"))
        assert result == UpsertResult(slug="old", created=False)
        assert update.call_args.kwargs["tags"][0] == "pipeline:scaffolded"
        get_node.assert_not_called()

    def test_upsert_preserves_existing_metadata(self):
        existing_node = {
            "slug": "old",
            "metadata": {"reference": {"doi": "10.old/x", "custom": "keep"}, "other": {"x": 1}},
            "tags": ["pipeline:queued"],
        }
        with patch("pipeline.ingestion.trellis.get_node") as get_node, patch(
            "pipeline.ingestion.trellis.update_node", return_value={"slug": "old"}
        ) as update, patch(
            "pipeline.ingestion.trellis.annotate_node"
        ):
            ingestion.upsert_node(resolved(), DedupResult(existing_node, "doi"))
        metadata = update.call_args.kwargs["metadata"]
        assert update.call_args.kwargs["tags"][0] == "pipeline:scaffolded"
        assert metadata["reference"]["doi"] == "10.old/x"
        assert metadata["reference"]["custom"] == "keep"
        assert metadata["other"] == {"x": 1}
        get_node.assert_not_called()

    def test_upsert_existing_node_empty_metadata_produces_reference_metadata(self):
        existing_node = {"slug": "old", "metadata": {}, "tags": []}
        with patch("pipeline.ingestion.trellis.get_node") as get_node, patch(
            "pipeline.ingestion.trellis.update_node", return_value={"slug": "old"}
        ) as update, patch(
            "pipeline.ingestion.trellis.annotate_node"
        ):
            ingestion.upsert_node(resolved(), DedupResult(existing_node, "doi"))
        reference = update.call_args.kwargs["metadata"]["reference"]
        assert reference["schema"] == "reference-v1"
        assert reference["doi"] == "10.1/x"
        assert reference["pmid"] == "123"
        get_node.assert_not_called()

    def test_upsert_citation_string_three_authors_joins_all_names(self):
        with patch("pipeline.ingestion.trellis.add_reference", return_value={"slug": "new"}) as add, patch(
            "pipeline.ingestion.trellis.annotate_node"
        ):
            ingestion.upsert_node(resolved(authors=["A One", "B Two", "C Three"]), DedupResult(None, None))
        assert add.call_args.kwargs["citation"].startswith("A One; B Two; C Three (2024).")

    @pytest.mark.xfail(
        reason="_citation_string currently renders missing years as n.d. instead of omitting the year segment",
        strict=True,
    )
    def test_upsert_citation_string_missing_year_omits_year_segment(self):
        with patch("pipeline.ingestion.trellis.add_reference", return_value={"slug": "new"}) as add, patch(
            "pipeline.ingestion.trellis.annotate_node"
        ):
            ingestion.upsert_node(resolved(year=None), DedupResult(None, None))
        citation = add.call_args.kwargs["citation"]
        assert "(n.d.)" not in citation
        assert "A microbiome paper" in citation

    # store_citations
    def test_store_merges_into_existing_metadata(self):
        node = {"metadata": {"reference": {"doi": "10.1/x"}, "other": {"x": 1}}}
        citations = citation_result([CitationItem("10.2/y", None, "s2", "Target", 2020)])
        with patch("pipeline.ingestion.trellis.get_node", return_value=node), patch(
            "pipeline.ingestion.trellis.update_node"
        ) as update:
            result = ingestion.store_citations("source", citations)
        metadata = update.call_args.kwargs["metadata"]
        assert result.stored == 1
        assert metadata["reference"]["doi"] == "10.1/x"
        assert metadata["reference"]["outbound_citations"]["items"][0]["title"] == "Target"
        assert metadata["other"] == {"x": 1}

    def test_store_replaces_existing_outbound_citations(self):
        node = {"metadata": {"reference": {"outbound_citations": {"items": [{"title": "Old"}]}}}}
        citations = citation_result([CitationItem(None, None, None, "New", None)])
        with patch("pipeline.ingestion.trellis.get_node", return_value=node), patch(
            "pipeline.ingestion.trellis.update_node"
        ) as update:
            ingestion.store_citations("source", citations)
        items = update.call_args.kwargs["metadata"]["reference"]["outbound_citations"]["items"]
        assert items == [{"doi": None, "pmid": None, "s2_id": None, "title": "New", "year": None}]

    def test_store_update_node_runtime_error_propagates(self):
        with patch("pipeline.ingestion.trellis.get_node", return_value={"metadata": {}}), patch(
            "pipeline.ingestion.trellis.update_node", side_effect=RuntimeError("update failed")
        ):
            with pytest.raises(RuntimeError, match="update failed"):
                ingestion.store_citations("source", citation_result())

    def test_store_empty_items_list_is_valid_outbound_citations(self):
        with patch("pipeline.ingestion.trellis.get_node", return_value={"metadata": {}}), patch(
            "pipeline.ingestion.trellis.update_node"
        ) as update:
            result = ingestion.store_citations("source", citation_result([]))
        outbound = update.call_args.kwargs["metadata"]["reference"]["outbound_citations"]
        assert result.stored == 0
        assert outbound["items"] == []
        assert outbound["source"] == "semantic-scholar"

    def test_store_citation_item_without_doi_preserves_title(self):
        item = CitationItem(doi=None, pmid=None, s2_id=None, title="Title only citation", year=2020)
        with patch("pipeline.ingestion.trellis.get_node", return_value={"metadata": {}}), patch(
            "pipeline.ingestion.trellis.update_node"
        ) as update:
            ingestion.store_citations("source", citation_result([item]))
        stored = update.call_args.kwargs["metadata"]["reference"]["outbound_citations"]["items"][0]
        assert stored["doi"] is None
        assert stored["title"] == "Title only citation"

    # link_citations
    def test_link_creates_edge_for_existing_target(self):
        citations = citation_result([CitationItem("10.2/y", None, "s2", "Target", 2020)])
        with patch("pipeline.ingestion.trellis.dedup_check", return_value={"slug": "target"}), patch(
            "pipeline.ingestion.trellis.link_nodes", return_value={"ok": True}
        ) as link:
            result = ingestion.link_citations("source", citations)
        assert result.linked == 1
        link.assert_called_once_with("source", "target", "references")

    def test_link_skips_missing_target(self):
        citations = citation_result([CitationItem("10.2/y", None, None, "Target", 2020)])
        with patch("pipeline.ingestion.trellis.dedup_check", return_value=None):
            result = ingestion.link_citations("source", citations)
        assert result.skipped == 1

    def test_link_idempotent_on_duplicate(self):
        citations = citation_result([CitationItem("10.2/y", None, "s2", "Target", 2020)])
        with patch("pipeline.ingestion.trellis.dedup_check", return_value={"slug": "target"}), patch(
            "pipeline.ingestion.trellis.link_nodes", return_value={"ok": True, "idempotent": True}
        ):
            result = ingestion.link_citations("source", citations)
        assert result.linked == 1

    def test_link_nodes_error_result_counts_as_skipped(self):
        citations = citation_result([CitationItem("10.2/y", None, "s2", "Target", 2020)])
        with patch("pipeline.ingestion.trellis.dedup_check", return_value={"slug": "target"}), patch(
            "pipeline.ingestion.trellis.link_nodes", return_value={"ok": False, "error": "boom"}
        ):
            result = ingestion.link_citations("source", citations)
        assert result.linked == 0
        assert result.skipped == 1

    def test_link_batch_counts_found_and_unfound_citations(self):
        citations = citation_result(
            [
                CitationItem("10.2/a", None, None, "Target A", 2020),
                CitationItem("10.2/b", None, None, "Target B", 2021),
                CitationItem("10.2/c", None, None, "Target C", 2022),
            ]
        )
        targets = [{"slug": "a"}, {"slug": "b"}, None]
        with patch("pipeline.ingestion.trellis.dedup_check", side_effect=targets), patch(
            "pipeline.ingestion.trellis.link_nodes", return_value={"ok": True}
        ):
            result = ingestion.link_citations("source", citations)
        assert result.linked == 2
        assert result.skipped == 1

    def test_link_s2_only_citation_calls_dedup_with_s2id_only(self):
        citations = citation_result([CitationItem(doi=None, pmid=None, s2_id="s2-only", title="", year=None)])
        with patch("pipeline.ingestion.trellis.dedup_check", return_value=None) as dedup:
            result = ingestion.link_citations("source", citations)
        dedup.assert_called_once_with(s2id="s2-only", doi=None, pmid=None, title="")
        assert result.skipped == 1

    # verify_outcome
    def test_verify_success(self):
        node = {
            "tags": ["pipeline:scaffolded"],
            "metadata": {"reference": {"outbound_citations": {"items": []}}},
        }
        with patch("pipeline.ingestion.trellis.get_node", return_value=node), patch(
            "pipeline.ingestion.trellis.grep_nodes", return_value=[]
        ):
            result = ingestion.verify_outcome("source")
        assert result.node_exists is True
        assert result.has_citation_metadata is True
        assert result.pipeline_status == "scaffolded"

    def test_verify_node_missing(self):
        with patch("pipeline.ingestion.trellis.get_node", side_effect=RuntimeError("missing")):
            result = ingestion.verify_outcome("source")
        assert result.node_exists is False

    def test_verify_node_without_pipeline_tag_returns_none_status(self):
        with patch("pipeline.ingestion.trellis.get_node", return_value={"tags": ["year:2024"], "metadata": {}}), patch(
            "pipeline.ingestion.trellis.grep_nodes", return_value=[]
        ):
            result = ingestion.verify_outcome("source")
        assert result.node_exists is True
        assert result.pipeline_status is None

    def test_verify_outbound_citations_with_items_sets_metadata_true(self):
        node = {"tags": [], "metadata": {"reference": {"outbound_citations": {"items": [{"title": "Target"}]}}}}
        with patch("pipeline.ingestion.trellis.get_node", return_value=node), patch(
            "pipeline.ingestion.trellis.grep_nodes", return_value=[]
        ):
            result = ingestion.verify_outcome("source")
        assert result.has_citation_metadata is True

    def test_verify_missing_outbound_citations_sets_metadata_false(self):
        node = {"tags": ["pipeline:scaffolded"], "metadata": {"reference": {}}}
        with patch("pipeline.ingestion.trellis.get_node", return_value=node), patch(
            "pipeline.ingestion.trellis.grep_nodes", return_value=[]
        ):
            result = ingestion.verify_outcome("source")
        assert result.node_exists is True
        assert result.has_citation_metadata is False

    # orchestrator
    def test_pipeline_stops_after_parse_failure(self):
        outcome = ingestion.ingest_reference_pipeline({"title": "hi"})
        assert outcome.errors
        assert outcome.resolve is None

    def test_pipeline_stops_after_resolve_failure(self):
        with patch("pipeline.ingestion.resolve_identity", side_effect=ValueError("no title")):
            outcome = ingestion.ingest_reference_pipeline({"doi": "10.1/x"})
        assert outcome.errors
        assert outcome.upsert is None

    def test_pipeline_full_success(self):
        citations = citation_result()
        with patch(
            "pipeline.ingestion.parse_input", return_value=ParseResult("Title", "10.1/x", None, None, [], None, None)
        ), patch("pipeline.ingestion.resolve_identity", return_value=resolved()), patch(
            "pipeline.ingestion.find_existing", return_value=DedupResult(None, None)
        ), patch("pipeline.ingestion.upsert_node", return_value=UpsertResult("source", True)), patch(
            "pipeline.ingestion.fetch_outbound_citations", return_value=citations
        ), patch("pipeline.ingestion.store_citations", return_value=ingestion.CitationStoreResult(0)), patch(
            "pipeline.ingestion.link_citations", return_value=ingestion.LinkResult(0, 0)
        ), patch(
            "pipeline.ingestion.verify_outcome", return_value=ingestion.VerifyResult(True, True, "scaffolded", 0)
        ):
            outcome = ingestion.ingest_reference_pipeline({"doi": "10.1/x"})
        assert not outcome.errors
        assert all(
            asdict(part) is not None
            for part in [
                outcome.parse,
                outcome.resolve,
                outcome.dedup,
                outcome.upsert,
                outcome.citation_store,
                outcome.link,
                outcome.verify,
            ]
        )

    def test_pipeline_upsert_runtime_error_skips_citation_storage(self):
        with patch("pipeline.ingestion.parse_input", return_value=ParseResult(None, "10.1/x", None, None, [], None, None)), patch(
            "pipeline.ingestion.resolve_identity", return_value=resolved()
        ), patch("pipeline.ingestion.find_existing", return_value=DedupResult(None, None)), patch(
            "pipeline.ingestion.upsert_node", side_effect=RuntimeError("upsert failed")
        ), patch("pipeline.ingestion.store_citations") as store:
            outcome = ingestion.ingest_reference_pipeline({"doi": "10.1/x"})
        assert outcome.errors == ["upsert failed"]
        store.assert_not_called()

    def test_pipeline_store_citations_runtime_error_skips_linking(self):
        with patch("pipeline.ingestion.parse_input", return_value=ParseResult(None, "10.1/x", None, None, [], None, None)), patch(
            "pipeline.ingestion.resolve_identity", return_value=resolved()
        ), patch("pipeline.ingestion.find_existing", return_value=DedupResult(None, None)), patch(
            "pipeline.ingestion.upsert_node", return_value=UpsertResult("source", True)
        ), patch("pipeline.ingestion.fetch_outbound_citations", return_value=citation_result()), patch(
            "pipeline.ingestion.store_citations", side_effect=RuntimeError("store failed")
        ), patch(
            "pipeline.ingestion.link_citations"
        ) as link:
            outcome = ingestion.ingest_reference_pipeline({"doi": "10.1/x"})
        assert outcome.errors == ["store failed"]
        link.assert_not_called()

    def test_pipeline_full_run_existing_dedup_created_false_path(self):
        with patch("pipeline.ingestion.parse_input", return_value=ParseResult(None, "10.1/x", None, None, [], None, None)), patch(
            "pipeline.ingestion.resolve_identity", return_value=resolved()
        ), patch(
            "pipeline.ingestion.find_existing", return_value=DedupResult({"slug": "existing"}, "doi")
        ), patch("pipeline.ingestion.upsert_node", return_value=UpsertResult("existing", False)), patch(
            "pipeline.ingestion.fetch_outbound_citations", return_value=citation_result()
        ), patch("pipeline.ingestion.store_citations", return_value=ingestion.CitationStoreResult(0)), patch(
            "pipeline.ingestion.link_citations", return_value=ingestion.LinkResult(0, 0)
        ), patch(
            "pipeline.ingestion.verify_outcome", return_value=ingestion.VerifyResult(True, True, "scaffolded", 0)
        ):
            outcome = ingestion.ingest_reference_pipeline({"doi": "10.1/x"})
        assert outcome.errors == []
        assert outcome.upsert.created is False
        assert outcome.upsert.slug == "existing"

    def test_pipeline_does_not_create_stub_nodes_for_unresolved_citations(self):
        citations = citation_result(
            [
                CitationItem("10.2/a", None, None, "Unresolved A", 2020),
                CitationItem(None, None, "s2-b", "Unresolved B", 2021),
            ]
        )
        with patch("pipeline.ingestion.resolve_identity", return_value=resolved()), patch(
            "pipeline.ingestion.find_existing", return_value=DedupResult(None, None)
        ), patch("pipeline.ingestion.trellis.add_reference", return_value={"slug": "source"}) as add, patch(
            "pipeline.ingestion.trellis.annotate_node"
        ), patch("pipeline.ingestion.fetch_outbound_citations", return_value=citations), patch(
            "pipeline.ingestion.trellis.get_node", return_value={"metadata": {"reference": {}}, "tags": ["pipeline:scaffolded"]}
        ), patch("pipeline.ingestion.trellis.update_node"), patch(
            "pipeline.ingestion.trellis.dedup_check", return_value=None
        ), patch(
            "pipeline.ingestion.trellis.grep_nodes", return_value=[]
        ):
            outcome = ingestion.ingest_reference_pipeline({"doi": "10.1/x"})
        assert outcome.errors == []
        assert outcome.upsert.slug == "source"
        add.assert_called_once()


@pytest.mark.integration
class TestIngestionIntegration:
    def test_parse_and_resolve_real_doi(self, live_trellis):
        parsed = ingestion.parse_input({"doi": "10.1038/s41564-023-01464-1"})
        result = ingestion.resolve_identity(parsed)
        assert isinstance(result.title, str) and result.title
        assert result.doi == "10.1038/s41564-023-01464-1"
        assert result.pmid is not None
        assert result.source in ("pubmed", "semantic-scholar", "input-only")

    def test_citations_fetch_for_real_doi(self, live_trellis):
        result = fetch_outbound_citations("10.1038/nature11225")
        assert result.source == "semantic-scholar"
        assert isinstance(result.items, list)
        assert len(result.items) > 0
        assert all(isinstance(item.title, str) and item.title for item in result.items)
        assert result.retrieved_at == date.today().isoformat()

    def test_full_pipeline_end_to_end_on_new_real_doi(self, live_trellis):
        parsed = ingestion.parse_input({"doi": "10.1073/pnas.2304441120"})
        resolved_real = ingestion.resolve_identity(parsed)
        if ingestion.find_existing(resolved_real).existing_node:
            pytest.skip("real DOI already exists in Trellis")

        outcome = ingestion.ingest_reference_pipeline({"doi": "10.1073/pnas.2304441120"})
        assert outcome.errors == []
        assert outcome.upsert.created is True
        assert outcome.upsert.slug is not None
        assert outcome.citation_store.stored >= 0

        live_trellis(outcome.upsert.slug)
        verify = ingestion.verify_outcome(outcome.upsert.slug)
        assert verify.node_exists is True
        assert verify.pipeline_status == "scaffolded"

    def test_idempotency_on_existing_node(self, live_trellis):
        outcome = ingestion.ingest_reference_pipeline({"doi": "10.1038/s41564-023-01464-1"})
        assert outcome.errors == []
        assert outcome.upsert.created is False
        assert outcome.upsert.slug == "akkermansia-muciniphila-exacerbates-food-allergy-in-fibre-deprived-mice"

    def test_no_stub_nodes_created_for_unresolved_citations(self, live_trellis):
        before = ingestion.trellis.find_nodes(tag="pipeline:queued")
        outcome = ingestion.ingest_reference_pipeline({"doi": "10.1038/s42255-023-00744-8"})
        assert outcome.errors == []
        live_trellis(outcome.upsert.slug)

        after = ingestion.trellis.find_nodes(tag="pipeline:queued")
        assert len(after) == len(before)
