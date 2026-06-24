import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import ingestion
from pipeline.ingestion import CitationStoreResult, IngestionOutcome, LinkResult


def node(slug, uri=None, tags=None, metadata=None):
    return {
        "slug": slug,
        "uri": uri,
        "tags": tags or ["pipeline:scaffolded"],
        "metadata": metadata or {"reference": {}},
    }


def make_outcome(linked=0, stored=0, errors=None):
    out = IngestionOutcome()
    out.link = LinkResult(linked=linked, skipped=0)
    out.citation_store = CitationStoreResult(stored=stored)
    if errors:
        out.errors = list(errors)
    return out


def run_backfill(nodes, ingest_outcomes=None, only_missing=False):
    # backfill delegates to the same ingest_batch pipeline as fresh instantiation;
    # patch it and capture the DOIs it is handed.
    ingest_outcomes = [] if ingest_outcomes is None else ingest_outcomes
    with patch(
        "pipeline.ingestion.trellis.get_by_pipeline_status", return_value=nodes
    ), patch(
        "pipeline.ingestion.ingest_batch", return_value=(ingest_outcomes, object())
    ) as ingest_batch:
        outcomes, result = ingestion.backfill_nodes(workers=2, only_missing=only_missing)
    return outcomes, result, ingest_batch


def test_full_scan_passes_all_doi_nodes_to_ingest_batch_and_aggregates():
    nodes = [node("a", uri="doi:10.1/a"), node("b", uri="doi:10.1/b")]
    ingest_outcomes = [make_outcome(linked=3, stored=10), make_outcome(linked=2, stored=7)]
    outcomes, result, ingest_batch = run_backfill(nodes, ingest_outcomes)

    ingest_batch.assert_called_once_with(["10.1/a", "10.1/b"], workers=2)
    assert result.candidates == 2
    assert result.resolvable == 2
    assert result.processed == 2
    assert result.edges_linked == 5
    assert result.citations_stored == 17
    assert result.errors == []
    assert outcomes is ingest_outcomes


def test_only_missing_skips_already_topical_nodes():
    nodes = [
        node("tagged", uri="doi:10.1/tagged", tags=["pipeline:scaffolded", "mesh:gut"]),
        node("missing", uri="doi:10.1/missing"),
    ]
    _outcomes, result, ingest_batch = run_backfill(
        nodes, [make_outcome(linked=1, stored=1)], only_missing=True
    )

    assert result.skipped_already_tagged == 1
    assert result.resolvable == 1
    ingest_batch.assert_called_once_with(["10.1/missing"], workers=2)


def test_node_without_doi_is_skipped_and_excluded():
    nodes = [node("no-doi", uri=None, metadata={"reference": {}})]
    outcomes, result, ingest_batch = run_backfill(nodes)

    assert result.skipped_no_doi == 1
    assert result.resolvable == 0
    assert outcomes == []
    ingest_batch.assert_not_called()


def test_outcome_errors_are_isolated_and_not_counted_as_processed():
    nodes = [node("ok", uri="doi:10.1/ok"), node("bad", uri="doi:10.1/bad")]
    ingest_outcomes = [
        make_outcome(linked=4, stored=9),
        make_outcome(errors=["boom"]),
    ]
    _outcomes, result, _ingest_batch = run_backfill(nodes, ingest_outcomes)

    assert result.processed == 1
    assert result.edges_linked == 4
    assert result.citations_stored == 9
    assert result.errors == ["boom"]


def test_doi_extraction_precedence_uri_before_metadata_before_tag():
    nodes = [
        node(
            "all",
            uri="doi:10.uri/a",
            tags=["pipeline:scaffolded", "doi:10.tag/c"],
            metadata={"reference": {"doi": "10.meta/b"}},
        ),
        node(
            "meta",
            uri=None,
            tags=["pipeline:scaffolded", "doi:10.tag/e"],
            metadata={"reference": {"doi": "https://doi.org/10.meta/d"}},
        ),
        node(
            "tag",
            uri=None,
            tags=["pipeline:scaffolded", "doi:10.tag/f"],
            metadata={"reference": {}},
        ),
    ]
    _outcomes, result, ingest_batch = run_backfill(nodes, [make_outcome(), make_outcome(), make_outcome()])

    assert result.resolvable == 3
    ingest_batch.assert_called_once_with(["10.uri/a", "10.meta/d", "10.tag/f"], workers=2)
