import sys
from pathlib import Path
from unittest.mock import call, patch

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


def run_backfill(nodes, ingest_outcomes=None, only_missing=False, statuses=None, chunk_size=100):
    # backfill delegates to the same ingest_batch pipeline as fresh instantiation;
    # patch it and capture the DOIs it is handed.
    ingest_outcomes = [] if ingest_outcomes is None else ingest_outcomes
    if isinstance(nodes, dict):
        nodes_by_status = nodes
    else:
        nodes_by_status = {
            "queued": [],
            "scaffolded": nodes,
            "failed": [],
        }

    def get_by_status(status):
        return nodes_by_status.get(status, [])

    kwargs = {"workers": 2, "only_missing": only_missing, "chunk_size": chunk_size}
    if statuses is not None:
        kwargs["statuses"] = statuses

    with patch(
        "pipeline.ingestion.trellis.get_by_pipeline_status", side_effect=get_by_status
    ) as get_by_pipeline_status, patch(
        "pipeline.ingestion.ingest_batch", return_value=(ingest_outcomes, object())
    ) as ingest_batch:
        outcomes, result = ingestion.backfill_nodes(**kwargs)
    return outcomes, result, ingest_batch, get_by_pipeline_status


def test_full_scan_passes_all_doi_nodes_to_ingest_batch_and_aggregates():
    nodes = [node("a", uri="doi:10.1/a"), node("b", uri="doi:10.1/b")]
    ingest_outcomes = [make_outcome(linked=3, stored=10), make_outcome(linked=2, stored=7)]
    outcomes, result, ingest_batch, _get_by_status = run_backfill(nodes, ingest_outcomes)

    ingest_batch.assert_called_once_with(["10.1/a", "10.1/b"], workers=2)
    assert result.candidates == 2
    assert result.resolvable == 2
    assert result.processed == 2
    assert result.edges_linked == 5
    assert result.citations_stored == 17
    assert result.failed == 0
    assert result.needs_review == 0
    assert result.errors == []
    assert outcomes == ingest_outcomes


def test_only_missing_skips_already_topical_nodes():
    nodes = [
        node("tagged", uri="doi:10.1/tagged", tags=["pipeline:scaffolded", "mesh:gut"]),
        node("missing", uri="doi:10.1/missing"),
    ]
    _outcomes, result, ingest_batch, _get_by_status = run_backfill(
        nodes, [make_outcome(linked=1, stored=1)], only_missing=True
    )

    assert result.skipped_already_tagged == 1
    assert result.resolvable == 1
    ingest_batch.assert_called_once_with(["10.1/missing"], workers=2)


def test_node_without_doi_is_skipped_and_excluded():
    nodes = [node("no-doi", uri=None, metadata={"reference": {}})]
    outcomes, result, ingest_batch, _get_by_status = run_backfill(nodes)

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
    _outcomes, result, _ingest_batch, _get_by_status = run_backfill(nodes, ingest_outcomes)

    assert result.processed == 1
    assert result.edges_linked == 4
    assert result.citations_stored == 9
    assert result.errors == ["boom"]
    assert result.failed == 1
    assert result.needs_review == 0


def test_failure_classifier_splits_permanent_from_transient_errors():
    permanent = [
        ["Could not resolve a title for the reference"],
        ["Provide a DOI, PMID, or title of at least 10 characters"],
        ["404 not found"],
        ["invalid DOI"],
        ["malformed identifier"],
        ["no DOI available"],
    ]
    for errors in permanent:
        assert ingestion._classify_failure(errors) == "needs-review"

    transient = [
        ["database is locked"],
        ["connection timed out"],
        ["Semantic Scholar 503"],
        ["RuntimeError: busy"],
    ]
    for errors in transient:
        assert ingestion._classify_failure(errors) == "failed"


def test_backfill_error_counters_split_failed_and_needs_review():
    nodes = [
        node("permanent", uri="doi:10.1/permanent"),
        node("transient", uri="doi:10.1/transient"),
    ]
    ingest_outcomes = [
        make_outcome(errors=["Could not resolve a title for the reference"]),
        make_outcome(errors=["database is locked"]),
    ]
    _outcomes, result, _ingest_batch, _get_by_status = run_backfill(nodes, ingest_outcomes)

    assert result.processed == 0
    assert result.failed == 1
    assert result.needs_review == 1
    assert result.errors == [
        "Could not resolve a title for the reference",
        "database is locked",
    ]


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
    _outcomes, result, ingest_batch, _get_by_status = run_backfill(
        nodes, [make_outcome(), make_outcome(), make_outcome()]
    )

    assert result.resolvable == 3
    ingest_batch.assert_called_once_with(["10.uri/a", "10.meta/d", "10.tag/f"], workers=2)


def test_backfill_scans_default_statuses_unions_and_dedupes_candidates():
    shared = node("shared", uri="doi:10.1/shared")
    nodes_by_status = {
        "queued": [
            node("queued", uri="doi:10.1/queued", tags=["pipeline:queued"]),
            shared,
        ],
        "scaffolded": [
            shared,
            node("scaffolded", uri="doi:10.1/scaffolded"),
        ],
        "failed": [
            {"id": "stable-id", "slug": "failed", "uri": "doi:10.1/failed", "tags": ["pipeline:failed"]},
            {"id": "stable-id", "slug": "failed-copy", "uri": "doi:10.1/failed-copy", "tags": ["pipeline:failed"]},
        ],
        "digested": [
            node("digested", uri="doi:10.1/digested", tags=["pipeline:digested"]),
        ],
    }

    _outcomes, result, ingest_batch, get_by_status = run_backfill(
        nodes_by_status,
        [make_outcome(), make_outcome(), make_outcome(), make_outcome()],
    )

    assert result.candidates == 4
    assert result.resolvable == 4
    ingest_batch.assert_called_once_with(
        ["10.1/queued", "10.1/shared", "10.1/scaffolded", "10.1/failed"],
        workers=2,
    )
    assert [call.args[0] for call in get_by_status.call_args_list] == ["queued", "scaffolded", "failed"]


def test_backfill_accepts_custom_statuses():
    nodes_by_status = {
        "failed": [node("failed", uri="doi:10.1/failed", tags=["pipeline:failed"])],
        "queued": [node("queued", uri="doi:10.1/queued", tags=["pipeline:queued"])],
    }

    _outcomes, result, ingest_batch, get_by_status = run_backfill(
        nodes_by_status,
        [make_outcome()],
        statuses=("failed",),
    )

    assert result.candidates == 1
    assert result.resolvable == 1
    ingest_batch.assert_called_once_with(["10.1/failed"], workers=2)
    assert [call.args[0] for call in get_by_status.call_args_list] == ["failed"]


def test_backfill_processes_candidates_in_chunks_and_aggregates():
    nodes = [
        node("a", uri="doi:10.1/a"),
        node("b", uri="doi:10.1/b"),
        node("c", uri="doi:10.1/c"),
        node("d", uri="doi:10.1/d"),
        node("e", uri="doi:10.1/e"),
    ]
    chunk_results = [
        ([make_outcome(linked=1, stored=2), make_outcome(linked=3, stored=4)], object()),
        ([make_outcome(errors=["database is locked"]), make_outcome(linked=5, stored=6)], object()),
        ([make_outcome(errors=["Could not resolve a title for the reference"])], object()),
    ]

    with patch("pipeline.ingestion.trellis.get_by_pipeline_status", return_value=nodes), patch(
        "pipeline.ingestion.ingest_batch", side_effect=chunk_results
    ) as ingest_batch:
        outcomes, result = ingestion.backfill_nodes(workers=2, chunk_size=2)

    assert ingest_batch.call_args_list == [
        call(["10.1/a", "10.1/b"], workers=2),
        call(["10.1/c", "10.1/d"], workers=2),
        call(["10.1/e"], workers=2),
    ]
    assert outcomes == [outcome for chunk, _metrics in chunk_results for outcome in chunk]
    assert result.candidates == 5
    assert result.resolvable == 5
    assert result.processed == 3
    assert result.edges_linked == 9
    assert result.citations_stored == 12
    assert result.failed == 1
    assert result.needs_review == 1
    assert result.errors == ["database is locked", "Could not resolve a title for the reference"]
