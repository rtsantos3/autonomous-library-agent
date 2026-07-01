import sys
import threading
from pathlib import Path
from unittest.mock import call, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import ingestion  # noqa: E402
from pipeline.aggregator import BatchResolved  # noqa: E402
from pipeline.citations import CitationItem, CitationResult  # noqa: E402
from pipeline.ingestion import (  # noqa: E402
    DedupResult,
    IngestionOutcome,
    ParseResult,
    ResolveResult,
    UpsertResult,
)


def resolved(**overrides):
    data = {
        "title": "A microbiome paper",
        "doi": "10.1/x",
        "pmid": "123",
        "s2_id": "s2-1",
        "abstract": "Abstract",
        "authors": ["Author A"],
        "year": "2024",
        "venue": "Journal",
        "alt_dois": [],
        "source": "input-only",
    }
    data.update(overrides)
    return ResolveResult(**data)


def citation_result(items=None, source="semantic-scholar"):
    return CitationResult(
        source=source,
        retrieved_at="2026-06-24",
        items=[] if items is None else items,
    )


def prefetched(**overrides):
    data = {
        "doi": "10.1/x",
        "s2_id": "s2-1",
        "title": "Batch title",
        "abstract": "Batch abstract",
        "pmid": "123",
        "year": "2024",
        "venue": "Batch Journal",
        "authors": ["Author A"],
        "citations": [CitationItem("10.2/y", None, None, "Target", 2020)],
    }
    data.update(overrides)
    return BatchResolved(**data)


class TestBatchWorkers:
    def test_resolve_and_upsert_populates_outcome_and_timings(self):
        outcomes = [IngestionOutcome()]
        resolve_timings = []
        upsert_timings = []
        lock = threading.Lock()
        fetched = prefetched()

        index = {"by_doi": {}}

        with patch(
            "pipeline.ingestion.resolve_identity", return_value=resolved()
        ) as resolve, patch(
            "pipeline.ingestion.find_existing_indexed",
            return_value=DedupResult(None, None),
        ) as dedup, patch(
            "pipeline.ingestion.upsert_node", return_value=UpsertResult("source", True)
        ) as upsert, patch(
            "pipeline.ingestion.trellis.set_pipeline_status"
        ):
            result = ingestion.resolve_and_upsert(
                (0, "10.1/x"),
                outcomes,
                lambda doi: fetched,
                resolve_timings,
                upsert_timings,
                lock,
                index,
            )

        assert result == (0, "10.1/x", "source")
        assert outcomes[0].parse == ParseResult(
            None, "10.1/x", None, None, [], None, None
        )
        assert outcomes[0].resolve == resolved()
        assert outcomes[0].dedup == DedupResult(None, None)
        assert outcomes[0].upsert == UpsertResult("source", True)
        assert outcomes[0].errors == []
        assert len(resolve_timings) == 1
        assert len(upsert_timings) == 1
        assert resolve_timings[0] >= 0
        assert upsert_timings[0] >= 0
        resolve.assert_called_once_with(outcomes[0].parse, prefetched=fetched)
        dedup.assert_called_once_with(outcomes[0].resolve, index)
        upsert.assert_called_once_with(
            outcomes[0].resolve, outcomes[0].dedup, index=index, node_lock=None
        )

    def test_resolve_and_upsert_catches_exception(self):
        outcomes = [IngestionOutcome()]

        with patch(
            "pipeline.ingestion.resolve_identity",
            side_effect=RuntimeError("resolve failed"),
        ):
            result = ingestion.resolve_and_upsert(
                (0, "10.1/x"),
                outcomes,
                lambda doi: None,
                [],
                [],
                threading.Lock(),
                {},
            )

        assert result is None
        assert outcomes[0].errors == ["resolve failed"]
        assert outcomes[0].upsert is None

    def test_fetch_and_store_uses_prefetched_citations(self):
        outcomes = [IngestionOutcome()]
        fetched = prefetched(
            citations=[CitationItem("10.2/y", None, None, "Target", 2020)]
        )

        with patch(
            "pipeline.ingestion.store_citations",
            return_value=ingestion.CitationStoreResult(1),
        ) as store, patch("pipeline.ingestion.fetch_outbound_citations") as fetch:
            result = ingestion.fetch_and_store(
                (0, "10.1/x", "source"), outcomes, lambda doi: fetched, ["10.1/x"]
            )

        assert result[0] == 0
        assert result[1] == "source"
        assert result[2].source == "s2-batch"
        assert result[2].items == fetched.citations
        assert outcomes[0].citation_store == ingestion.CitationStoreResult(1)
        store.assert_called_once_with("source", result[2])
        fetch.assert_not_called()

    def test_fetch_and_store_catches_exception(self):
        outcomes = [IngestionOutcome()]

        with patch(
            "pipeline.ingestion.store_citations",
            side_effect=RuntimeError("store failed"),
        ):
            result = ingestion.fetch_and_store(
                (0, "10.1/x", "source"), outcomes, lambda doi: prefetched(), ["10.1/x"]
            )

        assert result is None
        assert outcomes[0].errors == ["store failed"]

    def test_link_stored_populates_outcome(self):
        outcomes = [IngestionOutcome()]
        citations = citation_result()

        with patch(
            "pipeline.ingestion.link_citations", return_value=ingestion.LinkResult(2, 1)
        ) as link:
            result = ingestion.link_stored(
                (0, "source", citations), outcomes, {"target": {"slug": "target"}}
            )

        assert result == 0
        assert outcomes[0].link == ingestion.LinkResult(2, 1)
        link.assert_called_once_with(
            "source", citations, index={"target": {"slug": "target"}}
        )

    def test_link_stored_catches_exception(self):
        outcomes = [IngestionOutcome()]

        with patch(
            "pipeline.ingestion.link_citations", side_effect=RuntimeError("link failed")
        ):
            result = ingestion.link_stored(
                (0, "source", citation_result()), outcomes, {}
            )

        assert result is None
        assert outcomes[0].errors == ["link failed"]

    def test_verify_upserted_populates_outcome_and_returns_none(self):
        outcomes = [IngestionOutcome()]
        outcomes[0].link = ingestion.LinkResult(3, 1)
        verify = ingestion.VerifyResult(True, True, "scaffolded", 3)

        with patch(
            "pipeline.ingestion.verify_outcome", return_value=verify
        ) as verify_outcome:
            result = ingestion.verify_upserted((0, "10.1/x", "source"), outcomes)

        assert result is None
        assert outcomes[0].verify == verify
        verify_outcome.assert_called_once_with("source", edge_count=3)

    def test_verify_upserted_catches_exception_and_returns_none(self):
        outcomes = [IngestionOutcome()]

        with patch(
            "pipeline.ingestion.verify_outcome",
            side_effect=RuntimeError("verify failed"),
        ):
            result = ingestion.verify_upserted((0, "10.1/x", "source"), outcomes)

        assert result is None
        assert outcomes[0].errors == ["verify failed"]

    def test_set_final_pipeline_status_marks_success_and_classified_failures(self):
        outcomes = [
            IngestionOutcome(upsert=UpsertResult("ok", True)),
            IngestionOutcome(
                upsert=UpsertResult("permanent", True),
                errors=["Could not resolve a title for the reference"],
            ),
            IngestionOutcome(
                upsert=UpsertResult("transient", True), errors=["database is locked"]
            ),
        ]

        with patch(
            "pipeline.ingestion.trellis.set_pipeline_status"
        ) as set_status, patch("pipeline.ingestion.trellis.annotate_node") as annotate:
            ingestion.set_final_pipeline_status((0, "10.1/ok", "ok"), outcomes)
            ingestion.set_final_pipeline_status(
                (1, "10.1/permanent", "permanent"), outcomes
            )
            ingestion.set_final_pipeline_status(
                (2, "10.1/transient", "transient"), outcomes
            )

        assert outcomes[0].errors == []
        assert outcomes[1].errors == ["Could not resolve a title for the reference"]
        assert outcomes[2].errors == ["database is locked"]
        set_status.assert_has_calls(
            [
                call("ok", "digested"),
                call("permanent", "needs-review"),
                call("transient", "failed"),
            ]
        )
        today = ingestion.date.today().isoformat()
        annotate.assert_has_calls(
            [
                call(
                    "permanent",
                    f"[{today}] backfill needs-review: Could not resolve a title for the reference",
                ),
                call("transient", f"[{today}] backfill failed: database is locked"),
            ]
        )

    def test_set_final_pipeline_status_captures_status_update_error(self):
        outcomes = [IngestionOutcome(upsert=UpsertResult("ok", True))]

        with patch(
            "pipeline.ingestion.trellis.set_pipeline_status",
            side_effect=RuntimeError("status failed"),
        ):
            ingestion.set_final_pipeline_status((0, "10.1/ok", "ok"), outcomes)

        assert outcomes[0].errors == ["status failed"]

    def test_reverse_materialize_upserted_uses_resolved_doi(self):
        outcomes = [IngestionOutcome(resolve=resolved(doi="10.1/resolved"))]
        index = {"by_doi": {}}

        with patch("pipeline.ingestion.trellis.reverse_materialize") as reverse:
            result = ingestion.reverse_materialize_upserted(
                (0, "10.1/citation", "source"), outcomes, index
            )

        assert result is None
        assert outcomes[0].errors == []
        reverse.assert_called_once_with("source", doi="10.1/resolved", index=index)

    def test_reverse_materialize_upserted_captures_error_on_item(self):
        outcomes = [
            IngestionOutcome(resolve=resolved(doi=None)),
            IngestionOutcome(resolve=resolved(doi="10.1/ok")),
        ]
        index = {"by_doi": {}}

        def reverse(slug, **kwargs):
            if slug == "bad":
                raise RuntimeError("reverse failed")

        with patch(
            "pipeline.ingestion.trellis.reverse_materialize", side_effect=reverse
        ) as reverse_materialize:
            ingestion.reverse_materialize_upserted(
                (0, "10.1/fallback", "bad"), outcomes, index
            )
            ingestion.reverse_materialize_upserted(
                (1, "10.1/citation", "ok"), outcomes, index
            )

        assert outcomes[0].errors == ["reverse failed"]
        assert outcomes[1].errors == []
        assert [call.kwargs["doi"] for call in reverse_materialize.call_args_list] == [
            "10.1/fallback",
            "10.1/ok",
        ]


class TestIngestBatch:
    def test_ingest_batch_orchestrates_phases_and_isolates_item_failure(self):
        dois = ["10.1/a", "10.1/b", "10.1/c"]
        resolved_map = {doi: prefetched(doi=doi, s2_id=f"s2-{doi[-1]}") for doi in dois}

        def resolve_identity(parsed, prefetched=None):
            return resolved(
                title=prefetched.title,
                doi=parsed.doi,
                s2_id=prefetched.s2_id,
                source="s2-batch",
            )

        def upsert_node(resolve, dedup, **kwargs):
            if resolve.doi == "10.1/b":
                raise RuntimeError("upsert failed for b")
            return UpsertResult(f"slug-{resolve.doi[-1]}", True)

        with patch(
            "pipeline.aggregator.batch_resolve", return_value=resolved_map
        ) as batch_resolve, patch(
            "pipeline.ingestion.trellis.build_node_index",
            side_effect=[
                {"old": {"slug": "old"}},
                {"old": {"slug": "old"}, "new": {"slug": "new"}},
            ],
        ) as build_index, patch(
            "pipeline.ingestion.trellis.reverse_materialize"
        ) as reverse, patch(
            "pipeline.ingestion.resolve_identity", side_effect=resolve_identity
        ), patch(
            "pipeline.ingestion.find_existing_indexed",
            return_value=DedupResult(None, None),
        ), patch(
            "pipeline.ingestion.upsert_node", side_effect=upsert_node
        ), patch(
            "pipeline.ingestion.store_citations",
            return_value=ingestion.CitationStoreResult(1),
        ), patch(
            "pipeline.ingestion.link_citations", return_value=ingestion.LinkResult(1, 0)
        ), patch(
            "pipeline.ingestion.verify_outcome",
            return_value=ingestion.VerifyResult(True, True, "scaffolded", 1),
        ), patch(
            "pipeline.ingestion.trellis.set_pipeline_status"
        ), patch(
            "pipeline.ingestion.fetch_outbound_citations"
        ) as fetch:
            outcomes, metrics = ingestion.ingest_batch(dois, workers=3)

        assert len(outcomes) == len(dois)
        assert [outcome.parse.doi for outcome in outcomes] == dois
        assert outcomes[0].upsert == UpsertResult("slug-a", True)
        assert outcomes[1].upsert is None
        assert outcomes[1].errors == ["upsert failed for b"]
        assert outcomes[2].upsert == UpsertResult("slug-c", True)
        assert outcomes[0].citation_store == ingestion.CitationStoreResult(1)
        assert outcomes[2].link == ingestion.LinkResult(1, 0)
        assert outcomes[0].verify.node_exists is True
        assert outcomes[1].verify is None
        assert outcomes[2].verify.node_exists is True

        phase_names = {phase.name for phase in metrics.phases}
        assert {
            "phase0 batch resolve",
            "index build",
            "phase1 resolve+upsert",
            "phase1 resolve_identity aggregate",
            "phase1 upsert_node aggregate",
            "index rebuild",
            "reverse materialize",
            "phase2 fetch+store",
            "phase3 link",
            "phase4 verify",
            "phase5 status",
        } <= phase_names
        assert metrics.workers == 3
        assert metrics.total_seconds >= 0
        assert metrics.node_count_at_index == 1
        assert all(phase.wall_seconds >= 0 for phase in metrics.phases)
        assert all(phase.per_item_seconds >= 0 for phase in metrics.phases)
        phase_items = {phase.name: phase.items for phase in metrics.phases}
        assert phase_items["phase0 batch resolve"] == len(dois)
        assert phase_items["index build"] == 1
        assert phase_items["index rebuild"] == 2
        assert phase_items["reverse materialize"] == 2
        assert phase_items["phase2 fetch+store"] == 2
        assert phase_items["phase3 link"] == 2
        assert phase_items["phase4 verify"] == 2
        assert phase_items["phase5 status"] == 2
        batch_resolve.assert_called_once_with(dois)
        assert build_index.call_count == 2
        assert reverse.call_count == 2
        assert sorted(call.kwargs["doi"] for call in reverse.call_args_list) == [
            "10.1/a",
            "10.1/c",
        ]
        fetch.assert_not_called()

    def test_ingest_batch_phase1_dedup_uses_index_not_subprocess_chain(self):
        dois = ["10.1/a"]
        indexed_match = {
            "slug": "existing-a",
            "tags": ["pipeline:scaffolded"],
            "metadata": {},
        }
        index = {"by_doi": {"10.1/a": indexed_match}}

        def resolve_identity(parsed, prefetched=None):
            return resolved(
                doi=parsed.doi, s2_id=None, pmid=None, title="Indexed microbiome paper"
            )

        with patch(
            "pipeline.aggregator.batch_resolve",
            return_value={"10.1/a": prefetched(doi="10.1/a")},
        ), patch(
            "pipeline.ingestion.trellis.build_node_index",
            side_effect=[index, {"pending_citations": {}}],
        ), patch(
            "pipeline.ingestion.resolve_identity", side_effect=resolve_identity
        ), patch(
            "pipeline.ingestion.find_existing",
            side_effect=AssertionError("subprocess dedup should not run"),
        ), patch(
            "pipeline.ingestion.trellis.find_by_s2id",
            side_effect=AssertionError("find_by_s2id should not run"),
        ) as find_by_s2id, patch(
            "pipeline.ingestion.trellis.find_by_doi",
            side_effect=AssertionError("find_by_doi should not run"),
        ) as find_by_doi, patch(
            "pipeline.ingestion.trellis.find_by_pmid",
            side_effect=AssertionError("find_by_pmid should not run"),
        ) as find_by_pmid, patch(
            "pipeline.ingestion.trellis.find_by_title",
            side_effect=AssertionError("find_by_title should not run"),
        ) as find_by_title, patch(
            "pipeline.ingestion.trellis.grep_nodes",
            side_effect=AssertionError("grep_nodes should not run"),
        ) as grep_nodes, patch(
            "pipeline.ingestion.trellis.dedup_check_indexed", return_value=indexed_match
        ) as dedup_indexed, patch(
            "pipeline.ingestion.upsert_node",
            return_value=UpsertResult("existing-a", False),
        ), patch(
            "pipeline.ingestion.trellis.reverse_materialize", return_value=0
        ), patch(
            "pipeline.ingestion.store_citations",
            return_value=ingestion.CitationStoreResult(0),
        ), patch(
            "pipeline.ingestion.link_citations", return_value=ingestion.LinkResult(0, 0)
        ), patch(
            "pipeline.ingestion.verify_outcome",
            return_value=ingestion.VerifyResult(True, True, "scaffolded", 0),
        ), patch(
            "pipeline.ingestion.trellis.set_pipeline_status"
        ):
            outcomes, _metrics = ingestion.ingest_batch(dois, workers=1)

        assert outcomes[0].dedup == DedupResult(indexed_match, "doi")
        dedup_indexed.assert_called_once_with(index, doi="10.1/a")
        find_by_s2id.assert_not_called()
        find_by_doi.assert_not_called()
        find_by_pmid.assert_not_called()
        find_by_title.assert_not_called()
        grep_nodes.assert_not_called()


class TestBatchItemNormalization:
    def test_as_raw_input_wraps_string_doi(self):
        assert ingestion._as_raw_input("10.1/x") == {"doi": "10.1/x"}

    def test_as_raw_input_passes_dict_through(self):
        record = {"title": "T", "doi": "10.1/x", "authors": ["A"]}
        assert ingestion._as_raw_input(record) is record

    def test_as_raw_input_rejects_other_types(self):
        try:
            ingestion._as_raw_input(123)
        except TypeError as exc:
            assert "str or dict" in str(exc)
        else:
            raise AssertionError("expected TypeError")

    def test_item_doi_extracts_from_string_and_dict(self):
        assert ingestion._item_doi("10.1/x") == "10.1/x"
        assert ingestion._item_doi({"doi": "10.1/y"}) == "10.1/y"
        assert ingestion._item_doi({"title": "no doi"}) is None

    def test_resolve_and_upsert_handles_titleonly_dict_without_prefetch(self):
        # A DOI-less record must funnel through parse_input as a full dict and
        # must NOT consult the DOI-keyed prefetch map.
        outcomes = [IngestionOutcome()]
        record = {
            "title": "A title-only microbiome paper",
            "authors": ["Author A"],
            "year": "2024",
        }
        prefetch_calls = []

        def prefetch(doi):
            prefetch_calls.append(doi)
            return prefetched()

        with patch(
            "pipeline.ingestion.resolve_identity", return_value=resolved(doi=None)
        ) as resolve, patch(
            "pipeline.ingestion.find_existing_indexed",
            return_value=DedupResult(None, None),
        ), patch(
            "pipeline.ingestion.upsert_node", return_value=UpsertResult("slug-t", True)
        ), patch(
            "pipeline.ingestion.trellis.set_pipeline_status"
        ):
            result = ingestion.resolve_and_upsert(
                (0, record),
                outcomes,
                prefetch,
                [],
                [],
                threading.Lock(),
                {"by_doi": {}},
            )

        # No DOI resolved → citation_doi is None; downstream fetch no-ops.
        assert result == (0, None, "slug-t")
        assert outcomes[0].parse.title == "A title-only microbiome paper"
        assert outcomes[0].parse.doi is None
        assert prefetch_calls == []
        resolve.assert_called_once_with(outcomes[0].parse, prefetched=None)

    def test_ingest_batch_accepts_mixed_string_and_dict_items(self):
        items = [
            "10.1/a",
            {"title": "Title only paper about microbiome diversity", "year": "2024"},
            {"title": "Has DOI", "doi": "10.1/c"},
        ]

        c_prefetch = prefetched(doi="10.1/c", s2_id="s2-c", title="Has DOI batch")
        seen_prefetch = {}

        def resolve_identity(parsed, prefetched=None):
            seen_prefetch[parsed.doi] = prefetched
            return resolved(
                title=parsed.title or "resolved title",
                doi=parsed.doi,
                source="s2-batch" if prefetched else "input-only",
            )

        with patch(
            "pipeline.aggregator.batch_resolve", return_value={"10.1/c": c_prefetch}
        ) as batch_resolve, patch(
            "pipeline.ingestion.trellis.build_node_index",
            return_value={},
        ), patch(
            "pipeline.ingestion.trellis.reverse_materialize"
        ), patch(
            "pipeline.ingestion.resolve_identity", side_effect=resolve_identity
        ), patch(
            "pipeline.ingestion.find_existing_indexed",
            return_value=DedupResult(None, None),
        ), patch(
            "pipeline.ingestion.upsert_node",
            side_effect=lambda r, d, **kwargs: UpsertResult(
                f"slug-{r.title[:3]}", True
            ),
        ), patch(
            "pipeline.ingestion.store_citations",
            return_value=ingestion.CitationStoreResult(0),
        ), patch(
            "pipeline.ingestion.link_citations", return_value=ingestion.LinkResult(0, 0)
        ), patch(
            "pipeline.ingestion.verify_outcome",
            return_value=ingestion.VerifyResult(True, True, "digested", 0),
        ), patch(
            "pipeline.ingestion.trellis.set_pipeline_status"
        ), patch(
            "pipeline.ingestion.fetch_outbound_citations",
            return_value=citation_result(),
        ):
            outcomes, _metrics = ingestion.ingest_batch(items, workers=2)

        # Only the two DOI-bearing items contribute to the S2 batch prefetch.
        batch_resolve.assert_called_once_with(["10.1/a", "10.1/c"])
        assert len(outcomes) == 3
        assert outcomes[0].parse.doi == "10.1/a"
        assert outcomes[1].parse.doi is None
        assert outcomes[1].parse.title == "Title only paper about microbiome diversity"
        assert outcomes[2].parse.doi == "10.1/c"
        assert all(not o.errors for o in outcomes)
        # The DOI-bearing dict reuses the S2 batch prefetch; the title-only dict
        # gets no prefetch and falls back to per-paper enrichment.
        assert seen_prefetch["10.1/c"] is c_prefetch
        assert seen_prefetch[None] is None
