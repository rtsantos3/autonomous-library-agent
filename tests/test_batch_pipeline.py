import concurrent.futures
import sys
import threading
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import ingestion
from pipeline.aggregator import BatchResolved
from pipeline.citations import CitationItem, CitationResult
from pipeline.ingestion import (
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

        with patch("pipeline.ingestion.resolve_identity", return_value=resolved()) as resolve, patch(
            "pipeline.ingestion.find_existing", return_value=DedupResult(None, None)
        ) as dedup, patch("pipeline.ingestion.upsert_node", return_value=UpsertResult("source", True)) as upsert:
            result = ingestion.resolve_and_upsert(
                (0, "10.1/x"),
                outcomes,
                lambda doi: fetched,
                resolve_timings,
                upsert_timings,
                lock,
            )

        assert result == (0, "10.1/x", "source")
        assert outcomes[0].parse == ParseResult(None, "10.1/x", None, None, [], None, None)
        assert outcomes[0].resolve == resolved()
        assert outcomes[0].dedup == DedupResult(None, None)
        assert outcomes[0].upsert == UpsertResult("source", True)
        assert outcomes[0].errors == []
        assert len(resolve_timings) == 1
        assert len(upsert_timings) == 1
        assert resolve_timings[0] >= 0
        assert upsert_timings[0] >= 0
        resolve.assert_called_once_with(outcomes[0].parse, prefetched=fetched)
        dedup.assert_called_once_with(outcomes[0].resolve)
        upsert.assert_called_once_with(outcomes[0].resolve, outcomes[0].dedup)

    def test_resolve_and_upsert_catches_exception(self):
        outcomes = [IngestionOutcome()]

        with patch("pipeline.ingestion.resolve_identity", side_effect=RuntimeError("resolve failed")):
            result = ingestion.resolve_and_upsert(
                (0, "10.1/x"),
                outcomes,
                lambda doi: None,
                [],
                [],
                threading.Lock(),
            )

        assert result is None
        assert outcomes[0].errors == ["resolve failed"]
        assert outcomes[0].upsert is None

    def test_fetch_and_store_uses_prefetched_citations(self):
        outcomes = [IngestionOutcome()]
        fetched = prefetched(citations=[CitationItem("10.2/y", None, None, "Target", 2020)])

        with patch("pipeline.ingestion.store_citations", return_value=ingestion.CitationStoreResult(1)) as store, patch(
            "pipeline.ingestion.fetch_outbound_citations"
        ) as fetch:
            result = ingestion.fetch_and_store((0, "10.1/x", "source"), outcomes, lambda doi: fetched, ["10.1/x"])

        assert result[0] == 0
        assert result[1] == "source"
        assert result[2].source == "s2-batch"
        assert result[2].items == fetched.citations
        assert outcomes[0].citation_store == ingestion.CitationStoreResult(1)
        store.assert_called_once_with("source", result[2])
        fetch.assert_not_called()

    def test_fetch_and_store_catches_exception(self):
        outcomes = [IngestionOutcome()]

        with patch("pipeline.ingestion.store_citations", side_effect=RuntimeError("store failed")):
            result = ingestion.fetch_and_store((0, "10.1/x", "source"), outcomes, lambda doi: prefetched(), ["10.1/x"])

        assert result is None
        assert outcomes[0].errors == ["store failed"]

    def test_link_stored_populates_outcome(self):
        outcomes = [IngestionOutcome()]
        citations = citation_result()

        with patch("pipeline.ingestion.link_citations", return_value=ingestion.LinkResult(2, 1)) as link:
            result = ingestion.link_stored((0, "source", citations), outcomes, {"target": {"slug": "target"}})

        assert result == 0
        assert outcomes[0].link == ingestion.LinkResult(2, 1)
        link.assert_called_once_with("source", citations, index={"target": {"slug": "target"}})

    def test_link_stored_catches_exception(self):
        outcomes = [IngestionOutcome()]

        with patch("pipeline.ingestion.link_citations", side_effect=RuntimeError("link failed")):
            result = ingestion.link_stored((0, "source", citation_result()), outcomes, {})

        assert result is None
        assert outcomes[0].errors == ["link failed"]

    def test_verify_upserted_populates_outcome_and_returns_none(self):
        outcomes = [IngestionOutcome()]
        verify = ingestion.VerifyResult(True, True, "scaffolded", 3)

        with patch("pipeline.ingestion.verify_outcome", return_value=verify) as verify_outcome:
            result = ingestion.verify_upserted((0, "10.1/x", "source"), outcomes)

        assert result is None
        assert outcomes[0].verify == verify
        verify_outcome.assert_called_once_with("source")

    def test_verify_upserted_catches_exception_and_returns_none(self):
        outcomes = [IngestionOutcome()]

        with patch("pipeline.ingestion.verify_outcome", side_effect=RuntimeError("verify failed")):
            result = ingestion.verify_upserted((0, "10.1/x", "source"), outcomes)

        assert result is None
        assert outcomes[0].errors == ["verify failed"]


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

        def upsert_node(resolve, dedup):
            if resolve.doi == "10.1/b":
                raise RuntimeError("upsert failed for b")
            return UpsertResult(f"slug-{resolve.doi[-1]}", True)

        with patch("pipeline.aggregator.batch_resolve", return_value=resolved_map) as batch_resolve, patch(
            "pipeline.ingestion.trellis.build_node_index",
            side_effect=[{"old": {"slug": "old"}}, {"old": {"slug": "old"}, "new": {"slug": "new"}}],
        ) as build_index, patch("pipeline.ingestion.trellis.reverse_materialize") as reverse, patch(
            "pipeline.ingestion.resolve_identity", side_effect=resolve_identity
        ), patch(
            "pipeline.ingestion.find_existing", return_value=DedupResult(None, None)
        ), patch(
            "pipeline.ingestion.upsert_node", side_effect=upsert_node
        ), patch(
            "pipeline.ingestion.store_citations", return_value=ingestion.CitationStoreResult(1)
        ), patch(
            "pipeline.ingestion.link_citations", return_value=ingestion.LinkResult(1, 0)
        ), patch(
            "pipeline.ingestion.verify_outcome", return_value=ingestion.VerifyResult(True, True, "scaffolded", 1)
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

        assert [phase.name for phase in metrics.phases] == [
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
        ]
        assert metrics.workers == 3
        assert metrics.total_seconds >= 0
        assert metrics.node_count_at_index == 1
        assert all(phase.wall_seconds >= 0 for phase in metrics.phases)
        assert all(phase.per_item_seconds >= 0 for phase in metrics.phases)
        assert metrics.phases[0].items == len(dois)
        assert metrics.phases[1].items == 1
        assert metrics.phases[5].items == 2
        assert metrics.phases[6].items == 2
        assert metrics.phases[7].items == 2
        assert metrics.phases[8].items == 2
        assert metrics.phases[9].items == 2
        batch_resolve.assert_called_once_with(dois)
        assert build_index.call_count == 2
        assert reverse.call_count == 2
        fetch.assert_not_called()

    def test_resolve_and_upsert_thread_safety_smoke(self):
        dois = [f"10.1/{idx}" for idx in range(8)]
        outcomes = [IngestionOutcome() for _ in dois]
        resolve_timings = []
        upsert_timings = []
        lock = threading.Lock()

        def resolve_identity(parsed, prefetched=None):
            return resolved(doi=parsed.doi, s2_id=f"s2-{parsed.doi.rsplit('/', 1)[1]}")

        def upsert_node(resolve, dedup):
            return UpsertResult(f"slug-{resolve.doi.rsplit('/', 1)[1]}", True)

        with patch("pipeline.ingestion.resolve_identity", side_effect=resolve_identity), patch(
            "pipeline.ingestion.find_existing", return_value=DedupResult(None, None)
        ), patch("pipeline.ingestion.upsert_node", side_effect=upsert_node):
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                results = list(
                    executor.map(
                        lambda item: ingestion.resolve_and_upsert(
                            item,
                            outcomes,
                            lambda doi: None,
                            resolve_timings,
                            upsert_timings,
                            lock,
                        ),
                        enumerate(dois),
                    )
                )

        assert len([result for result in results if result is not None]) == len(dois)
        assert len(resolve_timings) == len(dois)
        assert len(upsert_timings) == len(dois)
        assert [outcome.parse.doi for outcome in outcomes] == dois
        assert [outcome.upsert.slug for outcome in outcomes] == [f"slug-{idx}" for idx in range(8)]
        assert all(outcome.errors == [] for outcome in outcomes)
