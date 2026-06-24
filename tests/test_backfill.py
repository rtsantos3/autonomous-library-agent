import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import ingestion
from pipeline.ingestion import DedupResult, ResolveResult, UpsertResult


def resolved(doi="10.1/x"):
    return ResolveResult(
        title="A microbiome paper",
        doi=doi,
        pmid=None,
        s2_id="s2-1",
        abstract="Abstract",
        authors=["Author A"],
        year="2024",
        venue="Journal",
        alt_dois=[],
        source="s2-batch",
        fields_of_study=["Biology"],
        publication_types=["Review"],
        mesh_terms=["Gastrointestinal Microbiome"],
        keywords=[],
        mesh_major=[],
        mesh_qualifiers=[],
    )


def node(slug, uri=None, tags=None, metadata=None):
    return {
        "slug": slug,
        "uri": uri,
        "tags": tags or ["pipeline:scaffolded"],
        "metadata": metadata or {"reference": {}},
    }


def backfill_with(nodes, batch_map=None, upsert_side_effect=None, **kwargs):
    batch_map = {} if batch_map is None else batch_map
    upsert_patch_kwargs = (
        {"side_effect": upsert_side_effect}
        if upsert_side_effect is not None
        else {"return_value": UpsertResult("existing", False)}
    )
    patches = [
        patch("pipeline.ingestion.trellis.get_by_pipeline_status", return_value=nodes),
        patch("pipeline.ingestion.trellis.build_node_index", return_value={}),
        patch("pipeline.aggregator.batch_resolve", return_value=batch_map),
        patch("pipeline.ingestion.find_existing", return_value=DedupResult({"slug": "existing"}, "doi")),
        patch("pipeline.ingestion.upsert_node", **upsert_patch_kwargs),
    ]
    started = [item.start() for item in patches]
    try:
        return ingestion.backfill_nodes(workers=1, **kwargs), started
    finally:
        for item in reversed(patches):
            item.stop()


def test_only_missing_skips_already_topical_and_processes_missing():
    nodes = [
        node("tagged", uri="doi:10.1/tagged", tags=["pipeline:scaffolded", "mesh:gut"]),
        node("missing", uri="doi:10.1/missing"),
    ]
    with patch("pipeline.ingestion.resolve_identity", return_value=resolved("10.1/missing")) as resolve:
        (outcomes, result), started = backfill_with(nodes)

    batch_resolve = started[2]
    assert result.candidates == 2
    assert result.skipped_already_tagged == 1
    assert result.resolvable == 1
    assert result.backfilled == 1
    assert len(outcomes) == 1
    batch_resolve.assert_called_once_with(["10.1/missing"])
    resolve.assert_called_once()


def test_node_without_doi_is_skipped_and_not_resolved():
    nodes = [node("no-doi", uri=None, tags=["pipeline:scaffolded"], metadata={"reference": {}})]
    with patch("pipeline.ingestion.resolve_identity") as resolve:
        (outcomes, result), started = backfill_with(nodes)

    batch_resolve = started[2]
    assert outcomes == []
    assert result.candidates == 1
    assert result.skipped_no_doi == 1
    assert result.resolvable == 0
    batch_resolve.assert_not_called()
    resolve.assert_not_called()


def test_happy_path_resolves_dedups_and_upserts():
    prefetched = object()
    nodes = [node("paper", uri="https://doi.org/10.1/x")]
    with patch("pipeline.ingestion.resolve_identity", return_value=resolved()) as resolve:
        (outcomes, result), started = backfill_with(nodes, batch_map={"10.1/x": prefetched})

    find_existing = started[3]
    upsert_node = started[4]
    assert result.backfilled == 1
    assert result.errors == []
    assert outcomes[0].parse.doi == "10.1/x"
    resolve.assert_called_once_with(outcomes[0].parse, prefetched=prefetched)
    find_existing.assert_called_once_with(outcomes[0].resolve)
    upsert_node.assert_called_once_with(outcomes[0].resolve, outcomes[0].dedup)


def test_upsert_exception_is_isolated_and_other_nodes_continue():
    nodes = [node("one", uri="doi:10.1/one"), node("two", uri="doi:10.1/two")]

    def resolve_from_parse(parsed, prefetched=None):
        return resolved(parsed.doi)

    def upsert_or_raise(resolved_item, dedup):
        if resolved_item.doi == "10.1/one":
            raise RuntimeError("boom")
        return UpsertResult("two", False)

    with patch("pipeline.ingestion.resolve_identity", side_effect=resolve_from_parse):
        (outcomes, result), _started = backfill_with(nodes, upsert_side_effect=upsert_or_raise)

    assert result.backfilled == 1
    assert len(result.errors) == 1
    assert result.errors[0] == "10.1/one: boom"
    assert outcomes[0].errors == ["boom"]
    assert outcomes[1].upsert == UpsertResult("two", False)


def test_doi_extraction_precedence_uri_before_metadata_before_tag():
    nodes = [
        node(
            "all",
            uri="doi:10.uri/a",
            tags=["pipeline:scaffolded", "doi:10.tag/c"],
            metadata={"reference": {"doi": "10.meta/b"}},
        ),
        node(
            "metadata",
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
    with patch("pipeline.ingestion.resolve_identity", side_effect=lambda parsed, prefetched=None: resolved(parsed.doi)):
        (_outcomes, result), started = backfill_with(nodes)

    batch_resolve = started[2]
    assert result.resolvable == 3
    batch_resolve.assert_called_once_with(["10.uri/a", "10.meta/d", "10.tag/f"])
