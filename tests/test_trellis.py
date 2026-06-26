import json
import sqlite3
import subprocess
import sys
import threading
from pathlib import Path
from unittest.mock import call, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import trellis  # noqa: E402

UUID_SOURCE = "11111111-1111-1111-1111-111111111111"
UUID_TARGET = "22222222-2222-2222-2222-222222222222"


def test_norm_title_normalizes_unicode_whitespace_case_and_trailing_periods():
    assert (
        trellis._norm_title("  The Café\tMicrobiome\nStudy...  ")
        == "the cafe microbiome study"
    )


def test_normalize_doi_uri_accepts_common_prefixes_and_preserves_doi_case():
    assert (
        trellis._normalize_doi_uri("doi:10.1000/ABC ") == "https://doi.org/10.1000/ABC"
    )
    assert (
        trellis._normalize_doi_uri("http://dx.doi.org/10.2000/X")
        == "https://doi.org/10.2000/X"
    )
    assert (
        trellis._normalize_doi_uri("https://doi.org/10.3000/Y")
        == "https://doi.org/10.3000/Y"
    )


def test_doi_key_returns_bare_lowercase_doi_or_none():
    assert trellis._doi_key(" DOI:10.1000/ABC ") == "10.1000/abc"
    assert trellis._doi_key("") is None


def test_unwrap_node_and_list_accept_trellis_response_shapes():
    node = {"slug": "paper"}
    assert trellis._unwrap_node({"node": node}) == node
    assert trellis._unwrap_node(node) == node
    assert trellis._unwrap_node(None) == {}

    nodes = [{"slug": "one"}, {"slug": "two"}]
    assert trellis._unwrap_list(nodes) == nodes
    assert trellis._unwrap_list({"results": nodes}) == nodes
    assert trellis._unwrap_list({"nodes": nodes}) == nodes
    assert trellis._unwrap_list(None) == []


def test_run_raises_runtime_error_on_subprocess_timeout():
    timeout = subprocess.TimeoutExpired(cmd=["trellis", "find"], timeout=120)

    with patch("pipeline.trellis.subprocess.run", side_effect=timeout) as run:
        with pytest.raises(
            RuntimeError, match="trellis find timed out after 120 seconds"
        ):
            trellis._run("find")

    assert run.call_args.kwargs["timeout"] == 120


def test_run_retries_busy_failure_and_returns_success():
    busy = subprocess.CompletedProcess(
        ["trellis", "find"],
        1,
        stdout="",
        stderr="database is locked",
    )
    success = subprocess.CompletedProcess(
        ["trellis", "find"],
        0,
        stdout="[]\n",
        stderr="",
    )

    with patch(
        "pipeline.trellis.subprocess.run", side_effect=[busy, success]
    ) as run, patch("pipeline.trellis.time.sleep") as sleep:
        assert trellis._run("find") == "[]"

    assert run.call_count == 2
    sleep.assert_called_once_with(0.1)


def test_run_persistent_busy_raises_after_max_attempts():
    busy = subprocess.CompletedProcess(
        ["trellis", "update"],
        1,
        stdout="database busy",
        stderr="",
    )

    with patch("pipeline.trellis.subprocess.run", return_value=busy) as run, patch(
        "pipeline.trellis.time.sleep"
    ) as sleep:
        with pytest.raises(RuntimeError, match="database busy"):
            trellis._run("update")

    assert run.call_count == 5
    assert [call.args[0] for call in sleep.call_args_list] == [0.1, 0.2, 0.4, 0.8]


def test_run_non_busy_failure_raises_without_retry():
    failure = subprocess.CompletedProcess(
        ["trellis", "find"],
        2,
        stdout="",
        stderr="bad args",
    )

    with patch("pipeline.trellis.subprocess.run", return_value=failure) as run, patch(
        "pipeline.trellis.time.sleep"
    ) as sleep:
        with pytest.raises(RuntimeError, match="bad args"):
            trellis._run("find")

    assert run.call_count == 1
    sleep.assert_not_called()


def test_node_identifier_prefers_id_then_uuid_then_slug():
    assert (
        trellis._node_identifier({"id": "id", "uuid": "uuid", "slug": "slug"}) == "id"
    )
    assert trellis._node_identifier({"uuid": "uuid", "slug": "slug"}) == "uuid"
    assert trellis._node_identifier({"slug": "slug"}) == "slug"
    assert trellis._node_identifier({}) is None


def test_build_node_index_indexes_identifiers_titles_alt_dois_and_pending_citations():
    source = {
        "id": UUID_SOURCE,
        "slug": "source-paper",
        "title": "  Source Café Paper. ",
        "uri": "https://doi.org/10.1/SOURCE",
        "tags": "pipeline:scaffolded,s2id:s2-source,pmid:123",
        "metadata": {
            "reference": {
                "uri": "doi:10.1/source-meta",
                "doi": "10.1/source-reference",
                "alt_dois": ["10.1/ALT-A", "https://doi.org/10.1/alt-b"],
                "outbound_citations": {
                    "items": [
                        {"doi": "10.9/PENDING"},
                        {"doi": ""},
                        "not-a-dict",
                    ]
                },
            }
        },
    }
    other = {
        "slug": "other-paper",
        "title": "Other microbiome paper",
        "tags": ["s2id:s2-other", "pmid:456"],
        "metadata": {"reference": {"doi": "doi:10.2/OTHER"}},
    }

    with patch("pipeline.trellis.find_nodes", return_value=[source, other]) as find:
        index = trellis.build_node_index()

    find.assert_called_once_with(limit=5000)
    assert index["by_doi"]["10.1/source"] == source
    assert index["by_doi"]["10.1/source-meta"] == source
    assert index["by_doi"]["10.1/source-reference"] == source
    assert index["by_doi"]["10.1/alt-a"] == source
    assert index["by_doi"]["10.1/alt-b"] == source
    assert index["by_doi"]["10.2/other"] == other
    assert index["by_s2id"]["s2-source"] == source
    assert index["by_s2id"]["s2-other"] == other
    assert index["by_pmid"]["123"] == source
    assert index["by_pmid"]["456"] == other
    assert index["by_title"]["source cafe paper"] == source
    assert index["pending_citations"]["10.9/pending"] == [(UUID_SOURCE, source)]


def test_dedup_check_indexed_match_precedence_and_misses():
    by_s2id = {"slug": "s2"}
    by_doi = {"slug": "doi"}
    by_pmid = {"slug": "pmid"}
    by_title = {"slug": "title"}
    index = {
        "by_s2id": {"s2": by_s2id},
        "by_doi": {"10.1/x": by_doi},
        "by_pmid": {"123": by_pmid},
        "by_title": {"title based microbiome paper": by_title},
    }

    assert (
        trellis.dedup_check_indexed(index, s2id="s2", doi="10.1/x", pmid="123")
        == by_s2id
    )
    assert (
        trellis.dedup_check_indexed(index, doi="https://doi.org/10.1/X", pmid="123")
        == by_doi
    )
    assert (
        trellis.dedup_check_indexed(
            index, pmid=123, title="Title Based Microbiome Paper"
        )
        == by_pmid
    )
    assert (
        trellis.dedup_check_indexed(index, title="Title Based Microbiome Paper.")
        == by_title
    )
    assert trellis.dedup_check_indexed(index, title="too short") is None
    assert (
        trellis.dedup_check_indexed(
            index, s2id="missing", doi="10.9/missing", pmid="999"
        )
        is None
    )


def test_find_by_s2id_and_pmid_return_first_tag_match_from_chokepoint():
    calls = []

    def fake_run(*args):
        calls.append(args)
        if args == ("find", "--tag", "s2id:s2-1", "--json"):
            return json.dumps([{"slug": "s2-match"}])
        if args == ("find", "--tag", "pmid:123", "--json"):
            return json.dumps({"results": [{"slug": "pmid-match"}]})
        raise AssertionError(args)

    with patch("pipeline.trellis._run", side_effect=fake_run):
        assert trellis.find_by_s2id("s2-1") == {"slug": "s2-match"}
        assert trellis.find_by_pmid("123") == {"slug": "pmid-match"}

    assert calls == [
        ("find", "--tag", "s2id:s2-1", "--json"),
        ("find", "--tag", "pmid:123", "--json"),
    ]


def test_find_by_doi_checks_uri_metadata_doi_and_grep_alt_dois_from_chokepoint():
    metadata_match = {
        "slug": "metadata-match",
        "metadata": {"reference": {"doi": "10.1/X"}},
    }
    alt_match = {"slug": "alt-match"}

    def fake_run(*args):
        if args == ("find", "--text", "https://doi.org/10.1/X", "--json"):
            return json.dumps([{"slug": "wrong", "uri": "https://doi.org/10.other/x"}])
        if args == ("find", "--text", "10.1/x", "--json"):
            return json.dumps([metadata_match])
        if args == ("find", "--text", "https://doi.org/10.9/ALT", "--json"):
            return json.dumps([])
        if args == ("find", "--text", "10.9/alt", "--json"):
            return json.dumps([])
        if args == ("grep", "10.9/ALT", "--json"):
            return json.dumps([alt_match])
        raise AssertionError(args)

    with patch("pipeline.trellis._run", side_effect=fake_run):
        assert trellis.find_by_doi("10.1/X") == metadata_match
        assert trellis.find_by_doi("10.9/ALT") == alt_match


def test_find_by_title_normalizes_query_and_requires_exact_normalized_match():
    def fake_run(*args):
        assert args == ("find", "--text", "microbiome cafe study", "--json")
        return json.dumps(
            [
                {"slug": "near", "title": "Microbiome cafe studies"},
                {"slug": "match", "title": "Microbiome Café Study."},
            ]
        )

    with patch("pipeline.trellis._run", side_effect=fake_run):
        assert trellis.find_by_title("  Microbiome Café Study... ") == {
            "slug": "match",
            "title": "Microbiome Café Study.",
        }

    with patch("pipeline.trellis._run", side_effect=AssertionError):
        assert trellis.find_by_title("short") is None


def test_get_by_pipeline_status_uses_explicit_backfill_limit():
    expected = [{"slug": "paper"}]

    with patch("pipeline.trellis.find_nodes", return_value=expected) as find:
        result = trellis.get_by_pipeline_status("scaffolded")

    assert result is expected
    find.assert_called_once_with(tag="pipeline:scaffolded", limit=5000)


def test_reverse_materialize_counts_successful_and_idempotent_links_only():
    index = {
        "pending_citations": {
            "10.9/target": [
                (UUID_SOURCE, {"slug": "source-paper"}),
                ("source-two", {"slug": "source-two-paper"}),
                ("source-three", {"slug": "source-three-paper"}),
            ]
        }
    }

    with patch(
        "pipeline.trellis.link_nodes",
        side_effect=[
            {"ok": True},
            {"ok": True, "idempotent": True},
            {"ok": False, "error": "failed"},
        ],
    ) as link:
        created = trellis.reverse_materialize(
            UUID_TARGET, doi="https://doi.org/10.9/TARGET", index=index
        )

    assert created == 2
    link.assert_has_calls(
        [
            call(UUID_SOURCE, UUID_TARGET, "references"),
            call("source-two", UUID_TARGET, "references"),
            call("source-three", UUID_TARGET, "references"),
        ]
    )


def test_reverse_materialize_skips_self_links_and_missing_waiting_sources():
    index = {
        "pending_citations": {
            "10.9/target": [
                (UUID_TARGET, {"slug": "self-paper"}),
                (None, {"slug": "bad-paper"}),
                (UUID_SOURCE, {"slug": "source-paper"}),
            ]
        }
    }

    with patch("pipeline.trellis.link_nodes", return_value={"ok": True}) as link:
        created = trellis.reverse_materialize(
            UUID_TARGET, doi="https://doi.org/10.9/TARGET", index=index
        )

    assert created == 1
    link.assert_called_once_with(UUID_SOURCE, UUID_TARGET, "references")


def test_reverse_materialize_links_multiple_waiting_sources_for_same_doi_key():
    index = {
        "pending_citations": {
            "10.9/target": [
                (UUID_SOURCE, {"slug": "source-paper"}),
                ("source-two", {"slug": "source-two-paper"}),
            ]
        }
    }

    with patch("pipeline.trellis.link_nodes", return_value={"ok": True}) as link:
        created = trellis.reverse_materialize(
            UUID_TARGET, doi="doi:10.9/TARGET", index=index
        )

    assert created == 2
    link.assert_has_calls(
        [
            call(UUID_SOURCE, UUID_TARGET, "references"),
            call("source-two", UUID_TARGET, "references"),
        ]
    )


def test_reverse_materialize_returns_zero_without_index_or_valid_doi():
    with patch("pipeline.trellis.link_nodes", side_effect=AssertionError):
        assert trellis.reverse_materialize(UUID_TARGET, doi="10.1/x", index=None) == 0
        assert trellis.reverse_materialize(UUID_TARGET, doi="10.1/x", index={}) == 0
        assert (
            trellis.reverse_materialize(UUID_TARGET, doi="10.1/x", index={"by_doi": {}})
            == 0
        )
        assert (
            trellis.reverse_materialize(
                UUID_TARGET, doi="", index={"pending_citations": {}}
            )
            == 0
        )
        assert (
            trellis.reverse_materialize(
                UUID_TARGET, doi="not-a-doi", index={"pending_citations": {}}
            )
            == 0
        )
        assert (
            trellis.reverse_materialize(
                "", doi="10.1/x", index={"pending_citations": {}}
            )
            == 0
        )
        with patch("pipeline.trellis._doi_key", return_value=None):
            assert (
                trellis.reverse_materialize(
                    UUID_TARGET, doi="10.1/x", index={"pending_citations": {}}
                )
                == 0
            )


def test_set_pipeline_status_rejects_unknown_status_before_subprocess():
    with patch("pipeline.trellis._run", side_effect=AssertionError):
        with pytest.raises(ValueError, match="Unknown pipeline status"):
            trellis.set_pipeline_status("paper", "unknown")


def test_link_nodes_treats_duplicate_errors_as_idempotent():
    with patch(
        "pipeline.trellis._run", side_effect=RuntimeError("already exists")
    ) as run:
        assert trellis.link_nodes(UUID_SOURCE, UUID_TARGET, "references") == {
            "ok": True,
            "idempotent": True,
        }

    run.assert_has_calls(
        [
            call(
                "link",
                "--source-uuid",
                UUID_SOURCE,
                "--target-uuid",
                UUID_TARGET,
                "--relationship",
                "references",
                "--actor-id",
                trellis.ACTOR,
                "--json",
            )
        ]
    )


def test_link_nodes_idempotent_when_edge_index_contains_key():
    key = (UUID_SOURCE.replace("-", ""), UUID_TARGET.replace("-", ""), "references")
    edge_index = {key}

    with patch("pipeline.trellis._run_json", side_effect=AssertionError) as run, patch(
        "pipeline.trellis._edge_exists", side_effect=AssertionError
    ):
        result = trellis.link_nodes(
            UUID_SOURCE, UUID_TARGET, "references", edge_index=edge_index
        )

    assert result == {"ok": True, "idempotent": True}
    run.assert_not_called()


def test_link_nodes_reserves_edge_index_key_before_linking():
    edge_index = set()
    edge_lock = threading.Lock()

    with patch("pipeline.trellis._run_json", return_value={}) as run, patch(
        "pipeline.trellis._edge_exists", side_effect=AssertionError
    ):
        first = trellis.link_nodes(
            UUID_SOURCE,
            UUID_TARGET,
            "references",
            edge_index=edge_index,
            edge_lock=edge_lock,
        )
        second = trellis.link_nodes(
            UUID_SOURCE,
            UUID_TARGET,
            "references",
            edge_index=edge_index,
            edge_lock=edge_lock,
        )

    assert first == {"ok": True}
    assert second == {"ok": True, "idempotent": True}
    assert edge_index == {
        (UUID_SOURCE.replace("-", ""), UUID_TARGET.replace("-", ""), "references")
    }
    run.assert_called_once_with(
        "link",
        "--source-uuid",
        UUID_SOURCE,
        "--target-uuid",
        UUID_TARGET,
        "--relationship",
        "references",
        "--actor-id",
        trellis.ACTOR,
        "--json",
    )


def test_link_nodes_idempotent_when_direct_edge_lookup_finds_existing_edge():
    with patch("pipeline.trellis._edge_exists", return_value=True) as exists, patch(
        "pipeline.trellis._run_json", side_effect=AssertionError
    ) as run:
        result = trellis.link_nodes(UUID_SOURCE, UUID_TARGET, "references")

    assert result == {"ok": True, "idempotent": True}
    exists.assert_called_once_with(UUID_SOURCE, UUID_TARGET, "references")
    run.assert_not_called()


def test_link_nodes_fails_closed_when_direct_edge_lookup_fails():
    with patch(
        "pipeline.trellis._edge_exists",
        side_effect=sqlite3.OperationalError("database is locked"),
    ) as exists, patch("pipeline.trellis._run_json", side_effect=AssertionError) as run:
        result = trellis.link_nodes(UUID_SOURCE, UUID_TARGET, "references")

    assert result == {
        "ok": False,
        "error": "edge existence check failed: database is locked",
    }
    exists.assert_called_once_with(UUID_SOURCE, UUID_TARGET, "references")
    run.assert_not_called()


def test_link_nodes_new_edge_calls_link_and_adds_key_to_edge_index():
    edge_index = set()

    with patch("pipeline.trellis._run_json", return_value={}) as run, patch(
        "pipeline.trellis._edge_exists", side_effect=AssertionError
    ):
        result = trellis.link_nodes(
            UUID_SOURCE, UUID_TARGET, "references", edge_index=edge_index
        )

    assert result == {"ok": True}
    assert edge_index == {
        (UUID_SOURCE.replace("-", ""), UUID_TARGET.replace("-", ""), "references")
    }
    run.assert_called_once_with(
        "link",
        "--source-uuid",
        UUID_SOURCE,
        "--target-uuid",
        UUID_TARGET,
        "--relationship",
        "references",
        "--actor-id",
        trellis.ACTOR,
        "--json",
    )
