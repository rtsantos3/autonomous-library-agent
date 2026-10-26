"""Authors reach Trellis as {family, given}; plain strings are silently dropped."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import ingestion, trellis  # noqa: E402
from pipeline.ingestion import ResolveResult  # noqa: E402


def _resolved(authors):
    return ResolveResult(
        title="A Structured Author Test Paper",
        doi="10.1234/authors-test",
        pmid=None,
        s2_id=None,
        abstract=None,
        authors=authors,
        year="2023",
        venue=None,
        alt_dois=[],
        source="input-only",
    )


def test_given_family_strings_become_structured():
    meta = ingestion._reference_metadata(_resolved(["Can Chen", "Yang-Yu Liu"]))
    assert meta["authors"] == [
        {"family": "Chen", "given": "Can"},
        {"family": "Liu", "given": "Yang-Yu"},
    ]


def test_family_comma_given_is_split_on_the_first_comma():
    meta = ingestion._reference_metadata(_resolved(["Ke, Zheng Tracy"]))
    assert meta["authors"] == [{"family": "Ke", "given": "Zheng Tracy"}]


def test_particles_and_suffixes_stay_in_the_family_name():
    meta = ingestion._reference_metadata(
        _resolved(["Johan de Kleer", "Ludwig van der Waals", "Martin Luther King Jr."])
    )
    assert meta["authors"] == [
        {"family": "de Kleer", "given": "Johan"},
        {"family": "van der Waals", "given": "Ludwig"},
        {"family": "King Jr.", "given": "Martin Luther"},
    ]


def test_single_names_dicts_and_blanks():
    meta = ingestion._reference_metadata(
        _resolved(["Darwin", {"family": "Leveson", "given": "Nancy"}, "", "  ", None])
    )
    assert meta["authors"] == [
        {"family": "Darwin", "given": ""},
        {"family": "Leveson", "given": "Nancy"},
    ]


def test_trellis_stores_the_authors(ephemeral_trellis):
    # The bug: Trellis accepted the node but dropped string authors silently.
    resolved = _resolved(["Can Chen", "Chen Liao"])
    node = trellis.add_reference(
        resolved.title,
        uri="https://doi.org/10.1234/authors-test",
        metadata={"reference": ingestion._reference_metadata(resolved)},
    )
    stored = trellis.get_node(node["slug"])["metadata"]["reference"]["authors"]
    assert stored == [
        {"family": "Chen", "given": "Can"},
        {"family": "Liao", "given": "Chen"},
    ]
