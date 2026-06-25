import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline._utils import bare_doi, extend_unique, slugify  # noqa: E402


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("doi:10.1000/ABC", "10.1000/abc"),
        ("https://doi.org/10.1000/ABC", "10.1000/abc"),
        ("http://dx.doi.org/10.1000/ABC", "10.1000/abc"),
        ("  10.1000/ABC  ", "10.1000/abc"),
        ("10.1000/already-bare", "10.1000/already-bare"),
        ("   ", None),
    ],
)
def test_bare_doi_normalizes_supported_string_inputs(value, expected):
    assert bare_doi(value) == expected


def test_bare_doi_non_string_input_is_not_supported_by_current_contract():
    with pytest.raises(AttributeError):
        bare_doi(123)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Microbiome, diet & IBD!", "microbiome-diet-ibd"),
        ("Crème brûlée", "cr-me-br-l-e"),
        ("  spaced   words  ", "spaced-words"),
        ("---", None),
        ("", None),
        (None, None),
    ],
)
def test_slugify_handles_punctuation_unicode_spaces_and_empty_edges(value, expected):
    assert slugify(value) == expected


def test_extend_unique_mutates_target_preserves_order_and_returns_target():
    target = ["alpha", "beta"]

    result = extend_unique(
        target, ["beta", None, "", [], " gamma ", "alpha", "delta", "gamma"]
    )

    assert result is target
    assert target == ["alpha", "beta", "gamma", "delta"]


def test_extend_unique_handles_empty_incoming():
    target = ["alpha"]

    assert extend_unique(target, None) is target
    assert target == ["alpha"]
