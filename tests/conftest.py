import subprocess

import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: requires network and live Trellis")


@pytest.fixture
def ephemeral_trellis(tmp_path, monkeypatch):
    """
    Create a throwaway Trellis workspace, run the test against it, then discard it.

    Integration tests must not touch the live library graph. This points the
    pipeline's workspace (resolved per-call via TRELLIS_WORKSPACE) at a fresh,
    empty instance, seeds the parent project node that upsert_node parents
    references under, and yields the workspace path.

    Isolation is restored two ways: monkeypatch reverts TRELLIS_WORKSPACE the
    moment the test ends, and the instance lives under tmp_path. The throwaway
    .trellis directory is not deleted immediately on teardown — pytest keeps the
    last N runs (tmp_path_retention_count=2 in pytest.ini) and garbage-collects
    older ones, so a recent failure's graph stays inspectable.
    """
    try:
        subprocess.run(
            ["trellis", "--help"], capture_output=True, text=True, check=True
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        pytest.skip("trellis not available")

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setenv("TRELLIS_WORKSPACE", str(workspace))

    # Import only after TRELLIS_WORKSPACE is set, so pipeline.trellis's
    # import-time PROJECT_ROOT (the back-compat constant) can never freeze to the
    # live workspace from inside this fixture. Runtime calls resolve _workspace()
    # per-call, but this keeps the fixture honest as defense-in-depth.
    from pipeline import trellis as trellis_mod

    def _run(*args):
        return subprocess.run(
            ["trellis", *args],
            cwd=str(workspace),
            capture_output=True,
            text=True,
            check=True,
        )

    _run("init")
    # The ingestion pipeline parents every reference under this project node;
    # a fresh instance needs it to exist before any add_reference.
    _run("add", "project", trellis_mod.PROJECT_SLUG)

    yield workspace


def pytest_collection_modifyitems(config, items):
    # Skip integration tests by default, but back off whenever the user's -m
    # expression references "integration" at all (e.g. "integration",
    # "integration and not slow", "not integration"). In those cases pytest's
    # own marker selection already includes/excludes them correctly; an
    # exact-string match would mis-skip every form except bare "integration".
    if "integration" in (config.option.markexpr or ""):
        return
    skip_integration = pytest.mark.skip(
        reason="integration test; run with -m integration"
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)
