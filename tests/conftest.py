import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: requires network and live Trellis")


def pytest_collection_modifyitems(config, items):
    if config.option.markexpr == "integration":
        return
    skip_integration = pytest.mark.skip(reason="integration test; run with -m integration")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)
