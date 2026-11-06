#!/usr/bin/env python3
"""Local stdio MCP adapter. The selected engine runs in an isolated subprocess."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

ROOT = Path(__file__).resolve().parents[1]
SERVER = FastMCP(
    "autonomous-library-agent",
    instructions=(
        "Operate only on explicitly requested paper records or exact RIS files in an "
        "explicit existing Trellis directory. No library registration or workspace-wide "
        "backfill is needed. library_preview is read-only and makes no provider calls; "
        "library_ingest writes bibliographic metadata and citations, not full-text findings. "
        "Use ingest only when the user requests the write. Provider data may change; "
        "save the returned receipt to identify the inputs, engine source, and outcomes."
    ),
)


def _provider_environment() -> dict[str, str]:
    environment = os.environ.copy()
    # MCP credentials must be selected for this plugin, not inherited from
    # unrelated tools. Direct Python/CLI ingestion retains its own configuration.
    for key in (
        "NCBI_API_KEY",
        "S2_API_KEY",
        "OPENALEX_API_KEY",
        "OPENCITATIONS_ACCESS_TOKEN",
        "CROSSREF_EMAIL",
    ):
        value = environment.get(f"LIBRARY_AGENT_{key}", "")
        # Clients without Claude userConfig support leave optional templates
        # unresolved. Never send those strings to a provider as credentials.
        environment[key] = "" if value.startswith("${user_config.") else value
    environment["CROSSREF_MAILTO"] = ""
    return environment


def _redact(text: str) -> str:
    for key, value in os.environ.items():
        if key.endswith(("_KEY", "_TOKEN", "_SECRET", "_PASSWORD")) and len(value) >= 6:
            text = text.replace(value, "[REDACTED]")
    return text


async def _stop(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    await process.wait()


async def invoke(
    operation: str,
    workspace: str,
    records: list[str | dict[str, Any]] | None,
    ris_paths: list[str] | None,
    parent: str | None,
) -> dict[str, Any]:
    python = os.environ.get("LIBRARY_AGENT_PYTHON", sys.executable)
    request = {
        "operation": operation,
        "workspace": workspace,
        "records": records or [],
        "ris_paths": ris_paths or [],
        "parent": parent,
    }
    # Neither a caller-controlled command nor a shell is involved. Each request
    # owns its target environment, so simultaneous calls cannot switch libraries.
    process = await asyncio.create_subprocess_exec(
        python,
        "-m",
        "pipeline.local_agent",
        cwd=ROOT,
        env=_provider_environment(),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(json.dumps(request).encode("utf-8")), timeout=1800
        )
    except (asyncio.CancelledError, asyncio.TimeoutError):
        await _stop(process)
        raise
    if stderr:
        print(
            _redact(stderr.decode("utf-8", errors="replace")), file=sys.stderr, end=""
        )
    try:
        result = json.loads(_redact(stdout.decode("utf-8")))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError(
            f"Engine returned invalid JSON (exit {process.returncode})"
        ) from error
    if process.returncode != 0:
        raise RuntimeError(
            result.get("error") or f"Engine failed with exit {process.returncode}"
        )
    return result


@SERVER.tool(annotations=ToolAnnotations(readOnlyHint=True, openWorldHint=False))
async def library_preview(
    workspace: str,
    records: list[str | dict[str, Any]] | None = None,
    ris_paths: list[str] | None = None,
    parent: str | None = None,
) -> dict[str, Any]:
    """Validate exact inputs and inspect existing identity matches without writing.

    workspace: Absolute path to any existing initialized Trellis directory.
    records: Explicit DOI strings or pipeline records with DOI, PMID, or title.
    ris_paths: Absolute paths to exact RIS files; no recursive directory import.
    parent: Optional existing parent node to check before a later ingestion.
    Provider resolution is not performed, so matches can change during ingestion.
    """
    return await invoke("preview", workspace, records, ris_paths, parent)


@SERVER.tool(
    annotations=ToolAnnotations(
        readOnlyHint=False, destructiveHint=True, openWorldHint=True
    )
)
async def library_ingest(
    workspace: str,
    parent: str,
    records: list[str | dict[str, Any]] | None = None,
    ris_paths: list[str] | None = None,
) -> dict[str, Any]:
    """Ingest only the requested papers into the explicit Trellis target.

    parent is the existing node slug/UUID under which new references are placed.
    Writes bibliographic metadata and citation links using the canonical pipeline.
    Does not initialize a library, process unrelated queued papers, extract full
    text, archive uploads, publish Git changes, or deliver Slack messages. May
    update existing papers and materialize their citation relationships. Repeating
    a call refreshes metadata; provider responses can change. Cancellation can
    leave partial writes: inspect returned/stored status before retrying.
    See library_preview for the argument contract.
    """
    return await invoke("ingest", workspace, records, ris_paths, parent)


if __name__ == "__main__":
    SERVER.run(transport="stdio")
