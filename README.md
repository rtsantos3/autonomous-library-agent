# Autonomous Library Agent

A literature ingestion pipeline for the [Trellis stack](https://github.com/rtsantos3/Trellis).
It resolves paper metadata, deduplicates indexed references, and stores papers
and citation links in an explicitly selected Trellis workspace.

This is the `deploy` branch. Development documentation, tests, and full-text
prompt templates remain on [`master`](https://github.com/rtsantos3/autonomous-library-agent/tree/master).

## Install

Requires Linux or a compatible POSIX environment, Conda, Node.js 18 or later,
and an initialized Trellis workspace with an existing parent node.

```bash
git clone --branch deploy --single-branch https://github.com/rtsantos3/autonomous-library-agent.git
cd autonomous-library-agent
export LIBRARY_AGENT_ROOT=/absolute/path/to/autonomous-library-agent

# Create only if this environment does not already exist.
conda env create -f environment.yml
conda activate autonomous-library-agent
python -m pip install -r requirements-mcp.txt

npm install -g @rtsantos3/trellis-app@0.17.0-beta.1
trellis --help
```

Trellis is a separate npm dependency. The version above is the tested prerelease.
For reproducible deployments, pin this checkout to a reviewed commit. The
`v0.1.0` tag is the full source release, not this stripped deployment tree.
Use the commands above, not the development-oriented `setup.sh`, which expects
the test suite.

## Connect a client

Start the client with the Python environment active and `trellis` on `PATH`.
Choose one:

```bash
omp --plugin-dir "$LIBRARY_AGENT_ROOT"
```

```bash
claude --plugin-dir "$LIBRARY_AGENT_ROOT"
```

For Codex, add this to `~/.codex/config.toml`, replacing both absolute paths:

```toml
[mcp_servers.library]
command = "/absolute/path/to/autonomous-library-agent-env/bin/python"
args = ["/absolute/path/to/autonomous-library-agent/scripts/agent_mcp.py"]
tool_timeout_sec = 1800
```

Use the Python executable inside the activated Conda environment.
If the MCP server and ingestion engine use different environments, set
`LIBRARY_AGENT_PYTHON` to the engine's Python executable in the server configuration.

The included marketplace catalog also supports OMP and Claude Code. Loading this
checkout with `--plugin-dir` uses `deploy`; the GitHub marketplace shorthand
without a branch selector follows `master`. Do not load both copies at once.

## Select papers and a target

Call `library_preview` first. Call `library_ingest` with the same arguments only
when the write is intended:

```json
{
  "workspace": "/absolute/path/to/initialized-trellis-workspace",
  "parent": "papers",
  "records": ["10.1038/s41575-025-01077-5"]
}
```

- `workspace` must already contain an initialized Trellis graph.
- `parent` must be an existing slug or UUID; it is required for ingestion.
  From that workspace, use `trellis get papers --json` to inspect its `id`.
- `records` accepts DOI strings or objects such as `{"pmid": "22699609"}`.
  A title-only record needs at least 10 characters.
- `ris_paths` accepts absolute paths to individual RIS files, instead of or
  alongside `records`. Directories are not accepted.

Preview makes no provider requests or graph writes. Ingestion queries
bibliographic providers, creates or updates selected papers, and links cited
papers already in the graph. An empty stored bibliography finishes as
`pipeline:needs-review`, not `pipeline:digested`.

## Provider credentials and data

In Claude Code, enter optional provider credentials through the plugin's
configuration dialog. Each field is marked sensitive and defaults to empty.
The MCP adapter does not reuse ambient provider keys or load workspace `.env`
files. Leaving a field empty makes unauthenticated requests where supported.

OMP does not currently resolve Claude's `userConfig` fields. Its plugin remains
usable without keys. For authenticated use in OMP or Codex, configure the server
directly and explicitly set the corresponding `LIBRARY_AGENT_` entries in that
server's `env` map:

| Server setting | Recipient |
|---|---|
| `LIBRARY_AGENT_NCBI_API_KEY` | `eutils.ncbi.nlm.nih.gov` |
| `LIBRARY_AGENT_S2_API_KEY` | `api.semanticscholar.org` |
| `LIBRARY_AGENT_OPENALEX_API_KEY` | `api.openalex.org` |
| `LIBRARY_AGENT_OPENCITATIONS_ACCESS_TOKEN` | `api.opencitations.net` |
| `LIBRARY_AGENT_CROSSREF_EMAIL` | `api.crossref.org` |

Keep credential-bearing client configuration private and out of Git. Direct
Python/CLI ingestion is separate and retains the conventional provider settings
listed in [`templates/.env.example`](templates/.env.example).

Provider requests send selected bibliographic identifiers or search terms, not
the workspace's filesystem path or arbitrary local files. Provider keys go to
their named services. OpenAlex documentation links and DOI URLs in metadata are
not credential destinations.

The adapter disables an inherited RIS sink and does not scan unrelated queues,
extract full text, publish Git changes, or send Slack messages. It runs with the
local user's permissions, not in a sandbox. Setup commands above are manual
installation instructions; the MCP server does not download code or run package installers.

Receipts identify inputs, engine source, runtime versions, and outcomes. They do
not archive provider responses or guarantee offline replay. Preview matches can
change after provider resolution. Repeated ingestion refreshes metadata;
cancellation can leave partial writes.

## License

[PolyForm Noncommercial 1.0.0](LICENSE). This is source-available software, not
MIT or OSI open source. Uses outside the license's grants require separate
permission. Trellis and other dependencies retain their own licenses.
