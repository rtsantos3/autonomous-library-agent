# Autonomous Library Agent

A literature ingestion pipeline for the [Trellis stack](https://github.com/rtsantos3/Trellis).
It resolves paper metadata, deduplicates records and references, and stores papers
and citation links in a selected Trellis graph.

Inputs are DOI strings, PMID-bearing records, title-bearing records, or RIS files.
The local MCP adapter exposes the same Python engine to OMP, Claude Code, and
Codex. Direct Python and CLI entrypoints are also available.

Each local invocation specifies an existing Trellis directory and explicit paper
inputs. Ingestion also requires an existing parent node. No library registration
or workspace-wide intake scan is required.

## Pipeline

1. Resolve bibliographic identity through PubMed, Semantic Scholar, Crossref,
   DataCite, and OpenAlex as available.
2. Enrich authors, abstracts, publication details, and topical metadata.
3. Collect and deduplicate indexed references from Semantic Scholar, Crossref,
   Europe PMC, OpenAlex, and OpenCitations.
4. Create or update reference nodes and link cited papers already in the graph.
5. Return input hashes, engine identity, and per-record results.

## Trellis dependency

Trellis is the separate graph application and storage layer. All graph mutations
go through its CLI.

Install Trellis from its npm package:

**[@rtsantos3/trellis-app](https://www.npmjs.com/package/@rtsantos3/trellis-app)**

The local integration was exercised with this version:

```bash
npm install -g @rtsantos3/trellis-app@0.17.0-beta.1
trellis --help
```

That is a **prerelease**, not a stable-version recommendation. Review the
[Trellis documentation and releases](https://github.com/rtsantos3/Trellis)
before changing versions. Pin your chosen version for repeatable runs.

The npm package provides the required `trellis` command. Trellis is not bundled
in this repository.

## Requirements

You need:

- Python 3.10 or later.
- Node.js 18 or later and npm for Trellis.
- The `trellis` executable on `PATH`.
- An initialized Trellis directory and an existing parent node for references.
- Network access to bibliographic providers for ingestion.

The local adapter is tested on Linux and uses POSIX process-group cancellation.
Windows users should use a compatible environment such as WSL.

## Installation

### 1. Source checkout

```bash
git clone https://github.com/rtsantos3/autonomous-library-agent.git
cd autonomous-library-agent
export LIBRARY_AGENT_ROOT="$PWD"
```

For a release-pinned checkout:

```bash
git checkout v0.1.0
```

Keep the checkout at a specific reviewed commit or release tag for repeatable
runs. Use the marketplace installation below if you prefer client-managed
plugin files.

### 2. Python environment

For a **new** Conda environment:

```bash
conda env create -f environment.yml
```

If the environment already exists, do not recreate it. Activate it, then install
the optional MCP adapter dependency:

```bash
conda activate autonomous-library-agent
python -m pip install -r requirements-mcp.txt
```

Using one Python environment for the server and engine is the simplest setup.
If you prefer separate environments, tell the server which Python runs the engine:

```bash
export LIBRARY_AGENT_PYTHON=/absolute/path/to/engine-python
```

Otherwise it uses its own Python interpreter. The plugin manifest starts
`python`, so the Python with the MCP SDK must be on `PATH` when you launch your
agent. The adapter itself does not install or download dependencies.

### 3. Trellis workspace

Use an existing initialized Trellis directory and an existing parent node for
references. No library registry is required.

To create a new graph explicitly:

```bash
mkdir -p "$HOME/trellis-papers"
cd "$HOME/trellis-papers"
trellis init
trellis add project papers
```

From the selected Trellis directory, verify the parent:

```bash
trellis get papers --json
```

The result includes the node's `id`. Use that UUID as `parent` in tool requests
when the slug is ambiguous.

The local tools never initialize a graph or invent a parent node for you.

### 4. Agent clients

All three local clients use the same stdio MCP server. These are agent-callable
tools, not separate implementations of the ingestion pipeline.

#### OMP

```bash
omp --plugin-dir "$LIBRARY_AGENT_ROOT"
```

#### Claude Code

```bash
claude --plugin-dir "$LIBRARY_AGENT_ROOT"
```

To check the package and connection:

```bash
claude plugin validate "$LIBRARY_AGENT_ROOT"
claude --plugin-dir "$LIBRARY_AGENT_ROOT" mcp list
```

The shared plugin manifest declares a 30-minute tool timeout. A shorter
process-wide MCP deadline in the host can still take precedence; adjust that
explicitly for longer runs.

#### Codex

Add the same server to `~/.codex/config.toml`, or to a trusted project's
`.codex/config.toml`. Replace the absolute paths with your Python executable and
source checkout:

```toml
[mcp_servers.library]
command = "/absolute/path/to/mcp-python"
args = ["/absolute/path/to/autonomous-library-agent/scripts/agent_mcp.py"]
tool_timeout_sec = 1800

[mcp_servers.library.env]
LIBRARY_AGENT_PYTHON = "/absolute/path/to/engine-python"
```

The environment table is only needed when the server and engine use different
Python interpreters. To find the executable in an activated environment, run
`python -c "import sys; print(sys.executable)"`.

Inspect the resolved Codex configuration:

```bash
codex mcp get library --json
```

Some Codex versions use `tool_call_timeout_sec` instead of `tool_timeout_sec`.
Use the spelling supported by your version and confirm that the resolved timeout
is not null.

#### Hermes

The Hermes integration lives in
[LAD-LAB/KB-agent](https://github.com/LAD-LAB/KB-agent), under
`workflows/ladlab.literature`.

It has its own setup, authenticated transport context, publication, and feedback
behavior. This local plugin does not recreate or bypass those shared-lab controls.

## Targeted ingestion

The MCP server exposes two tools. Clients may add their own prefixes to the names.

| Tool | What it does |
|---|---|
| `library_preview` | Validates exact inputs and inspects existing identity matches. No provider requests or graph writes. |
| `library_ingest` | Resolves, enriches, deduplicates, and links only the selected papers. Writes to the specified graph. |

Use the **same arguments** for preview and the explicitly requested ingestion.
Replace the example directory and parent with values from your graph:

```json
{
  "workspace": "/absolute/path/to/your-trellis-directory",
  "parent": "papers",
  "records": ["10.1038/s41575-025-01077-5"]
}
```

- `workspace`: an absolute path to an existing initialized Trellis directory.
- `parent`: an existing node slug or UUID; required for ingestion. Prefer a UUID
  when a slug is ambiguous.
- `records`: explicit DOI strings or pipeline record objects containing DOI,
  PMID, or a title of at least 10 characters.
- `ris_paths`: absolute paths to individual RIS files, instead of or alongside
  `records`. Directories are not accepted.

For PMID input, use a record object instead of a bare string:

```json
{
  "workspace": "/absolute/path/to/your-trellis-directory",
  "parent": "papers",
  "records": [{"pmid": "22699609"}]
}
```

For example, ask your agent:

> Preview this paper in my specified Trellis directory under `papers`. Do not
> ingest it yet. Show me the target and input receipt.

Then, after checking the target:

> Ingest exactly those inputs into the same directory and parent. Report the
> committed node status and any errors.

Preview uses supplied identifiers and titles. Provider resolution during ingestion
can discover a different canonical identity, so preview is not a guaranteed final
match. Ingestion may also materialize citation relationships with existing papers.

## Scope and citation completeness

The local tools do **not** extract PDF text, generate scientific findings, scan
for more papers, process unrelated queued records, archive uploads, publish Git
changes, or send Slack messages. They also disable an inherited `RIS_SINK_PATH`
so a local request does not append to some other vault's bibliography.

In this pipeline, `digested` means bibliographic ingestion and citation processing
succeeded. It does **not** mean full-text extraction or scientific review is done.

An empty **stored** bibliography finishes as `pipeline:needs-review`, not
`pipeline:digested`. Zero citation links can still be valid if none of the cited
papers is already in your graph. A non-empty bibliography does not prove that
every published reference was recovered.

Reference collection is a **union, not a fallback chain**. Each item retains its
sources. Provider outcomes include `available`, `empty`, `unavailable`,
`withheld`, `partial`, and `failed`. A `withheld` notice indicates that the
publisher did not make references available. A failed provider does not discard
usable references from another provider, and an empty fetch does not erase a
previously stored non-empty bibliography.

The pipeline does not create a paper node for every cited reference. Expanding a
bibliography into hundreds of new papers is a different decision, not a side
effect of this tool.

## Reproducibility

Every successful local invocation returns a receipt with:

- The canonical target directory, selected parent, and explicit records.
- RIS paths and SHA-256 hashes of the input bytes.
- A hash of the complete input selection.
- The Python-source hash and the engine Git revision when Git metadata is available.
- Python interpreter and dependency versions.

Ingestion also reports node identities, committed pipeline status, provider
outcomes, and a hash of the stored citation records. `links_processed` includes
already-existing links; it is not a count of newly created edges.

Identical preview and ingestion arguments produce the same input hash. Each
request runs in a separate worker process and overrides inherited workspace
settings, so one request cannot switch another request's target environment.
Repeated ingestion reuses matched nodes and guards existing reference edges;
metadata, status, and annotations can still be refreshed.

Live providers and graph contents change. Receipts identify a run; they do not
archive provider responses or provide byte-identical offline replay. Pin the
source revision and runtime versions when comparing runs.

## Credentials and permissions

Provider credentials come from the host environment, not tool arguments. The MCP
adapter does not load a target directory's `.env` file. Optional settings are
listed in [`templates/.env.example`](templates/.env.example). Public API access
is subject to provider availability and rate limits.

The server runs with the local user's permissions. The workspace argument routes
the operation; it is **not a filesystem sandbox**. Enable the plugin only in
trusted clients and retain their write-approval policy.

Cancellation can leave partial graph writes. Inspect the affected node status
before retrying.

## Python and CLI usage

The canonical entrypoint is `pipeline.ingestion.ingest_batch`. Direct Python and
legacy CLI use environment/config bindings instead of per-call MCP arguments:

```bash
cd "$LIBRARY_AGENT_ROOT"
export TRELLIS_WORKSPACE=/absolute/path/to/your-trellis-directory
export TRELLIS_PROJECT_SLUG=papers
```

```python
from pipeline.ingestion import ingest_batch

outcomes, metrics = ingest_batch([
    "10.1038/s41575-025-01077-5"
])

for outcome in outcomes:
    print(outcome.upsert, outcome.errors)
```

Direct engine workspace resolution is `TRELLIS_WORKSPACE`, then `config.yml`
`workspace:`, then the parent of the source checkout. The MCP adapter overrides
that implicit selection with the explicit request target. Direct callers default
to parent `microbiome-research-library` unless `TRELLIS_PROJECT_SLUG` is set.

The legacy scripts provide broader operations. Check their `--help` output before
using directory import or backfill:

```bash
python scripts/import_ris_network.py /absolute/path/to/papers.ris
python scripts/backfill.py --help
./scripts/export_graph.sh
```

The RIS importer also supports explicit parent selection, archival, stub notes,
and a deduplicated RIS sink. Those are CLI options, not hidden MCP side effects.
The sink API is `pipeline.ris_sink.RisSink(path).append(paper)`; ingestion also
accepts `ris_sink=`. Monitor target selection uses `TRELLIS_WORKSPACE` rather than
`config.yml`.

For the older managed-library setup, `./setup.sh` prepares the named Conda
environment and local configuration, verifies prerequisites, can hydrate a graph
from an existing export, and runs offline checks. It is not required to register
a target for the local MCP tools.

## Architecture

- [`scripts/agent_mcp.py`](scripts/agent_mcp.py): shared local MCP server.
- [`pipeline/local_agent.py`](pipeline/local_agent.py): one explicit request per
  worker process, with receipts and selected-target binding.
- [`pipeline/ingestion.py`](pipeline/ingestion.py): parse, resolve, deduplicate,
  store, link, verify, and assign status.
- [`pipeline/citations.py`](pipeline/citations.py) and
  [`pipeline/providers/`](pipeline/providers/): indexed reference collection and
  union/deduplication.
- [`pipeline/trellis.py`](pipeline/trellis.py): the graph-write boundary.
- [`scripts/migrations/`](scripts/migrations/): explicit data-repair utilities;
  inspect each script's options before applying a migration.

The same ingestion engine serves local tools, RIS import, and backfill. It
reuses non-empty S2 batch references, paginates S2 and Europe PMC retrieval, and
hydrates OpenAlex targets in bounded batches. DOI conflicts remain distinct;
ambiguous secondary identifiers cannot select a citation target.

See [`AGENT-CONTRACT.md`](AGENT-CONTRACT.md) for the runtime contract and
[`docs/citation_provider_prd.md`](docs/citation_provider_prd.md) for provider
identity rules and acceptance criteria. Full-text extraction is a separate stage
from this bibliographic pipeline.

## Distribution

The repository includes a self-hosted catalog at
[`.claude-plugin/marketplace.json`](.claude-plugin/marketplace.json).
The same catalog supports OMP and Claude Code.

After completing the Python/MCP and Trellis setup above, install with OMP:

```bash
omp plugin marketplace add rtsantos3/autonomous-library-agent
omp plugin install autonomous-library-agent@autonomous-library-agent
```

Or install with Claude Code:

```bash
claude plugin marketplace add rtsantos3/autonomous-library-agent
claude plugin install autonomous-library-agent@autonomous-library-agent
```

Restart the client after installation. Start it with the Python environment
containing the MCP SDK active, and keep `trellis` on `PATH`. These installs
fetch plugin files; they do not install Python dependencies or Trellis.

Marketplace installation and `--plugin-dir` are alternative ways to load the
plugin. Do not load both copies in the same session. Codex uses the direct MCP
configuration above rather than this catalog.

The [GitHub releases](https://github.com/rtsantos3/autonomous-library-agent/releases)
identify published versions. The marketplace follows the repository's default
branch; use a release-pinned source checkout when you need a fixed revision.
This plugin has no separate npm package. Trellis has its own npm distribution.

This is a self-hosted marketplace, not a listing in an Anthropic- or
OMP-curated directory. Those directories have separate submission processes.

References: [Claude Code marketplace documentation](https://code.claude.com/docs/en/plugins/marketplace-reference)
and the [OMP project](https://github.com/can1357/oh-my-pi).

## Testing

Run the offline suite in the ingestion environment:

```bash
python -m pytest tests/ -q -m "not integration"
```

Exercise the local target-boundary checks with real isolated Trellis workspaces:

```bash
python -m pytest tests/test_local_agent.py -q -m integration
```

Other integration tests can require network access. Do not point tests at a
personal or shared library unless you explicitly intend to modify it.

## License

This project is source-available under the
[PolyForm Noncommercial License 1.0.0](LICENSE)
(`PolyForm-Noncommercial-1.0.0`). It is not an unrestricted MIT or OSI-approved
open-source release.

The license permits noncommercial purposes and the research, personal, and
organizational uses specified in its terms. Commercial uses outside those grants
require separate permission from the applicable copyright holders.

Trellis and other dependencies retain their own licenses.
