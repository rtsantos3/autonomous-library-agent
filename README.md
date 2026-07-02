# autonomous-library-agent

**What this is:** a tool that turns your collection of papers into a searchable,
connected knowledge base you can ask research questions against. Point it at a list
of papers — DOIs, PMIDs, or an EndNote/RIS export — and it looks each one up, fills
in the details (abstract, authors, journal, topics), removes duplicates, and links
the papers together by their citations. The result is a map of the literature you've
collected that you (or an AI assistant) can later query.

Under the hood it is a literature **ingestion pipeline** plus a **persistent-agent
contract** that maintain the library as a
[Trellis](https://github.com/rtsantos3/Trellis) knowledge graph — every paper is a
*node*, every citation is an *edge*. It is built to seed an offline **RAG** corpus
(retrieval-augmented generation: an AI that answers from *your* papers rather than
from guesswork). It is **library-agnostic** — it works for any collection, and a
library is just a folder the tool is pointed at — resolves and enriches each paper
across Semantic Scholar, PubMed, and Crossref, and is **idempotent**: safe for an
autonomous agent to re-run unattended without ever creating duplicates.

This repository is the **tooling**. The **data** (a materialized graph) lives in a
separate library repository that includes this one as a submodule — see
[`rtsantos3/LAD_library`](https://github.com/rtsantos3/LAD_library) for an example.

**New here?** First install the Trellis CLI — it ships as an npm package and that
is the main download route:

```bash
npm install -g @rtsantos3/trellis-app     # provides the `trellis` command on PATH
```

Then run `./setup.sh` (see [Setup](#setup)) to get a working environment and
hydrate a graph, read [`AGENT-CONTRACT.md`](AGENT-CONTRACT.md) for the agent-facing
runtime contract, and see the [Ingestion pipeline](#ingestion-pipeline) map below
for how a paper flows through end to end.

## What it does

Given paper identifiers (DOIs, PMIDs, or an EndNote/RIS export), the pipeline:

1. **Resolves** each paper's identity across Semantic Scholar, PubMed/NCBI, and
   Crossref (Unpaywall for open-access links).
2. **Enriches** it with abstract, authors, venue, fields of study, MeSH terms, and
   publication types.
3. **Upserts** a `reference` node into Trellis (deduplicated by s2_id → doi → pmid →
   title), tagged with a flat topical vocabulary.
4. **Links** citation edges to other papers already in the graph.

The graph is built to **seed an offline RAG corpus**, so records are denormalized and
export-complete rather than optimized for live traversal.

## Architecture

```
pipeline/
  aggregator.py   batch DOI resolution (one S2 batch call for many papers)
  citations.py    fetch a paper's outbound citations (references)
  ingestion.py    the core: parse → resolve → dedup → upsert → link; ingest_batch()
  trellis.py      thin wrapper over the `trellis` CLI; workspace resolution; indexes
  _utils.py       slug / tag normalization (pub_type_slug, canonical_type_tag)
  _http.py        HTTP with retry/backoff
scripts/
  backfill.py     resumable batch orchestrator over a DOI list
  import_ris_network.py  RIS importer; parses records and feeds ingest_batch()
  export_graph.sh slim JSONL snapshot of the graph (mutation_log stripped)
  monitor.py      live pipeline-status dashboard
  migrations/     one-time data-repair migrations (dry-run by default)
```

The pipeline **shells out to the `trellis` CLI** for all graph writes; Trellis (a
local SQLite-backed knowledge graph) is an external dependency, not vendored here.

### Trellis dependency

Trellis is distributed as the npm package
[`@rtsantos3/trellis-app`](https://www.npmjs.com/package/@rtsantos3/trellis-app)
(source: [`github.com/rtsantos3/Trellis`](https://github.com/rtsantos3/Trellis)).
It bundles a Python (Click) CLI behind a Node launcher, so the `trellis` command
must be on your `PATH` before this pipeline can write to a graph:

```bash
npm install -g @rtsantos3/trellis-app
trellis --version        # sanity check
```

`setup.sh` verifies the `trellis` CLI is present but does not install it — install
the npm package first. The pipeline is pinned to the CLI surface of Trellis
`0.16.x`.

### Workspace resolution

The library a run targets is resolved with this precedence:

```
TRELLIS_WORKSPACE (env or .env)  >  config.yml `workspace:`  >  parent of this repo
```

`pipeline.trellis`, `setup.sh`, and `export_graph.sh` follow this (`setup.sh` also
reads `TRELLIS_WORKSPACE` from `.env`). `scripts/monitor.py` currently honours only
the `TRELLIS_WORKSPACE` env var, not `config.yml`.

Secrets (API keys) live only in `.env` (gitignored). Non-secret tuneables live in
`config.yml` (gitignored; `config.yml.example` is tracked).

## Ingestion pipeline

There is **one** ingestion pipeline: `pipeline.ingestion.ingest_batch`. RIS
import, backfill, and single-paper requests all feed it — nothing creates nodes
by hand-rolling `trellis add`/`link`.

```
 ENTRYPOINTS (every path that ingests a paper)
 ─────────────────────────────────────────────
  scripts/import_ris_network.py   parse RIS ─┐
  scripts/backfill.py → backfill_nodes ──────┤  (bare DOI str ── or ── full record dict)
  pipeline/ingestion.py  --doi/--pmid/--title┤
  scripts/benchmarks/* ──────────────────────┤
                                             ▼
             ╔═══════════════════════════════════════════════╗
             ║        ingest_batch(list[str | dict])         ║   ◄── THE ONE PIPELINE
             ╚═══════════════════════════════════════════════╝
                                             │
   phase 0 │ batch_resolve ................. one S2 batch call for every DOI in the set
           │ build_node_index ............. snapshot the graph into an in-memory index
           ▼
   phase 1 │ resolve_and_upsert (parallel, per item):
           │     parse_input .............. normalize str|dict
           │     resolve_identity ......... enrich: PubMed → S2 → Crossref (fill missing)
           │     find_existing_indexed .... dedup: s2_id ▸ doi ▸ pmid ▸ title
           │     upsert_node .............. create (index+lock) │ or merge into existing
           ▼
   phase 1½│ reverse_materialize .......... link papers already in graph that cite this one
           │ build_edge_index ............. snapshot edges (dedupe guard)
           ▼
   phase 2 │ fetch_and_store → store_citations .. fetch outbound cites, write onto node
           ▼
   phase 3 │ link_stored → link_citations ...... edges to targets already present
           ▼
   phase 4 │ verify_upserted .............. confirm Trellis state
           ▼
   phase 5 │ set_final_pipeline_status .... queued/digesting ─▸ digested
                                                         └────▸ needs-review │ failed
                                             │
                                             ▼
                 pipeline/trellis.py — the ONE write layer (add / update / link /
                 annotate). Every graph write goes through here.
```

A single paper is just a one-item batch: `ingest_batch([record])[0]`. Nothing
creates, enriches, or links a node outside this box; the only other graph
writers are the one-time `scripts/migrations/` tag-repair scripts.

**Input** is `list[str | dict]`: a string is a bare DOI; a dict is a full record
(`title`, optional `doi`/`pmid`/`abstract`/`authors`/`year`/`venue`). Both
normalize through `parse_input`, so a DOI-less record (e.g. a title-only RIS
entry) takes the same path — it just skips the DOI batch prefetch and is enriched
per-paper. A dict needs at least a DOI, a PMID, or a ≥ 10-char title.

The phase-by-phase flow is the diagram above. Two behaviours the diagram only
hints at:

Enrichment guards: a PMID found by *search* is accepted only if its title fuzzy-
matches the known title (≥ 85), rejecting PubMed false matches on unindexed DOIs.

**Idempotency** (the agent re-runs unsupervised, so re-ingest must be a no-op):
dedup converges re-ingests onto one node; identity/status tags are re-derived
each pass; topical tags (`mesh:`/`kw:`/`field:`/`type:`) are re-derived only when
the resolved record carries topical data and otherwise **preserved**, so a sparse
re-ingest never wipes a previously enriched node; and `store_citations` never
overwrites a non-empty citation set with an empty one. The pipeline produces
`pipeline:digested` directly and never emits `pipeline:scaffolded` (a legacy stub
state from the retired single-paper scaffolder).

See `AGENT-CONTRACT.md` → *Ingestion Pipeline* for the agent-facing contract.

## Setup

```bash
./setup.sh
```

`setup.sh` is idempotent and safe to re-run. It creates a conda prefix env at
`./setup`, creates `.env` and `config.yml` (prompting once for the workspace,
defaulting to the parent directory), verifies the `trellis` CLI and `git-lfs`, hydrates
the graph from a committed export if one exists, and runs the offline test suite.

Then edit `.env` with your keys (`NCBI_API_KEY`, `S2_API_KEY`, `CROSSREF_EMAIL`,
`UNPAYWALL_EMAIL`) and activate the env (`conda activate ./setup`).

## Usage

```bash
# ingest a batch of DOIs end-to-end (resolve + enrich + link)
python -c "from pipeline.ingestion import ingest_batch; \
dois=[l.strip() for l in open('samples/seed_dois.txt') if l.strip() and not l.startswith('#')]; \
o,m=ingest_batch(dois); print(len(o),'ingested')"

# import an RIS file/dir end-to-end (parse → ingest_batch: enrich + dedup + link)
python scripts/import_ris_network.py path/to/library.ris

# resumable backfill: (re)process nodes already in the graph, selected by status
python scripts/backfill.py --statuses queued,scaffolded,failed

# snapshot the graph to the shareable JSONL export
./scripts/export_graph.sh

# watch pipeline status (set TRELLIS_WORKSPACE if config.yml is not the default)
TRELLIS_WORKSPACE=<library-dir> python scripts/monitor.py
```

## Persistent-agent model

`AGENT-CONTRACT.md` is the runtime contract for an autonomous agent that operates this
pipeline in a loop. It keeps **no memory across sessions** — all state lives in the
Trellis graph and is driven by `pipeline:*` status tags. The canonical pipeline
drives a node:

```
queued → digesting → digested
                   ↘ needs-review / failed
```

(`scaffolded` is a legacy stub state; `ingest_batch` does not emit it.)

Because the agent re-runs unsupervised, **every operation is idempotent**: identity
tags are re-derived from the resolved record each pass (never accumulated) while
topical tags and stored citations are preserved when a re-ingest resolves none
(no downgrade); citation linking is guarded against duplicate edges by a read-only
edge check (a pipeline-side workaround for the lack of an edge-uniqueness constraint
in Trellis); and an in-flight `pipeline:digesting` marker lets the agent reset stale
in-flight nodes to `failed` on its next startup (per `AGENT-CONTRACT.md`).
Re-ingesting deduplicates nodes and never creates duplicate citation edges, though
it may refresh a node's metadata, status, and annotations.

## Migrations

`scripts/migrations/` holds one-time data-repair scripts (e.g. identity-tag
decontamination, cross-wired-identity repair). All default to a dry run; pass
`--apply` to mutate, and all writes route through the Trellis CLI.

## Testing

```bash
python -m pytest tests/ -q
```

The suite is offline (no network); ingestion paths are exercised with mocked HTTP and
a temporary Trellis workspace.
