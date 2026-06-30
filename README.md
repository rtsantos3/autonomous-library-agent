# autonomous-library-agent

A literature **ingestion pipeline** and **persistent-agent contract** that build and
maintain a research library as a [Trellis](https://github.com/rtsantos3/trellis-app)
knowledge graph: every paper is a node, every citation is an edge. The pipeline is
**library-agnostic** — one agent can serve many libraries; each library is just a
workspace directory the agent is pointed at.

This repository is the *tooling*. The *data* (a materialized graph) lives in a
separate library repository that includes this one as a submodule — see
[`rtsantos3/LAD_library`](https://github.com/rtsantos3/LAD_library) for an example.

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

**Input** is `list[str | dict]`: a string is a bare DOI; a dict is a full record
(`title`, optional `doi`/`pmid`/`abstract`/`authors`/`year`/`venue`). Both
normalize through `parse_input`, so a DOI-less record (e.g. a title-only RIS
entry) takes the same path — it just skips the DOI batch prefetch and is enriched
per-paper. A dict needs at least a DOI, a PMID, or a ≥ 10-char title.

Per batch, in order:

1. **batch resolve** — one Semantic Scholar batch call prefetches every DOI in
   the batch.
2. **resolve + upsert** — for each item: `parse_input` → `resolve_identity`
   (enrich via PubMed → Semantic Scholar → Crossref, merging only missing
   fields) → dedup against an in-memory index (**s2_id → doi → pmid → title**) →
   `upsert_node` (create new, or merge into the existing node) → mark
   `pipeline:digesting`.
3. **reverse materialize** — existing nodes that cite this paper get a
   `references` edge to it (inbound linking).
4. **fetch + store** — fetch the paper's outbound citations and store them on the
   node.
5. **link** — materialize `references` edges to every cited target already in the
   graph (deduped against a pre-built edge index).
6. **verify + status** — confirm state, then set `pipeline:digested` (success) or
   `pipeline:needs-review` / `pipeline:failed`.

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
