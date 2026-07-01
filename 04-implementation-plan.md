# Implementation Plan

## Overview

A 2-step autonomous pipeline that ingests knowledge from any source into Trellis, starting with an EndNote seed library and expanding via a PubMed RSS agent.

## Stack

- **Trellis** — knowledge graph store (`reference`, `finding`, `hypothesis`, `concept`, `method` nodes)
- **Hermes Agent** — scheduled autonomous agent runtime
- **z.ai** — LLM backend
- **Marker** — PDF → Markdown extraction
- **PubMed E-utilities + RSS** — live feed source

---

## Step 1: Ingestion (Scaffold)

Lightweight first pass. Creates a `reference` node with metadata only — no full text yet.

### Inputs accepted
- DOI
- PubMed ID (PMID)
- arXiv ID
- BibTeX / RIS / EndNote `.enl`
- Raw title string
- RSS item

### Sources (priority order)
1. Semantic Scholar — metadata + citation graph
2. PubMed E-utilities — metadata + abstract
3. Crossref — DOI-based metadata
4. arXiv — preprints
5. OpenAlex — bulk/fallback

### API Rate Limits

| Source | Limit | Strategy |
|--------|-------|----------|
| Semantic Scholar | 1 req/s (unauth), 10 req/s (with API key) | exponential backoff, API key recommended |
| PubMed E-utilities | 3 req/s (unauth), 10 req/s (with API key) | API key via NCBI account |
| Crossref | ~50 req/s (polite pool with email) | add `mailto=` param |
| arXiv | 3 req/s | fixed delay between requests |
| OpenAlex | 10 req/s (unauth), higher with email | add `mailto=` param |
| PMC (full text) | 3 req/s | same as PubMed |
| Unpaywall | 100k req/day with email | add `email=` param |

All sources use exponential backoff on 429/503. Delays are configurable in `config.yaml`.

### Citation Graph (Semantic Scholar)

After resolving paper metadata, fetch the citation graph:

1. Resolve paper to Semantic Scholar ID (via DOI or title search)
2. `GET /graph/v1/paper/{id}/references` — outbound citations (papers this paper cites)
3. `GET /graph/v1/paper/{id}/citations` — inbound citations (papers citing this paper)
4. For each cited/citing paper:
   - Check if already in Trellis (`node_exists`)
   - If not: create minimal scaffold node (`status:queued`) — will be fully ingested in next cycle
   - `trellis link <source> <target> --relation cites`
5. Citation expansion depth is configurable (`max_expansion_depth` in config)

### Output
- Trellis `custom` node with fields stored in description/tags:
  - `title`, `authors`, `year`, `venue`, `abstract`, `doi`, `pmid`, `arxiv_id`
  - `source_url` — best available full-text URL
  - `status:scaffolded`
- Trellis `concept` nodes for keywords/topics, linked via `related-to`
- `cites` edges to/from other `custom` nodes (cited papers scaffolded minimally if not already present)

### Idempotency
- Keyed on DOI (preferred) or deterministic hash of title + first author + year
- Re-run updates existing node, does not duplicate

---

## Step 2: Digestion (Full Content)

Heavier second pass. Pulls full text and extracts structured knowledge into child nodes.

### Full-text acquisition (fallback chain)
1. PubMed Central (PMC) — free full text via API
2. Europe PMC
3. Unpaywall — open access PDF
4. Semantic Scholar hosted PDF
5. arXiv PDF
6. Publisher HTML (scrape)
7. **Partial** — abstract only, flag `status: partial`

### Extraction
- PDF path: **Marker** → Markdown
- HTML path: BeautifulSoup → Markdown
- LLM pass over extracted Markdown to produce structured output

### LLM Extraction Targets
From the full text, extract and create child Trellis nodes:

| Extracted item | Trellis node type | Linked via |
|---|---|---|
| Key findings | `finding` | `HAS_FINDING` |
| Identified gaps | `finding` (tagged `gap`) | `HAS_FINDING` |
| Hypotheses / claims | `hypothesis` | `HAS_HYPOTHESIS` |
| Methods / tools used | `method` | `USES_METHOD` |
| Concepts / topics | `concept` | `HAS_TOPIC` |
| Datasets used | `dataset` | `USES_DATASET` |

### Cross-linking
After extraction, link new nodes to existing Trellis nodes where relevant:
- New `concept` nodes merged with existing matching concepts
- New `finding` linked to contradicting or supporting findings already in graph
- New `reference` linked to existing references it cites

### Output
- `reference` node updated with `full_text`, `sections` (JSON), `status: digested`
- Child `finding`, `hypothesis`, `method`, `concept`, `dataset` nodes created and linked

---

## Phase 0: Seed Ingestion

Ingest the existing EndNote library as baseline.

1. Parse `data/My EndNote Library-9.3.enl` → extract all references
2. Run Step 1 (scaffold) for each entry — use existing PDFs in `data/endnote-extracted/PDF/` where available
3. Run Step 2 (digest) on all scaffolded entries
4. Result: Trellis populated with ~N reference nodes, fully digested

---

## Phase 1: PubMed RSS Agent

Autonomous agent that watches PubMed for new microbiome papers.

### Feed
- PubMed RSS for query: `microbiome[MeSH Terms]` (configurable)
- Poll interval: configurable (default 6h)

### Per item workflow
1. Check if PMID already exists in Trellis → skip if `status: digested`
2. Run Step 1 (scaffold)
3. Run Step 2 (digest)
4. Cross-link to existing graph nodes

### Agent runtime
- Hermes scheduled job (runs on interval, exits cleanly after each run)
- State tracked in Trellis (no separate state file needed)

---

## File Structure

```
autonomous_library_agent/
  pipeline/
    ingest.py          # Step 1: scaffold a reference
    digest.py          # Step 2: full content extraction + LLM structuring
    sources/
      pubmed.py        # PubMed E-utilities + PMC full text
      semantic_scholar.py
      crossref.py
      arxiv.py
      endnote.py       # Parse .enl / RIS / BibTeX
    extractors/
      marker_pdf.py    # PDF → Markdown via Marker
      html.py          # HTML → Markdown
    trellis.py         # Trellis CLI wrapper (add, link, update, find)
    llm.py             # z.ai LLM calls
  agents/
    rss_agent.py       # PubMed RSS watcher agent
    seed_agent.py      # One-shot EndNote library ingestion
  config.yaml
```

---

## Configuration

```yaml
trellis:
  project_root: /home/articulatus/git_repos/autonomous_library_agent

llm:
  provider: z.ai
  model: # TBD
  max_tokens: 8192

ingestion:
  sources_priority: [pubmed, semantic_scholar, crossref, arxiv, openalex]
  expand_citations: true
  max_expansion_depth: 2

digestion:
  pdf_storage: ./papers/pdf/
  extraction_tool: marker
  partial_on_failure: true

rss:
  feeds:
    - url: https://pubmed.ncbi.nlm.nih.gov/rss/search/?term=microbiome%5BMeSH+Terms%5D
      label: pubmed_microbiome
  poll_interval_hours: 6
```

---

## Long-Term Plans

Deferred work, not started. Gated behind finishing materialization (the
backfill that digests the scaffolded library into the Trellis graph). Recorded
here so the design seam is not lost.

### RIS ingestion pipeline (reference-manager interoperability)

**Intent.** Make RIS the canonical input format so the pipeline networks cleanly
with Zotero, Mendeley, EndNote, and Papers — all of which export RIS. A user
exports their library to `.ris` and feeds it in to generate a citation-graph /
RAG backbone in Trellis. Trellis is the sink; RIS is the front door.

**Architecture.**
```
library.ris -> ris_adapter -> normalized refs -> ingest_batch -> Trellis nodes + edges
              (parse + map)   [{doi,pmid,title,  (resolve -> dedup
                               year,venue,authors, -> enrich -> link
                               abstract,kw,type}]  -> state machine)
```

**What already exists.** `scripts/import_ris_network.py:parse_ris_text` is a
working RIS parser (tag regex, `ER` record flush, continuation lines, full field
mapping: T1/TI, AB/N2, DO/M3, PY/Y1/DA, JO/T2/J2, AU, KW, UR). It now feeds
the unified `pipeline/ingestion.py:ingest_batch` state machine directly.

**Work already done.**
- `parse_ris_text` feeds normalized reference dicts into `ingest_batch`.
- Unified ingestion: RIS records go directly to `pipeline:digested` (no scaffolded
  intermediate).
- RIS `TY` types (`JOUR`, `BOOK`, `CHAP`, `CONF`, ...) map to canonical
  `type:*` slugs via `pub_type_slug` / `canonical_type_tag`.

**Open design points.**
- *DOI-keying gap.* The pipeline resolves/enriches by DOI; RIS from reference
  managers is frequently DOI-less (books, theses, older or hand-entered items).
  The adapter needs a title/author resolve step (S2/Crossref) to recover a DOI
  before digestion; no match -> scaffold-only (`needs-review`), never digested.
- *Format scope.* RIS first (most universally GUI-exported). CSL-JSON and
  BibTeX adapters can follow on the same seam if needed.

### Other deferred items
- Type-tag cleanup on already-digested nodes: source path and re-ingestion now
  self-heal the camelCase `type:journalarticle` duplication; the ~1.2k
  already-`digested` nodes keep harmless duplicate tags.
  `scripts/migrations/canonicalize_type_tags.py --apply` cleans them if pristine
  facets are wanted.
- Slug normalization for `case-reports` (plural) vs `casereport`, and S2's
  combined `lettersandcomments` category, which have no clean PubMed twin.

---

## Enrichment: MeSH-tree denormalization (for RAG seeding)

**Purpose.** The graph is built to **seed a RAG corpus** (offline), not for live
graph retrieval. So enrichment optimizes for self-contained, richly-filterable
per-paper export records — denormalize structure onto each paper rather than
building navigable graph entities. (No concept nodes: those serve live traversal
a RAG seed never does. The MeSH hierarchy is baked onto each paper as tags.)

**Correctness rule (non-negotiable).** Key everything off the MeSH
**DescriptorUI**, never the slugified term name. Searching MeSH by name
mis-resolves common terms (observed: `Bacteria → Y05.070`, `Diet → Y11.010`,
both wrong; `Obesity` returned a UID, not a tree number). PubMed already supplies
the UI in each article's MeSH headings (`<DescriptorName UI="D001419">`); capture
it at enrichment time and resolve tree numbers by UID.

**Tag scheme — ancestor-expanded.** For each major MeSH heading, emit not just
the leaf tree number but its whole ancestor chain + category, so the exported
record is filterable at every taxonomic level with no graph traversal:
```
Gastrointestinal Microbiome (G06.591.375) ->
  meshtree:G06.591.375  meshtree:G06.591  meshtree:G06  meshcat:G
```
Existing `mesh:` / `mesh-major:` *name* tags stay (human-readable); `meshtree:` /
`meshcat:` are the machine-navigable layer. Ancestor expansion is pure prefix
math — no extra API calls. (Tree numbers are a DAG: a term may have several, each
expanded independently.) Optionally fold MeSH **entry terms** to collapse
synonyms onto the canonical descriptor.

**Promotion set.** Build tree tags from `mesh-major` (2,357 distinct after a
MeSH check-tag stop-list — `Humans/Animals/Male/Female`, age bands, model-organism
strains), not plain `mesh:` (polluted with check-tags).

**Structure (3 files).**
- `pipeline/mesh_tree.py` *(new)* — pure resolver `UID -> {tree_numbers,
  ancestors[], category, canonical_name, entry_terms}`, backed by a cached
  `data/mesh_tree_cache.json` (build-once, ~2.3k entries). Offline-testable.
- `pipeline/ingestion.py` — capture `DescriptorUI` in the MeSH enrichment step;
  emit ancestor-expanded `meshtree:` / `meshcat:` tags.
- `scripts/backfill_mesh_tree.py` *(new)* — one-time retrofit over the digested
  corpus, **anchored on `pmid:`** (re-fetch PubMed record -> UIDs -> tags), so
  the existing papers get the same UID-based correctness.

**Net effect.** Zero new nodes/edges; richer flat tags on existing papers.
