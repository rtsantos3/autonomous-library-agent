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
