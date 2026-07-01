# Handoff — Autonomous Library Agent

## What This Is

An autonomous microbiome research agent that ingests, digests, and queries academic literature. Two complementary stores: **Trellis** (enrichment knowledge graph) and **EndNote** (bibliography list via `.ris` exports).

## Current State

### Done
- `AGENT-CONTRACT.md` — full agent orientation / system prompt with 4 operating modes (autonomous loop, interactive query, research command, review notifier)
- `SYSTEM_PROMPT.md` — short bootstrap prompt to pass to Hermes
- `prompts/extract.md` — LLM extraction template (findings, hypotheses, methods, concepts, datasets, gaps → strict JSON)
- `prompts/verify.md` — LLM verification template (three-way: confirmed/uncertain/rejected)
- `prompts/research_report.md` — Telegram summary template
- `pipeline/trellis.py` — Python CLI wrapper (EXISTS but NOT NEEDED — agent uses CLI/API directly)
- `pipeline/ingestion.py` — single unified ingestion pipeline (ACTIVE — parses RIS and batch-ingests into Trellis)
- `seed.py` — seeds EndNote library into Trellis
- `.env` — NCBI API key configured
- `docs/journal_log.md` — full architecture documentation
- `04-implementation-plan.md` — original plan (partially outdated, superseded by AGENT-CONTRACT.md)
- Trellis project node `microbiome-research-library` created
- 10 test papers seeded and scaffolded by the agent, 46 citation-expanded papers queued
- Paper Search MCP installed in Hermes
- Conda env at `setup/auto_research_bot/` with marker-pdf, feedparser, pymed, requests, beautifulsoup4, lxml

### In Progress / Blocked
- **Trellis CLI doesn't expose `abstract`, `citation`, `metadata` fields** — issue filed at rtsantos3/Trellis#51. Agent currently stores abstract in `description` and metadata in tags, which is incorrect. Fix options:
  1. PR to Trellis adding `--abstract`, `--citation`, `--metadata` to CLI (preferred, it's our repo)
  2. Use REST API via `curl` with `trellis serve` running
  3. Direct SQLite writes (not recommended)
- **Existing 53 nodes are type `reference`** (converted from `custom` via direct SQLite update). BUT `reference` is NOT in the Trellis `NodeType` enum (`core/enums.py`). It works via the CLI's `coerce_unknown_type_to_custom` validator. Need to either add `reference` to the enum or use `custom` type.
- **Duplicate nodes exist**: "Bilophila wadsworthia isolates from clinical specimens" (exact dupe), "E. coli" vs "Escherichia coli" (near dupe). Dedup rules updated in AGENT-CONTRACT.md but existing dupes need cleanup.
- **Nougat** not yet installed in the conda env (needed as OCR fallback for scanned PDFs)

### Not Started
- Full seed of 3,940 papers (only 10 test papers seeded so far)
- Digestion loop (no papers digested yet — needs the Trellis field issue resolved first)
- `vault/` structure (directory created, no content yet)
- `references/` RIS generation
- blogwatcher configuration with PubMed RSS feed
- `setup.sh` one-step setup script
- Remote deployment / SSH tunnel setup

## Key Architecture Decisions

- **Hermes Agent** is the runtime. LLM backend is z.ai. No separate Python orchestration needed.
- **Trellis** is the hyperledger — all state, queue, memory, audit log. Pipeline state via `pipeline:*` tags.
- **Paper Search MCP** replaces custom API call scripts for metadata fetching.
- **Semantic Scholar** for citation graph (API key application submitted, currently 1 req/s).
- **Marker** primary PDF extractor, **Nougat** OCR fallback.
- **Node type `reference`** for all papers (needs enum fix). Child nodes: `finding`, `hypothesis`, `method`, `concept`, `dataset`.
- **DOI as canonical URI** (`https://doi.org/<doi>`). Dedup: DOI → PMID tag → normalized title.
- **Citation expansion**: max 2 hops. Off-topic citations ingested anyway.
- **Verification**: three-way (confirmed/uncertain/rejected). Uncertain → `pipeline:needs-review`.
- **Batch size**: 50 papers per cycle.
- **Tags**: `pipeline:*` (state), `mesh:*` (MeSH terms), `kw:*` (author keywords), `domain:*`, `method:*`, `organism:*`, `sample:*`, `condition:*` (content-derived).
- **Stale nodes**: `pipeline:digesting` on startup → flag as `pipeline:failed`, notify user.
- **Report format**: bullet summary with Trellis slug citations via Telegram.

## Trellis Schema (from source)

**Node fields**: `title`, `description`, `abstract_`, `uri`, `citation`, `metadata_` (JSON), `file_path`, `tags` (JSON array), `status` (draft/active/archived/deleted), `references` (JSON array)

**NodeType enum**: project, notebook, finding, concept, hypothesis, method, paper, url, dataset, artifact, custom (NO `reference` — needs adding)

**RelationshipType enum**: supports, contradicts, extends, depends_on, references, derived_from, parent_of, related_to, custom

**NodeStatus enum**: draft, active, archived, deleted

## File Layout

```
autonomous_library_agent/
  AGENT-CONTRACT.md              # Master agent instructions
  SYSTEM_PROMPT.md       # Bootstrap prompt for Hermes
  04-implementation-plan.md
  seed.py                # Seeds EndNote → Trellis
  .env                   # API keys
  prompts/
    extract.md
    verify.md
    research_report.md
  pipeline/              # Unified RIS→Trellis ingestion pipeline
    trellis.py
    ingestion.py
  vault/                 # Empty, will hold per-slug extracted content
  docs/
    journal_log.md
  data/
    My EndNote Library-9.3.enl    # SQLite, 3940 papers
    endnote-extracted/
      PDF/               # 269 local PDFs
      sdb/sdb.eni        # SQLite mapping refs_id → PDF file_path
  setup/
    auto_research_bot/   # Conda env
```

## Next Steps (Priority Order)

1. PR to Trellis: add `reference` to NodeType enum + expose `--abstract`, `--citation`, `--metadata` in CLI
2. Clean up duplicate nodes in Trellis
3. Run full seed of 3,940 papers
4. Test digestion loop end-to-end on one paper
5. Configure blogwatcher with PubMed microbiome RSS feed
6. Install nougat in conda env
7. Write `setup.sh`
