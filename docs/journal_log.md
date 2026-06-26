# Journal Log

## 2026-04-01 — Architecture Design Session

### Overview

Designed the full architecture for an autonomous microbiome research agent that ingests, digests, and queries academic literature autonomously. The system is built around two complementary stores: **Trellis** as the enrichment knowledge graph and **EndNote** as the bibliography list.

---

### Stack Decisions

- **Hermes Agent** (NousResearch) — persistent agent runtime with SQLite-backed memory, messaging gateway (Telegram), and skill generation. Handles the autonomous loop and user interaction.
- **z.ai** — LLM backend for Hermes.
- **Trellis** — knowledge graph / hyperledger. All pipeline state, node relationships, findings, hypotheses, and methods live here. Acts as the work queue via `pipeline:*` tags.
- **Marker** — PDF → Markdown extraction for digestion.
- **Semantic Scholar API** — primary source for citation graph resolution.
- **PubMed E-utilities + blogwatcher** — live RSS feed for new microbiome literature.
- **EndNote** — bibliography list. Agent generates `.ris` files per paper, stored in `references/`, for manual or watched-folder sync.

---

### Two-Store Architecture

| Store | Purpose |
|-------|---------|
| **Trellis** | Enrichment graph — findings, hypotheses, methods, citation links, pipeline state |
| **EndNote** | Bibliography list — standard reference metadata, what you cite in papers |

Trellis nodes point to `vault/<slug>/` for full content. `references/<slug>.ris` files are EndNote-compatible exports generated during digestion.

---

### Pipeline: Two Steps

#### Step 1 — Ingestion (Scaffold)
Lightweight. No LLM. Pure API calls.

1. Takes a DOI, PMID, arXiv ID, or title
2. Checks Trellis for existing node by DOI (canonical URI) → skips if already present
3. Fetches metadata from PubMed → Semantic Scholar → Crossref → arXiv → OpenAlex (priority order)
4. Creates a `custom` Trellis node under the `microbiome-research-library` project, tagged `pipeline:queued` → `pipeline:scaffolded`
5. Fetches citation graph from Semantic Scholar (`/references` + `/citations`)
6. For each cited/citing paper: checks Trellis → links with `cites` relation if exists, adds as `pipeline:queued` if not
7. Citation expansion is depth-configurable

#### Step 2 — Digestion (Full Content)
Handled by Hermes agent loop. LLM-driven.

1. Picks up `pipeline:scaffolded` nodes
2. Sets `pipeline:digesting` to claim the node (prevents double-processing)
3. Fetches full text: PMC → Europe PMC → Unpaywall → Semantic Scholar PDF → arXiv PDF → abstract only (`pipeline:partial`)
4. PDF path: Marker → Markdown. HTML path: BeautifulSoup → Markdown
5. LLM pass 1: extracts findings, hypotheses, methods, concepts, datasets from full text
6. LLM pass 2: verifies each extraction against source paragraph, scores confidence
7. Low confidence extractions tagged `pipeline:needs-review` → Hermes notifies via Telegram
8. Creates child Trellis nodes (`finding`, `hypothesis`, `method`, `concept`, `dataset`), linked to parent reference
9. Writes `vault/<slug>/` — full_text.md, findings.json, hypotheses.json, methods.json, concepts.json, metadata.json
10. Writes `references/<slug>.ris` — EndNote-compatible export
11. Sets `pipeline:digested`

---

### Seed Process

The existing EndNote library (`data/My EndNote Library-9.3.enl`) is a SQLite database with 3,940 papers. Key fields: `electronic_resource_number` (DOI), `accession_number` (PMID), `title`, `author`, `year`, `abstract`, `keywords`.

Seed flow:
1. Parse all 3,940 records from the `.enl` SQLite DB → add all as `pipeline:queued` in Trellis (fast, no API calls)
2. Agent ingestion loop picks them up one by one → resolves via Semantic Scholar → builds citation graph
3. For each citation: already in the 3,940? → link. Not present? → add as `pipeline:queued`
4. Digestion loop processes scaffolded nodes → full content extraction

Trellis serves as the progress tracker throughout — no separate index or checkbox system needed.

---

### Autonomous Research Loop

Triggered by: `research <topic>` via Telegram to Hermes.

1. Query Trellis — what do we already know about the topic? Summarize existing findings, identify gaps.
2. Search PubMed + Semantic Scholar for top N papers on the topic
3. Deduplicate against existing Trellis nodes by DOI
4. Add new papers as `pipeline:queued`
5. Ingestion loop: scaffold → fetch citation graph → expand citations autonomously
6. Digestion loop: full text → extract → verify → write vault + references
7. Cross-link new findings to existing graph (supports/contradicts existing hypotheses)
8. Report back via Telegram: findings summary, citation slugs, flagged items for review

Progress updates sent to Telegram at each major milestone.

---

### Operational Decisions (Interview Outcomes)

#### Stale Nodes
Nodes stuck at `pipeline:digesting` after a Hermes restart are flagged as `pipeline:failed` and the user is notified via Telegram for manual review.

#### Citation Expansion Depth
2 hops maximum. Direct citations + citations of citations. Keeps the graph useful without noise.

#### PDF Extraction Fallback
Primary: Marker. If Marker fails (scanned, corrupted, image-heavy), use Nougat OCR as secondary extractor. Nougat added to dependencies.

#### Off-Topic Citations
Ingest anyway. Citation context matters even if the cited paper is outside the microbiome domain.

#### LLM Verification (Three-Way)
Each extraction is verified against the source text:
- **Confirmed** — extraction accurately reflects the source. Stored normally.
- **Uncertain** — may be inaccurate. Tagged `pipeline:needs-review`, user notified via Telegram.
- **Rejected** — does not reflect the source. Discarded, not stored.

#### Batch Size
50 papers per cycle. Agent processes up to 50 queued/scaffolded nodes per cycle before sleeping, to stay within API rate limits and provide frequent checkpoints.

#### Research Report Format
Bullet summary with Trellis slug citations, sent via Telegram. Example:
```
Research: "gut-brain axis in microbiome"
- [slug-1] Found that vagal signaling mediates gut-brain communication
- [slug-2] Bifidobacterium reduces anxiety-like behavior in mouse models
- 3 findings flagged for review: [slug-3], [slug-4], [slug-5]
```

---

### Pipeline State Machine

```
pipeline:queued → pipeline:scaffolded → pipeline:digesting → pipeline:digested
                                                           → pipeline:partial (no full text)
                                                           → pipeline:needs-review (low confidence)
                                                           → pipeline:failed
```

Trellis native `status` field (`draft`, `in_progress`, `completed`, `archived`) is separate from pipeline state tags.

---

### Deduplication

- **Primary key**: DOI stored as node URI (`https://doi.org/<doi>`)
- **Fallback**: PMID tag search
- **Last resort**: title fuzzy match → flagged for manual review
- No URI = cannot guarantee deduplication = skip and log

---

### API Rate Limits

| Source | Limit | Strategy |
|--------|-------|----------|
| Semantic Scholar | 1 req/s (unauth), 10 req/s (with key) | exponential backoff, API key recommended |
| PubMed E-utilities | 3 req/s (unauth), 10 req/s (with NCBI key) | NCBI API key configured |
| Crossref | ~50 req/s (polite pool) | `mailto=` param |
| arXiv | 3 req/s | fixed delay |
| OpenAlex | 10 req/s | `mailto=` param |
| Unpaywall | 100k req/day | `email=` param |

---

### File Structure

```
autonomous_library_agent/
  pipeline/
    trellis.py              # Trellis CLI wrapper
    ingest.py               # Step 1: scaffold a reference
    sources/
      endnote.py            # Parse .enl SQLite DB
      pubmed.py             # PubMed E-utilities + PMC full text
      semantic_scholar.py   # Metadata + citation graph
      crossref.py
      arxiv.py
    extractors/
      marker_pdf.py         # PDF → Markdown
      html.py               # HTML → Markdown
  agents/
    rss_agent.py            # blogwatcher glue → ingest pipeline
  vault/
    <slug>/
      full_text.md
      findings.json
      hypotheses.json
      methods.json
      concepts.json
      metadata.json
  references/
    <slug>.ris              # EndNote-compatible exports
  docs/
    journal_log.md
  AGENT-CONTRACT.md                 # Agent orientation / system prompt
  04-implementation-plan.md
  config.yaml
  .env                      # API keys (not committed)
  setup/
    auto_research_bot/      # Conda env
```
