# Paper Ingestion

## Goal
Scaffold academic papers into a graph database idempotently, inferring relationships via citations.

## Overview
Ingestion is the first pass — we create nodes and edges from paper metadata and citation data without needing the full text. This step is intentionally lightweight so we can build the graph quickly and fill in details later.

## Graph Schema

### Nodes
- **Paper** — represents a single paper
  - `id` (unique, hash of DOI or title+authors)
  - `title`
  - `doi`
  - `authors` (list)
  - `year`
  - `venue` (journal, conference, etc.)
  - `abstract`
  - `arxiv_id`
  - `status` — one of: `scaffolded`, `digested`, `enriched`
  - `created_at`
  - `updated_at`

- **Author** — represents a person
  - `id`
  - `name`
  - `orcid` (optional)

- **Venue** — journal, conference, preprint server
  - `id`
  - `name`
  - `type` (journal, conference, workshop, preprint)

- **Topic** / **Keyword**
  - `id`
  - `label`

### Edges
- **CITES** — Paper -> Paper (directed)
  - `context` (optional — the in-text citation context)
  - `confidence` (how confident we are this citation exists)

- **AUTHORED_BY** — Paper -> Author
  - `position` (author order)

- **PUBLISHED_IN** — Paper -> Venue

- **HAS_TOPIC** — Paper -> Topic
  - `relevance` (weight/score)

## Idempotency Rules
1. Each Paper node is keyed on a canonical `id` derived from DOI (preferred) or a deterministic hash of title + first author + year.
2. Re-running ingestion on the same paper updates existing nodes rather than creating duplicates.
3. Citation edges are deduplicated — a (source, target) pair is unique.
4. Author nodes merge on name + ORCID when available; fuzzy matching on name alone flags for manual review.
5. Status field tracks pipeline progress — `scaffolded` means ingestion complete, not yet digested.

## Ingestion Sources
1. **Semantic Scholar API** — metadata + citation graph (free, good coverage)
2. **OpenAlex** — open metadata, works well for bulk
3. **Crossref** — DOI-based metadata
4. **arXiv API** — preprints, useful for CS/Physics
5. **DBLP** — CS bibliography
6. **BibTeX / RIS files** — user-provided

## Ingestion Pipeline Steps

1. **Input Resolution**
   - Accept: DOI, arXiv ID, title string, Semantic Scholar ID, BibTeX entry
   - Resolve to canonical Paper node ID
   - Check if already scaffolded — skip if so

2. **Metadata Fetch**
   - Query sources in priority order (Semantic Scholar -> Crossref -> arXiv -> OpenAlex)
   - Normalize fields into schema
   - Handle missing fields gracefully (abstract may be absent at this stage)

3. **Author Resolution**
   - Parse author list from metadata
   - Match or create Author nodes
   - Create AUTHORED_BY edges with position

4. **Citation Extraction**
   - Fetch outbound citations (papers this paper cites)
   - Fetch inbound citations (papers citing this paper)
   - For each cited paper: create a scaffold Paper node if it doesn't exist (minimal metadata — just enough to have a node)
   - Create CITES edges

5. **Venue Resolution**
   - Parse venue from metadata
   - Match or create Venue node
   - Create PUBLISHED_IN edge

6. **Persist**
   - Write all nodes and edges to graph DB
   - Set paper status = `scaffolded`
   - Log ingestion summary

## Error Handling
- Rate limiting: exponential backoff on API calls
- Missing metadata: create node with what we have, flag as `partial`
- Ambiguous matches: log for review, don't auto-merge authors on low confidence
- Failed API: try next source in priority list

## Output
- Graph DB populated with Paper, Author, Venue, Topic nodes
- CITES, AUTHORED_BY, PUBLISHED_IN, HAS_TOPIC edges
- All scaffolded papers marked `status: scaffolded`
- Ready for Paper Digestion (next phase)
