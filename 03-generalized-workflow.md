# Paper-to-Graph: Generalized Workflow

## Goal
Define the end-to-end workflow for turning a collection of academic papers into a queryable, structured knowledge graph.

## Architecture

```
[Paper Input] -> [Ingestion] -> [Graph (Scaffolded)] -> [Digestion] -> [Graph (Digested)] -> [Downstream Use]
                                                                                                  |
                                                                                            RAG / Queries
                                                                                            Summarization
                                                                                            Trend Analysis
                                                                                            Literature Review
```

## Three Phases

### Phase 1: Paper Ingestion (see 01-paper-ingestion.md)
- Input: paper identifiers (DOIs, arXiv IDs, titles, BibTeX)
- Output: graph populated with scaffolded Paper nodes + citation edges
- Key properties: idempotent, lightweight, fast
- Status: `scaffolded`

### Phase 2: Paper Digestion (see 02-paper-digestion.md)
- Input: scaffolded papers with source URLs/PDFs
- Output: graph enriched with full structured content + citation context
- Key properties: heavier, involves PDF/HTML parsing, enriches existing nodes
- Status: `digested`

### Phase 3: Downstream (consuming the graph)
Not the focus of ingestion/digestion, but the graph enables:
- Semantic search over paper content
- Citation network analysis (impact, clusters, trends)
- Automated literature reviews
- RAG-augmented Q&A over papers
- Gap analysis (what's not cited, what's disconnected)

## Workflow: Seed and Expand

### Option A: Seed Paper Approach
1. Start with one or a few seed papers
2. Ingest seeds -> get their citation networks
3. Expand: ingest all cited and citing papers (breadth-first or depth-limited)
4. Repeat expansion up to configurable depth (e.g. 2 hops)
5. Digest all scaffolded papers
6. Graph is now deep and rich

### Option B: Topic/Bulk Approach
1. Define a topic or query (e.g. "transformer architectures", "RLHF")
2. Search Semantic Scholar / OpenAlex for top N results
3. Ingest all results -> scaffold
4. Expand citation networks
5. Digest all

### Option C: BibTeX/Library Import
1. Import from Zotero, Mendeley, or BibTeX export
2. Ingest all entries -> scaffold
3. Expand citation networks if desired
4. Digest all

## Orchestration

### Queue-Based Pipeline
```
[Ingestion Queue]  -> Worker picks job -> Ingest paper -> Queue cited papers -> ...
[Digestion Queue]  -> Worker picks job -> Digest paper -> Update graph
```

- Papers start in ingestion queue
- After ingestion, cited papers (not yet scaffolded) get added to ingestion queue
- After ingestion, the paper itself moves to digestion queue
- Workers process queues independently

### Scheduling
- Ingestion and digestion can run in parallel (different workers)
- Ingestion is fast — can batch hundreds of papers quickly
- Digestion is slow (PDF download + parse) — rate limit carefully
- Prioritize digestion of papers most relevant to user queries

### State Machine per Paper
```
[new] --ingest--> [scaffolded] --digest--> [digested] --enrich--> [enriched]
                         |                       |
                    (can expand             (ready for
                     citation net)            downstream)
```

## Configuration

```yaml
graph:
  backend: neo4j  # or networkx, arangodb, etc.
  uri: bolt://localhost:7687

ingestion:
  sources_priority: [semantic_scholar, crossref, arxiv, openalex]
  expand_citations: true
  max_expansion_depth: 2
  rate_limit: 100  # requests per minute

digestion:
  pdf_storage: ./papers/pdf/
  extraction_tool: grobid  # or marker, pymupdf
  grobid_url: http://localhost:8070
  timeout_per_paper: 120  # seconds
  retry_failed: true

logging:
  level: INFO
  file: ./pipeline.log
```

## Monitoring and Metrics
- Total papers scaffolded vs digested vs failed
- Citation linkage rate (% of citations resolved to graph nodes)
- Extraction quality score (sections extracted, references parsed)
- Processing time per paper
- Queue depth and throughput

## Common Operations

### Ingest a single paper
```
pipeline ingest --doi 10.1234/xyz
pipeline ingest --arxiv 2301.00001
pipeline ingest --title "Attention Is All You Need"
```

### Bulk ingest from BibTeX
```
pipeline ingest --bibtex references.bib
```

### Digest all scaffolded papers
```
pipeline digest --status scaffolded
```

### Re-digest a specific paper
```
pipeline digest --doi 10.1234/xyz --force
```

### Expand citation graph
```
pipeline expand --depth 2 --source doi:10.1234/xyz
```

### Status check
```
pipeline status
# Scaffolded: 342 | Digested: 289 | Failed: 12 | Partial: 3
```

## File Structure
```
paper-to-graph/
  01-paper-ingestion.md
  02-paper-digestion.md
  03-generalized-workflow.md    (this file)
  config.yaml
  papers/
    pdf/          # downloaded PDFs
    html/         # scraped HTML
  logs/
    pipeline.log
```
