# Single Reference Ingestion Pipeline

## Goal

Build one robust end-to-end ingestion path for a single paper before adding bulk loops.

The pipeline takes one input reference, resolves its identity, creates or updates one Trellis `reference` node, retrieves outbound citations, stores unresolved citation metadata on that source node, and links citation edges only when the cited references already exist in Trellis.

Trellis remains the only persistent source of truth.

## Non-Goals

- No external pending citation queue.
- No stub nodes for every unresolved citation.
- No bulk import loop until the single-reference path is verified.
- No guessed DOI, PMID, title, slug, or citation edge.

## Trellis Assumptions

- Use the global `trellis` executable.
- Use canonical `reference` nodes.
- Every literature node lives under `microbiome-research-library`.
- Store reference-specific structured metadata under `metadata.reference`.
- Use `references` for paper-to-paper citation links.
- Slugs are assigned by Trellis and must not be constructed manually.

## Metadata Schema

Store temporary outbound citation data on the source reference node:

```json
{
  "reference": {
    "schema": "reference-v1",
    "title": "Source paper title",
    "doi": "10.1000/source",
    "pmid": "12345678",
    "year": 2026,
    "authors": ["Author A", "Author B"],
    "outbound_citations": {
      "source": "semantic-scholar",
      "retrieved_at": "2026-06-23",
      "items": [
        {
          "doi": "10.1000/target",
          "title": "Target paper title",
          "year": 2021,
          "s2_id": "semantic-scholar-paper-id"
        }
      ]
    }
  }
}
```

`outbound_citations.items` is not a queue. It is source-node metadata. A later reconciliation pass can materialize edges from this metadata when both endpoints exist in Trellis.

## CLI Access Pattern

Add one reference:

```bash
trellis add reference "Paper title" \
  --abstract "Abstract text" \
  --metadata '{"schema":"reference-v1","title":"Paper title","doi":"10.1000/source"}' \
  --tags "pipeline:scaffolded,year:2026" \
  --parent microbiome-research-library \
  --actor-id daedalus \
  --json
```

Read a reference before mutating metadata:

```bash
trellis get <slug-or-uuid> --json
```

Update reference metadata after read-merge-write:

```bash
trellis update <slug-or-uuid> \
  --metadata '<merged-reference-metadata-json>' \
  --actor-id daedalus \
  --json
```

Materialize a citation edge only when both nodes exist:

```bash
trellis link <source-slug> <target-slug> references \
  --actor-id daedalus \
  --json
```

## Orchestrator Function

Single entry point:

```python
def ingest_reference_pipeline(input_ref: ReferenceInput) -> IngestionOutcome:
    resolved = resolve_reference_identity(input_ref)
    source_node = upsert_reference_node(resolved)
    outbound = fetch_outbound_citations(resolved)
    source_node = store_outbound_citations(source_node, outbound)
    linked = link_resolved_outbound_citations(source_node, outbound)
    return IngestionOutcome(source_node=source_node, citations=outbound, links=linked)
```

The orchestrator should not hide phase failures. Each phase returns structured results with enough context to test and debug independently.

## Phase Flow

### Phase 1: Parse Input

Accepted inputs:

- RIS record
- DOI
- PMID
- title string

Output:

- normalized title
- normalized DOI when present
- PMID when present
- authors, year, venue, abstract when present

Failure behavior:

- If the input cannot produce either a DOI, PMID, or unambiguous title, stop before writing to Trellis.

### Phase 2: Resolve Identity

Resolution priority:

1. DOI
2. PMID
3. normalized title

Source priority for missing metadata:

1. PubMed
2. Semantic Scholar
3. Crossref
4. arXiv
5. OpenAlex

Failure behavior:

- Do not invent missing identifiers.
- If title-only resolution is ambiguous, stop and report ambiguity.

### Phase 3: Deduplicate In Trellis

Deduplication order:

1. Exact DOI in metadata or text.
2. PMID tag or metadata.
3. normalized title match.

Failure behavior:

- Ambiguous title matches stop the pipeline.
- Existing nodes are updated only by additive metadata merge.

### Phase 4: Add Or Update Reference Node

Create a canonical `reference` node when no duplicate exists.

Minimum fields:

- title
- abstract when known
- `metadata.reference.schema`
- DOI, PMID, year, venue, authors when known
- exactly one `pipeline:*` tag
- parent `microbiome-research-library`

Failure behavior:

- If Trellis add/update fails, stop before citation retrieval.
- Do not continue with an unknown slug.

### Phase 5: Retrieve Outbound Citations

Initial source:

```text
GET /graph/v1/paper/DOI:<doi>/references?fields=title,externalIds,year,paperId&limit=100
```

Extract from each `citedPaper`:

- DOI from `externalIds.DOI`
- PMID from `externalIds.PubMed` when available
- title
- year
- Semantic Scholar paper ID

Failure behavior:

- If citation retrieval fails, keep the source reference node and annotate the failure.
- If no DOI exists for the source, skip Semantic Scholar DOI lookup unless another verified S2 ID exists.

### Phase 6: Store Outbound Citation Metadata

Read the current node first, merge into `metadata.reference`, then update. This prevents clobbering unrelated metadata.

Rules:

- Preserve existing `metadata.reference` fields.
- Replace only `outbound_citations` from the same citation source and retrieval timestamp.
- Normalize citation DOI values.
- Drop citation entries with neither DOI nor title.
- Do not create target nodes here.

### Phase 7: Materialize Resolved Citation Edges

Build an in-memory DOI or PMID index from Trellis search results.

For each outbound citation:

- If a target reference exists, link source to target with `references`.
- If target does not exist, leave it only in source metadata.
- If multiple possible targets exist, skip linking and annotate ambiguity.

Failure behavior:

- Edge creation failures must not corrupt metadata.
- Duplicate edge creation should be treated as idempotent success if Trellis already has the edge.

### Phase 8: Verify Outcome

For a single test reference:

- Confirm one source `reference` exists.
- Confirm `metadata.reference.outbound_citations.items` exists when citations were retrieved.
- Confirm resolved existing targets have `references` edges.
- Confirm unresolved targets did not create stub nodes.
- Confirm rerunning the same input does not duplicate the source node.

## File Targets

Recommended implementation boundaries:

- `scripts/import_ris_network.py`: keep CLI wrapper and RIS input glue.
- `pipeline/trellis.py`: Trellis command wrapper only.
- `pipeline/citations.py`: citation retrieval and normalization.
- `pipeline/ingestion.py`: central orchestrator and phase result types.
- `tests/test_ris_smoke.py`: single-reference add/get/delete smoke test.
- `tests/test_single_reference_ingestion.py`: orchestrator-level test with mocked citation retrieval.

## Acceptance Criteria

- One command can ingest one RIS reference end-to-end.
- Global `trellis` is used; no local Trellis assumption.
- The source node is a canonical `reference`.
- Existing metadata survives citation metadata updates.
- Outbound citations are persisted in `metadata.reference`.
- Citation edges are created only for targets already present in Trellis.
- No unresolved citation creates a stub node.
- The single-reference path is idempotent.
