# Ingestion TODOs By Phase

## Resolved Design Gaps

1. URI format — standardized on `https://doi.org/<doi>`; `find_by_doi` normalizes both forms.
2. URI update limitation — URI is set-once at create time; CLI does not support updating uri; DOI always stored in `metadata.reference.doi` as fallback.
3. `--metadata` behavior — replaces entire `metadata_` blob; read-merge-write required in `store_citations`.
4. `pipeline:queued` → `pipeline:scaffolded` transition — explicit `set_pipeline_status` call in `upsert_node` for existing nodes; new nodes include `pipeline:scaffolded` in initial tags.
5. Link idempotency — `link_nodes` returns `{"ok": True/False, "error": ...}`; duplicate edges treated as success.

## Phase 0: CLI Contract

- [ ] Verify global `trellis --help` works in the active shell.
- [x] Verify `trellis add reference --help` documents `--metadata`, `--abstract`, `--parent`, `--actor-id`, and `--json`.
- [x] Verify `trellis get <node> --json` returns `metadata.reference`.
- [ ] Verify `trellis update <node> --metadata ... --json` updates only the reference metadata payload expected by the CLI.
- [x] Verify `trellis link` uses `--relation` flag.

Robustness checks:

- [ ] CLI failures return structured stderr/stdout in our wrapper.
- [ ] Wrapper never silently falls back to a local Trellis installation.
- [ ] Tests fail clearly if global `trellis` is missing or broken.

## Phase 1: Input Parsing

- [ ] Parse one RIS file into a normalized internal record.
- [ ] Extract title, DOI, PMID, year, authors, journal, abstract, and URL when present.
- [ ] Normalize DOI forms such as `doi:`, `https://doi.org/`, and `http://dx.doi.org/`.
- [ ] Preserve the original raw RIS path and source fields for debugging.

Robustness checks:

- [ ] Missing DOI with present title is accepted for later resolution.
- [ ] Empty or malformed RIS fails before Trellis writes.
- [ ] Multiple DOI-like fields resolve to one normalized DOI or fail as ambiguous.

## Phase 2: Identity Resolution

- [ ] Implement `resolve_reference_identity(record)`.
- [ ] Prefer DOI, then PMID, then normalized title.
- [ ] Fill missing metadata from source priority: PubMed, Semantic Scholar, Crossref, arXiv, OpenAlex.
- [ ] Return a structured result with provenance for every filled field.

Robustness checks:

- [ ] No DOI, PMID, or usable title stops the pipeline.
- [ ] Ambiguous title resolution stops the pipeline.
- [ ] Conflicting DOI/PMID metadata stops or flags the record instead of overwriting.

## Phase 3: Trellis Deduplication

- [ ] Implement `find_existing_reference(resolved)`.
- [ ] Search by DOI first.
- [ ] Search by PMID second.
- [ ] Search by normalized title third.
- [ ] Return exactly one existing node or an ambiguity result.

Robustness checks:

- [ ] Rerunning the same RIS does not create a duplicate reference.
- [ ] Ambiguous title matches do not create or update any node.
- [ ] Dedup code compares normalized metadata, not only substring search output.

## Phase 4: Reference Upsert

- [ ] Implement `upsert_reference_node(resolved)`.
- [ ] Create `reference` under `microbiome-research-library` when absent.
- [ ] Add `pipeline:scaffolded` as the single pipeline status for new nodes.
- [ ] Merge metadata into `metadata.reference` for existing nodes without clobbering unrelated fields.
- [ ] Annotate ingestion source and timestamp.

Robustness checks:

- [ ] Trellis add failure stops the pipeline.
- [ ] Trellis update failure leaves the previous node state intact.
- [ ] Existing metadata survives a read-merge-write update.
- [ ] The returned node includes a real Trellis slug or UUID from CLI output.

## Phase 5: Citation Retrieval

- [ ] Implement `fetch_outbound_citations(resolved)`.
- [ ] Use Semantic Scholar references endpoint for DOI-backed records first.
- [ ] Extract cited paper DOI, PMID, title, year, and S2 paper ID.
- [ ] Normalize and deduplicate outbound citation items.
- [ ] Support rate-limit and transient-error retries.

Robustness checks:

- [ ] Missing source DOI skips DOI-based citation retrieval cleanly.
- [ ] API 429/503 uses bounded backoff.
- [ ] Citation entries without DOI but with title are retained as metadata only.
- [ ] Empty citation responses are stored as a valid empty retrieval result.

## Phase 6: Citation Metadata Persistence

- [ ] Implement `store_outbound_citations(source_node, citations)`.
- [ ] Read the latest node JSON before updating metadata.
- [ ] Merge citations into `metadata.reference.outbound_citations`.
- [ ] Include `source`, `retrieved_at`, and normalized `items`.
- [ ] Keep unresolved citations as metadata only.

Robustness checks:

- [ ] Existing `metadata.reference` fields are preserved.
- [ ] Existing non-reference metadata is not touched.
- [ ] Update command uses JSON serialization safe for shell execution.
- [ ] Failed metadata update is reported and does not trigger edge linking.

## Phase 7: Citation Edge Materialization

- [ ] Implement `link_resolved_outbound_citations(source_node, citations)`.
- [ ] Build a Trellis lookup for target references by DOI and PMID.
- [ ] Link only when exactly one target reference exists.
- [ ] Use relation `references`.
- [ ] Annotate skipped ambiguous target matches.

Robustness checks:

- [ ] Missing target references do not create stubs.
- [ ] Ambiguous targets do not create edges.
- [ ] Duplicate edge attempts are treated as idempotent success.
- [ ] Edge-link failure does not remove citation metadata.

## Phase 8: Central Orchestrator

- [ ] Implement `ingest_reference_pipeline(input_ref)`.
- [ ] Call phases in strict order: parse, resolve, dedup, upsert, fetch citations, store metadata, link resolved edges.
- [ ] Return `IngestionOutcome` with phase results, source node, stored citations, created links, skips, and errors.
- [ ] Make the bulk importer call this function later instead of duplicating logic.

Robustness checks:

- [ ] Each phase can be unit tested in isolation.
- [ ] A failed phase stops later mutating phases.
- [ ] Partial success is explicit, especially node-created-but-citations-failed.
- [ ] Logs and annotations contain enough context to reproduce failures.

## Phase 9: Tests

- [ ] Keep `tests/test_ris_smoke.py` as the global Trellis add/get/delete smoke test.
- [ ] Add an orchestrator test with mocked citation retrieval.
- [ ] Add a metadata merge test proving existing fields survive outbound citation writes.
- [ ] Add a no-stub test proving unresolved citations do not create target references.
- [ ] Add an idempotency test proving the same RIS input does not duplicate the source.

Robustness checks:

- [ ] Tests clean up Trellis nodes they create.
- [ ] Tests do not require network except explicitly marked integration tests.
- [ ] Network citation retrieval is isolated behind a mockable boundary.
- [ ] Test failures name the failed phase.

## Phase 10: Bulk Import Later

- [ ] Wrap the single-reference orchestrator in a bulk loop only after Phases 0-9 pass.
- [ ] Add rate limiting across records.
- [ ] Add per-record result reporting.
- [ ] Add resume behavior based only on Trellis state.

Robustness checks:

- [ ] Bulk import does not maintain an external queue.
- [ ] Bulk import can be interrupted and safely rerun.
- [ ] One bad record does not corrupt another record.
- [ ] Summary reports created, updated, skipped, linked, and failed counts.
