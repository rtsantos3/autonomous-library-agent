# Changelog

All notable changes to the autonomous-library-agent are recorded here. Format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- RIS-based ingestion: `ingest_batch` accepts `list[str | dict]`, so DOI-less
  records (e.g. title-only RIS entries) flow through the same pipeline as bare
  DOIs via `parse_input`.
- Intra-batch node dedup: identical records within one batch converge onto a
  single node under a live-index re-check (`node_lock`).
- README: ASCII pipeline map (entrypoints → phases 0–5 → single write layer).
- AGENT-CONTRACT: graph read/query surface (`grep`, `subgraph`, `path`) added to
  the CLI reference and to Mode 2, plus `ingest_batch` recipes and edge cases
  (single item, mixed batch, title-only, partial/PMID-only, failure isolation).

### Changed
- Single ingestion path: `ingest_batch` is the one entry point; single-paper
  requests are a one-item batch (`ingest_batch([record])[0]`).
- Backfill re-feeds DOI-less nodes as record dicts instead of dropping them.
- Title-only records with sufficient basic metadata still enrich (removed the
  `has_sufficient_title_only_metadata` short-circuit); Crossref runs when a DOI
  is absent or no title resolved.
- `_make_tags`: topical tags (`mesh:`/`kw:`/`field:`/`type:`) are re-derived only
  when the resolved record carries topical data, otherwise preserved, so a sparse
  re-ingest never wipes a previously enriched node.
- `_classify_failure`: `locator` errors route to `needs-review`.

### Removed
- Divergent `ingest_reference_pipeline` second path and the per-node
  `find_existing` subprocess dedup (superseded by the index-based
  `find_existing_indexed`).
- Legacy single-paper scaffolder scripts (`scaffold_from_endnote.py`,
  `bulk_scaffold.py`, `feed_csv.py`, `ingest.py`, `link_citations.py`) and the
  `test_pipeline_20` benchmark.

### Fixed
- RIS keywords are no longer dropped unconditionally. `parse_input` now accepts a
  `keywords` field and `resolve_identity` keeps source-side RIS `KW` as a
  fallback floor when enrichment resolves no keywords of its own — enrichment
  keywords still win when present, so only enrichment-miss records are affected
  (previously they were left with no `kw:` tags).
- AGENT-CONTRACT: `o.verify.pipeline_status` is a phase-4 (pre-final) snapshot,
  not the committed status; documented that final status is derived from
  `o.errors` (`digested`, else `_classify_failure`).

### Verified
- All `ingest_batch` input shapes exercised against the live LAD_library graph
  (2958 nodes): single DOI string, single dict, mixed batch, title-only,
  PMID-only, failure isolation, and intra-batch duplicate. Net new nodes: 0 (all
  dedup-merged); cross-identifier dedup converged DOI/PMID/title onto one node;
  one malformed record errored in isolation without aborting its batch; repeated
  re-ingest added no duplicate edges (idempotent).
- Offline suite: 221 passed, 8 skipped.
