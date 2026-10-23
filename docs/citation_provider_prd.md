# Indexed citation provider requirements

Date: 2026-10-23. Actor: CODEX.
Status: Implemented and independently reviewed. Verification and rollout are
recorded in `CURRENT_STATUS.md`.

## Goal

Collect the available indexed outbound references for each intake paper. Merge
provider results into one deduplicated bibliography. Keep intake and persistence
independent of the retrieval API.

## Scope

Use Semantic Scholar, Crossref, Europe PMC, OpenAlex, and OpenCitations. Each
provider contributes to the union. Do not use a fallback chain.

Use the existing `pipeline.ingestion.ingest_batch` entrypoint and canonical
Trellis write path. RIS import, DOI intake, and backfill keep that entrypoint.
Do not add dependencies, Docker services, full-text extraction, or cited-paper
stub nodes. Bibliographic metadata is the only input to provider adapters.

## Provider contract

Each module in `pipeline/providers/` exposes:

```python
retrieve(doi: str) -> CitationResult
```

`pipeline/citation_types.py` owns the common records:

- `CitationResult`: `source`, `retrieved_at`, `items`, and `providers`.
- `CitationItem`: `doi`, `pmid`, `s2_id`, `title`, `year`, and `sources`.
- Optional `openalex_id` and `omid` preserve index-only references.

All providers return this structure. A usable item has a supported identifier or
a structured title. Do not invent titles from unstructured bibliography strings.
Do not use a citing work's publication date as a cited work's year.

Use the existing shared HTTP retry helper and separate provider rate limiters.
Keep API credentials out of logs. `OPENALEX_API_KEY` and
`OPENCITATIONS_ACCESS_TOKEN` are optional. Europe PMC needs no key.

### Retrieval requirements

1. Reuse non-empty S2 batch references. Follow pagination for per-paper S2 calls.
   Preserve publisher-withheld notices and PMID-only or S2-ID-only batch targets.
2. Collect Crossref's deposited reference entries, not its reference count.
3. Resolve Europe PMC records by exact DOI. Retrieve indexed reference pages.
   Interpret MED identifiers as PMIDs; do not interpret PMC identifiers as PMIDs.
   Do not let a MED hit marked `hasReferences: N` suppress a usable PMC hit.
4. Retrieve OpenAlex `referenced_works`. Hydrate cited IDs in bounded batches.
   Keep known target IDs if hydration is incomplete or fails. Do not use incoming
   citation counts or related-work lists as outgoing references.
5. Retrieve OpenCitations outgoing references. Parse `cited`, not `citing`.
   Preserve usable DOI-, PMID-, and OMID-only records without metadata hydration.

## Union and identity requirements

- Normalize DOI forms before matching.
- Match DOI, PMID, S2 ID, OpenAlex ID, and OMID identities.
- Use exact, unambiguous normalized-title matches for identifier-poor records.
  Do not introduce fuzzy bibliography matching.
- Merge identical DOIs even when secondary identifiers also match a distinct
  DOI. Preserve distinct preprint and publication DOIs.
- Resolve direct and transitive identity ambiguity across the full input, not
  only records that arrived earlier. Do not assign an identifier-only reference
  arbitrarily to a DOI through a shared secondary identifier or title chain.
- Generic titles cannot contaminate distinct stable-ID anchors. An unambiguous
  exact-title bridge can still join an existing identifier-only group.
- Preserve per-reference provider sources. Preserve provider outcomes separately
  from the union's usable records.

## Failure and completion requirements

A provider failure must not discard usable results from other providers. Retain
already retrieved records after later page or hydration failures. Distinguish
`available`, `empty`, `unavailable`, `withheld`, `failed`, and `partial` outcomes
where the provider supports them.

A node with **zero stored citation records must not finish as `digested`**.
Provider labels, including `available` and `partial`, cannot override this rule.
An empty stored bibliography produces an incomplete-enrichment error and
`pipeline:needs-review`.

Zero outgoing graph links are allowed when the stored bibliography is non-empty.
Link only cited targets already in the library. Do not create missing targets.
Prefer the cited DOI over secondary identifiers when linking. Reject known
conflicting DOIs. Suppress ambiguous secondary identifiers for all lookups,
including DOI-bearing fallbacks. Do not treat non-DOI URLs as DOI conflicts.
Preserve a previously stored non-empty bibliography when a later fetch is empty.
Re-ingestion must reuse the paper node and avoid duplicate, self, or dangling
citation edges.

`available` means usable indexed records exist. It does not establish an
exhaustive published bibliography or complete scientific grounding.

## Acceptance and evidence

| Area | Acceptance |
|---|---|
| Provider parsing | Normalize structured metadata and identifiers; distinguish empty and unusable responses; preserve outgoing direction |
| Pagination and hydration | Retain indexed targets and earlier usable records; mark partial retrieval accurately |
| Union | Preserve provider-exclusive references, identifier bridges, provenance, DOI conflicts, and ambiguous secondary matches |
| Empty output | Actual Trellis nodes with empty stored lists finish `needs-review` for every provider status label |
| Graph persistence | Link existing targets only; preserve node and edge identity on repeat intake |
| Live intake | Exercise all six RIS files in `samples/microbiome_citation_intake/` in an isolated Trellis workspace |
| Crossref independence | Retrieve and merge real references from the other providers without invoking Crossref |
| Mutation validation | Focused coverage passes, fails under a relevant semantic fault, and passes after restoration |

Use the `autonomous-library-agent` conda environment for behavioral checks. Run
the repository pre-commit hooks for code and document checks. Store local evidence
in ignored `tests/results/`. `CURRENT_STATUS.md` records observed results and
unverified rollout acceptance.

## Rollout boundary

Publish through PR #7 against `master`. Do not merge or deploy without approval.
The original two-file Slack/dev acceptance run is separate from isolated local
RIS intake and remains unverified. Do not claim that opening or updating the PR
repairs the original dev environment or deploys the change.
