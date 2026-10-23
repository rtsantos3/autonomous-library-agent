# Citation enrichment fix — current status

Recorded: 2026-10-23. Actor: CODEX.

## Overall status

**Five-provider indexed retrieval is implemented locally and verified with six
microbiome RIS papers in an isolated Trellis workspace. The original two-provider
fix was also verified against the local LAD library. The original two-file Slack
acceptance run remains unverified.**

- Branch: `fix/citation-union-completeness`.
- Original two-provider implementation commit: `8d91b09`.
- Pull request: [#7](https://github.com/rtsantos3/autonomous-library-agent/pull/7).
- Related issues: [#5](https://github.com/rtsantos3/autonomous-library-agent/issues/5)
  and [#6](https://github.com/rtsantos3/autonomous-library-agent/issues/6).
- PR #7 is open, not merged. Its base is `master`, the repository default branch.
- Worktree: `/home/articulatus/git_repos/autonomous_library_agent/.worktrees/citation_union_completeness`.
- Publication target: PR #7. Merge and downstream deployment are separate actions.

## Implemented

1. **Collect five providers.** Semantic Scholar, Crossref, Europe PMC, OpenAlex,
   and OpenCitations contribute indexed references through separate
   `retrieve(doi) -> CitationResult` adapters. Non-empty S2 batch data is reused.
   S2 and Europe PMC follow pagination. OpenAlex hydrates indexed IDs in batches
   and preserves unhydrated targets. No full-text extraction is used.
2. **Deduplicate the union.** Normalize DOI forms and merge shared DOI, PMID, S2,
   OpenAlex, or OMID identifiers. Use exact, unambiguous title matches for
   identifier-poor references. Keep conflicting DOIs separate. Resolve direct,
   transitive, and title-based ambiguity across the full input before assigning
   provenance. A shared preprint/publication PMID does not prevent an exact DOI
   merge. Citation links prefer the cited DOI and reject ambiguous secondary IDs.
3. **Preserve provenance.** Store per-reference `sources`, provider outcomes, and
   publisher-withheld notices in `metadata.reference.outbound_citations`.
4. **Reject empty completion.** A node needs a non-empty stored bibliography to
   pass citation verification. Empty or unavailable bibliographies receive an
   incomplete-enrichment error and finish as `pipeline:needs-review`, not
   `pipeline:digested`.
5. **Reuse the existing write path.** Store citations and link existing library
   targets through the canonical ingestion pipeline. Do not create cited-paper
   stubs. Preserve a previously stored non-empty bibliography on an empty fetch.
6. **Update coverage and documentation.** Add parsing, union, provenance, status,
   and isolated graph-repair coverage. Update README, changelog, and runtime
   contract.

`available` means usable reference records exist. It does not assert an exhaustive
bibliography or complete scientific grounding.

## Five-provider verification

| Check | Observed result |
|---|---|
| Full suite, including all network integration cases, in `autonomous-library-agent` | 358 passed |
| Focused provider, batch, and union checks | 148 passed |
| Mutation: suppress full-union ambiguity detection | 11 expected assertion failures; 137 passed |
| Restore ambiguity detection | 148 passed |
| DOI-less target checks in isolated Trellis | 4 passed; unambiguous PMID/S2 matches link, ambiguous matches do not |
| Empty stored bibliographies | Remain `needs-review` for all six provider status labels, including `available` and `partial` |
| Six RIS papers, plus two cited target papers, in isolated Trellis | All six intake papers finished `digested`; all expected links to the seeded diet–microbiome target existed |
| Repeat intake of the review | Same node and edge IDs; no extra reference nodes |
| Graph integrity | No duplicate, self, or dangling edges; exactly eight reference nodes; no cited-paper stubs |
| Real retrieval without invoking Crossref | 75 unique references from S2, Europe PMC, OpenAlex, and OpenCitations for DOI `10.1038/nature12820` |

The first live run exposed duplicate DOIs when S2 associated a preprint DOI with
the published paper's PMID. A regression reproduced this conflict. The merger now
keeps the two DOIs separate while deduplicating each exact DOI.

| Intake paper DOI | Stored union references |
|---|---:|
| `10.1038/s41586-025-09854-7` | 92 |
| `10.1038/s41579-024-01068-4` | 183 |
| `10.1038/s41522-025-00817-4` | 113 |
| `10.1038/s41467-026-77928-9` | 76 |
| `10.1038/s41467-026-72588-1` | 95 |
| `10.1002/imt2.70046` | 79 |

OpenAlex returned partial results for some papers. OpenCitations and OpenAlex
returned empty indexes for some newer papers. These outcomes did not discard
other providers' records. Counts are deduplicated indexed records, not a claim of
complete published bibliographies.

Evidence is in this worktree's ignored `tests/results/pr7-*.log` files.
Publication-gate test output is credential-redacted before logging. The isolated
live run did not modify the LAD library.

## Independent model review

Separate Codex subscription sessions reviewed the full proposed change and PRD.
These were source reviews, not executed verification or GitHub approvals.

| Model | Final focused closure |
|---|---|
| `gpt-6-sol` | No remaining concrete P0/P1/P2 findings in the reviewed corrections |
| `gpt-6-luna` | No remaining actionable findings in the reviewed corrections |

Accepted findings produced regressions for provider-order ambiguity, transitive
identifier/title bridges, DOI-conflicting links, DOI-less target boundaries,
PMID/S2-only batch references, and Europe PMC record selection. One claimed
malformed-S2 parser crash was rejected by executing the supplied payload; the
existing type guard returned an unavailable result without an exception.

The authenticated test run also exposed an incidental mock-header assertion.
That assertion was removed, not repinned. Input/alias mapping checks remain and
the authenticated mapping check passed. An older sentinel test now requires
`needs-review` for its deliberately unresolvable DOI; its node-deduplication
assertions remain unchanged.

## Original two-provider verification

| Check | Observed result |
|---|---|
| Full offline suite in the `autonomous-library-agent` conda environment | 221 passed; 17 integration cases deselected |
| Focused parsing and isolated Trellis checks | 18 passed |
| Semantic mutation: remove the non-empty-bibliography verification guard | 4 expected assertion failures; 14 passed |
| Restore that guard and rerun the same focused checks | 18 passed |
| Repository pre-commit checks | Passed, including Black, isort, flake8, and secrets checks |

Other network-dependent integration tests were not run. Results above were
observed during implementation; this status document does not represent a new
suite run.

## Actual LAD library test

Workspace: `/home/articulatus/git_repos/LAD_library`.

Paper: **The gut microbiome connects nutrition and human health**,
DOI `10.1038/s41575-025-01077-5`.

The review was absent from this local library before testing. The first run
therefore ingested the review; it did not repair an already-existing review node.
The library contained all 16 cited targets identified by DOI.

| Check | First ingestion | Repeat ingestion |
|---|---|---|
| Library nodes | 2,958 → 2,959 | 2,959 → 2,959 |
| Stored usable references | 273 | 273 |
| Verified outgoing links to the expected existing targets | 16 | Same 16 edge IDs |
| New review node | One | None; same node ID |
| Duplicate, self, or dangling incident citation edges | None observed | None observed |
| Final status | `digested` | `digested` |
| Pipeline errors | None | None |

Semantic Scholar reported publisher-withheld references. Crossref supplied the
usable bibliography. The provider outcome and withholding notice were preserved.
All verified reference edges point from the review to the cited paper.

Local evidence is retained in the separate `master/` worktree under
`tests/results/`; these logs are not committed:

- `citation-offline-final.log`
- `citation-mutation-baseline.log`
- `citation-mutation-red.log`
- `citation-mutation-restored.log`
- `lad-citation-first-run.log`
- `lad-citation-repeat-run.log`

## Remaining acceptance and rollout

1. Re-run the original two RIS files through the downstream Slack/dev integration.
   The local DOI test did not replay those uploads or verify the other six papers
   from that acceptance run.
2. Apply or verify the repair in the original dev environment referenced by issue
   #6. That separate environment was not modified by the local LAD library test.
3. Review and merge PR #7, then update the downstream integration to the approved
   revision. Opening the PR did not deploy the change.

The five-provider expansion is separate from the original implementation commit
cited above. Review and merge the approved revision before rollout. Zero stored
citation records must remain `needs-review`, regardless of provider status labels.
Zero outgoing graph links alone do not block `digested`.
