# PRD — Persistent Agent Runtime (infrastructure)

- **Status:** Draft
- **Date:** 2026-07-17
- **Owner:** rts43
- **Scope:** `autonomous-library-agent` **infrastructure only.**

This PRD defines the generic, KG-agnostic mechanisms the tooling repo must provide
to run the ingestion pipeline as a persistent agent that serves multiple Trellis
knowledge graphs from one host.

It is **infrastructure only**. Every place-specific value — `kg_id`, RSS feed URLs
and watch-topic definitions, the per-KG agent profile/identity, deployment/host
specifics — lives as **YAML in the KG's own library repo** (e.g. `LAD_library`),
**not here**. The infra *consumes* those YAMLs; the library repo *supplies* them.
See §3.

It does **not** change `pipeline/ingestion.py` — the ingestion pipeline is treated
as fixed and correct.

---

## 1. Purpose

Turn the one-shot ingestion pipeline into a long-running agent that:

1. Serves several Trellis KGs from one host without cross-contaminating them.
2. Ingests new literature on a schedule (topic RSS) and on demand (user request).
3. Reinforces the "single bound pipeline" invariant so no agent bypasses
   `ingest_batch`.
4. Remembers what it has already rejected or given up on, so it does not loop.

---

## 2. Deployment context (generic)

- **Host:** a shared, multi-tenant Linux box, modest resources (order 2 vCPU /
  4 GB, no GPU). Node/npm in user space; Trellis via
  `npm install -g @rtsantos3/trellis-app`.
- **Multi-tenant:** the host carries **several Trellis KGs** (workspaces). The
  agent must never write into the wrong tenant.
- Concrete host names, addresses, and access details are **not** part of this
  repo; they live with the deploying KG/operator.

---

## 3. Configuration boundary (infra vs. library)

| Concern | Lives in `autonomous-library-agent` (infra) | Lives in the KG library repo (YAML) |
|---------|---------------------------------------------|-------------------------------------|
| Ingestion pipeline | ✅ `pipeline/` | — |
| Agent-loop mechanics, cron script, ledger mechanism, command surface, guards | ✅ generic code + templates | — |
| `kg_id`, root node slug | schema/placeholder only | ✅ actual value |
| RSS feeds / watch topics | mechanism (reads them) | ✅ feed URLs, topic list |
| Agent identity / profile / tag vocabulary | contract template | ✅ per-KG `AGENT-CONTRACT.md` |
| Host / deployment specifics | — | ✅ operator/library side |

**Rule:** infra code and docs stay generic (`<kg_id>`, "each KG supplies…").
Concrete values are never committed to the tooling repo.

---

## 4. Requirements

### R1 — Tenancy & workspace isolation
- **R1.1** A **single agent process** serves all KGs on the host, iterating one
  workspace at a time. One process = one writer, which removes SQLite
  write-contention by construction.
- **R1.2** Each cycle binds `TRELLIS_WORKSPACE` to exactly one KG before any
  read or write.
- **R1.3** **Hard, fail-closed workspace assert:** before any write in a cycle,
  the agent asserts the resolved `TRELLIS_WORKSPACE` matches the KG it intends to
  process — the workspace path corresponds to the expected `<kg_id>` **and** the
  KG's root node slug matches the loaded profile. On mismatch: **abort the cycle,
  notify, write nothing.**
- **R1.4** A crash of the single process halts all KGs; it therefore runs under a
  supervisor (systemd or equivalent) that restarts on exit.

### R2 — Host role (ingest-only profile)
- **R2.1** The infra supports an **ingest + enrich + link only** profile: final
  state is abstract-grade `pipeline:digested`.
- **R2.2** Full-text extraction (Marker / Nougat, contract Mode 1 Phase C) is a
  **profile-gated** stage. On a resource-constrained shared host it is disabled;
  a KG/host that wants it opts in. The default profile leaves it off.
- **R2.3** When full-text is off, query mode is grounded in abstracts + citation
  structure; the contract must state `vault/<slug>/full_text.md` is not
  guaranteed, so the agent must not promise full-text content.

### R3 — Bound-pipeline reinforcement
- **R3.1 Prime Directive:** every `reference` node is created **only** by
  `pipeline.ingestion.ingest_batch`. Agents call the pipeline; they never
  hand-roll `trellis add reference`.
- **R3.2 De-footgun the docs:** in the contract's CLI reference, relabel the
  `trellis add reference …` example as **pipeline-internal**. Raw `trellis add`
  examples remain only for hand-authored types (`concept`, `finding`, `method`,
  `dataset`).
- **R3.3 Provenance audit (no code change):** the pipeline already annotates
  every node it creates with `"Created via ingestion pipeline; source: …"`
  (`ingestion.py:967`). Startup audits for any `reference` node **lacking** that
  annotation and flags it `pipeline:needs-review` + notifies — that node was
  hand-rolled outside the pipeline (a contract violation).
- **R3.4 CI guard:** a test greps the repo for `trellis add reference` outside
  `pipeline/` and `scripts/migrations/` and fails if found, preventing a future
  script from hard-coding a bypass.

### R4 — On-demand citation requests (interactive)
- **R4.1** Contract Mode 2 sub-flow. On a citation request, resolve the paper to
  an identifier and run the existing dedup chain (uri → `pmid:` tag → normalized
  title).
- **R4.2** If **present**, return the formatted citation + slug (+ RIS path if
  available). If **absent**, ingest it as a **length-1 batch**
  (`ingest_batch([identifier])`) — never a hand-rolled `trellis add` — then
  return the result. New nodes from this path are tagged `source:on-demand`.
- **R4.3** Two distinct request types, specified separately:
  - **Retrieve** ("what supports claim X?") → graph search, slug-cited answer.
  - **Export** ("give me the citation for X") → formatted reference + RIS.
- **R4.4** Fixed citation response format:
  ```
  <Authors> (<year>). <title>. <venue>. https://doi.org/<doi>
    → Trellis: [<slug>]   status: pipeline:digested
    → RIS: vault/<slug>/reference.ris   (if available)
  ```
  Never fabricate a DOI/PMID/slug; show only what resolved.

### R5 — Topic RSS (scheduled discovery)
- **R5.1** RSS runs as a **daily cron** (`scripts/rss_watch.py`), decoupled from
  the tight autonomous loop.
- **R5.2 Enqueue-only:** the cron adds discovered papers as `pipeline:queued`
  tagged `source:rss` and `topic:<slug>`. The autonomous loop performs the actual
  `ingest_batch`. This yields a **human approval gate** (see R9 `approve`).
- **R5.3 Graph-native watch list:** topics are Trellis nodes (children of an
  `rss-watchlist` root, tagged `watch:topic`) holding their feed URLs in metadata.
  The **topic/feed *values* are supplied per-KG** (library-repo YAML, §3); the
  infra provides the node convention and the cron that reads them.
- **R5.4 No seen-DB for accepts:** because `ingest_batch` is idempotent and the
  graph dedups, re-reading a feed re-merges existing nodes (no-op) and enqueues
  only genuinely new identifiers.

### R6 — Suppressed-identifier ledger
- **R6.1** A single **per-KG ledger node** records identifiers not to re-process,
  unifying two cases:
  - **Declines** (R5 gate): a rejected candidate is tombstoned
    `declined:<doi|pmid>`. Without this it has no node and re-surfaces daily.
  - **Dead-letter** (R6.2): a permanently failing identifier.
- **R6.2 Retry cap:** each failed ingest increments a `retry:N` tag (count lives
  in the graph). At N ≥ 3 the node is tagged `pipeline:dead-letter`, dropped from
  auto-backfill, re-triable only by an explicit human `retry` command.
- **R6.3** The RSS cron consults this ledger before enqueuing and skips any
  suppressed identifier. The ledger is `<kg_id>`-bound — suppression in one KG
  does not suppress the same paper in another.

### R7 — Commit-back / snapshots
- **R7.1** The host holds live truth. **No automated git push from the host** (no
  push credentials on a shared multi-tenant box).
- **R7.2** Snapshots are a **manual operator step**: `export_graph.sh` → commit
  the JSONL → tag the meaningful ones (`snapshot-YYYY-MM-DD`). Git + LFS version
  each committed blob, so every commit is an archived snapshot.
- **R7.3** The agent **never touches git.** Export/commit/tag is an ops procedure
  outside the autonomous loop.

### R8 — Notifications
- **R8.1** Notification channel is **Slack** (integration lives on the host/
  operator side; the infra targets a configured channel). Contract references to
  Telegram are replaced with Slack.
- **R8.2 Fallback:** if a Slack notification fails, the agent drops to a
  `pipeline:needs-review` tag + a log line so no event is lost silently.

### R9 — Command surface (interactive)
The agent accepts these operator commands, plus the existing `research <topic>`:
- `approve <slug|all>` — promote queued candidate(s) for ingestion.
- `reject <slug>` — decline a candidate; writes a tombstone (R6).
- `add-feed <topic> <url>` — add/extend a watch-topic node (R5.3).
- `remove-feed <topic> [url]` — remove a topic or one of its feeds.
- `status` — report queue / needs-review / dead-letter counts.
- `retry <slug>` — force a dead-lettered node back into processing.

### R10 — Contract versioning
- **R10.1** Each KG's `AGENT-CONTRACT.md` carries a header with
  `contract_version` and `kg_id`, so the agent knows which contract it runs and
  can distinguish tenants.
- **R10.2** Contracts are **per-KG and self-contained** (fused): each KG ships a
  complete contract. Universal mechanics are duplicated across KGs by design;
  divergence is managed manually.

---

## 5. Decision log

| # | Topic | Decision |
|---|-------|----------|
| Q1 | Host role | Ingest + enrich + link only (default profile); full-text off; query = abstract+citation-grounded |
| Q2 | Tenancy | One agent, many KGs, per-workspace |
| Q3 | Contract loading | Loaded per-KG by workspace |
| Q4 | Contract structure | Fused / self-contained per KG (accepts duplication) |
| Q5 | Decline memory | Tombstone ledger, unified with dead-letter |
| Q6 | Commit-back | Manual export → tag snapshot; agent never pushes |
| Q7 | Failures | Retry cap N=3 → `pipeline:dead-letter` |
| Q8 | Workspace guard | Hard-assert fail-closed (per-node `kg:` stamp dropped to avoid ingestion.py changes) |
| — | RSS | Enqueue-only, daily, graph-native watch nodes, approval gate |
| — | Notifications | Slack + needs-review fallback |
| — | Reinforcement | Prime Directive + de-footgun docs + annotation-based audit + CI grep guard |
| — | Config boundary | Infra generic here; place-specific YAMLs in the KG library repo |

---

## 6. Open items / deferred

- **Full-text extraction** offload target (where Phase C runs, if enabled) is
  undecided; off by default.
- **Per-node `kg:` provenance stamp** and after-the-fact mis-bind audit are
  deferred: they require threading a KG id through `_make_tags`, and this PRD
  holds `ingestion.py` fixed. Workspace safety rests on the R1.3 fail-closed
  assert alone until that changes.

---

## 7. Downstream deliverables

- Edits to each KG's `AGENT-CONTRACT.md` (per KG, in the library repo).
- New generic `scripts/rss_watch.py` (infra).
- One CI guard test (infra).
- Per-KG YAML (feeds, topics, `kg_id`, profile) in the library repo.

No changes to `pipeline/ingestion.py`.
