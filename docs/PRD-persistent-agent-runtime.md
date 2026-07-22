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

### R2 — Digestion boundary (ingest-only automation)
- **R2.1** The automated pipeline is **ingest + enrich + link only**: digestion
  ends at `pipeline:digested` (resolve → enrich → dedup → link). That is the
  terminal state the autonomous loop produces.
- **R2.2 Full-text extraction is user-prompted, never automated.** The full-text
  stage (findings / hypotheses / methods via Marker / Nougat, contract Mode 1
  Phase C) is **not part of the autonomous pipeline**. It runs **only when a user
  explicitly prompts it** for a specific paper (an on-demand `digest <slug>`
  action). The autonomous loop skips it entirely.
- **R2.3** Because full-text is not produced automatically, query mode is grounded
  in abstracts + citation structure; the agent must not promise
  `vault/<slug>/full_text.md` content that a user has not explicitly requested.

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
- **R5.1 Daily cron, mechanical.** RSS runs as a daily cron
  (`scripts/rss_watch.py`), decoupled from the tight loop. Discovery is
  mechanical (no LLM): read watch nodes → fetch feeds → extract DOI/PMID → filter
  → stage candidates.
- **R5.2 Pre-graph candidates (not reference stubs).** Discovered papers are
  staged as `rss-candidate` nodes — a **distinct type, never `reference`** —
  tagged `rss:pending`, `source:rss`, `topic:<slug>` (schema §5). This keeps the
  reference space 100% `ingest_batch`-made (R3). The candidate *is* the approval
  queue; it is not a paper in the graph.
- **R5.3 Graph-native watch list.** Topics are `watch` nodes (children of an
  `rss-watchlist` root, tagged `watch:topic`) holding feed URLs and `last_run` in
  metadata. Topic/feed *values* are per-KG (library YAML, §3); the infra provides
  the node convention and the cron.
- **R5.4 Missed-run catch-up.** Each watch node stores `last_run` (UTC); the cron
  issues a **date-windowed eutils `esearch`** (`mindate=last_run`, `maxdate=now`)
  rather than consuming raw RSS, so papers published while the cron was down are
  recovered on the next run. `last_run` advances on success; first run uses a
  30-day default window.
- **R5.5 Agent-driven drain (not a cron).** After a human approves (R9), the
  **agent** calls `ingest_batch` on approved candidates itself — it is the drain.
  Every drain pass **re-sweeps all** `rss:approved` candidates except
  dead-lettered ones, so approvals left un-ingested (agent down) are auto-retried
  next pass. No separate drain cron.
- **R5.6 Burst politeness.** The drain caps concurrency to NCBI's limit (≤ 10
  req/s with an API key, 3/s without): small worker cap (`workers ≤ 3`) +
  `_http.py` backoff; feed fetches are sequential per feed.
- **R5.7 Stale housekeeping.** A candidate unactioned for N days transitions
  `rss:pending → rss:stale`: **kept, not deleted, not tombstoned.** It drops out
  of the daily digest but stays queryable and bulk-actionable, and the idempotent
  upsert never recreates it. Neither lost nor nagging.
- **R5.8 No seen-DB for accepts.** `ingest_batch` idempotency + graph dedup mean
  re-reading a feed re-merges existing nodes (no-op) and stages only new
  identifiers; only declines/dead-letters need the ledger (R6).
- **R5.9 A candidate leaves the queue only on confirmed successful ingest** —
  any failure leaves it `rss:approved` for the next drain to retry (see §6).

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

### R9 — Command surface & hooks (interactive)
The agent monitors for these operator commands/hooks (event-driven; message
delivery is the messenger layer's concern — see `docs/messenger-integration.md`).
Every hook runs the R1.3 fail-closed workspace assert before any write, and mints
`reference` nodes only via `ingest_batch` (R3).
- `research <topic>` — existing research command.
- `approve <slug|all>` — approve candidate(s); the agent drains via `ingest_batch`
  (R5.5).
- `reject <slug>` — decline a candidate; writes a tombstone (R6).
- `add-feed <topic> <url>` — **manual**: `<url>` is a PubMed RSS feed URL the user
  obtained via PubMed's *Create RSS*. Validate it, find-or-create the watch node,
  append the feed (dedup), init `last_run`. The agent does **not** build queries
  from free text; feeds are added by hand (see the contract's *Finding & adding
  feeds*).
- `remove-feed <topic> [url]` — remove a feed URL, or the whole topic.
- `scan now <topic>` — run RSS discovery for a topic immediately.
- `status` — queue / needs-review / dead-letter / pending counts.
- `retry <slug>` — force a dead-lettered node back into processing.

**Adding a search tag through the agent** is the manual `add-feed` hook: the user
supplies a PubMed RSS URL and the `<topic>` becomes the `topic:<slug>` filter tag
stamped on every candidate and every paper later ingested from that feed. Feed
mutations update the runtime `watch` node (source of truth) and **emit the
corresponding `config/rss_feeds.yml` line** so the change can be committed back to
the KG library repo. The agent never builds queries from free text (see the
contract's *Finding & adding feeds*).

### R10 — Contract versioning
- **R10.1** Each KG's `AGENT-CONTRACT.md` carries a header with
  `contract_version` and `kg_id`, so the agent knows which contract it runs and
  can distinguish tenants.
- **R10.2** Contracts are **per-KG and self-contained** (fused): each KG ships a
  complete contract. Universal mechanics are duplicated across KGs by design;
  divergence is managed manually.

### R11 — Observability
- **R11.1 Runtime logs** → `<workspace>/logs/rss_watch.log` and `drain.log`
  (rotated). Each run logs the counters
  `found / new / skipped-suppressed / already-present / pending / ingested /
  failed`. (`tests/results/` is test output, separate.)
- **R11.2 Daily bulletin** — the per-run summary + the approval digest.
- **R11.3 Weekly bulletin** — a rollup: references added this week by topic,
  `needs-review` count, `dead-letter` count, per-feed health (last success), and
  pending backlog. Bulletin *delivery* is the messenger layer's concern.

### R12 — Tunables (configuration)
Runtime behavior is configurable **per KG**. The infra ships defaults; each KG
overrides in `<library-repo>/config/agent_tuning.yml` (place-specific, §3). Any
unset key falls back to the infra default. Previously hard-coded values (retry
cap, stale window, drain workers, cadence) are all tunables.

| Knob | Default | Controls | Req |
|------|---------|----------|-----|
| `rss.scan_cron` | `0 7 * * *` (daily 07:00) | discovery cadence | R5.1 |
| `rss.catchup_window_days` | 30 | first-run / max lookback | R5.4 |
| `rss.max_candidates_per_digest` | 25 | digest size cap | R11.2 |
| `notifications.digest` | `daily` | **how often new-paper digests post** (`daily` \| `2x-daily` \| `weekly` \| `off`) | R11.2 |
| `notifications.digest_time` | `08:00` | when the daily digest posts | R11.2 |
| `notifications.max_per_day` | 3 | cap on pings per day | R8 |
| `notifications.quiet_hours` | `22:00–07:00` | suppress pings in this window | R8 |
| `notifications.weekly_bulletin` | `mon 09:00` | rollup cadence (or `off`) | R11.3 |
| `ingestion.drain_workers` | 3 | burst cap (≤ NCBI limit) | R5.6 |
| `ingestion.retry_cap` | 3 | fails → dead-letter | R6.2 |
| `gate.auto_approve_topics` | `[]` | topics that skip the approval gate | R5.2 |
| `housekeeping.stale_after_days` | 14 | `pending → stale` | R5.7 |

```yaml
# <library-repo>/config/agent_tuning.yml  — per-KG overrides; omit a key to keep the default
rss:
  scan_cron: "0 7 * * *"
  catchup_window_days: 30
  max_candidates_per_digest: 25
notifications:
  digest: daily            # daily | 2x-daily | weekly | off
  digest_time: "08:00"
  max_per_day: 3
  quiet_hours: ["22:00", "07:00"]
  weekly_bulletin: "mon 09:00"
ingestion:
  drain_workers: 3
  retry_cap: 3
gate:
  auto_approve_topics: []
housekeeping:
  stale_after_days: 14
```

The infra reads this via the same resolution precedence as other config
(env > config file > default) and validates unknown keys (warn, ignore).

---

## 5. Node schemas

> Custom node types (`watch`, `rss-candidate`, `rss-tombstone`) assume Trellis
> accepts arbitrary type strings. If it restricts to the core set, tag-type them
> under a generic node (e.g. a `concept` tagged `kind:rss-candidate`).

**Watch-topic node** — one per topic; runtime source of truth for feeds.
```
type:   watch          parent: rss-watchlist
slug:   watch-<topic-slug>
tags:   watch:topic, topic:<slug>
metadata:
  feeds:    ["<eutils-esearch-url>", ...]
  last_run: "2026-07-17T07:00:00Z"      # drives R5.4 catch-up
```

**rss-candidate node** — the pre-graph queue (never a `reference`).
```
type:   rss-candidate  parent: watch-<topic-slug>
slug:   cand-<sha1(identifier)[:12]>    # stable → idempotent, no dupes
tags:   rss:pending | rss:approved | rss:stale,
        source:rss, topic:<slug>, id:<doi|pmid>, retry:<n>
metadata:
  identifier: {doi | pmid}
  title, feed_url, discovered: "<date>"
```

**rss-tombstone node** — suppressed-identifier ledger (R6), one tiny node per id
(O(1) `is_suppressed` by slug; scales better than one mega-tag node).
```
type:   rss-tombstone
slug:   tomb-<sha1(identifier)[:12]>
tags:   suppressed, declined | dead-letter, id:<doi|pmid>
metadata: { reason, date }
```

**Feeds config (library repo, declarative seed)** —
`<library-repo>/config/rss_feeds.yml`:
```yaml
kg_id: <kg_id>
topics:
  <topic-slug>:
    feeds:
      - "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=..."
```
The graph `watch` nodes are the **runtime** source of truth; this YAML is the
**declarative** definition (version-controlled per-KG). A `sync-feeds` step
imports YAML → watch nodes idempotently, so a fresh clone reconstructs the
watchlist. `watch`/`add-feed`/`remove-feed` mutate the watch node directly.

---

## 6. Failure conditions

Governing principles: **fail isolated** (one bad item never aborts a batch),
**fail safe** (a mid-way crash is a safe re-run because every step is idempotent),
**fail loud** (surface to alerts; if Slack send fails, fall back to a
`pipeline:needs-review` tag + log so nothing is lost silently — R8.2).

| Stage | Failure | Handling |
|-------|---------|----------|
| Feed fetch | URL down / timeout / 5xx | backoff (`_http.py`); after N, skip that feed this run, log, continue others |
| Feed fetch | malformed / partial XML | parse valid entries, skip the rest, log count |
| ID extraction | entry has no DOI/PMID | skip entry; log per-feed "no-identifier" count |
| Candidate write | Trellis write fails | log + skip; next cron retries (stable slug → no dupes) |
| Candidate write | wrong workspace | **fail-closed assert aborts the run before any write** (R1.3) |
| Approval | slug missing / already ingested | no-op + reply; never errors |
| Drain (`ingest_batch`) | one item fails | **per-item isolation** — error in `outcomes[i].errors`, batch continues |
| Drain | unresolvable id | `pipeline:failed`; candidate **kept**; `retry:N`++ |
| Drain | locator-less/title-only unresolved | `pipeline:needs-review`; posted to alerts |
| Drain | same paper fails 3× | `pipeline:dead-letter` + ledger tombstone; candidate deleted |
| Mid-drain crash | died after ingest, before delete | **safe** — candidate deleted only after confirmed success; re-run dedups |

**The rule that ties it together:** a candidate leaves the queue **only on a
confirmed successful ingest** (R5.9). Any failure leaves it `rss:approved` for the
next drain to retry; `ingest_batch` idempotency means retries merge, never
duplicate. Permanently-broken items dead-letter out after 3 tries. Open tunables:
retry cap (default 3); feed-fetch alerting only after a feed fails N days running.

---

## 7. Testing

Framework: **pytest** (existing). Reuse `tests/conftest.py`'s two tiers — the
`ephemeral_trellis` fixture (throwaway real workspace, no mock) and the
`integration` marker (network + live Trellis, skipped by default).

**No mocks**, per project rule, achieved by design:
- Parsing is a **pure function over recorded fixtures** — real captured RSS XML in
  `tests/fixtures/rss/*.xml`, no network.
- The drain is split `ingest_batch()` (network) vs `handle_results(candidates,
  outcomes)` (pure state machine), so all failure handling is tested with real
  `IngestionOutcome` objects; the live `ingest_batch` runs only in the integration
  tier.

| File | Tier | Covers |
|------|------|--------|
| `test_rss_feeds.py` | offline (fixtures) | valid feed → ids; malformed XML → partial; no-identifier entry → skipped |
| `test_rss_candidates.py` | offline (`ephemeral_trellis`) | idempotent upsert (no dupes); approve = tag flip; reject = tombstone + delete |
| `test_rss_ledger.py` | offline (`ephemeral_trellis`) | suppress → `is_suppressed`; declined/dead-letter round-trip |
| `test_rss_drain.py` | offline (pure) | success → deleted; failure → kept + `retry`++; 3rd fail → dead-letter |
| `test_rss_integration.py` | `-m integration` | full chain: fixture feed → candidate → approve → real `ingest_batch` → reference exists, candidate gone |

Conventions: run in the Docker env; fixtures committed (deterministic); logs →
`tests/results/`; offline suite stays network-free.

---

## 8. Decision log

| # | Topic | Decision |
|---|-------|----------|
| Q1 | Digestion boundary | Automated digestion ends at `pipeline:digested`; full-text extraction is user-prompted only (never automated); query = abstract+citation-grounded |
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
| — | RSS queue | Pre-graph `rss-candidate` nodes (not `pipeline:queued` reference stubs) |
| — | Ingestion trigger | Agent-driven drain on approval (re-sweeps all `rss:approved`); no drain cron |
| — | Catch-up | Date-windowed eutils `esearch` via per-watch `last_run` |
| — | Burst | Drain workers ≤ NCBI limit |
| — | Stale pending | `rss:pending → rss:stale` (kept, no re-nag, not lost) |
| — | Observability | Runtime logs + daily and weekly bulletins |
| — | Tunables | Per-KG `config/agent_tuning.yml` (cadence, notify frequency, retry cap, stale window, workers); infra defaults |

---

## 9. Open items / deferred

- **Full-text extraction** offload target (where Phase C runs, if enabled) is
  undecided; off by default.
- **Per-node `kg:` provenance stamp** and after-the-fact mis-bind audit are
  deferred: they require threading a KG id through `_make_tags`, and this PRD
  holds `ingestion.py` fixed. Workspace safety rests on the R1.3 fail-closed
  assert alone until that changes.

---

## 10. Downstream deliverables

- `pipeline/rss/` — `feeds.py` (date-windowed eutils fetch + id extraction),
  `candidates.py` (rss-candidate CRUD), `ledger.py` (tombstone/suppress),
  `ingest_approved.py` (drain: `handle_results` split, re-sweep, retry cap).
- `scripts/rss_watch.py` — the discovery cron entry (infra).
- `tests/test_rss_*.py` + `tests/fixtures/rss/*.xml` (per §7).
- Edits to each KG's `AGENT-CONTRACT.md` (Hooks section; per KG, in the library
  repo).
- One CI guard test for the Prime Directive (R3.4).
- Per-KG YAML in the library repo: `config/rss_feeds.yml`, `config/agent_tuning.yml`
  (R12), `kg_id`, profile.
- A config loader for `agent_tuning.yml` (defaults + override resolution).
- The Slack delivery layer — see `docs/messenger-integration.md`.

No changes to `pipeline/ingestion.py`.
