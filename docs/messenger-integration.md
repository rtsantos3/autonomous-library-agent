# Messenger Integration (Slack)

- **Status:** Draft
- **Date:** 2026-07-17
- **Owner:** rts43
- **Scope:** `autonomous-library-agent` infrastructure — the Slack surface for a
  persistent multi-KG agent.

This spec defines how the agent is integrated with Slack. **Slack is the agent's
primary human interface**: queries, RSS approvals, paper submission, and reporting
all happen there. It complements the runtime PRD
(`docs/PRD-persistent-agent-runtime.md`) — that PRD defines the mechanical
ingestion lanes; this doc defines the conversational/agent lane on top of them.

Place-specific values (channel IDs, KG↔channel map, Slack tokens) live as
**per-KG YAML / secrets in the KG library repo or host**, not in this repo.

---

## 1. Design principle — two lanes, one surface

- **Mechanical lane (no LLM):** `rss_watch.py` is a plain cron script for RSS
  *discovery only* — it fetches feeds, filters the suppressed-identifier ledger,
  and collates candidates. It never reasons and never ingests.
- **Agent lane (LLM, on Slack):** the agent owns the human-facing surface **and
  the ingestion trigger**. Once RSS has collated candidates and a human approves,
  the agent calls `ingest_batch` on the approved identifiers itself — it is the
  drain, activated by approval (no separate drain cron). It also answers queries,
  accepts `#add-paper` submissions, and posts status. **This is the full agent
  integration**: the agent lives in Slack and drives ingestion from there.

The boundary: **RSS discovery is mechanical; everything from approval onward —
including invoking `ingest_batch` — is the agent.** The agent still uses the one
pipeline (never hand-rolls `trellis add`), so the Prime Directive holds. The only
thing the agent needs from the mechanical lane is the collated candidate list.

---

## 2. Transport — poller, not endpoint

The existing sibling agent `slack-cc-linear` is a **Node/TS cron-poller**: it
*reads* Slack on a schedule and has no inbound endpoint. Therefore:

- **Selection is reaction/reply-based**, not Block Kit buttons/checkboxes.
  Interactive components require a live interactivity endpoint (Socket Mode or a
  request URL) the poller does not have.
- The poller reads new messages and reactions each cycle and acts on them.

**Block Kit upgrade path (deferred):** to get real checkboxes/buttons, add an
interactive Slack app + a Socket Mode listener running alongside the poller, and
swap the digest renderer for Block Kit. The mechanical lanes underneath are
unchanged. Not required for v1.

---

## 3. Channels

Per-KG channels so workspace routing is unambiguous (channel → workspace), which
feeds the runtime PRD's R1.3 fail-closed workspace assert.

| Channel (per KG) | Purpose | Direction |
|------------------|---------|-----------|
| `#<kg>-add-paper` | Human submits a DOI/PMID/link/RIS to ingest | inbound → pipeline |
| `#<kg>-rss-digest` | Daily RSS candidates awaiting approval | outbound + reactions |
| `#<kg>-agent` | Interactive research queries + commands | two-way |
| `#<kg>-alerts` | needs-review, dead-letter, contract-violation, stale-digesting notices | outbound |

Channel IDs and the KG↔channel map are per-KG config (library repo / host),
consumed by the poller.

---

## 4. Front doors to ingestion

Both mint `reference` nodes only via `ingest_batch` (Prime Directive), and both
are idempotent (re-submitting a paper is a harmless dedup no-op).

### 4.1 `#<kg>-add-paper` — human submission (no gate)
A human deliberately posts a paper, so intent is explicit → ingest directly.
```
  user pastes: 10.1038/nature11234 | doi.org link | PMID | RIS attachment
        │  poller extracts identifier(s)
        ▼
  ingest_batch([id])
        │
   ✅ reply: "added: [<slug>]  pipeline:digested"
   ❌ reply: "couldn't resolve <id>"   (never fabricates)
```

### 4.2 `#<kg>-rss-digest` — auto-discovered (gated)
RSS is auto-discovered, so it is vetted before ingestion (see runtime PRD R5/R6).
Daily digest, threaded, reaction- or reply-driven:
```
  🗞️ RSS digest — YYYY-MM-DD — N new candidates      (header message)
    ├─ 📄 [topic] Title — Author Year — doi:…         ✅ approve  ❌ reject
    ├─ 📄 [topic] Title — Author Year — doi:…         ✅         ❌
    └─ …
  Reply on header:  approve 1 3 | reject 2 | approve all
```
- ✅ / `approve` → the agent flips the candidate `rss:pending → rss:approved` and
  ingests it via `ingest_batch` (the agent is the drain).
- ❌ / `reject` → write `declined:<id>` to the suppressed-identifier ledger;
  delete the candidate (never re-surfaces).
- no reaction → stays `pending`.

---

## 5. Interactive query + command surface (`#<kg>-agent`)

The agent answers research questions against the graph (runtime PRD Mode 2):
grounded in abstracts + citation structure, every claim cited by node slug, never
fabricated. Two request types:
- **Retrieve** — "what supports claim X?" → graph search → slug-cited answer.
- **Export** — "cite paper Y" → formatted reference + RIS path.

Operator commands (same set as the runtime PRD R9), issued in-channel:
`approve <slug|all|topic:…>`, `reject <slug>`, `add-feed <topic> <url>`,
`remove-feed <topic> [url]`, `status`, `retry <slug>`, `research <topic>`.

---

## 6. Notifications (`#<kg>-alerts`)

The agent posts, and on Slack-send failure falls back to a `pipeline:needs-review`
tag + a log line (runtime PRD R8.2):
- stale `pipeline:digesting` reset at startup,
- `pipeline:needs-review` candidates,
- `pipeline:dead-letter` (retry cap hit),
- contract violations (a `reference` lacking the "Created via ingestion pipeline"
  annotation — runtime PRD R3.3).

---

## 7. Multi-KG routing

- One channel set per KG; the poller maps each channel to its `TRELLIS_WORKSPACE`.
- Before any write triggered from Slack, the agent runs the R1.3 fail-closed
  workspace assert. A message in `#lad-add-paper` can only ever write to
  LAD_library's workspace.

---

## 8. Configuration boundary

| Lives in `autonomous-library-agent` (infra) | Lives in KG library repo / host |
|---------------------------------------------|---------------------------------|
| Poller integration, digest renderer, command parser, reaction handler | Channel IDs, KG↔channel map |
| Front-door / gate logic | Slack bot token / app credentials (secret) |
| — | Per-KG topic feeds (see runtime PRD) |

---

## 9. Failure conditions

Governing principles: **fail isolated** (one bad item never aborts a batch),
**fail safe** (a mid-way crash is a safe re-run because every step is idempotent),
**fail loud** (surface to `#<kg>-alerts`; if Slack send fails, fall back to a
`pipeline:needs-review` tag + log so nothing is lost silently).

| Stage | Failure | Handling |
|-------|---------|----------|
| Feed fetch | URL down / timeout / 5xx | backoff (`_http.py`); after N, skip that feed this run, log, continue others |
| Feed fetch | malformed / partial XML | parse valid entries, skip the rest, log count |
| ID extraction | entry has no DOI/PMID | skip entry; log per-feed "no-identifier" count |
| Candidate write | Trellis write fails | log + skip; next cron retries (stable slug → no dupes) |
| Candidate write | wrong workspace | **fail-closed assert aborts the run before any write** (PRD R1.3) |
| Approval | slug missing / already ingested | no-op + Slack reply; never errors |
| Approval | unparseable reply | ignored; agent asks to clarify |
| Drain (`ingest_batch`) | one item fails | **per-item isolation** — error in `outcomes[i].errors`, batch continues |
| Drain | unresolvable id | `pipeline:failed`; candidate **kept** (not deleted); `retry:N`++ |
| Drain | locator-less/title-only unresolved | `pipeline:needs-review`; posted to alerts |
| Drain | same paper fails 3× | `pipeline:dead-letter` + ledger tombstone; candidate deleted |
| Notify | Slack send fails | fall back to `pipeline:needs-review` tag + log (PRD R8.2) |
| Mid-drain crash | died after ingest, before delete | **safe** — candidate deleted only after confirmed success; re-run dedups |

**The rule that ties it together:** a candidate leaves the bucket **only on a
confirmed successful ingest**. Any failure leaves it `rss:approved` for the next
drain to retry; `ingest_batch` idempotency means retries merge, never duplicate.
The chain is crash-safe and self-healing; permanently-broken items dead-letter out
after 3 tries instead of looping forever.

Open tunables: retry cap (default **3**); feed-fetch alerting (alert only after a
feed fails **N days running**, not on a one-off blip).

---

## 10. Testing

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

Conventions: run in the Docker env; fixtures committed (deterministic, no
ephemeral files); logs → `tests/results/`; offline suite stays network-free.

---

## 11. Open items

- **Block Kit interactive UI** (checkboxes/buttons) — deferred; needs an
  interactive endpoint alongside the poller.
- **Auto-approve policy** — per-feed "trust" flag to skip the gate for a feed
  (maps to `approve all` on that topic). Off by default.
- **Agent-assisted ranking** — optional: the agent pre-ranks RSS candidates
  ("these 3 look most relevant") in the digest. Opt-in; baseline digest is
  mechanical.
- Slack token handling / rotation on a shared multi-tenant host.

---

## 12. Relationship to other docs

- `docs/PRD-persistent-agent-runtime.md` — mechanical lanes, ledger, workspace
  binding, command semantics.
- Each KG's `AGENT-CONTRACT.md` — the agent's per-KG identity and behavior.
