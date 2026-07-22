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

- **Mechanical lanes (no LLM):** `rss_watch.py` (discovery) and
  `ingest_approved.py` (drain) are plain cron scripts. They never reason; they
  call `ingest_batch`. See the runtime PRD.
- **Agent lane (LLM, on Slack):** everything a human touches happens in Slack and
  is mediated by the agent — answering questions, presenting candidates for
  approval, accepting paper submissions, and posting status. **This is the full
  agent integration**: the agent lives in Slack; the pipeline does not.

The boundary: the agent decides *what to say to a human and how to interpret a
human's reply*; the cron scripts do the ingestion. The only thing crossing from
Slack into the mechanical lane is an approve/reject tag flip.

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
- ✅ / `approve` → flip candidate `rss:pending → rss:approved`; the drain cron
  ingests it.
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

## 9. Open items

- **Block Kit interactive UI** (checkboxes/buttons) — deferred; needs an
  interactive endpoint alongside the poller.
- **Auto-approve policy** — per-feed "trust" flag to skip the gate for a feed
  (maps to `approve all` on that topic). Off by default.
- **Agent-assisted ranking** — optional: the agent pre-ranks RSS candidates
  ("these 3 look most relevant") in the digest. Opt-in; baseline digest is
  mechanical.
- Slack token handling / rotation on a shared multi-tenant host.

---

## 10. Relationship to other docs

- `docs/PRD-persistent-agent-runtime.md` — mechanical lanes, ledger, workspace
  binding, command semantics.
- Each KG's `AGENT-CONTRACT.md` — the agent's per-KG identity and behavior.
