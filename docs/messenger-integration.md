# Messenger Integration (Slack)

- **Status:** Draft
- **Date:** 2026-07-17
- **Owner:** rts43
- **Scope:** the **Slack delivery layer** for the persistent multi-KG agent.

This doc covers **only how the agent connects to and communicates over Slack** —
transport, channels, routing, how messages/digests are rendered and received, and
credentials. **All agent behavior and mechanics** (RSS discovery, candidates,
ledger, drain, schemas, failure handling, testing, command semantics) live in
`docs/PRD-persistent-agent-runtime.md`. This doc references the PRD; it does not
restate it.

Place-specific values (channel IDs, KG↔channel map, Slack tokens) live as per-KG
YAML / secrets in the KG library repo or host, not in this repo.

---

## 1. Boundary — two lanes, one surface

- **Mechanical lane (no LLM):** RSS discovery (`scripts/rss_watch.py`) collates
  candidates. Defined in the PRD (R5).
- **Agent lane (LLM, on Slack):** the agent answers queries, presents candidates,
  accepts submissions, drives ingestion on approval, and reports. **Slack is the
  agent's surface.**

The only thing this doc owns is the **Slack transport** for the agent lane. What
the agent *does* once a message arrives is the PRD's concern (R5, R9); what a
failure *does* is the PRD's (§6). Here we define how it is delivered and received.

---

## 2. Transport — poller, not endpoint

The sibling agent `slack-cc-linear` is a **Node/TS cron-poller**: it *reads* Slack
on a schedule and has no inbound endpoint. Therefore:

- **Selection is reaction/reply-based**, not Block Kit buttons/checkboxes.
  Interactive components require a live interactivity endpoint (Socket Mode or a
  request URL) the poller does not have.
- The poller reads new messages and reactions each cycle and hands recognized
  commands/approvals to the agent.

**Block Kit upgrade path (deferred):** add an interactive Slack app + Socket Mode
listener alongside the poller and swap the digest renderer for Block Kit. The
mechanical lanes and agent logic underneath are unchanged. Not required for v1.

---

## 3. Channels (per KG)

Per-KG channels so routing is unambiguous (channel → workspace).

| Channel (per KG) | Purpose | Direction |
|------------------|---------|-----------|
| `#<kg>-add-paper` | human submits a DOI/PMID/link/RIS | inbound |
| `#<kg>-rss-digest` | daily RSS candidates + approval reactions | outbound + reactions |
| `#<kg>-agent` | queries + commands/hooks | two-way |
| `#<kg>-alerts` | needs-review, dead-letter, contract-violation, stale-digesting, weekly bulletin | outbound |

Channel IDs and the KG↔channel map are per-KG config (library repo / host).

---

## 4. Multi-KG routing

- One channel set per KG; the poller maps each channel to its `TRELLIS_WORKSPACE`.
- Before any write triggered from Slack, the agent runs the PRD's R1.3 fail-closed
  workspace assert. A message in `#lad-add-paper` can only ever write to
  LAD_library's workspace.

---

## 5. What gets rendered/received where

The *actions* below are specified in the PRD; this section defines only their
Slack representation.

**`#<kg>-add-paper`** — a message containing an identifier/RIS is picked up; the
agent ingests directly (PRD R5.5) and replies:
```
✅ added: [<slug>]  pipeline:digested
❌ couldn't resolve <id>
```

**`#<kg>-rss-digest`** — the daily digest is posted as a header message with each
candidate as a threaded reply; approval is by reaction or reply:
```
🗞️ RSS digest — YYYY-MM-DD — N candidates          (header)
  ├─ 📄 [topic] Title — Author Year — doi:…   ✅ approve  ❌ reject
  └─ …
Reply on header:  approve 1 3 | reject 2 | approve all
```
✅/`approve` and ❌/`reject` are handed to the agent, which performs the PRD R5/R9
actions (drain / tombstone).

**`#<kg>-agent`** — free-text queries (PRD Mode 2) and the command/hook surface
(PRD R9: `approve`, `reject`, `watch`, `add-feed`, `remove-feed`, `scan now`,
`status`, `retry`, `research`). This doc only carries the messages; the semantics
are the PRD's.

**`#<kg>-alerts`** — the agent posts needs-review, dead-letter, contract-violation,
and stale-digesting notices, plus the **weekly bulletin** (content per PRD R11).

---

## 6. Notify fallback

If a Slack send fails, the agent drops to a `pipeline:needs-review` tag + a log
line so no event is lost silently (PRD R8.2). Slack is best-effort delivery; the
graph is the durable record.

---

## 7. Configuration & secrets

| Lives in `autonomous-library-agent` (infra) | Lives in KG library repo / host |
|---------------------------------------------|---------------------------------|
| Poller integration, digest/bulletin renderer, command parser, reaction handler | Channel IDs, KG↔channel map |
| Reaction/reply selection logic | Slack bot token / app credentials (secret) |

Slack tokens are host/operator secrets — never committed to this repo.

---

## 8. Open items

- **Block Kit interactive UI** (checkboxes/buttons) — deferred; needs an
  interactive endpoint alongside the poller.
- **Slack token handling / rotation** on a shared multi-tenant host.
- **Agent-assisted ranking** of digest candidates (opt-in; baseline digest is
  mechanical). Behavior would be specified in the PRD; only its rendering is here.

---

## 9. Relationship to other docs

- `docs/PRD-persistent-agent-runtime.md` — **all** agent behavior: RSS mechanics,
  candidates, ledger, drain, schemas, failure conditions, testing, command/hook
  semantics, observability content.
- Each KG's `AGENT-CONTRACT.md` — the agent's per-KG identity, and the Hooks
  section (triggers delivered via this Slack layer).
