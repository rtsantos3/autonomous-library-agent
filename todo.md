# RSS Ingestion Subsystem (MVP-2) — Implementation TODO

Orchestration plan. Each task below is a **single, self-contained Codex-sized
unit** — hand them out one at a time, in order. Spec lives in this repo:

- `docs/PRD-persistent-agent-runtime.md` — R5 (RSS), R6 (ledger), R11
  (observability), R12 (tunables), §5 (schemas), §6 (failure), §7 (testing).
- `AGENT-CONTRACT.md` — "Hooks", "add-feed — agent steps", "Finding & adding feeds".

---

## Shared constraints (apply to EVERY task)

- **No new dependencies.** Parse RSS/Atom with stdlib `xml.etree.ElementTree`.
  Network via `pipeline/_http.py`. YAML via the already-present `pyyaml`.
- **Do NOT modify `pipeline/ingestion.py`.** Reuse `ingest_batch`,
  `IngestionOutcome`, `_classify_failure`, `bare_doi`.
- **All graph writes go through `pipeline/trellis.py`** (add/update/link/annotate/
  find). No hand-rolled subprocess calls.
- **Prime Directive:** `reference` nodes are created ONLY by `ingest_batch`. New
  types `watch`, `rss-candidate`, `rss-tombstone` are DISTINCT and never
  `reference`.
- **Idempotent:** stable slugs (`sha1(identifier)[:12]`); every upsert is a no-op
  on re-run.
- **No mocks** (project rule): parser tests use recorded XML fixtures; drain
  failure logic tested via a PURE `handle_results(...)` fed real
  `IngestionOutcome`s; live `ingest_batch` only in the `integration`-marked test.
- **Style:** match `pipeline/ingestion.py` — Declaration → Body → End; comment the
  WHY (constraints/invariants), not the WHAT.
- Reuse `tests/conftest.py`'s `ephemeral_trellis` fixture and `integration` marker.

---

## Task 1 — package skeleton + config loader
**Files:** `pipeline/rss/__init__.py`, `pipeline/rss/config.py`
- `load_tuning()` → dict: the R12 defaults, overlaid by `config/agent_tuning.yml`
  if present (precedence env > file > default). Include a helper to resolve a
  per-feed override over the KG default (per-feed wins).
- Defaults exactly as R12: `rss.scan_cron`, `rss.catchup_window_days=30`,
  `rss.max_candidates_per_digest=25`, `notifications.*`, `ingestion.drain_workers=3`,
  `ingestion.retry_cap=3`, `gate.auto_approve_topics=[]`, `housekeeping.stale_after_days=14`.
**Done when:** `load_tuning()` returns merged defaults; unit test covers default +
override + unknown-key-ignored.

## Task 2 — feeds.py (+ fixtures + test)
**Files:** `pipeline/rss/feeds.py`, `tests/fixtures/rss/*.xml`, `tests/test_rss_feeds.py`
- `fetch_feed(url) -> list[dict]` — stdlib parse of RSS **and** Atom → entries
  `{title, link, raw}`. Malformed/empty → return `[]` (never raise).
- `extract_identifier(entry) -> str | None` — DOI (`10.\d{4,}/\S+`, via `bare_doi`)
  or PMID; None if neither.
- `esearch_window(term, mindate, maxdate) -> url` + fetch — PubMed eutils
  `esearch` for R5.4 date-windowed catch-up.
- Fixtures: valid PubMed RSS (several entries), an Atom feed, a malformed XML, one
  entry with no identifier.
**Done when (offline test):** valid→ids; malformed→[]; no-identifier entry→skipped.

## Task 3 — ledger.py (+ test)
**Files:** `pipeline/rss/ledger.py`, `tests/test_rss_ledger.py`
- `tombstone_slug(id)`, `is_suppressed(id) -> bool`, `tombstone(id, reason)` —
  `rss-tombstone` nodes tagged `suppressed, declined|dead-letter, id:<...>` (§5).
**Done when (`ephemeral_trellis`):** tombstone → `is_suppressed` true; round-trip.

## Task 4 — candidates.py (+ test)
**Files:** `pipeline/rss/candidates.py`, `tests/test_rss_candidates.py`
- `candidate_slug(id)`; `upsert_candidate(topic, id, title, feed_url)` (type
  `rss-candidate`; tags `rss:pending,source:rss,topic:<slug>,id:<...>`; idempotent);
  `find_candidates(tag)`; `approve(slug)`; `reject(slug)` (tombstone + delete);
  `delete_candidate(slug)`; `already_a_reference(id) -> bool`.
**Done when (`ephemeral_trellis`):** idempotent upsert (no dupes); approve = tag
flip; reject → tombstone + delete.

## Task 5 — watch.py
**Files:** `pipeline/rss/watch.py`
- `read_watch_nodes() -> {topic: [feed-settings]}`; `find_or_create_watch(topic)`;
  `validate_feed(url) -> (ok, error)` per the add-feed steps (fetch once; valid
  RSS/Atom with ≥1 entry; else error, create nothing); `add_feed(topic, url,
  settings)` (dedup, store per-feed settings + `last_run`); `remove_feed(topic,
  url=None)`.
**Done when:** unit coverage of validate (good/malformed/empty) + add/remove/read.

## Task 6 — ingest_approved.py (drain) (+ test)
**Files:** `pipeline/rss/ingest_approved.py`, `tests/test_rss_drain.py`
- `handle_results(candidates, outcomes, tuning) -> summary` — PURE: success →
  delete; failure → `retry:N`++; `retry >= retry_cap` → dead-letter (tombstone) +
  delete. No I/O in this function beyond the trellis mutations it returns/applies —
  keep the decision logic pure and separately testable.
- `ingest_approved(tuning)` — find `rss:approved` (all, re-sweep, minus
  dead-lettered), call `ingest_batch`, then `handle_results`, apply.
**Done when (offline, pure):** success→delete; failure→retry++; 3rd→dead-letter,
using real `IngestionOutcome` objects.

## Task 7 — scripts/rss_watch.py (discovery cron)
**Files:** `scripts/rss_watch.py`
- Read watch nodes → per feed: fetch (date-windowed via `last_run`) → extract ids →
  skip if `is_suppressed` / `already_a_reference` / existing candidate →
  `upsert_candidate`. Advance `last_run` on success.
- Log R11.1 counters: found / new / skipped-suppressed / already-present / pending.
- `__main__` CLI entrypoint.
**Done when:** dry-run over a fixture feed produces the expected candidate set +
counters (can be an offline test with a local feed file).

## Task 8 — integration test
**Files:** `tests/test_rss_integration.py`
- `@pytest.mark.integration`: fixture feed → candidate → approve → real
  `ingest_batch` → assert `reference` exists and candidate is gone.

---

## Final gate
- `python -m pytest tests/ -q` — offline suite green (integration skipped by
  default; `ephemeral_trellis` tests skip if `trellis` CLI absent).
- No changes to `pipeline/ingestion.py`. No new deps. Prime Directive intact.

## Suggested order
1 → 2 → 3 → 4 → 5 → 6 → 7 → 8. Tasks 2–4 are independent after 1; 6 depends on 4;
7 depends on 2/3/4/5; 8 depends on all.
