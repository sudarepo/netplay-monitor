# Domain Options — Enrichment Platform

Single source of truth for the analytical platform. Read this before writing any
enrichment code. It defines conventions, the adapter contract every API client
conforms to, the data model, and the scoring approach. Everything else in
`app/enrichment/` implements what is described here.

This platform extends `sudarepo/netplay-monitor`. It reuses that codebase's
patterns deliberately — the same SQLite + WAL persistence, the same async
`httpx` concurrency model from `app/checker.py`, the same idempotent migration
style from `app/db.py`. If you are unsure how to structure something, match the
existing repo rather than inventing a new pattern.

---

## What the platform does

Given a portfolio of domains (a customer's CSV export), the platform enriches
each domain with data from a set of third-party APIs, scores each domain on a
composite of value / authority / technical-health / risk signals, and assigns a
disposition tier (A–E). The output is the data behind a Domain Options audit
deliverable.

The enrichment layer is the part that calls the external APIs and stores their
results. Scoring and tiering read from the stored enrichment data.

---

## Enrichment stack

The stack spans two adapter *types* (see "Two adapter types" below). Three core
API adapters carry confirmed access; four additional sources are being onboarded
(access info in progress), plus Ahrefs (confirmed; built last — see below).
Together they cover valuation, registration/provenance, technical health,
authority/backlinks, and age/activity.

### Core API adapters — confirmed access

| Adapter   | Dimension            | Auth                         | Base URL                       | Concurrency |
|-----------|----------------------|------------------------------|--------------------------------|-------------|
| `estibot` | Algorithmic valuation | API key (param)             | `https://www.estibot.com/api.php` *(confirm)* | Concurrent (≤10) |
| `whoxy`   | WHOIS / registration history; **Year First Registered** | API key (query param) | `https://api.whoxy.com` | Concurrent (≤20) |
| `domscan` | Technical health + reputation + availability; **Number of TLDs registered** (via bulk `/status` across popular TLDs) | `X-API-Key` header | `https://domscan.net/v1` | Concurrent (≤25) |

### Additional sources — access being onboarded

| Adapter   | Type | Dimension / parameter | Access | Cost |
|-----------|------|-----------------------|--------|------|
| `archiveorg` | per-domain API | Archive.org crawl count + first-seen date | Wayback CDX API, no key | Free |
| `wikipedia`  | per-domain API | Number of links from Wikipedia | MediaWiki `list=exturlusage`, no key | Free |
| `majestic_million` | **dataset** | Majestic Million global rank + TLD rank | Daily CSV download, no key | Free |
| `seokicks`   | per-domain API | Domain Pop (domainpop / linkpop / ippop / netpop) | `appid` param, XML/JSON | €9.90/mo |
| `ahrefs`     | per-domain API | Authority / backlinks (Domain Rating, referring-domain / backlink counts) | API key | $249/mo Standard floor |

### Ahrefs — confirmed, built last

`ahrefs` (authority / backlinks, $249/mo Standard floor) is **confirmed for the
stack** (API key in hand 2026-06-09), slotted as the **final adapter (#8)**. It is
built only after the cheaper authority signals (Majestic Million rank, SEOkicks
Domain Pop, Wikipedia links, Archive.org activity) ship, so the original
triangulation question — do several independent low-cost indices make Ahrefs'
cost unnecessary? — still informs how much scoring leans on it.

### Parked — re-evaluate later

- `namebio` (comparable sales — **sequential only**, ≤30 req/min, no
  multi-threading). Additive when ready.

### Removed

- `dotdb` — API access lapsed and renewal is not cost-justified. Its one needed
  parameter (number of TLDs a name is registered in) is now served by `domscan`'s
  bulk `/status` availability check across the popular TLDs.
- `curlie` — directory-listed signal. **Removed 2026-06-09** (not deferred): its
  dump is hosted on an LRZ bucket (`vm-138-246-238-70.cloud.mwn.de:9000`) that
  refused connections across three reachability checks (first 2026-06-02). A
  high-specificity / low-recall bonus signal — never a baseline axis — so its
  maintenance overhead exceeded its value. Not to be re-probed.

Each adapter's concurrency cap is conservative by default. Tune per the provider's
documented rate limits once tested against live keys. DomScan and SEOkicks are
credit-metered, so concurrency also controls credit/credit-pack burn rate.

---

## Two adapter types

The original design assumed every adapter makes a per-domain HTTP call. The
expanded stack adds a second pattern, so the architecture supports both.

**1. Per-domain API adapters** (`EnrichmentAdapter`, `app/enrichment/base.py`).
One HTTP request per domain, run concurrently under a semaphore. This is the
original pattern: `estibot`, `whoxy`, `domscan`, `archiveorg`, `wikipedia`,
`seokicks`, `ahrefs`. Subclasses implement `enrich_one()`.

**2. Dataset adapters** (`DatasetAdapter` — to build). Some sources publish a
whole-corpus dataset that is far cheaper to download once and query locally than
to hit per-domain. One confirmed case:

- **Majestic Million** — ~1M-row CSV, daily, free, no key. Yields global rank.

Contract:

```
class SomeDataset(DatasetAdapter):
    name = "majestic_million"
    async def refresh(self): ...        # download/refresh the local dataset
    def lookup(self, domain) -> dict     # synchronous local lookup, no network
    # enrich_many() inherited: calls refresh() once, then lookup() per domain
```

Dataset adapters produce the **same result-dict shape** and write to the **same
`enrichment` table** via the same `save_enrichment` helpers — only the acquisition
differs. They are fast (one download, then in-memory/local lookups) and mostly
free.

**Disk note:** the Mac has 2TB, so the "download the full dataset, query locally,
delete, re-download next day" cycle is fine. Dataset refreshes write to a scratch
location, get queried, and can be discarded — no need to retain corpora between
runs.

---

## Adapter contract

Every adapter is a subclass of `EnrichmentAdapter` (`app/enrichment/base.py`) and
follows this contract. The contract mirrors `checker.py`'s `run_checks()` so the
orchestration is familiar.

```
class SomeAdapter(EnrichmentAdapter):
    name = "some"                  # short identifier, used as the DB adapter key
    BASE_URL = "https://..."
    MAX_CONCURRENCY = 20           # per-adapter cap
    SEQUENTIAL = False             # True forces single-flight + RATE_LIMIT throttle
    RATE_LIMIT_PER_MIN = None      # set when SEQUENTIAL (e.g. NameBio = 30)

    def _headers(self): ...        # auth headers, reads self.api_key
    async def enrich_one(self, client, domain) -> dict
    # enrich_many() is inherited — do not reimplement unless necessary
```

### Result dict convention

`enrich_one` always returns a **flat dict**, never raises. On failure it returns
a dict with `ok=False` and an `error` string — exactly like `check_target` in
`checker.py`. Shape:

```python
{
    "domain":     "example.com",   # normalized (see _normalize in db.py)
    "adapter":    "domscan",
    "ok":         True,            # False on any error
    "error":      None,            # str when ok is False
    "fetched_at": "2026-...T...",  # ISO8601 UTC, datetime.utcnow().isoformat()
    "data":       { ... },         # adapter-specific payload (stored as JSON)
}
```

The `data` payload is whatever the provider returned, lightly normalized. Do not
flatten provider fields into top-level columns at the adapter layer — keep them
in `data` and let the scoring layer pull what it needs. This keeps adapters thin
and lets us change scoring without touching adapters.

### Error handling

Catch everything. Map to `ok=False` with a short `error` string. Follow the
provider's documented status codes. For credit-metered providers (DomScan),
treat HTTP 402 as a hard stop signal — surface it clearly so a run can halt
rather than burning the remaining portfolio against an empty balance. For rate
limits (HTTP 429), back off; do not hammer.

---

## Concurrency model

Copied from `checker.py.run_checks()`:

- `asyncio.Semaphore(MAX_CONCURRENCY)` caps in-flight requests.
- One shared `httpx.AsyncClient` per run (connection pooling).
- `asyncio.as_completed` to collect results as they finish.
- `on_progress(done, total)` callback fired after each completion, wrapped in
  try/except so a progress-callback error never kills the run.
- `httpx.Timeout` per phase, tight values.

The one exception: adapters with `SEQUENTIAL = True` (NameBio, later) ignore the
semaphore, run one request at a time, and sleep to honor `RATE_LIMIT_PER_MIN`.
The base class handles this branch; sequential adapters need no special code
beyond setting the two class attributes.

---

## Data model

Enrichment data lives in one table, keyed by `(domain, adapter)`, storing the
raw normalized payload as JSON. This mirrors the JSON-column pattern already used
in `checker`/`db.py` (`redirect_chain`, `dns_a_records` are JSON TEXT columns).

```sql
CREATE TABLE IF NOT EXISTS enrichment (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    domain      TEXT NOT NULL,
    adapter     TEXT NOT NULL,
    fetched_at  TEXT NOT NULL,
    ok          INTEGER NOT NULL DEFAULT 0,   -- 0/1
    error       TEXT,
    data        TEXT NOT NULL DEFAULT '{}',   -- JSON payload
    UNIQUE(domain, adapter)                   -- one current row per pair; upsert
);
CREATE INDEX IF NOT EXISTS idx_enrichment_domain ON enrichment(domain);
CREATE INDEX IF NOT EXISTS idx_enrichment_adapter ON enrichment(adapter, fetched_at DESC);
```

Why one table instead of 30 columns on `domains`: adapters come and go, payloads
differ wildly, and we do not want a schema migration every time a provider adds a
field. The scoring layer reads JSON and computes a flat `domain_metrics` row
(composite score + tier + the handful of headline numbers) — that flat table is
what the deliverable renders from. Schema and migrations live in
`app/enrichment/schema.py`, written in the idempotent style of `db.init_db()`.

### Multi-engagement note

`netplay-monitor`'s schema is single-portfolio (one `domains` table). Domain
Options runs multiple client engagements. Phase 1 adds an `engagements` table and
an `engagement_id` foreign key on the portfolio rows. The `enrichment` table
keys on bare `domain`, which is fine — the same domain enriched once can serve
multiple engagements, and enrichment data is not engagement-specific. Only the
portfolio membership and scoring are engagement-scoped.

---

## Conventions

- **Domain normalization:** reuse `db._normalize()`. Never store raw user input.
- **Timestamps:** `datetime.utcnow().isoformat()`, UTC, always.
- **Config / secrets:** `os.environ.get(...)`. API keys never hardcoded, never
  committed. Each adapter reads its key from a documented env var:
  `ESTIBOT_API_KEY`, `WHOXY_API_KEY`, `DOMSCAN_API_KEY`, `SEOKICKS_APP_ID`.
  Archive.org, Wikipedia, and Majestic Million need no key.
- **No secrets in the repo.** `.env` is gitignored; document required vars in the
  adapter docstring.
- **Thin adapters, fat scoring.** Adapters fetch and normalize. They do not score,
  tier, or make business decisions.
- **Match the existing repo.** When in doubt, copy the pattern from `db.py` /
  `checker.py` rather than introducing a new one.
- **Tests:** pytest, in `tests/` mirroring `app/` (e.g. `tests/enrichment/`). Run
  with `pytest` from the repo root. Dev dependencies live in `requirements-dev.txt`,
  not the runtime `requirements.txt`.

---

## Build order

1. `base.py` — per-domain adapter contract + orchestration. (done)
2. `schema.py` — enrichment table + migrations. (done)
3. `dataset.py` — `DatasetAdapter` base (download → local lookup → discard).
   (to build — needed before Majestic Million)
4. Core API clients, built from docs and field-confirmed against live responses
   on the Mac: `estibot.py`, `whoxy.py`, `domscan.py` (DomScan also yields the
   TLD-count parameter via bulk `/status`).
5. Free per-domain clients: `archiveorg.py` (CDX crawl count + first-seen),
   `wikipedia.py` (`exturlusage` link count).
6. `majestic_million.py` (dataset: daily CSV → rank lookup). `seokicks.py`
   (per-domain Domain Pop). `ahrefs.py` (per-domain authority/backlinks) is built
   LAST — after the cheaper authority signals — so its design is informed by how
   they performed (the original triangulation question).
7. Scoring layer — composite score + A–E tiering, reads `enrichment`, writes
   `domain_metrics`.
8. Multi-engagement schema refactor (Phase 1 of the original build plan).

---

## Build history

**Phase 1 — completed 2026-06-07.** Landed as four commits: (1) architecture
docs (this file), BUILD_BLUEPRINT.md, and pytest dev scaffolding; (2) the
`EnrichmentAdapter` and `DatasetAdapter` bases plus the enrichment persistence
layer (`schema.py`), with a 5-test suite covering the dataset base; (3) the
multi-engagement schema migration (`engagements` table, composite-key `domains`
rebuild with legacy backfill) and engagement-scoped CSV ingestion; and (4) this
build history note. Definition of
done met: create an engagement, import a customer CSV into it, query that
engagement's domains from the DB; `DatasetAdapter` exists with tests passing.

**Deliberate deferrals and follow-ups.** The UI-facing domain endpoints in
`app/main.py` (delete, set_expiry, set_renewal, mark_renewed, results, export,
execute_run, add/clear domains) were **left on their old single-portfolio
signatures by design** — they raise `ValueError` when called until the deferred
UI rework updates them. This is intentional, not an oversight; do not "fix"
them piecemeal ahead of that rework. **Curlie was evaluated and removed from the
build (2026-06-09)** — not deferred: its LRZ-hosted dump
(`vm-138-246-238-70.cloud.mwn.de:9000`) refused connections across three
reachability checks (first 2026-06-02), and as a high-specificity / low-recall
bonus signal (never a baseline axis) its maintenance overhead exceeded its value;
it will not be re-probed. **Ahrefs moved from parked to confirmed (2026-06-09,
API key in hand)** and slots in as the final adapter (#8), built after the cheaper
authority signals so the original triangulation question still informs its design.
Phase 2 also folds the `refresh_error` row tag into `_error()` so both adapter
bases change together. Security follow-up:
**inherited netplay-monitor deploy bearer token committed in `app/main.py`**
(the `/api/deploy` endpoint) — needs rotation and a git history scrub before
the next deploy. The repo (`sudarepo/netplay-monitor`) was confirmed **public**
on GitHub on 2026-06-07 (`"private": false` via the API), so treat this as
urgent, not hygiene.

**Phase 5 validation data — received 2026-06-07.** The real DTI portfolio CSV
has been provided by the client and is broader than originally scoped: **1700
domains** rather than the ~572 from the original engagement scope. A stratified
**500-domain test batch** (`DTI_500_Test_Domains.csv`) was prepared for
first-run validation. As of this commit both CSVs exist only in the upload
context they arrived in and have **not been transferred to local storage**;
staging them outside the repo (e.g. `~/Domain_Options/clients/dti/`) is a
Phase 5 prerequisite. The 500-domain batch will validate the pipeline
end-to-end and measure actual per-source enrichment cost before any decision
on full-portfolio scaling.
