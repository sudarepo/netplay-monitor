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
API adapters carry confirmed access; five additional sources are being onboarded
(access info in progress). Together they cover valuation, registration/provenance,
technical health, authority/backlinks, age/activity, and directory trust.

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
| `curlie`     | **dataset** | Listed in the Curlie web directory (human-edited; a real editorial-merit signal) | TSV tar/gzip dump (~200MB), monthly, no key | Free |

### Parked — re-evaluate later

- `ahrefs` (authority / backlinks, $249 Standard-tier floor). **Pulled from the
  active stack.** Decision deferred until we see how the cheaper authority signals
  (Majestic Million rank, SEOkicks Domain Pop, Wikipedia links, Archive.org
  activity) perform in real audits. The working hypothesis: triangulating several
  independent low-cost indices may make Ahrefs' cost unjustifiable. Revisit only
  if those signals prove insufficient.
- `namebio` (comparable sales — **sequential only**, ≤30 req/min, no
  multi-threading). Additive when ready.

### Removed

- `dotdb` — API access lapsed and renewal is not cost-justified. Its one needed
  parameter (number of TLDs a name is registered in) is now served by `domscan`'s
  bulk `/status` availability check across the popular TLDs.

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
`seokicks`. Subclasses implement `enrich_one()`.

**2. Dataset adapters** (`DatasetAdapter` — to build). Some sources publish a
whole-corpus dataset that is far cheaper to download once and query locally than
to hit per-domain. Two confirmed cases:

- **Majestic Million** — ~1M-row CSV, daily, free, no key. Yields global rank.
- **Curlie** — TSV (tar/gzip, ~200MB), monthly, free, no key. ~2.9M human-edited
  entries; per entry: URL, title, editorial description, full category path. The
  lookup is a boolean: is this domain present in the directory.

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

**Curlie — notes for the build and scoring layers:**

- *License.* Curlie data carries a Creative Commons Attribution license (inherited
  from the original ODP terms). Internal use — scoring, analysis — is fine with no
  action. But if "listed in Curlie" is **surfaced in a client-facing deliverable**,
  confirm the attribution requirement and, if needed, add a one-line credit/footnote
  to the report. Decide this before Curlie data appears in any deliverable, not after.
- *Low recall — score asymmetrically.* Curlie has ~2.9M entries against hundreds of
  millions of active domains, and skews toward established content sites. The
  portfolios we audit lean parked / brandable / domainer-held, so **most domains
  will not be listed**. This makes Curlie a high-specificity, low-recall signal:
  present = a strong, hard-to-fake editorial-merit marker (meaningful positive);
  absent = tells us almost nothing (treat as **neutral, never a penalty**). It is a
  bonus signal that adds confidence on the rare hits, not a baseline scoring axis.
- *Availability — on probation.* The Curlie dump is **not hosted by Curlie**: the
  `https://curlie.org/directory-dl` link redirects to a bucket on LRZ (Leibniz
  Supercomputing Centre) infrastructure — `vm-138-246-238-70.cloud.mwn.de:9000`,
  the file being `curlie-rdf-all.tar.gz`. During the Phase 1 build (2026-06-02)
  that endpoint was **down**: DNS resolved and `curlie.org:443` was up, but the
  bucket port refused connections (confirmed with the sandbox disabled, so not a
  local network artifact). Majestic Million was reachable the same day. Curlie is
  therefore **on probation pending a Phase 2 reachability test**: build `curlie.py`
  only once the dump downloads cleanly. If it stays unreachable, the stack proceeds
  without it — Curlie is a bonus signal, never a baseline axis (see the low-recall
  note above), so its absence is not a blocker. The `DatasetAdapter` base is built
  to handle exactly this: a download failure becomes a clean `ok=False` batch +
  `refresh_error`, leaving the halt-vs-skip decision to the orchestration layer.

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
  Archive.org, Wikipedia, Majestic Million, and Curlie need no key.
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
   (to build — needed before Majestic Million / Curlie)
4. Core API clients, built from docs and field-confirmed against live responses
   on the Mac: `estibot.py`, `whoxy.py`, `domscan.py` (DomScan also yields the
   TLD-count parameter via bulk `/status`).
5. Free per-domain clients: `archiveorg.py` (CDX crawl count + first-seen),
   `wikipedia.py` (`exturlusage` link count).
6. `majestic_million.py` (dataset: daily CSV → rank lookup). `seokicks.py`
   (per-domain Domain Pop). `curlie.py` (dataset: monthly TSV dump → boolean
   listed/not-listed lookup).
7. Scoring layer — composite score + A–E tiering, reads `enrichment`, writes
   `domain_metrics`. This is also where the Ahrefs go/no-go gets decided: if the
   free + cheap authority signals score well, Ahrefs stays parked.
8. Multi-engagement schema refactor (Phase 1 of the original build plan).
