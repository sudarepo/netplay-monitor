# Domain Options — Build Blueprint

The sequenced construction plan for the enrichment platform. PROJECT.md is the
*architecture* (conventions, contracts, data model); this is the *construction
sequence* — what gets built in what order, what each phase depends on, how you
know a phase is done, and how long each should take.

Read PROJECT.md first. This document assumes it.

---

## How to read the day estimates

The build runs on the Mac with Claude Code generating the code. So the day counts
are **not** "time to write code" — code generation is fast. They are driven by the
three things that actually eat time:

1. **Live-API reality** — real responses never exactly match the docs. Field names
   differ, edge cases surface, rate limits bite. Discovering and handling this is
   the bulk of adapter time.
2. **Scoring calibration** — the judgment loop. Looking at real output and saying
   "that domain shouldn't be tier E," then adjusting weights and re-running.
3. **Validation** — running the real portfolio end-to-end and fixing what's wrong.

**Estimates are in focused working days (~6 productive hours), assuming:**

- Claude Code is set up and you're available throughout to test, run, and judge.
- All API access is gathered before the relevant phase (keys, the SEOkicks appid,
  confirmed logins for Estibot/WhoXY/DomScan).
- One honest caveat that applies to every software estimate: they skew optimistic.
  Each phase gives a **realistic range**, and the total carries a contingency line.
  Treat the low end as "everything cooperates" and the high end as "normal friction."

---

## Timeline at a glance

| Phase | What | Depends on | Days (realistic) |
|-------|------|-----------|------------------|
| 0 | Foundation (base, schema, PROJECT.md) | — | **done** |
| 1 | Environment + data-model refactor | 0 | 1 – 1.5 |
| 2 | The adapters (8 sources, 2 types) | 1 | 3 – 4.5 |
| 3 | Scoring & tiering engine | 2 (data to score) | 2 – 3 |
| 4 | The pipeline (CSV → enrich → score → tier → output) | 2, 3 | 1 – 1.5 |
| 5 | Proof-of-concept validation (real portfolio) | 4 | 1 – 2 |
| 6 | Deliverable generation | 5 | 2 – 2.5 |
| | **Subtotal** | | **10 – 14.5** |
| | **+ contingency (~20%)** | | **+2 – 3** |
| | **Total** | | **~12 – 17 working days** |

At full availability with near-consecutive days, that's roughly **2.5 to 3.5
calendar weeks** to a platform that takes a real portfolio in and produces a real
audit deliverable. The single biggest swing factor is Phase 3 (scoring) — see its
risk note.

---

## Phase 0 — Foundation  *(done)*

**Built:** `app/enrichment/base.py` (per-domain adapter contract + concurrency),
`app/enrichment/schema.py` (enrichment table + persistence), PROJECT.md.

**Definition of done:** ✅ already met. These drop into the repo as-is.

---

## Phase 1 — Environment + data-model refactor

**Depends on:** Phase 0.

**What gets built:**

- Claude Code set up on the Mac (~1 hr, guided walkthrough).
- Repo cloned, foundation files in place, runs locally against a scratch SQLite DB.
- `DatasetAdapter` base class (`app/enrichment/dataset.py`) — the download → local
  lookup → discard pattern. Built here because two Phase-2 sources depend on it.
- Multi-engagement schema refactor: an `engagements` table, and portfolio
  membership scoped by `engagement_id`. (netplay-monitor is single-portfolio today.)
- CSV ingestion: import a customer's domain export into an engagement, using the
  existing `db._normalize()`.

**Definition of done:** you can create an engagement, import a CSV of domains, and
query that engagement's domains from the DB. `DatasetAdapter` base exists with a
trivial test stub passing.

**Estimate: 1 – 1.5 days.** Well-understood patterns; the refactor is the only real
work and it's contained. Setup friction is the main variable.

**Risk:** low.

---

## Phase 2 — The adapters

**Depends on:** Phase 1 (DatasetAdapter base + a DB to write into).

**What gets built** — eight sources, in dependency/ease order:

1. **`archiveorg.py`** (free, per-domain) — Wayback CDX: crawl count + first-seen
   date. Simple GET, no key. *Easiest; build first to exercise the pipeline.*
2. **`wikipedia.py`** (free, per-domain) — `exturlusage` link count. Simple GET, no key.
3. **`whoxy.py`** (core, per-domain) — WHOIS, registration date / Year First
   Registered. Documented JSON.
4. **`domscan.py`** (core, per-domain) — health + reputation + the TLD-count signal
   (bulk `/status` across a fixed curated ~50-TLD list; the list lives as a constant
   so the count is comparable across the whole portfolio). Credit-aware (402 = halt).
5. **`estibot.py`** (core, per-domain) — valuation. Field shapes confirmed against a
   live response (docs are behind login).
6. **`seokicks.py`** (paid, per-domain) — Domain Pop (domainpop/linkpop/ippop/netpop).
   `appid` param, credit-metered.
7. **`majestic_million.py`** (free, dataset) — daily CSV → global-rank lookup.
8. **`curlie.py`** (free, dataset) — monthly TSV dump → boolean listed/not-listed.

Each adapter: write client → run against live source → reconcile real response →
handle errors/edge cases → confirm stored fields. The free per-domain ones (1–2)
are quick. The credit-metered (4, 6) and dataset (7, 8) ones take longer.

**Definition of done:** each adapter, run against its live source for a sample of
~20 real domains, writes correct rows to the `enrichment` table with `ok=True` and
the expected fields populated; failures produce clean `ok=False` rows, not crashes.

**Estimate: 3 – 4.5 days.** Roughly: free per-domain pair ~0.5d; three core API
adapters ~1.5–2d (live-response reconciliation is the time sink); SeoKicks ~0.5d;
two dataset adapters ~1d. This is the phase most exposed to live-API surprises.

**Risk:** medium. Any single provider behaving oddly (auth quirk, undocumented rate
limit, messy response) can add half a day. The estimate assumes no provider is
badly broken.

---

## Phase 3 — Scoring & tiering engine

**Depends on:** Phase 2 (needs real enrichment data to calibrate against).

This is the heart of the product and the phase most likely to run long — not
because the code is hard (it isn't) but because calibrating it to match your expert
judgment is iterative. The code reads `enrichment`, computes sub-scores and a
composite, and assigns an A–E disposition tier, writing a flat `domain_metrics` row
per domain.

### First-pass scoring model  *(draft — for Greg to red-pen)*

This is a starting point built from standard domain-valuation logic. **Your 25
years of judgment overrides every number here.** Expect to rewrite the weights and
thresholds against real output.

**Five dimension sub-scores (0–100 each):**

| Dimension | Built from | Notes |
|-----------|-----------|-------|
| Value | Estibot valuation (+ NameBio comps later) | Log-scaled; $0–$50 → low, $5k+ → high |
| Authority | Majestic Million rank, SEOkicks domain pop, Wikipedia links | Triangulated; any one strong signal lifts it |
| Provenance / age | WhoXY first-registered year, Archive.org first-seen + crawl count, Curlie listed | Older + long archive history + Curlie = high |
| Usage / technical | DomScan health + reputation, existing checker (apex/www resolves) | Actively resolving & healthy = high; dead = low |
| Defensive / brand | DomScan TLD-count, name characteristics | High TLD coverage suggests a defended brand asset |

**Composite (first-pass weights — calibrate):**

```
composite = 0.35*Value + 0.25*Authority + 0.20*Provenance
          + 0.15*Usage  + 0.05*Defensive
```

**Tier assignment** — composite *plus* decision rules, because disposition is a
decision, not just a score:

- **A — Mission-critical:** actively resolving + healthy + high Value or Authority.
  The domains the business runs on. Keep, no question.
- **B — Strategic / defensive:** high Value or strong brand/TLD coverage, even if
  not actively used. Keep for defensive or future value.
- **C — Hold / review:** middling composite, or conflicting signals. Human review.
- **D — Consolidate / redirect:** low standalone value but some residual authority
  or traffic worth redirecting rather than dropping.
- **E — Sell or drop:** low Value + low Authority + not resolving + no provenance.
  The dead weight. Sell if it has any market value, drop if not.

**Cost-gap overlay:** independent of tier, flag any domain whose annual renewal cost
exceeds a threshold fraction of its estimated value — the "you're paying $X/yr to
hold a $Y domain" finding that drives the savings number in the deliverable.

**Asymmetric signals (from PROJECT.md):** Curlie present = positive, absent =
neutral (never a penalty). Same logic for any low-recall signal.

### Definition of done

Run against the real portfolio, the tiers match your judgment on a spot-check of
~30–50 domains you know well, with disagreements either resolved by weight
adjustment or understood and accepted. The cost-gap flag fires correctly.

**Estimate: 2 – 3 days.** First-pass model: a few hours to code. The rest is the
calibration loop with you — and that loop is genuine work, because it's where your
expertise gets encoded. Budget for several rounds of "adjust, re-run, review."

**Risk:** medium-high — the most likely phase to exceed estimate. If your mental
triage turns out to be more nuanced than a weighted composite captures (e.g., it
depends on factors not in the data), we may add rules, which adds time. Worth it —
this is the product.

---

## Phase 4 — The pipeline

**Depends on:** Phases 2 and 3.

**What gets built:** the orchestration that ties it together — CSV in → run all
adapters (concurrent where possible, respecting credit limits) → store enrichment →
score → tier → write `domain_metrics`. Run tracking (reuse the existing `runs`
table pattern), progress reporting, and resumability (re-run skips already-enriched
domains so a credit exhaustion or crash doesn't re-spend — `domains_missing()` is
already in schema.py for this).

**Definition of done:** one command takes an engagement from imported CSV to fully
scored-and-tiered `domain_metrics`, shows progress, and survives an interruption
without losing or re-spending completed work.

**Estimate: 1 – 1.5 days.** The pieces exist; this wires them with orchestration and
run-tracking, both following existing repo patterns.

**Risk:** low-medium. Credit-limit handling across multiple metered providers in one
run is the fiddly part.

---

## Phase 5 — Proof-of-concept validation

**Depends on:** Phase 4.

**What gets built:** nothing new — this is the first real end-to-end run, against the
Domain Tech Investments portfolio. Import it, run the full pipeline, and sanity-check
the output: do the tiers make sense, does the cost-gap finding hold up, are there
domains tiered wrongly that reveal a scoring gap (loops back to Phase 3)?

**Definition of done:** the full real portfolio runs clean end-to-end, the tier
distribution is defensible, and you'd be comfortable showing the underlying numbers
to Rafael Calvo. Any scoring corrections this surfaces are folded back in.

**Estimate: 1 – 2 days.** Depends on portfolio size and how much recalibration the
real data triggers. This phase and Phase 3 are coupled — time spent here may flow
back into scoring.

**Risk:** medium — this is where reality tests the scoring model. Allow that it may
send you back to Phase 3 for a round.

---

## Phase 6 — Deliverable generation

**Depends on:** Phase 5 (validated, trustworthy output).

**What gets built:** turn the scored/tiered `domain_metrics` into the actual audit
deliverable. The Findings Report template already exists (`Domain_Options_
Deliverable_Template.docx`). This phase populates it from pipeline output: the tier
breakdown, the disposition tables, the cost-gap savings figure, the per-domain
detail. Generates a clean, formatted report (docx/PDF) per engagement.

**Definition of done:** running it on the validated portfolio produces a complete,
correctly formatted Findings Report you could hand to a paying customer, with the
headline savings number and tier dispositions populated from real data.

**Estimate: 2 – 2.5 days.** Generating a populated, well-formatted deliverable from
data — tables, the five-tier breakdown, summary figures — is real work, and the last
10% of formatting polish always takes longer than expected. The template existing
saves the design time.

**Risk:** low-medium. Formatting polish is the usual overrun; the data is solid by now.

---

## Critical path & what to have ready

The chain is strictly sequential at the phase level: **1 → 2 → 3 → 4 → 5 → 6.** You
cannot score without adapter data (3 needs 2), can't validate without a pipeline
(5 needs 4), can't generate a deliverable without validated output (6 needs 5).
Within Phase 2, adapters are independent and can be built in any order.

**To avoid stalls, have ready before each phase:**

- **Before Phase 1:** the Mac, set up; the repo accessible.
- **Before Phase 2:** all access gathered — Estibot/WhoXY/DomScan confirmed working,
  SeoKicks `appid`, and confirmation that the Majestic Million CSV and Curlie TSV
  download URLs are reachable. *Gathering these during Phase 1 keeps Phase 2 unblocked.*
- **Before Phase 3:** your own clear sense of how you triage a portfolio — the model
  is a draft and you'll be correcting it live, so the faster you can react to output,
  the faster this phase goes.
- **Before Phase 5:** the Domain Tech Investments portfolio CSV, and the written
  permission to use it as the case study (the "CONFIRM THIS WEEK" item from the
  marketing plan).
- **Before Phase 6:** confirm the Curlie attribution requirement if Curlie data will
  appear in the deliverable (the license note from PROJECT.md).

---

## The honest bottom line

**~12–17 focused working days; ~2.5–3.5 calendar weeks at full availability.**

The number that moves the total is scoring (Phase 3) and its validation loop (Phase
5), because that's where your judgment gets encoded and reality tests it — the only
part that isn't mechanical. Everything else is well-understood construction. If you
want a single planning number, **three weeks** is the honest, slightly-conservative
estimate to a working platform that produces a real deliverable from a real portfolio.
