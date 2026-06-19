"""Estibot (algorithmic valuation) enrichment adapter.

Per-domain adapter over the Estibot public API. Adapter #5 (estibot -> whoxy ->
domscan -> archiveorg -> wikipedia). Yields the Value dimension: Estibot's
algorithmic appraisal. CACHE-ONLY in v1.

Endpoint (verified against github.com/domainret/estibot-api, which supersedes the
legacy login-gated PDF; the old PROJECT.md row's www.estibot.com/api.php was wrong):
  GET https://public-api.estibot.com/api ? k=<key> & a=appraise & d=<domain> & t=cache
  One domain per call (no '>>' bulk batching in v1).

Cache-only, and why live-retry is deferred:
  t=cache returns Estibot's already-computed appraisal if the domain is in cache, and
  lists uncached domains in a top-level not_found[]. Cache-only is the simplest version
  that fits the enrich_one contract. The live-retry-of-not_found pass (t=live for the
  misses) is deferred to Phase 2.5, pending a validation run that measures actual cache
  COVERAGE on the DTI set -- don't build batch/live machinery before confirming it's
  needed. Cache misses are recoverable by design (see result shape).

Cost model -- NOT a finite credit pool (contrast domscan):
  The Domain Appraiser allowance is 25,000 hits/day with a daily reset at 00:00 CST -- a
  rate allowance, not a depletable pool. So estibot has NO finite-credit halt like
  domscan's 402; the ONLY failure-halt is auth (HTTP 400 invalid key). The 800-domain
  DTI batch is ~3% of one day, so cost is a non-issue. Open validation question is purely
  cache COVERAGE (% cached vs not_found), which decides whether Phase 2.5 live-retry gets
  promoted -- a quality/coverage question, not cost. (Nuance to confirm at validation:
  whether a t=cache lookup counts as a full appraiser hit like t=live; the limits page
  doesn't distinguish, and at 25K/day it doesn't affect this batch either way.)

Response envelope (verified) -- always gate on `success` first:
  * success:true  -> results is an ARRAY of row objects (one per queried domain);
                     plus top-level not_found[], cache, bulk, item_count, result_count.
  * success:false -> results is an OBJECT {total,count,start,end,data:[]}, NOT an array.
  Because the shape of `results` flips, NOTHING reads results without checking success.

Fields extracted (thin -- Value dimension only; the other ~95 row fields are skipped):
    estimated_value      <- appraised_value            (USD int, arrives as a string)
    wholesale_value      <- appraised_wholesale_value  (USD int, arrives as a string)
    price_range_retail   <- price_range_retail         (bracket ID 1-24)
  The bracket-ID -> USD-range mapping (for narrative) is a Phase 2.5 follow-up: the
  1-24 bracket table lives in the field-reference doc, not yet in hand, so v1 stores the
  raw bracket ID and never invents a dollar range it can't source.

None != $0 discipline (load-bearing for a log-scaled Value signal):
  -1 / -1.00 is Estibot's documented "not available" sentinel across all numeric fields.
  A sentinel maps to None (UNKNOWN for that field), NEVER 0. A genuine measured 0 (if it
  occurs) is preserved and is distinct from -1/missing. Collapsing "no appraisal" into $0
  would feed the log scale a fake "worthless" reading.

Result shape:
  * success:true, a result row returned        -> ok=True (MEASURED). Per-field: a real
        value is kept; a -1/missing field is None. A row whose value fields are ALL -1 is
        still ok=True -- Estibot HAS the domain cached and assigns no value (a measured
        "not available"), which is real information, distinct from a cache miss.
        WHY all-(-1) is ok=True while domscan's all-None is ok=False: the discriminator is
        "did we reach the source and get an answer," NOT "are the values None." A cached
        all-(-1) row IS an answer from estibot ("no value") -> a real measurement -> ok=True.
        domscan's all-None means every endpoint failed to RESPOND (non-measurement) -> its
        guard yields ok=False. Same None-valued data, opposite ok, because the question is
        whether the source answered -- so there is deliberately NO domscan-style all-None
        guard here.
  * cache miss: domain in top-level not_found[] -> ok=False, reason "cache_miss
        (not_found)". A not_found is "no value YET" (genuinely UNKNOWN, not measured-
        empty): ok=False keeps it in domains_missing() so a re-run retries it AND the
        Phase 2.5 live-retry pass inherits the exact miss set to target.
  * HTTP 400 "Invalid API key."                -> ok=False "auth_failed", and HALT: every
        call fails identically, so (like domscan's 402) latch self._halted and short-
        circuit every later enrich_one with NO HTTP call.
  * HTTP 429 after the bounded 429-only retry   -> ok=False "rate_limited" (UNKNOWN,
        recovered on re-run).
  * other HTTP >=400 / non-JSON / transport     -> ok=False (UNKNOWN), single attempt.

Single-attempt + a surgical 429-only retry (the divergence, documented so a future
reader needn't reconstruct it):
  Single-attempt + resumability via domains_missing() is the paid-adapter FAMILY posture
  (whoxy, domscan), kept uniform here even though estibot's daily-reset allowance removes
  the double-charge cost pressure that originally motivated it. The one exception is 429:
  A 429 is rejected before processing, so the request is unbilled and the appraisal never
  ran -- a bounded 429-only retry is spend-safe and simply completes work the rate-limiter
  refused; transport errors and 503s stay single-attempt because they may represent a
  partially-processed request, with domains_missing() as the family-consistent recovery
  path. The retry is adapter-local (NOT base _get_with_retries, which also retries
  transport + 503 and would violate "429-only").

Concurrency: MAX_CONCURRENCY=2. The documented limit is 3 req/s + 100 req/min per IP;
  2 in-flight stays under both with margin (only affects speed, not spend).

Phase 2.5 follow-ups (deferred): (a) live-retry pass (t=live) for not_found domains, if
  coverage validation shows it's warranted; (b) price_range_retail bracket-ID -> USD-range
  mapping once the bracket table is in hand.
"""
import asyncio

import httpx

from app.enrichment.base import EnrichmentAdapter


class EstibotAdapter(EnrichmentAdapter):
    name = "estibot"
    BASE_URL = "https://public-api.estibot.com/api"
    API_KEY_ENV = "ESTIBOT_API_KEY"         # keyed; passed as the `k` query param
    MAX_CONCURRENCY = 2                      # 3 req/s + 100 req/min per IP -> 2 keeps margin
    TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0)

    MAX_429_RETRIES = 3                      # 1 attempt + 3 retries = 4 max, 429 ONLY
    RETRY_BASE_DELAY = 1.0

    def __init__(self, api_key=None):
        super().__init__(api_key)
        # Latched by an HTTP 400 invalid-key response; short-circuits later enrich_one.
        self._halted = False

    async def enrich_one(self, client, domain):
        # Auth already failed once -> no call, no point; ok=False so a fixed-key re-run
        # recovers via domains_missing().
        if self._halted:
            return self._error(domain, "estibot: skipped (halted on auth_failed)")

        ascii_domain = self._punycode(domain)
        resp = await self._appraise(client, ascii_domain)

        # --- HTTP-level typed outcomes -------------------------------------
        if resp.status_code == 400:
            # Documented 400 "Invalid API key." Every call fails identically -> latch+halt.
            self._halted = True
            return self._error(domain, "estibot: auth_failed (HTTP 400 invalid API key)")
        if resp.status_code == 429:
            # Still throttled after the bounded 429-only retry -> UNKNOWN, recovered on re-run.
            return self._error(domain, "estibot: rate_limited (HTTP 429 after retries)")
        if resp.status_code >= 400:
            # 5xx / other 4xx: single-attempt UNKNOWN (may be partial; domains_missing recovers).
            return self._error(domain, f"estibot: HTTP {resp.status_code}")
        try:
            payload = resp.json()
        except Exception:
            return self._error(domain, "estibot: non-JSON response")

        # --- Envelope: gate on success (results flips array<->object) ------
        if not payload.get("success"):
            msg = payload.get("message") or "unknown error"
            if "invalid api key" in str(msg).lower():   # defensive: auth msg on a 200 body
                self._halted = True
                return self._error(domain, "estibot: auth_failed (invalid API key)")
            return self._error(domain, f"estibot: request unsuccessful ({msg})")

        # --- Cache miss: top-level not_found[] (t=cache) -> UNKNOWN, retry later ---
        if self._domain_in(ascii_domain, payload.get("not_found") or []):
            return self._error(domain, "estibot: cache_miss (not_found)")

        results = payload.get("results")
        # Live API returns results as an OBJECT {data:[...], total, count} on success,
        # NOT a bare array (the earlier docstring had this backwards). Unwrap results.data
        # to get the row list; tolerate a bare list defensively in case a response differs.
        if isinstance(results, dict):
            rows = results.get("data")
        elif isinstance(results, list):
            rows = results
        else:
            rows = None
        if not isinstance(rows, list) or not rows:
            # success:true, not in not_found, yet no row -> UNKNOWN, not a measurement.
            return self._error(domain, "estibot: success with empty results")

        row = rows[0]                                     # one domain per call -> one row
        data = {
            "estimated_value": _usd(row.get("appraised_value")),
            "wholesale_value": _usd(row.get("appraised_wholesale_value")),
            "price_range_retail": _bracket(row.get("price_range_retail")),
        }
        return self._result(domain, data)

    async def _appraise(self, client, ascii_domain):
        """Single logical appraisal call with a bounded, 429-ONLY retry loop. Transport
        errors are NOT caught here -> they propagate to _guarded -> ok=False (they may be
        partially processed; domains_missing() recovers them). 503s are NOT retried. Honors
        an integer Retry-After when present (capped by the base RETRY_AFTER_CAP), else
        exponential backoff with jitter. Returns the final httpx Response."""
        params = {"k": self.api_key, "a": "appraise", "d": ascii_domain, "t": "cache"}
        resp = None
        for attempt in range(self.MAX_429_RETRIES + 1):
            resp = await client.get(self.BASE_URL, params=params)
            if resp.status_code == 429 and attempt < self.MAX_429_RETRIES:
                await asyncio.sleep(
                    self._retry_after(resp) or self._backoff(attempt, self.RETRY_BASE_DELAY)
                )
                continue
            return resp
        return resp                                      # exhausted, still 429

    @staticmethod
    def _domain_in(domain, items):
        d = domain.lower()
        return any(str(x).lower() == d for x in items)


def _usd(v):
    """Estibot USD field (arrives as a string like '2500') -> int, or None. The documented
    -1 / -1.00 'not available' sentinel -> None (any negative is treated as not-available;
    no real USD value is negative). A genuine 0 is preserved, distinct from -1."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f < 0:               # -1 / -1.00 sentinel (and any stray negative) -> not available
        return None
    return int(f)


def _bracket(v):
    """price_range_retail bracket ID -> int in [1,24], or None. -1/0/missing/out-of-range
    -> None. The bracket-ID -> USD-range mapping is deferred (Phase 2.5); v1 keeps the raw
    ID and never fabricates a dollar range."""
    if v is None:
        return None
    try:
        i = int(float(v))
    except (TypeError, ValueError):
        return None
    if i < 1 or i > 24:     # -1 sentinel, 0, or out-of-range
        return None
    return i
