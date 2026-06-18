"""SEOkicks (backlink / link-popularity) enrichment adapter.

Per-domain adapter over the SEOkicks Backlink API. Adapter #6 in the v1 stack
(estibot -> whoxy -> domscan -> archiveorg -> wikipedia -> seokicks). Yields the
AUTHORITY dimension via SEOkicks' "Domain Pop" family of link-popularity counts
(BUILD_BLUEPRINT.md:109,157). Authority is TRIANGULATED in the blueprint -- SEOkicks
domain pop is ONE signal among Majestic Million rank and Wikipedia links, and Ahrefs
(#8) later adds Domain Rating + referring-domain/backlink counts. So this adapter owns
ONLY the *pop family and must not invent Ahrefs-style fields; the two authority sources
are complementary, not competing for field names.

Endpoint (verified against en.seokicks.de/backlink-api.html):
  GET https://en.seokicks.de/SEOkicksService/V1/inlinkData
        ? appid=<key> & query=<domain> & format=json
  OVERVIEW-ONLY by design: we send NO `details` param. The overview response carries the
  four pop counts and costs exactly 1 credit/request. Passing details=1/2/3 would add
  0.1 credit per returned dataset (individual linking pages/domains) -- detail we don't
  need for an authority MAGNITUDE signal, and a cost we deliberately avoid on a
  portfolio-wide sweep. (Per-link detail is a Phase 2.5 follow-up if the audit narrative
  ever wants example backlinks.)

Fields extracted (thin -- Authority magnitude only):
    domainpop  <- domainpop   distinct linking DOMAINS  (primary authority signal)
    linkpop    <- linkpop      total individual backlinks
    ippop      <- ippop        distinct linking IPs
    netpop     <- netpop       distinct linking class-C networks
  domainpop is the load-bearing one: a domain can show huge linkpop from sitewide
  footer/sidebar links that all originate from ONE site, so linkpop alone overstates
  authority. domainpop (breadth of distinct linking domains) is the more meaningful
  signal and should carry the weight in scoring. All four are kept so the scorer can see
  the linkpop/domainpop spread (a wide spread flags sitewide-link inflation).

None != 0 discipline (load-bearing for a log-scaled Authority signal):
  A missing/blank/negative count maps to None (UNKNOWN for that field), NEVER 0. A
  genuine measured 0 (domain with no links in the index) is preserved and is distinct
  from missing -- collapsing "unknown" into 0 would feed the scorer a fake "no authority"
  reading. SEOkicks counts arrive as STRINGS (e.g. "14853") in the JSON body, same as
  estibot's USD fields, so coercion mirrors estibot._usd.

Response envelope (no documented `success` boolean -- contrast estibot):
  A 200 with a parseable body carrying an `Overview` object IS the answer. "Did the
  source answer" therefore == "got 200 + an Overview block", NOT a success flag. The
  XML form nests pop counts as attributes on <resultset>; we request format=json so the
  same four values arrive under top-level "Overview". We read ONLY Overview here; the
  "Results" array (present only with details=) is ignored.

Result shape:
  * 200 + Overview present                       -> ok=True (MEASURED). Per-field: a real
        count is kept; a missing/blank/negative field is None. An all-zero / all-missing
        Overview is still ok=True -- SEOkicks reached its index and reported the counts it
        has (possibly zero), which is a real measurement, distinct from a transport miss.
        (Same posture as estibot's all-(-1) row: the discriminator is "did the source
        answer," not "are the values empty." So there is deliberately NO domscan-style
        all-None guard here.)
  * 200 but no Overview / non-JSON               -> ok=False (UNKNOWN), single attempt.
  * auth OR credit exhaustion (see HALT below)   -> ok=False + LATCH-HALT.
  * HTTP 429 after the bounded 429-only retry     -> ok=False "rate_limited" (UNKNOWN,
        recovered on re-run).
  * other HTTP >=400 / transport                  -> ok=False (UNKNOWN), single attempt.

HALT on auth-or-credit (the estibot+domscan hybrid, documented because SEOkicks'
exhaustion signal is NOT in the published spec):
  SEOkicks credits are a DEPLETABLE monthly pool (domscan-like), unlike estibot's daily
  RATE allowance. So unlike estibot there genuinely IS a credit-exhaustion halt. BUT the
  published API page documents neither the out-of-credits HTTP status nor its body shape
  -- so, unlike domscan (which keys on a TYPED 402 + error.code), we CANNOT key on a
  specific typed signal yet. Both failure modes here share one root cause -- the single
  `appid` is unusable (wrong key, or its credit pool is empty) -- and both fail
  IDENTICALLY on every subsequent call, so both warrant the same latch-halt. We therefore
  treat 401/403, AND a 200/4xx error-envelope whose message names a key/credit/quota
  problem, as a single "auth_or_credit" halt: latch self._halted and short-circuit every
  later enrich_one with NO HTTP call (estibot's auth-400 halt + domscan's 402 halt,
  unified). The triggering domain returns ok=False with a "credit_exhausted" substring so
  orchestration keys on the same family token as whoxy/domscan.

  VALIDATION GATE (before any batch run; NOT a blocker to shipping this file): make ONE
  live call with a deliberately-bad appid and ONE with a (if obtainable) drained appid,
  and record the EXACT status + body for each. If the two are distinguishable (e.g.
  credit exhaustion returns a typed code), SPLIT this into estibot-style auth_failed vs
  domscan-style credit_exhausted with separate reason tokens. Until then the unified halt
  is the safe posture: it never keeps spending into a wall, and it never mislabels data.

Single-attempt + a surgical 429-only retry (identical posture to estibot):
  Single-attempt + resumability via domains_missing() is the paid-adapter FAMILY posture.
  The one exception is 429: a 429 is rejected before processing, so the request is
  unbilled and nothing was measured -- a bounded 429-only retry is spend-safe and simply
  completes work the rate-limiter refused. Transport errors and 5xx stay single-attempt
  (they may represent a partially-processed/charged request), with domains_missing() as
  the family-consistent recovery path. The retry is adapter-local (NOT base
  _get_with_retries, which also retries transport + 503 and would violate "429-only").

Concurrency: MAX_CONCURRENCY=2. SEOkicks does not publish a hard rate limit; 2 in-flight
  keeps the credit overspend-past-halt bound tiny (a depletable pool, so the domscan
  overspend reasoning applies) and is polite to a single-crawler service.

Phase 2.5 follow-ups (deferred): (a) split auth vs credit halt once the validation gate
  records distinguishable signals; (b) details= pass to pull example backlinks if the DTI
  audit narrative wants them (costs +0.1 credit/dataset).
"""
import asyncio

import httpx

from app.enrichment.base import EnrichmentAdapter

# Substrings that, in an error-envelope message, indicate the appid is unusable
# (bad key OR drained credit pool). Lowercased compare. Best-effort: the spec doesn't
# document the message text, so this is intentionally broad and revisited at the
# validation gate above.
_HALT_MESSAGE_HINTS = ("api key", "appid", "credit", "quota", "limit reached",
                       "insufficient", "not authorized", "unauthorized", "invalid key")


class SeokicksAdapter(EnrichmentAdapter):
    name = "seokicks"
    BASE_URL = "https://en.seokicks.de/SEOkicksService/V1/inlinkData"
    API_KEY_ENV = "SEOKICKS_API_KEY"         # keyed; passed as the `appid` query param
    MAX_CONCURRENCY = 2                       # depletable pool -> keep overspend-past-halt small
    TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0)

    MAX_429_RETRIES = 3                       # 1 attempt + 3 retries = 4 max, 429 ONLY
    RETRY_BASE_DELAY = 1.0

    def __init__(self, api_key=None):
        super().__init__(api_key)
        # Latched by an auth-or-credit failure; short-circuits later enrich_one.
        self._halted = False

    async def enrich_one(self, client, domain):
        # Auth/credit already failed once -> no call, no spend; ok=False so a re-run with
        # a fixed/refunded appid recovers via domains_missing().
        if self._halted:
            return self._error(domain, "seokicks: skipped (halted on auth_or_credit)")

        ascii_domain = self._punycode(domain)
        resp = await self._lookup(client, ascii_domain)

        # --- HTTP-level typed outcomes -------------------------------------
        if resp.status_code in (401, 403):
            # Unusable appid (bad key or drained pool) -> latch + halt (unified; see docstring).
            self._halted = True
            return self._error(
                domain, f"seokicks: credit_exhausted (auth_or_credit, HTTP {resp.status_code})")
        if resp.status_code == 429:
            return self._error(domain, "seokicks: rate_limited (HTTP 429 after retries)")
        if resp.status_code >= 400:
            return self._error(domain, f"seokicks: HTTP {resp.status_code}")

        try:
            payload = resp.json()
        except Exception:
            return self._error(domain, "seokicks: non-JSON response")

        # --- Error envelope on a 2xx body (auth/credit named in a message) -
        # Some providers return 200 with an error message rather than a 4xx. If the
        # message names a key/credit/quota problem, treat it as the same unified halt.
        msg = self._envelope_error_message(payload)
        if msg and self._looks_like_auth_or_credit(msg):
            self._halted = True
            return self._error(domain, f"seokicks: credit_exhausted (auth_or_credit: {msg})")

        overview = self._overview(payload)
        if not isinstance(overview, dict):
            # 200 but no Overview block -> we did not get a measurement. UNKNOWN.
            if msg:
                return self._error(domain, f"seokicks: request unsuccessful ({msg})")
            return self._error(domain, "seokicks: no Overview in response")

        data = {
            "domainpop": _count(overview.get("domainpop")),
            "linkpop": _count(overview.get("linkpop")),
            "ippop": _count(overview.get("ippop")),
            "netpop": _count(overview.get("netpop")),
        }
        return self._result(domain, data)

    async def _lookup(self, client, ascii_domain):
        """Single logical overview call with a bounded, 429-ONLY retry loop. Transport
        errors are NOT caught here -> they propagate to _guarded -> ok=False (single
        attempt; domains_missing() recovers). 5xx is NOT retried. Honors an integer
        Retry-After (capped by the base RETRY_AFTER_CAP) else exponential backoff with
        jitter. Returns the final httpx Response. NOTE: no `details` param -> overview
        only -> exactly 1 credit."""
        params = {"appid": self.api_key, "query": ascii_domain, "format": "json"}
        resp = None
        for attempt in range(self.MAX_429_RETRIES + 1):
            resp = await client.get(self.BASE_URL, params=params)
            if resp.status_code == 429 and attempt < self.MAX_429_RETRIES:
                await asyncio.sleep(
                    self._retry_after(resp) or self._backoff(attempt, self.RETRY_BASE_DELAY)
                )
                continue
            return resp
        return resp                                   # exhausted, still 429

    @staticmethod
    def _overview(payload):
        """Pull the Overview object from the JSON body. Accepts the documented
        capitalized "Overview" and a lowercase "overview" defensively. Returns the dict
        or None."""
        if not isinstance(payload, dict):
            return None
        ov = payload.get("Overview")
        if ov is None:
            ov = payload.get("overview")
        return ov if isinstance(ov, dict) else None

    @staticmethod
    def _envelope_error_message(payload):
        """Best-effort extraction of an error message from a 2xx body. Returns a string
        or None. The spec doesn't document an error envelope, so we probe common keys."""
        if not isinstance(payload, dict):
            return None
        for key in ("error", "Error", "message", "Message", "status", "Status"):
            v = payload.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return None

    @staticmethod
    def _looks_like_auth_or_credit(message):
        m = message.lower()
        return any(h in m for h in _HALT_MESSAGE_HINTS)


def _count(v):
    """SEOkicks pop count (arrives as a string like '14853') -> int, or None. Missing /
    blank / non-numeric / negative -> None (UNKNOWN), never 0. A genuine measured 0 is
    preserved and is distinct from missing. Mirrors estibot._usd's sentinel discipline:
    any negative is treated as not-available (no real link count is negative)."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f < 0:               # not-available sentinel / stray negative -> None
        return None
    return int(f)
