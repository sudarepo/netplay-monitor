"""OpenPageRank (DomCop) enrichment adapter.

Per-domain adapter over the free OpenPageRank API. Stands in as the budget AUTHORITY
corroborator in slot #6, REPLACING seokicks (parked: its API key is blocked pending a
support ticket as of 2026-06-18). Authority is triangulated (BUILD_BLUEPRINT.md:157) from
the Ahrefs anchor (#8: Domain Rating), Majestic Million global rank (#7), Wikipedia links,
and a cheap third backlink-derived signal -- which was seokicks' domain pop and is now
OpenPageRank's PageRank score. OPR is the weakest/cheapest of the authority signals (a
0-10 PageRank-style score over open Common Crawl data), so it corroborates; it never
outweighs Ahrefs.

Endpoint + response VERIFIED against DomCop's official docs (domcop.com/openpagerank/
documentation) 2026-06-19 -- field shapes below are CONFIRMED, not guessed:
  GET https://openpagerank.com/api/v1.0/getPageRank ? domains[]=<d>
  Auth: header  API-OPR: <key>   (NOT a query param, NOT Bearer)
  Natively bulk (up to 100 domains/request), but v1 calls ONE domain per enrich_one to
  fit the per-domain EnrichmentAdapter contract unchanged -- the 10,000 calls/hour limit
  makes the extra calls free in practice, and uniformity with the other adapters beats a
  base-class override (enrich_many is "do not override"). Bulk is a Phase 2.5
  optimization if it ever matters.

  Response envelope (verified):
    { "status_code": 200,
      "response": [ { "status_code": 200, "error": "",
                      "page_rank_integer": 10, "page_rank_decimal": 10,
                      "rank": "6", "domain": "google.com" } ],
      "last_updated": "28th Mar 2026" }
  One domain per call -> response[0] is our row.

Fields extracted (thin -- Authority magnitude only):
    page_rank        <- page_rank_decimal   (0.0-10.0 float; THE signal, lead on this)
    page_rank_int    <- page_rank_integer   (0-10 int; coarse bucket)
    global_rank      <- rank                (global position string -> int; lower=stronger)
  Lead scoring on page_rank_decimal (the fine-grained authority score). global_rank is a
  position (like Majestic's GlobalRank, lower=stronger) and corroborates it.

THE LOAD-BEARING SENTINEL TRAP (404 'Domain not found' returns ZEROS, not nulls):
  A domain absent from OPR's index returns an ITEM (not a top-level error) shaped:
      {"status_code":404, "error":"Domain not found",
       "page_rank_integer":0, "page_rank_decimal":0, "rank":null, "domain":...}
  That page_rank 0 is a SENTINEL for "not in our index", NOT a measured PageRank of 0.
  Collapsing it to 0 would feed the scorer a fake "zero authority" reading for a domain we
  simply have no data on. So: an item whose inner status_code != 200 maps ALL fields to
  None (UNKNOWN) -- the None != 0 discipline shared with estibot/ahrefs/majestic. A genuine
  measured low-authority domain returns status_code 200 with a real small page_rank (e.g.
  0.0-2.0), which IS kept and IS distinct from the 404 sentinel. The discriminator is the
  inner item status_code, exactly.

Result shape:
  * inner item status_code 200                 -> ok=True (MEASURED). Real values kept; a
        genuine 0.0 page_rank on a 200 item is preserved (distinct from the 404 sentinel).
  * inner item status_code 404 / non-200       -> ok=False, reason "not_found" (UNKNOWN;
        the domain isn't in OPR's index). Stays in domains_missing() for a later re-run if
        OPR's index grows. A 404 is NOT a measured zero.
  * HTTP 403 (bad/over-quota key)              -> ok=False "auth_failed" + HALT: a bad key
        fails identically on every call, so latch self._halted and short-circuit every
        later enrich_one with NO HTTP call (estibot family).
  * HTTP 429 after the bounded 429-only retry  -> ok=False "rate_limited" (UNKNOWN).
  * other HTTP >=400 / non-JSON / empty resp   -> ok=False (UNKNOWN), single attempt.

Auth-halt vs the free-but-keyed model: OPR requires a key but is free with a 10k/hr quota.
  A 403 means the key is bad or the hourly quota is blown; both fail identically across the
  batch, so 403 latch-halts (no point hammering 800 domains with a dead key). 429 (if OPR
  uses it for the hourly cap) is a transient throttle -> bounded 429-only retry, then
  ok=False rate_limited, recovered on re-run -- same posture as estibot.

Single-attempt + 429-only retry: identical to estibot/ahrefs. Transport errors propagate
  to _guarded -> ok=False (recovered via domains_missing()); a 429 is unbilled/unmeasured
  so a bounded retry is safe; everything else is single-attempt.

Concurrency: MAX_CONCURRENCY=5. The 10k/hr limit is generous and OPR is free, so this is
  about politeness + a small overspend-irrelevant bound, not cost.
"""
import asyncio

import httpx

from app.enrichment.base import EnrichmentAdapter


class OpenPageRankAdapter(EnrichmentAdapter):
    name = "openpagerank"
    BASE_URL = "https://openpagerank.com/api/v1.0/getPageRank"
    API_KEY_ENV = "OPENPAGERANK_API_KEY"     # keyed; sent as the API-OPR header
    MAX_CONCURRENCY = 5                        # 10k/hr limit is generous; politeness cap
    TIMEOUT = httpx.Timeout(connect=5.0, read=20.0, write=5.0, pool=5.0)

    MAX_429_RETRIES = 3                        # 1 attempt + 3 retries = 4 max, 429 ONLY
    RETRY_BASE_DELAY = 1.0

    def __init__(self, api_key=None):
        super().__init__(api_key)
        # Latched by an HTTP 403 (bad/over-quota key); short-circuits later enrich_one.
        self._halted = False

    def _headers(self):
        h = super()._headers()
        h["API-OPR"] = self.api_key          # OPR's auth header (not query param, not Bearer)
        return h

    async def enrich_one(self, client, domain):
        if self._halted:
            return self._error(domain, "openpagerank: skipped (halted on auth_failed)")

        ascii_domain = self._punycode(domain)
        resp = await self._lookup(client, ascii_domain)

        if resp.status_code == 403:
            self._halted = True
            return self._error(domain, "openpagerank: auth_failed (HTTP 403 bad/over-quota key)")
        if resp.status_code == 429:
            return self._error(domain, "openpagerank: rate_limited (HTTP 429 after retries)")
        if resp.status_code >= 400:
            return self._error(domain, f"openpagerank: HTTP {resp.status_code}")

        try:
            payload = resp.json()
        except Exception:
            return self._error(domain, "openpagerank: non-JSON response")

        items = payload.get("response") if isinstance(payload, dict) else None
        if not isinstance(items, list) or not items:
            return self._error(domain, "openpagerank: empty response array")

        item = items[0]                          # one domain per call -> one item
        if not isinstance(item, dict):
            return self._error(domain, "openpagerank: malformed response item")

        # The discriminator: a 404 'Domain not found' carries page_rank 0 as a SENTINEL,
        # not a measurement. Inner status != 200 -> UNKNOWN, never a measured zero.
        inner = item.get("status_code")
        if inner != 200:
            err = item.get("error") or "not found"
            return self._error(domain, f"openpagerank: not_found (item status {inner}: {err})")

        data = {
            "page_rank": _opr_score(item.get("page_rank_decimal")),
            "page_rank_int": _opr_int(item.get("page_rank_integer")),
            "global_rank": _rank(item.get("rank")),
        }
        return self._result(domain, data)

    async def _lookup(self, client, ascii_domain):
        """Single logical call with a bounded, 429-ONLY retry loop. Transport errors are
        NOT caught here -> propagate to _guarded -> ok=False (single attempt; recovered via
        domains_missing()). 5xx is NOT retried. Honors an integer Retry-After (capped by the
        base RETRY_AFTER_CAP) else exponential backoff with jitter. Returns the final
        Response. Sends the domain as a single-element domains[] bulk param."""
        params = {"domains[]": ascii_domain}
        resp = None
        for attempt in range(self.MAX_429_RETRIES + 1):
            resp = await client.get(self.BASE_URL, params=params)
            if resp.status_code == 429 and attempt < self.MAX_429_RETRIES:
                await asyncio.sleep(
                    self._retry_after(resp) or self._backoff(attempt, self.RETRY_BASE_DELAY)
                )
                continue
            return resp
        return resp                              # exhausted, still 429


def _opr_score(v):
    """page_rank_decimal -> float in [0.0, 10.0], or None. Blank/negative/out-of-range/
    non-numeric -> None. A genuine measured 0.0 ON A 200 ITEM is preserved by the caller
    (the 404-sentinel zero never reaches here -- it's filtered by the inner status guard)."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f < 0 or f > 10:
        return None
    return f


def _opr_int(v):
    """page_rank_integer -> int in [0, 10], or None. Out-of-range/non-numeric -> None."""
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    try:
        i = int(float(v))
    except (TypeError, ValueError):
        return None
    if i < 0 or i > 10:
        return None
    return i


def _rank(v):
    """Global rank arrives as a STRING ('6', '40') or null -> positive int, or None.
    null/blank/0/negative/non-numeric -> None (a rank is >= 1; OPR sends null for a
    not-found domain, already filtered, but guard anyway)."""
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        i = int(float(s))
    except (TypeError, ValueError):
        return None
    return i if i >= 1 else None
