"""Ahrefs (authority / backlinks) enrichment adapter.

Per-domain adapter over the Ahrefs API v3. Adapter #8 in the v1 stack -- built LAST by
design (BUILD_BLUEPRINT.md:112) so the cheaper authority signals (seokicks #6 domain pop,
majestic_million #7 global rank) ship first and inform this one. Ahrefs is the ANCHOR of
the triangulated AUTHORITY dimension (BUILD_BLUEPRINT.md:157): Domain Rating is the
industry-standard backlink-strength score, and seokicks/majestic corroborate it. Most
expensive source in the stack -- treat its budget with care (see cost model).

Two endpoints per domain, both verified LIVE 2026-06-18 against the free ahrefs.com target
(field shapes below are CONFIRMED, not guessed -- a first for this stack):

  GET /v3/site-explorer/domain-rating?target=<d>&date=<YYYY-MM-DD>
      -> {"domain_rating": {"domain_rating": 91.0, "ahrefs_rank": 635}}
  GET /v3/site-explorer/backlinks-stats?target=<d>&mode=domain&date=<YYYY-MM-DD>
      -> {"metrics": {"live": 19347349, "all_time": 296694532,
                      "live_refdomains": 98410, "all_time_refdomains": 310181}}

Fields extracted (the Authority anchor + corroborating volume):
    domain_rating       <- domain_rating.domain_rating   (float 0-100; THE anchor signal)
    ahrefs_rank         <- domain_rating.ahrefs_rank      (int; global rank, lower=stronger)
    live_backlinks      <- metrics.live                   (current backlinks)
    live_refdomains     <- metrics.live_refdomains        (current referring domains)
    all_time_refdomains <- metrics.all_time_refdomains    (incl. lost links)
  Scoring should lead on domain_rating + live_refdomains: "live" is the honest CURRENT
  profile, while all_time includes dead links and overstates standing. all_time_refdomains
  is kept so the scorer can see the live/all-time spread (a wide gap = a decayed profile
  that once ranked) -- same rationale as seokicks keeping linkpop beside domainpop.

date= is REQUIRED on these endpoints. v1 uses today's UTC date (the most recent index).
  Ahrefs returns the latest available snapshot at/with that date.

Cost model -- EXPENSIVE, depletable monthly pool (domscan-family halt, not estibot-family):
  v3 is unit-metered: base 50 units/request + per-field costs, minimum 50/request. So TWO
  calls/domain is ~100+ units/domain; an 800-domain batch is ~80,000+ units. On a Lite/
  Standard allowance (e.g. 150,000 units/mo) that is a large fraction of a month in ONE
  run -- so resumability (domains_missing) and a hard credit-halt are load-bearing here,
  more than anywhere else in the stack. Units once consumed are non-refundable; cache hits
  cost 0. (BUILD_BLUEPRINT.md's "$249/mo Standard floor" note is stale: v3 runs on Lite+
  with per-plan unit/row caps; left as a doc note, not a code concern -- the key works.)

  Concurrency: MAX_CONCURRENCY=2. Standard's documented limit is 60 req/min; 2 in-flight
  stays well under it AND keeps the overspend-past-halt bound tiny (domscan reasoning:
  once the first exhaustion lands, only ~1 more in-flight call can overspend).

HALT on auth-or-exhaustion (domscan-style latch; documented because the exact exhaustion
signal is NOT reliably typed in the v3 docs):
  Unlike domscan's clean typed 402, Ahrefs v3 does not document a single machine-readable
  unit-exhausted code, and community reports vary (the practical observation is "the API
  stops working" when units run out). Both a bad/over-capped key and a drained allowance
  fail IDENTICALLY on every subsequent call, so both warrant the same latch-halt to avoid
  burning ~100 units/domain across the rest of an 800-domain batch. We therefore latch on:
    * HTTP 401 / 403            -> auth or key over-cap
    * HTTP 429                  -> rate/again unit-limit; with Ahrefs's metering a 429 most
                                   often signals the unit wall, so it HALTS (does not just
                                   skip one domain) -- the conservative spend-protective
                                   choice for an expensive source.
    * a 4xx/200 error body whose message names units/limit/quota/subscription
  On any of these: latch self._halted, return ok=False with a "credit_exhausted" substring
  (whoxy/domscan family token so orchestration keys on one string), and short-circuit every
  later enrich_one with NO HTTP call -> no further spend.

  VALIDATION GATE (before a full 800-domain run; not a blocker to shipping this file):
  deliberately exhaust or use a unit-capped key and record the EXACT status + body Ahrefs
  returns at the wall. If it is distinct from auth (it likely is), SPLIT into a typed
  credit_exhausted vs auth_failed with separate tokens, mirroring domscan's typed 402. Also
  confirm whether X-RateLimit-* / units headers appear (community docs say yes; official
  docs don't enumerate them) and, if so, pre-empt the halt by reading remaining units.
  Until measured, the unified spend-protective halt above is the safe default.

Per-endpoint partial degradation (domscan posture, NOT whoxy gating):
  The two endpoints are INDEPENDENT (no data dependency), so if one returns data and the
  other fails non-fatally (5xx/non-JSON), the failed axis degrades to None and the row
  stays ok=True with the axis that succeeded. A halt-class status on EITHER call halts the
  whole adapter (see above). If BOTH axes are unavailable (and it wasn't a halt), the row
  is ok=False (UNKNOWN) -- never an all-None "measurement" -- so domains_missing() re-runs
  it. A reached-and-answered domain with genuine zero/None metrics is still ok=True (the
  discriminator is "did Ahrefs answer," not "are the values empty").

Single-attempt + a surgical 429-only... NO: 429 HALTS here (see above), so there is no
  429-retry. Each endpoint is a single attempt; transport errors propagate to _guarded ->
  ok=False (a charged-but-misread paid call must not be silently retried -> double-spend);
  domains_missing() is the family-consistent recovery path. 5xx degrades one axis to None.

None != 0 discipline: a sentinel/blank/negative numeric -> None (UNKNOWN), never 0. A
  genuine measured 0 (a domain with DR 0 / 0 refdomains -- common for parked/unlinked
  names) is PRESERVED and is real, valuable signal for the audit (low authority), distinct
  from "not measured". _num()/_dr() enforce this.
"""
from datetime import datetime, timezone

import httpx

from app.enrichment.base import EnrichmentAdapter

_HALT_MESSAGE_HINTS = ("unit", "limit", "quota", "subscription", "not authorized",
                       "unauthorized", "forbidden", "exhaust", "insufficient")


class AhrefsAdapter(EnrichmentAdapter):
    name = "ahrefs"
    BASE_URL = "https://api.ahrefs.com/v3"
    API_KEY_ENV = "AHREFS_API_KEY"          # keyed; Authorization: Bearer <key>
    MAX_CONCURRENCY = 2                       # 60 req/min limit + tiny overspend-past-halt bound
    TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0)

    def __init__(self, api_key=None):
        super().__init__(api_key)
        # Latched by an auth-or-exhaustion outcome; short-circuits later enrich_one.
        self._halted = False

    def _headers(self):
        h = super()._headers()
        h["Authorization"] = f"Bearer {self.api_key}"
        return h

    async def enrich_one(self, client, domain):
        if self._halted:
            return self._error(domain, "ahrefs: skipped (halted on auth_or_units)")

        ascii_domain = self._punycode(domain)
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        data = {}

        # --- Axis 1: domain-rating (the anchor) ----------------------------
        payload, st = await self._call(client, "/site-explorer/domain-rating",
                                       {"target": ascii_domain, "date": date})
        if st == "halt":
            self._halted = True
            return self._error(domain, "ahrefs: credit_exhausted (auth_or_units, domain-rating)")
        data.update(self._extract_dr(payload))

        # --- Axis 2: backlinks-stats (independent; corroborating volume) ---
        payload, st = await self._call(client, "/site-explorer/backlinks-stats",
                                       {"target": ascii_domain, "mode": "domain", "date": date})
        if st == "halt":
            self._halted = True
            return self._error(domain, "ahrefs: credit_exhausted (auth_or_units, backlinks-stats)")
        data.update(self._extract_backlinks(payload))

        # All-axes-UNKNOWN guard: nothing measured -> UNKNOWN, not an all-None measurement.
        if not any(v is not None for v in data.values()):
            return self._error(domain, "ahrefs: all signals unavailable")

        return self._result(domain, data)

    async def _call(self, client, path, params):
        """One single-attempt GET. Returns (payload|None, status):
            "ok"   -> parsed JSON (HTTP 200)
            "fail" -> this axis UNKNOWN (5xx / non-JSON) -> None fields; degrades locally
            "halt" -> auth or unit exhaustion (401/403/429, or an error body naming units)
                      -> caller latches self._halted and stops the whole adapter
        Transport errors are NOT caught here -> propagate to _guarded -> ok=False
        (single-attempt; domains_missing() recovers; a charged call is never auto-retried)."""
        resp = await client.get(self.BASE_URL + path, params=params)

        if resp.status_code in (401, 403, 429):
            # Spend-protective: with unit metering, all three most likely mean the wall or a
            # dead/capped key -> halt rather than burn ~100 units/domain across the batch.
            return None, "halt"
        if resp.status_code >= 400:
            # other 4xx / 5xx: this axis UNKNOWN, degrade locally (unless body names units).
            msg = self._error_message(resp)
            if msg and self._looks_like_units_or_auth(msg):
                return None, "halt"
            return None, "fail"
        try:
            payload = resp.json()
        except Exception:
            return None, "fail"
        # 200 with an error envelope naming units/limit -> halt.
        msg = self._error_message_from_body(payload)
        if msg and self._looks_like_units_or_auth(msg):
            return None, "halt"
        return payload, "ok"

    def _extract_dr(self, payload):
        keys = {"domain_rating": None, "ahrefs_rank": None}
        if not isinstance(payload, dict):
            return keys
        dr = payload.get("domain_rating")
        if not isinstance(dr, dict):
            return keys
        return {
            "domain_rating": _dr(dr.get("domain_rating")),
            "ahrefs_rank": _num(dr.get("ahrefs_rank")),
        }

    def _extract_backlinks(self, payload):
        keys = {"live_backlinks": None, "live_refdomains": None, "all_time_refdomains": None}
        if not isinstance(payload, dict):
            return keys
        m = payload.get("metrics")
        if not isinstance(m, dict):
            return keys
        return {
            "live_backlinks": _num(m.get("live")),
            "live_refdomains": _num(m.get("live_refdomains")),
            "all_time_refdomains": _num(m.get("all_time_refdomains")),
        }

    @staticmethod
    def _error_message(resp):
        try:
            return AhrefsAdapter._error_message_from_body(resp.json())
        except Exception:
            return None

    @staticmethod
    def _error_message_from_body(payload):
        # Ahrefs error bodies have been seen as ["Error","Unauthorized"] and as dicts.
        if isinstance(payload, list):
            return " ".join(str(x) for x in payload) or None
        if isinstance(payload, dict):
            for k in ("error", "Error", "message", "Message"):
                v = payload.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
        return None

    @staticmethod
    def _looks_like_units_or_auth(message):
        m = message.lower()
        return any(h in m for h in _HALT_MESSAGE_HINTS)


def _dr(v):
    """Domain Rating -> float in [0,100], or None. Blank/negative/non-numeric -> None.
    A real measured 0.0 (DR 0, common for unlinked/parked domains) is PRESERVED."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f < 0 or f > 100:
        return None
    return f


def _num(v):
    """Count/rank -> non-negative int, or None. Blank/negative/non-numeric -> None.
    A genuine measured 0 is preserved (distinct from missing)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f < 0:
        return None
    return int(f)
