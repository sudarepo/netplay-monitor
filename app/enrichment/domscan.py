"""DomScan (technical health + reputation + TLD-count) enrichment adapter.

Per-domain adapter over the DomScan API. Adapter #4 in the v1 stack
(estibot -> whoxy -> domscan -> archiveorg -> wikipedia). Yields three INDEPENDENT
signal axes from three endpoints, ~5 credits/domain total:

  * /health      -> health_score, health_grade, health_checks{}        (Usage/technical)
  * /reputation  -> reputation_score, reputation_grade, risk_level,
                    grade_capped_by_parking, reputation_factors{}        (reputation/risk)
  * /status      -> tld_count, tlds_registered[], tlds_checked           (Defensive: TLD breadth)

The /status call is natively "bulk" INSIDE one enrich_one: one brand label x a
curated ~50-TLD list in a single 1-credit call (see CURATED_TLDS). It feeds the
"Number of TLDs registered" defensive/brand signal (BUILD_BLUEPRINT.md:160).

DEFERRED — parking (/parking is_for_sale + aftermarket_listings):
  Not called in v1 for two reasons. (1) The spec is silent on its credit cost
  (validation item #2 measures it). (2) The reputation axis already carries
  `grade_capped_by_parking`, but that is a WEAK proxy: it fires only when parking
  CLAMPED the reputation grade, not as a clean parked-yes/no. Re-adding /parking is
  a deliberate, separate decision that weighs is_for_sale + aftermarket_listings as
  audit-NARRATIVE value for the parking-heavy DTI portfolio — not a signal gap to
  paper over now.

SKIPPED — owned by other adapters, no double-spend:
  whois -> whoxy (adapter #2);  value -> estibot (#1);  dns -> already inside /health.

Result shape (None != 0 discipline):
  * ok=True  -> MEASURED. At least one axis returned data. A dead/failing domain is
               ok=True with low/failing scores — "measured as bad," never ok=False.
  * ok=False -> UNKNOWN. We did not measure it (transport/429/402, or all three axes
               unavailable). Never fabricate a 0 for a failed axis — a failed
               sub-signal is None (unknown for that axis), distinct from a real 0.

Partial-signal, NO inter-endpoint gating:
  The three axes are genuinely INDEPENDENT — a deliberate contrast with whoxy, whose
  History call is GATED on Live succeeding AND the domain being registered (a true
  data dependency: no point pricing history for an unregistered name). DomScan has no
  such dependency, so there is no gating. If one axis is unavailable (auth/5xx/
  non-JSON) while another succeeds, its fields degrade locally to None and the row
  stays ok=True. EXCEPTION: a 402 discards the whole domain to ok=False + incomplete
  (see below) so the re-run redoes it cleanly; and if ALL THREE axes are unavailable
  the row is ok=False (UNKNOWN), never an all-None "measurement," so domains_missing()
  re-runs it.

  Cost of local degradation (deliberate, accepted): a transient single-axis 5xx is
  recorded ok=True with that axis None; because domains_missing() treats ok=True as
  done, that axis stays unknown until a forced refresh. This is the deliberate cost of
  keeping the two good (paid) axes rather than discarding them on a momentary blip.
  Read an all-None reputation axis as "not measured," NOT as "measured as bad."

402 HARD-STOP (in-adapter, immediate) — the load-bearing contrast with whoxy:
  whoxy's credit-exhaustion signal is a FUZZY free-text status_reason matched by
  substring; a single hit could be a misread, so whoxy keeps marching and defers the
  halt to an orchestration circuit-breaker that needs N-in-a-row. DomScan's signal is
  TYPED and machine-readable: error.code == "INSUFFICIENT_CREDITS" on a dedicated
  HTTP 402, with credits_remaining / credits_required in the body and an
  X-Credits-Remaining header. Zero misread risk -> a SINGLE occurrence is trustworthy
  enough to halt immediately, in-adapter. Signal reliability drives the policy:
  fuzzy -> corroborate; typed -> halt.

  Mechanics: on 402 we (1) parse the typed body + X-Credits-Remaining header; (2) set
  self._halted; (3) return ok=False for the triggering domain with a stable
  "credit_exhausted" token (same family as whoxy, so orchestration keys on one
  substring); (4) every subsequent enrich_one short-circuits on self._halted to
  ok=False "skipped (halted ...)" with NO HTTP call. All within the enrich_one-only
  contract (enrich_many is NOT overridden). Concurrency-overspend caveat: up to
  MAX_CONCURRENCY calls may already be in flight when the first 402 lands, so a few
  more credits can be spent before the halt takes hold — bounded (see MAX_CONCURRENCY
  comment), not zero.

Single-attempt (no _get_with_retries) — same posture as whoxy:
  Each of the three calls is a single attempt. A paid call that succeeded server-side
  (charged) but whose response was read wrong must not be retried (double-charge). A
  transport error propagates to _guarded -> ok=False; a 429 -> ok=False. Both are
  recovered by run-level resumability via domains_missing(), never by in-request
  retry. (429-only bounded retry is a lowest-priority Phase 2.5 note.)

Architecture — per-domain enrich_one, NOT bulk:
  The base contract is enrich_one-only (enrich_many = "do not override"); per-domain
  + domains_missing keeps the resumability boundary clean; and DomScan's bulk caps are
  tiny (reputation max 3, health max 10) for a poor complexity/savings ratio. The one
  "bulk" we DO use is /status's one-name-x-50-TLDs single call. (Health-bulk is a
  Phase 2.5 cost-optimization, only if validation shows per-domain draw is too high.)

TWO RUNTIME GATES before any batch RUN (validation, like whoxy's — NOT blockers to
shipping this file): (1) confirm /status with name + ~50 TLDs costs 1 credit TOTAL,
not per-TLD — if per-TLD, the cost model inverts and the TLD signal is redesigned;
(2) confirm whether the 10K pool is monthly-renewing free credits or one-time.

Validation plan (3-5 domains, each bracketed by GET /user/credits to measure real
draw; NO deliberate exhaustion): (1) /status per-TLD billing [gate 1]; (2) /parking
cost; (3) whether X-Credits-Remaining appears on 200s, not only 402; (4)
free_credits vs paid_credits draw order; (5) per-endpoint cost + VERBATIM field
names (every field read below is a GUESS until this confirms it).

Phase 2.5 follow-ups (deferred, separate commits):
  (a) 429-only bounded retry.
  (b) health-bulk cost-optimization if per-domain draw proves too high.
  (c) re-add /parking if the DTI audit narrative wants is_for_sale / aftermarket.
  (d) a 401/403 auth-failed LATCH-HALT (mirroring the 402 halt, reason "auth_failed"):
      a bad key fails identically on every axis of every domain, burning a full
      three-call loop per domain before the all-axes-fail guard rolls each up to
      ok=False. The guard already prevents data corruption, so this is an EFFICIENCY
      refinement (skip the wasted calls), not a correctness fix.
"""
import httpx

from app.enrichment.base import EnrichmentAdapter


class DomscanAdapter(EnrichmentAdapter):
    name = "domscan"
    BASE_URL = "https://domscan.net/v1"     # no trailing slash; paths begin with "/"
    API_KEY_ENV = "DOMSCAN_API_KEY"         # keyed; base warns if unset (correct here)
    # Not the 1000 req/min edge limit — the BINDING constraint is the 402 overspend
    # bound: up to (MAX_CONCURRENCY-1) calls in flight x <=5 credits each ~= <=45
    # credits can overspend past the moment the first 402 lands. 10 keeps that small.
    MAX_CONCURRENCY = 10
    TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0)

    # Curated ~50-TLD set for the single /status TLD-count fan-out. One call, one
    # credit (gate 1 confirms). FINAL list TBD before the run; this is a representative
    # cut of the TLDs a defended brand most often registers. Do NOT tune before the
    # billing model is known (gate 1 may change how many TLDs we can afford).
    CURATED_TLDS = (
        "com", "net", "org", "info", "biz",                      # legacy gTLD
        "io", "co", "ai", "app", "dev", "xyz", "online", "site",
        "store", "tech", "shop", "cloud", "design", "studio", "agency",  # new gTLD
        "me", "tv", "cc",                                        # repurposed cc
        "us", "uk", "ca", "de", "fr", "es", "it", "nl", "eu",    # ccTLD
        "ru", "cn", "jp", "in", "au", "br", "mx", "ch", "se",
        "no", "dk", "fi", "pl", "nz", "za", "sg", "hk", "kr",
    )

    def __init__(self, api_key=None):
        super().__init__(api_key)
        # Set once the first typed 402 lands; latches every later enrich_one to a
        # no-HTTP short-circuit. Per-instance (one batch == one adapter instance).
        self._halted = False
        self._halt_detail = "INSUFFICIENT_CREDITS"

    def _headers(self):
        # DomScan authenticates via the x-api-key header (whoxy used a query param).
        h = super()._headers()
        h["x-api-key"] = self.api_key
        return h

    async def enrich_one(self, client, domain):
        # Short-circuit: a prior domain already tripped INSUFFICIENT_CREDITS. No call,
        # no spend; ok=False so domains_missing() re-runs it cleanly after re-funding.
        if self._halted:
            return self._error(domain, "domscan: skipped (halted on INSUFFICIENT_CREDITS)")

        ascii_domain = self._punycode(domain)
        # Brand label for the TLD-count fan-out. v1 takes the first label
        # ("example.com" -> "example"); PSL-aware extraction ("example.co.uk" ->
        # "example") is a known simplification to revisit if DTI has multi-label names.
        label = ascii_domain.split(".")[0]

        data = {}

        # --- Axis 1: health -------------------------------------------------
        payload, st = await self._call(client, "/health", {"domain": ascii_domain})
        if st == "halt":
            return self._halt_result(domain)
        if st == "abort":
            return self._error(domain, "domscan: rate_limited (HTTP 429)")
        data.update(self._extract_health(payload))

        # --- Axis 2: reputation (NOT gated on health — independent) ----------
        payload, st = await self._call(client, "/reputation", {"domain": ascii_domain})
        if st == "halt":
            return self._halt_result(domain)
        if st == "abort":
            return self._error(domain, "domscan: rate_limited (HTTP 429)")
        data.update(self._extract_reputation(payload))

        # --- Axis 3: status / TLD-count (one name x ~50 TLDs, one credit) ----
        payload, st = await self._call(
            client, "/status", {"domain": ascii_domain},  # full domain; /status is per-domain RDAP
        )
        if st == "halt":
            return self._halt_result(domain)
        if st == "abort":
            return self._error(domain, "domscan: rate_limited (HTTP 429)")
        data.update(self._extract_status(payload))

        # All-axes-UNKNOWN guard: nothing measured -> this is UNKNOWN, not an all-None
        # "measurement." ok=False keeps it in domains_missing() for a clean re-run.
        if not any(v is not None for v in data.values()):
            return self._error(domain, "domscan: all signals unavailable")

        return self._result(domain, data)

    # --- single-attempt call --------------------------------------------
    async def _call(self, client, path, params):
        """One attempt against one endpoint. Transport errors are NOT caught here —
        they propagate to _guarded -> whole-domain ok=False (single-attempt; recovered
        via domains_missing()). Returns (payload|None, status):
            "ok"    -> payload is parsed JSON (HTTP 200)
            "fail"  -> this axis is UNKNOWN (auth/5xx/non-JSON) -> None fields; degrades
                       locally, and rolls up to ok=False only if ALL axes fail
            "abort" -> transient throttle (429) -> caller returns whole-domain ok=False
            "halt"  -> typed INSUFFICIENT_CREDITS (402) -> self._halted set; caller halts
        """
        resp = await client.get(self.BASE_URL + path, params=params)

        if resp.status_code == 402:
            self._note_credit_exhaustion(resp)      # latches self._halted
            return None, "halt"
        if resp.status_code == 429:
            # 429 ABORTS the whole domain (ok=False) where a 5xx DEGRADES one axis.
            # Why the asymmetry: a 429 is a key-wide throttle — the call was refused,
            # nothing was measured, adjacent axes are likely throttled too, and a
            # re-run recovers cleanly via domains_missing(). A 5xx is an
            # endpoint-specific blip while the sibling axes returned real PAID data;
            # discarding the domain would re-spend those on re-run, so we keep them
            # and let the one failed axis be None.
            return None, "abort"
        if resp.status_code >= 400:
            # auth (401/403) and 5xx: this axis is UNKNOWN, and is NOT charged. A bad
            # key makes all three axes "fail" -> the all-axes guard yields ok=False.
            return None, "fail"
        try:
            return resp.json(), "ok"
        except Exception:
            return None, "fail"                      # 200 but non-JSON body

    def _note_credit_exhaustion(self, resp):
        """Latch the halt and stash a human-readable detail. The HALT DECISION keys on
        the dedicated HTTP 402 status (unambiguous); the typed code + credit counts
        only enrich the triggering domain's error string, so body parsing is
        best-effort and never gates the halt."""
        self._halted = True
        remaining = resp.headers.get("x-credits-remaining")
        code = "INSUFFICIENT_CREDITS"
        required = None
        try:
            body = resp.json()
            code = (body.get("error") or {}).get("code") or code
            remaining = body.get("credits_remaining", remaining)
            required = body.get("credits_required")
        except Exception:
            pass
        self._halt_detail = (
            f"{code}; credits_remaining={remaining}; credits_required={required}"
        )

    def _halt_result(self, domain):
        # The domain that TRIPPED the 402. Stable "credit_exhausted" token (whoxy
        # family) so orchestration keys on one substring. Any axis data gathered before
        # the 402 is DISCARDED -> ok=False + incomplete so the re-run redoes it clean.
        return self._error(domain, f"domscan: credit_exhausted ({self._halt_detail})")

    # --- per-axis extractors (field names GUESSED — validation item #5) --
    def _extract_health(self, payload):
        if not isinstance(payload, dict):
            return {"health_score": None, "health_grade": None, "health_checks": None}
        return {
            "health_score": _as_int(payload.get("health_score")),
            "health_grade": payload.get("grade") or None,        # live field is "grade"
            "health_checks": payload.get("health_checks") or None,
        }

    def _extract_reputation(self, payload):
        keys = ("reputation_score", "reputation_grade", "risk_level",
                "reputation_factors")
        if not isinstance(payload, dict):
            return {k: None for k in keys}
        return {
            "reputation_score": _as_int(payload.get("reputation_score")),
            "reputation_grade": payload.get("grade") or None,        # live field is "grade"
            "risk_level": payload.get("risk_level") or None,
            "reputation_factors": payload.get("factors") or None,    # live field is "factors"
        }

    def _extract_status(self, payload):
        # REPURPOSED (was a TLD-count fan-out): the live /status endpoint is a per-domain
        # RDAP registration check, NOT a multi-TLD breadth count. It returns
        # {"name":..., "results":[{domain, available, status, lifecycle_phase,
        # registry_status:[...]}]} -- the fields live one level down in results[0]. We
        # surface authoritative registration status + lifecycle + lock flags (a signal
        # whoxy doesn't expose). The old TLD-count signal needs a different approach and
        # is deferred (see BUILD_BLUEPRINT note).
        if not isinstance(payload, dict):
            return {"registered": None, "lifecycle_phase": None, "registry_status": None}
        results = payload.get("results")
        row = results[0] if isinstance(results, list) and results else None
        if not isinstance(row, dict):
            return {"registered": None, "lifecycle_phase": None, "registry_status": None}
        available = row.get("available")
        # registered = NOT available; None if availability wasn't reported (never fabricate).
        registered = (not available) if isinstance(available, bool) else None
        phase = row.get("lifecycle_phase") or row.get("status") or None
        flags = row.get("registry_status")
        if not isinstance(flags, list):
            flags = None
        return {"registered": registered, "lifecycle_phase": phase,
                "registry_status": flags}


def _as_int(v):
    """int or None — never fabricate a number. bool is excluded (it subclasses int)."""
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return int(v)
    return None
