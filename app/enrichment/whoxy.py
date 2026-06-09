"""WhoXY (WHOIS) enrichment adapter.

Per-domain adapter over the WhoXY API. Yields Provenance signals from WHOIS:
the headline is "year first registered," plus current registrar, registrant, and
registration status. Requires WHOXY_API_KEY (key passed as a query param).

Active endpoints (base https://api.whoxy.com/, key in `key=`):
  * Live WHOIS    ?key=…&whois=<domain>    1 credit  — current parsed WHOIS
  * WHOIS History ?key=…&history=<domain>  ~2.5x     — all historical records

Reverse WHOIS is an available WhoXY endpoint not used by this adapter. It's a
one-to-many lookup (search by registrant name, email, company, or domain-name
keyword; returns matching domains paginated 100/page) — architecturally different
from the per-domain enrichment pattern this adapter implements. Potential future
use cases (registrant portfolio mapping, thematic portfolio discovery,
acquisition-target identification) would require a separate adapter or
post-enrichment step, not extension of this one.

Signals extracted -> dimension:
    first_registered          earliest create_date across History  -> Provenance (headline)
    first_registered_source   "history" | "live_fallback" | None   -> signal-quality marker
    create_date               current registration (Live)          -> Provenance
    registered                domain_registered == "yes" (Live)    -> Usage / discriminator
    registrar, registrant_*   Live                                 -> Provenance / context
    domain_status             Live EPP codes                       -> Usage / defensive (locks)
    expiry_date               Live                                 -> feeds cost-gap overlay
    history_record_count      History total_results                -> churn proxy

Live primary, History gated (cost discipline):
  History is called ONLY when the Live lookup succeeds AND the domain is currently
  registered. A failed Live lookup spends no History call; an unregistered domain
  has no useful history. When History succeeds, first_registered comes from the
  earliest historical create_date (first_registered_source="history" — the
  authoritative answer, which catches drop-and-re-register that Live's current
  create_date misses). When History is skipped (failure) on a registered domain,
  first_registered falls back to Live's create_date (source="live_fallback"). For
  an UNREGISTERED domain, History is not queried and both first_registered and
  first_registered_source stay None — no source of truth applies. These are
  qualitatively DIFFERENT signals — "registered in 1999 per history" vs "current
  registration dates to 1999" — so the source flag is preserved, never collapsed.

History pagination: this adapter queries only the first page. Most domains have
<10 history records, so this is sufficient. For domains with extensive history
(frequently-transferred old domains), the earliest record may live on a later page
and be missed; first_registered would then be later than the true earliest.
Validation will reveal if any DTI domains hit this case; pagination is a Phase 2.5
fast-follow if so.

Why this adapter does NOT use the base _get_with_retries helper:
  Free sources (archiveorg, wikipedia) tolerate retry-with-backoff because each
  retry costs only wall-clock time. Paid sources have a different risk profile: a
  call that succeeded server-side (charged) but whose response was read wrong
  would, on retry, charge again. Single-attempt eliminates that risk. Transient
  failures are recovered via run-level resumability (domains_missing()), not
  in-request retries.

Error model (note: WhoXY returns HTTP 200 even on logical failure, so we branch on
the `status` field, NOT the HTTP status code — except the discriminated HTTP cases
below, which are genuine transport/server-level failures):
  * ok=False                 -> UNKNOWN. Live failed (HTTP 401/403 auth, HTTP 402
                                or status:0 credits-exhausted, other HTTP >=400,
                                transport error, non-JSON, or status 0). Treat as
                                "we don't know," never as "not registered."
  * ok=True, registered=False -> KNOWN-ABSENT. Lookup succeeded, domain is not
                                currently registered (available / expired). A real
                                Usage signal.
  * ok=True, registered=True  -> full data.
  Always check `ok` before reading `data["registered"]`.

Credit exhaustion: signaled by WhoXY two possible ways, both mapped to a typed
ok=False whose reason CONTAINS "credit_exhausted" — (a) HTTP 402, (b) status:0 with
an insufficient-balance reason string. The adapter does NOT halt — a single
transient misread must not abort a batch. A consecutive-credit_exhausted CIRCUIT
BREAKER belongs in the orchestration layer; that's a Phase 2.5 follow-up. The typed
reason string is the hook orchestration keys on.

Cache discipline: WHOIS is stable (create_date / first_registered never change;
registrar/registrant rarely). The adapter does not cache; orchestration filters
the candidate list through domains_missing("whoxy", domains) so a re-run re-spends
NOTHING on already-enriched domains. For a paid adapter that is the spend guard.

Tracked follow-up (Phase 2.5, separate commits, both deferred for clean reasons):
  1. Credit-exhaustion CIRCUIT BREAKER in orchestration — halt this adapter after
     N consecutive credit_exhausted results (3–5), log "WhoXY credit pool exhausted
     — check balance and re-fund." Ships once we see it's needed.
  2. History PAGINATION — fetch later pages so first_registered is correct for
     domains with extensive history. Ships if validation shows DTI domains hit the
     first-page limit.
"""
import httpx

from app.enrichment.base import EnrichmentAdapter


class WhoxyAdapter(EnrichmentAdapter):
    name = "whoxy"
    BASE_URL = "https://api.whoxy.com/"
    API_KEY_ENV = "WHOXY_API_KEY"          # keyed; base warns if unset (correct here)
    MAX_CONCURRENCY = 20                    # PROJECT.md; only affects speed, not spend
    TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0)

    # Substrings that mark an insufficient-balance status:0 reason, matched
    # case-insensitively. EXACT live string to confirm in validation (item #1).
    CREDIT_EXHAUSTED_MARKERS = ("credit", "balance", "insufficient")

    async def enrich_one(self, client, domain):
        ascii_domain = self._punycode(domain)

        # --- Live WHOIS (primary). Single attempt: a transport error propagates to
        #     _guarded (no retry — see module docstring). ---
        live_resp = await client.get(self.BASE_URL, params={
            "key": self.api_key, "whois": ascii_domain, "format": "json",
        })
        # Discriminate HTTP-level failures (no retry, no spend on a failed call).
        # 402 is surfaced as credit_exhausted because WhoXY may signal an empty
        # balance via HTTP 402 in addition to (or instead of) status:0 + reason.
        if live_resp.status_code in (401, 403):
            return self._error(domain, f"whoxy: auth error HTTP {live_resp.status_code}")
        if live_resp.status_code == 402:
            return self._error(domain, "whoxy: credit_exhausted (HTTP 402)")
        if live_resp.status_code >= 400:
            return self._error(domain, f"whoxy: live HTTP {live_resp.status_code}")
        try:
            live = live_resp.json()
        except Exception:
            return self._error(domain, "whoxy: live non-JSON response")

        # WhoXY returns 200 even on logical failure -> branch on the status field.
        if str(live.get("status")) != "1":
            reason = live.get("status_reason") or live.get("error") or "unknown"
            if self._is_credit_exhausted(reason):
                return self._error(domain, "whoxy: credit_exhausted")
            return self._error(domain, f"whoxy: live status 0 ({reason})")

        registered = str(live.get("domain_registered", "")).lower() == "yes"
        create_date = live.get("create_date") or None
        registrar = (live.get("domain_registrar") or {}).get("registrar_name") or None
        registrant = live.get("registrant_contact") or {}

        data = {
            "registered": registered,
            "create_date": create_date,
            "expiry_date": live.get("expiry_date") or None,
            "registrar": registrar,
            "registrant_name": registrant.get("full_name") or None,
            "registrant_org": registrant.get("company_name") or None,
            "domain_status": live.get("domain_status") or None,
            "first_registered": None,
            "first_registered_source": None,
            "history_record_count": 0,
        }

        # --- WHOIS History (gated: only when registered). Failures degrade locally;
        #     a History hiccup must never discard the paid Live result. An
        #     unregistered domain keeps first_registered/source = None. ---
        if registered:
            earliest, count = await self._fetch_history(client, ascii_domain)
            if earliest is not None:
                data["first_registered"] = earliest
                data["first_registered_source"] = "history"
                data["history_record_count"] = count
            else:
                data["first_registered"] = create_date
                data["first_registered_source"] = "live_fallback"

        return self._result(domain, data)

    async def _fetch_history(self, client, ascii_domain):
        """Return (earliest_create_date | None, record_count). Single attempt; ANY
        failure returns (None, 0) so the caller falls back to the live create_date.
        Never raises — a History failure must not discard the paid Live result.

        v1 queries only the first page (see "History pagination" in the module
        docstring). `total_results` is a GUESSED field name (confirm in validation
        item #1); on its absence we fall back to len(records), which can understate
        a paginated history."""
        try:
            resp = await client.get(self.BASE_URL, params={
                "key": self.api_key, "history": ascii_domain, "format": "json",
            })
            if resp.status_code >= 400:
                return None, 0
            payload = resp.json()
        except Exception:
            return None, 0
        if str(payload.get("status")) != "1":
            return None, 0
        records = payload.get("whois_records") or []
        # create_date is ISO YYYY-MM-DD (confirm in validation), so min() over the
        # strings yields the earliest registration lexicographically.
        dates = [r.get("create_date") for r in records if r.get("create_date")]
        if not dates:
            return None, 0
        count = payload.get("total_results")
        if not isinstance(count, int):
            count = len(records)
        return min(dates), count

    def _is_credit_exhausted(self, reason):
        r = str(reason).lower()
        return any(m in r for m in self.CREDIT_EXHAUSTED_MARKERS)
