"""Wikipedia (MediaWiki) enrichment adapter.

Per-domain adapter over the English Wikipedia MediaWiki API. Yields an Authority
signal: how many Wikipedia articles cite / externally link the domain. No API key.

Endpoint:
    https://en.wikipedia.org/w/api.php
    ?action=query&list=exturlusage&euquery=<domain>&eunamespace=0
    &eulimit=500&format=json&formatversion=2&maxlag=5

Signal extracted (Authority):
    link_count   number of article-namespace pages citing the domain
    linked       bool discriminator: link_count > 0

Signal B (deferred — Phase 5 fast-follow): a Wikidata "official website" (P856)
match -> entity sitelink count would add an orthogonal, cross-lingual Authority
signal ("is this domain the official site of a notable entity, in how many
language editions?"). It needs a second service plus URL canonicalization, so it
is deferred; revisit it in Phase 5 only if Signal A is shown to undershoot during
validation.

Scope decisions (see PROJECT.md and the design proposal):
  * en.wikipedia.org ONLY. exturlusage is per-wiki; aggregating ~300 language
    wikis is ~300x cost for marginal gain. English Wikipedia is the largest,
    most authoritative, and a consistent cross-portfolio proxy.
  * Article namespace only (eunamespace=0): real article citations, not Talk /
    User / Template mentions.
  * No eutotal: list=exturlusage exposes no total-hits field, so a true count
    means walking eucontinue page by page (hundreds of requests for a heavily
    cited domain). The signal is ORDINAL and SATURATING — ">=500 citations" is
    already maximal authority — so we fetch ONE page (eulimit=500) and cap:
    a `continue` token => link_count=500, link_count_capped=True. One request
    per domain (mirrors archiveorg's capture_count_capped).
  * euquery uses the bare apex (no wildcard). MediaWiki's externallinks index is
    keyed on the REVERSED host, so an apex query prefix-matches the apex AND its
    subdomains (www., etc.) in one shot; `*.domain` would restrict to subdomains
    only. Confirmed in the 20-domain live validation.

CRITICAL for the scoring layer — two different "negatives":
    * ok=False                -> lookup FAILED (network / throttle / maxlag
                                 exhausted / API error). Knowledge ABSENT; treat
                                 as UNKNOWN, never as "not linked."
    * ok=True, linked=False   -> lookup SUCCEEDED, zero citations found. A real
                                 (negative) Authority signal. Safe to score.
    Always check `ok` before reading `data["linked"]`.

MediaWiki etiquette / quirks:
  * maxlag=5 on every request. When replication lag exceeds it, the API returns
    HTTP 503 + Retry-After — already handled politely by the inherited
    _get_with_retries (it retries 503 and honors Retry-After). Exhausted -> ok=False.
  * MediaWiki can also return HTTP 200 with an {"error": {...}} body. enrich_one
    inspects the body and splits it: a RETRYABLE_ERROR_CODES code (maxlag /
    readonly / ratelimited) is transient -> ok=False "throttled (...)", re-tried
    next run via domains_missing(); ANY OTHER code means our query is malformed
    (badvalue, invalidparammix, ...) -> ok=False "api error (...)" with the info
    field, so a grep of error rows surfaces our bug instead of silently re-spending
    the request forever. Both are ok=False (UNKNOWN to scoring); the message is
    what distinguishes "their hiccup" from "our bug."
  * User-Agent: Wikimedia's UA policy REQUIRES a descriptive, identifying agent
    with contact info and blocks generic/browser-spoofing UAs — so _headers is
    overridden (the base's Chrome UA would risk a block). The contact is the
    PRODUCT identity (Domain Options), not the git/IP identity (GEC Media Inc.):
    a Wikimedia ops person wants "what tool, who to contact," and the tool is
    Domain Options.
  * Low concurrency (MAX_CONCURRENCY=2): Wikimedia prefers requests in series
    rather than parallel; 2 + maxlag is polite without being fully serial.
"""
import httpx

from app.enrichment.base import EnrichmentAdapter

# This module owns its User-Agent copy (repo convention; cf. base.py / checker.py).
# Wikimedia policy requires an identifying agent with contact info. The contact is
# the product identity (Domain Options), independent of the git author identity.
USER_AGENT = "DomainOptions/1.0 (Domain portfolio audit; info@domainoptions.com)"


class WikipediaAdapter(EnrichmentAdapter):
    name = "wikipedia"
    BASE_URL = "https://en.wikipedia.org/w/api.php"
    API_KEY_ENV = ""                       # keyless; no warning (guarded in base)
    MAX_CONCURRENCY = 2                     # Wikimedia prefers low parallelism
    # TIMEOUT inherited from base (connect=5, read=20) — the JSON API is fast.

    PAGE_LIMIT = 500                        # eulimit max for anonymous requests
    MAXLAG = 5                              # seconds; polite replication-lag ceiling

    # MediaWiki error codes (in a HTTP 200 error body) that mean "transient, try
    # again" rather than "our query is malformed." A retryable code yields an
    # ok=False UNKNOWN row the next run re-attempts via domains_missing(); anything
    # else is surfaced loudly as our own bug.
    RETRYABLE_ERROR_CODES = {"maxlag", "readonly", "ratelimited"}

    def _headers(self):
        # Override the base Chrome UA: Wikimedia requires an identifying agent.
        return {"User-Agent": USER_AGENT, "Accept": "application/json"}

    async def enrich_one(self, client, domain):
        # Punycode only at the wire boundary; the stored row keeps `domain`.
        ascii_domain = self._punycode(domain)
        params = {
            "action": "query",
            "list": "exturlusage",
            "euquery": ascii_domain,        # reversed-index prefix: apex + subdomains
            "eunamespace": 0,               # article/main namespace only
            "eulimit": self.PAGE_LIMIT,
            "format": "json",
            "formatversion": 2,
            "maxlag": self.MAXLAG,
        }
        # Transport errors persisting past the last retry propagate to _guarded for
        # a clean ok=False row. We deliberately do not try/except here.
        resp = await self._get_with_retries(client, self.BASE_URL, params=params)

        # maxlag (503) and CDN 429 are retried+honored by the base helper; a still
        # -throttled response here means retries were exhausted -> UNKNOWN.
        if resp.status_code in (429, 503):
            return self._error(
                domain, f"wikipedia: rate_limited/maxlag after retries (HTTP {resp.status_code})"
            )
        if resp.status_code >= 400:
            return self._error(domain, f"wikipedia: HTTP {resp.status_code}")

        try:
            payload = resp.json()
        except Exception:
            return self._error(domain, "wikipedia: non-JSON response")

        # HTTP 200 + {"error": {...}} body: split transient throttle from our-bug.
        # Both are ok=False (UNKNOWN to scoring); the message distinguishes them.
        err = payload.get("error")
        if err:
            code = err.get("code", "unknown")
            if code in self.RETRYABLE_ERROR_CODES:
                return self._error(domain, f"wikipedia: throttled ({code})")
            info = err.get("info", "")
            return self._error(domain, f"wikipedia: api error ({code}): {info}")

        entries = (payload.get("query") or {}).get("exturlusage") or []
        capped = "continue" in payload     # a continue token => >= PAGE_LIMIT hits
        count = len(entries)               # == PAGE_LIMIT when capped
        return self._result(domain, {
            "linked": count > 0,
            "link_count": count,
            "link_count_capped": capped,
        })
