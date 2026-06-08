"""Base adapter for enrichment API clients.

Every per-domain enrichment source subclasses EnrichmentAdapter. The current v1
stack of per-domain adapters is: estibot, whoxy, domscan (core, confirmed
access); archiveorg, wikipedia, seokicks (additional). Dataset sources
(majestic_million, curlie) use the separate DatasetAdapter base. NameBio is a
later sequential-only addition; Ahrefs is parked pending the cheaper authority
signals. See PROJECT.md for the full stack and rationale.

The orchestration here mirrors checker.py.run_checks(): a shared
httpx.AsyncClient, an asyncio.Semaphore to cap in-flight requests,
asyncio.as_completed to collect results, and an on_progress callback wrapped so
a callback error never kills a run.

Subclasses implement exactly one method, enrich_one(). They do not reimplement
enrich_many(). See PROJECT.md for the adapter contract and result-dict shape.

Secrets come from environment variables (never hardcoded). Each subclass names
its env var via API_KEY_ENV.
"""
import asyncio
import os
import random
from datetime import datetime

import httpx

# A real-browser UA. Some providers / WAFs reject the default httpx UA. Mirrors
# the rationale in checker.py.
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


class EnrichmentAdapter:
    """Base class for an enrichment source.

    Subclass attributes to set:
        name              short identifier, used as the DB `adapter` key
        BASE_URL          API base, no trailing slash
        API_KEY_ENV       env var holding the key, e.g. "DOMSCAN_API_KEY"
        MAX_CONCURRENCY   per-adapter in-flight cap (ignored when SEQUENTIAL)
        TIMEOUT           httpx.Timeout
        SEQUENTIAL        True => one request at a time + rate throttle
        RATE_LIMIT_PER_MIN  required when SEQUENTIAL (e.g. NameBio = 30)

    Subclass methods to implement:
        _headers(self) -> dict      auth + content headers
        async enrich_one(self, client, domain) -> dict
    """

    name = "base"
    BASE_URL = ""
    API_KEY_ENV = ""
    MAX_CONCURRENCY = 10
    TIMEOUT = httpx.Timeout(connect=5.0, read=20.0, write=5.0, pool=5.0)
    SEQUENTIAL = False
    RATE_LIMIT_PER_MIN = None

    def __init__(self, api_key=None):
        self.api_key = api_key or os.environ.get(self.API_KEY_ENV, "")
        # One-shot guard so _punycode logs its first encoding failure but not one
        # line per malformed domain across a large portfolio (cf. keyless warning).
        self._punycode_warned = False
        if self.API_KEY_ENV and not self.api_key:
            # Not fatal at construction — let the run surface auth errors per
            # domain so a missing key produces clean ok=False rows rather than a
            # crash. But make the misconfiguration obvious in logs. Guarded on
            # API_KEY_ENV so keyless adapters (archiveorg, wikipedia, dataset
            # sources) don't emit a spurious warning for a key they never need.
            print(f"[{self.name}] warning: no API key (set {self.API_KEY_ENV})")

    # --- subclass hooks -------------------------------------------------

    def _headers(self):
        """Return auth + content headers. Override per provider."""
        return {"User-Agent": USER_AGENT, "Accept": "application/json"}

    async def enrich_one(self, client, domain):
        """Enrich a single domain. Must return a flat result dict, never raise.

        Subclasses override this. Use self._result(...) / self._error(...) to
        build the return value so the shape stays consistent.
        """
        raise NotImplementedError

    # --- result helpers -------------------------------------------------

    def _result(self, domain, data):
        """Build a success result dict."""
        return {
            "domain": domain,
            "adapter": self.name,
            "ok": True,
            "error": None,
            "fetched_at": datetime.utcnow().isoformat(),
            "data": data or {},
        }

    def _error(self, domain, message, refresh_error=None):
        """Build a failure result dict.

        refresh_error: when set, the tag is added to the returned dict so the
        persistence layer refuses to persist the row. Omitted entirely when falsy,
        so a normal per-domain error row keeps its exact shape. EnrichmentAdapter
        never sets it — per-domain adapters have no refresh step — but the parameter
        exists so both adapter bases share one _error() signature (see dataset.py).
        """
        result = {
            "domain": domain,
            "adapter": self.name,
            "ok": False,
            "error": str(message)[:500],
            "fetched_at": datetime.utcnow().isoformat(),
            "data": {},
        }
        if refresh_error:
            result["refresh_error"] = refresh_error
        return result

    # --- request helpers (shared by all per-domain adapters) ------------

    def _punycode(self, domain):
        """Return the IDNA/punycode ASCII form of a normalized domain for use in
        external API calls.

        db._normalize() lowercases and strips scheme/www/path but does NOT
        punycode-encode, so a Unicode IDN (café.com, Cyrillic/CJK names) reaches
        adapters in Unicode form — which the Wayback CDX index and most provider
        APIs key by xn-- ASCII. This converts it; pure-ASCII domains pass through
        unchanged.

        Best-effort by design: on ANY encoding failure (malformed IDN, codec edge
        case, over-long label) it returns the input UNCHANGED and lets the
        downstream API decide whether to accept or reject it — the adapter's job is
        to try, not to refuse. It never raises. The first failure per adapter
        instance is logged for later audit; subsequent failures are suppressed to
        avoid one log line per bad domain across a large portfolio (same posture as
        the keyless-key warning). The blanket except mirrors _guarded's
        catch-everything-at-the-boundary stance in this same module.
        """
        try:
            return domain.encode("idna").decode("ascii")
        except Exception as e:
            if not self._punycode_warned:
                self._punycode_warned = True
                print(f"[{self.name}] warning: IDNA encoding failed for {domain!r} "
                      f"({type(e).__name__}); passing through unchanged "
                      f"(further such failures suppressed)")
            return domain

    async def _get_with_retries(self, client, url, *, params=None, headers=None,
                                max_retries=3, base_delay=1.0,
                                retry_statuses=(429, 503)):
        """GET with bounded exponential-backoff-with-jitter retries.

        Shared by per-domain adapters because the free sources (Wayback, Wikipedia,
        ...) throttle under load with no documented limit. Retries on the given HTTP
        status codes (default 429/503) and on transient transport errors (timeouts,
        connection resets). Honors an integer-seconds Retry-After header when present
        (capped at RETRY_AFTER_CAP so a hostile or absurd value can't stall a run).

        After max_retries is exhausted it RETURNS the final Response rather than
        raising on a still-throttled status — so the caller maps it to a typed
        ok=False reason (e.g. "rate_limited after N retries"). A transport error on
        the final attempt propagates and is caught by _guarded().

        Jitter is load-bearing, not cosmetic: without it, the N concurrent tasks
        that all hit a 429 at the same instant back off to the SAME future moment
        and thundering-herd the server again. The additive random term decorrelates
        their wake-ups:
            delay = base_delay * 2**attempt + random.uniform(0, base_delay)
        """
        last_resp = None
        for attempt in range(max_retries + 1):
            # 1 initial attempt + max_retries additional retries = max_retries + 1 total
            try:
                last_resp = await client.get(url, params=params, headers=headers)
            except (httpx.TimeoutException, httpx.TransportError):
                if attempt == max_retries:
                    raise                          # exhausted -> _guarded handles
                await asyncio.sleep(self._backoff(attempt, base_delay))
                continue
            if last_resp.status_code in retry_statuses and attempt < max_retries:
                await asyncio.sleep(
                    self._retry_after(last_resp) or self._backoff(attempt, base_delay)
                )
                continue
            return last_resp
        return last_resp

    # Cap prevents await asyncio.sleep on a pathological Retry-After (e.g. 86400)
    # from stalling a whole run.
    RETRY_AFTER_CAP = 30.0

    @staticmethod
    def _backoff(attempt, base_delay):
        """Exponential backoff with additive jitter (see _get_with_retries)."""
        return base_delay * (2 ** attempt) + random.uniform(0, base_delay)

    def _retry_after(self, resp):
        """Parse an integer-seconds Retry-After header, capped at RETRY_AFTER_CAP.
        None if absent or not a plain integer (HTTP-date form is ignored — Wayback
        uses seconds)."""
        raw = resp.headers.get("retry-after")
        if not raw:
            return None
        try:
            return min(float(int(raw)), self.RETRY_AFTER_CAP)
        except ValueError:
            return None

    # --- orchestration (do not override) --------------------------------

    async def enrich_many(self, domains, on_progress=None):
        """Enrich a list of domains. Returns list of result dicts.

        Concurrent by default (Semaphore-capped). If SEQUENTIAL is set, runs one
        request at a time and throttles to RATE_LIMIT_PER_MIN. Mirrors
        checker.py.run_checks() for the concurrent path.
        """
        domains = [d for d in domains if d]
        total = len(domains)
        done = 0
        results = []

        limits = httpx.Limits(
            max_connections=self.MAX_CONCURRENCY * 2,
            max_keepalive_connections=self.MAX_CONCURRENCY,
        )

        async with httpx.AsyncClient(
            headers=self._headers(), limits=limits, timeout=self.TIMEOUT,
            http2=False, follow_redirects=True,
        ) as client:

            if self.SEQUENTIAL:
                # Single-flight + throttle. For providers that forbid
                # multi-threading and cap requests/minute (e.g. NameBio).
                delay = 60.0 / self.RATE_LIMIT_PER_MIN if self.RATE_LIMIT_PER_MIN else 0
                for domain in domains:
                    r = await self._guarded(client, domain)
                    results.append(r)
                    done += 1
                    _fire(on_progress, done, total)
                    if delay:
                        await asyncio.sleep(delay)
                return results

            # Concurrent path.
            sem = asyncio.Semaphore(self.MAX_CONCURRENCY)

            async def _one(domain):
                nonlocal done
                async with sem:
                    r = await self._guarded(client, domain)
                    done += 1
                    _fire(on_progress, done, total)
                    return r

            tasks = [asyncio.create_task(_one(d)) for d in domains]
            for coro in asyncio.as_completed(tasks):
                results.append(await coro)

        return results

    async def _guarded(self, client, domain):
        """Call enrich_one but guarantee a result dict even if it raises."""
        try:
            r = await self.enrich_one(client, domain)
            # Defensive: ensure subclasses honored the contract.
            if not isinstance(r, dict) or "ok" not in r:
                return self._error(domain, "adapter returned malformed result")
            return r
        except httpx.TimeoutException:
            return self._error(domain, "timeout")
        except httpx.HTTPError as e:
            return self._error(domain, f"http error: {e}")
        except Exception as e:
            return self._error(domain, f"{type(e).__name__}: {e}")


def _fire(on_progress, done, total):
    """Call the progress callback, swallowing any error it raises."""
    if on_progress:
        try:
            on_progress(done, total)
        except Exception:
            pass
