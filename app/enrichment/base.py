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
        if not self.api_key:
            # Not fatal at construction — let the run surface auth errors per
            # domain so a missing key produces clean ok=False rows rather than a
            # crash. But make the misconfiguration obvious in logs.
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

    def _error(self, domain, message):
        """Build a failure result dict."""
        return {
            "domain": domain,
            "adapter": self.name,
            "ok": False,
            "error": str(message)[:500],
            "fetched_at": datetime.utcnow().isoformat(),
            "data": {},
        }

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
