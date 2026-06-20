"""HumbleWorth domain valuation adapter (via Replicate).

A second, INDEPENDENT value signal to triangulate against Estibot. Estibot runs
known-HIGH; HumbleWorth runs known-CONSERVATIVE (low). Bracketing a domain's true value
between a high and a low estimator is the point — the scorer blends them (geometric mean
of Estibot estimate and HumbleWorth marketplace) so neither source's bias dominates.

Accessed through Replicate (the model humbleworth/price-predict-v1), because that's where
Greg has an account/token. NOTE: the direct HumbleWorth API (valuation.humbleworth.com)
is free and unauthenticated; Replicate is metered (~$0.0001/run) but trivial at portfolio
scale (~6 cents for 572 domains). If cost or the Replicate dependency ever matters, this
adapter can be repointed at the direct endpoint — same {auction,marketplace,brokerage}
output shape.

Replicate is normally ASYNC (POST a prediction -> poll until succeeded). We avoid the
poll loop with the `Prefer: wait` header (synchronous mode): Replicate holds the
connection open until the ~1s prediction completes and returns the finished object
directly, so this stays a one-call-per-domain adapter matching the EnrichmentAdapter
contract. A poll fallback handles the rare case where `wait` returns still-processing.

Endpoint:  POST https://api.replicate.com/v1/predictions
Auth:      Authorization: Bearer <REPLICATE_API_TOKEN>
Body:      {"version": MODEL_VERSION, "input": {"domains": "<one-domain>"}}
Headers:   Prefer: wait   (synchronous)
Output (prediction.output): {"valuations": [{domain, auction, marketplace, brokerage,
                             error}]}  -- one element since we send one domain.

Fields extracted (all USD, or None):
    auction       <- valuations[0].auction        (50th pct; conservative floor)
    marketplace   <- valuations[0].marketplace     (97.5th pct; THE headline number,
                                                     the one analysts compare to Estibot)
    brokerage     <- valuations[0].brokerage        (99.25th pct; optimistic ceiling)
  marketplace is the value the scorer pairs with Estibot. auction/brokerage give a range.

Sentinels / discipline:
  * a per-domain valuations[].error (non-null) -> that domain ok=False (the model couldn't
    value it), never a fabricated 0.
  * a value <= 0 or missing -> None (UNKNOWN), not 0. A real model output of 0 is treated
    as no-signal (the model doesn't emit a meaningful $0 valuation).
  * prediction.status "failed"/"canceled" -> ok=False.
  * HTTP 401/403 -> ok=False "auth_failed" + HALT (bad token fails identically every call).
  * HTTP 429 -> bounded 429-only retry, then ok=False "rate_limited".
  * still "processing" after `Prefer: wait` -> bounded poll on the prediction URL, then
    give up -> ok=False (recovered on re-run via domains_missing()).
"""
import asyncio

import httpx

from app.enrichment.base import EnrichmentAdapter

# Pinned model version (humbleworth/price-predict-v1). Replicate accepts a version hash;
# this is the published version from the model's API docs. If Replicate returns a
# "version not found" 422, update this from replicate.com/humbleworth/price-predict-v1/versions.
MODEL_VERSION = "a925db842c707850e4ca7b7e86b217692b0353a9ca05eb028802c4a85db93843"


class HumbleworthAdapter(EnrichmentAdapter):
    name = "humbleworth"
    BASE_URL = "https://api.replicate.com/v1/predictions"
    API_KEY_ENV = "REPLICATE_API_TOKEN"
    MAX_CONCURRENCY = 4                       # politeness; Replicate is metered
    TIMEOUT = httpx.Timeout(connect=5.0, read=60.0, write=5.0, pool=5.0)  # sync wait can hold

    MAX_429_RETRIES = 3
    RETRY_BASE_DELAY = 1.0
    MAX_POLLS = 10                            # fallback if Prefer:wait returns unfinished
    POLL_DELAY = 1.0

    def __init__(self, api_key=None):
        super().__init__(api_key)
        self._halted = False

    def _headers(self):
        h = super()._headers()
        h["Authorization"] = f"Bearer {self.api_key}"
        h["Prefer"] = "wait"                  # synchronous: return the finished prediction
        h["Content-Type"] = "application/json"
        return h

    async def enrich_one(self, client, domain):
        if self._halted:
            return self._error(domain, "humbleworth: skipped (halted on auth_failed)")

        ascii_domain = self._punycode(domain)
        body = {"version": MODEL_VERSION, "input": {"domains": ascii_domain}}

        resp = await self._submit(client, body)
        if resp.status_code in (401, 403):
            self._halted = True
            return self._error(domain, f"humbleworth: auth_failed (HTTP {resp.status_code})")
        if resp.status_code == 429:
            return self._error(domain, "humbleworth: rate_limited (HTTP 429 after retries)")
        if resp.status_code == 422:
            return self._error(domain, "humbleworth: invalid version/input (HTTP 422) — "
                                       "check MODEL_VERSION")
        if resp.status_code >= 400:
            return self._error(domain, f"humbleworth: HTTP {resp.status_code}")

        try:
            pred = resp.json()
        except Exception:
            return self._error(domain, "humbleworth: non-JSON response")

        # With Prefer:wait the prediction is usually terminal already; if not, poll.
        pred = await self._await_terminal(client, pred)
        if pred is None:
            return self._error(domain, "humbleworth: prediction did not complete (timeout)")

        status = pred.get("status")
        if status in ("failed", "canceled"):
            return self._error(domain, f"humbleworth: prediction {status}: {pred.get('error')}")
        if status != "succeeded":
            return self._error(domain, f"humbleworth: prediction not ready ({status})")

        out = pred.get("output")
        vals = out.get("valuations") if isinstance(out, dict) else None
        if not isinstance(vals, list) or not vals:
            return self._error(domain, "humbleworth: empty valuations")
        item = vals[0]
        if not isinstance(item, dict):
            return self._error(domain, "humbleworth: malformed valuation item")
        if item.get("error"):
            return self._error(domain, f"humbleworth: per-domain error: {item.get('error')}")

        data = {
            "auction": _usd(item.get("auction")),
            "marketplace": _usd(item.get("marketplace")),
            "brokerage": _usd(item.get("brokerage")),
        }
        if data["auction"] is None and data["marketplace"] is None and data["brokerage"] is None:
            return self._error(domain, "humbleworth: no usable values")
        return self._result(domain, data)

    async def _submit(self, client, body):
        """POST the prediction with a bounded 429-only retry. Transport errors propagate to
        _guarded. Returns the final Response."""
        resp = None
        for attempt in range(self.MAX_429_RETRIES + 1):
            resp = await client.post(self.BASE_URL, json=body)
            if resp.status_code == 429 and attempt < self.MAX_429_RETRIES:
                await asyncio.sleep(
                    self._retry_after(resp) or self._backoff(attempt, self.RETRY_BASE_DELAY))
                continue
            return resp
        return resp

    async def _await_terminal(self, client, pred):
        """If Prefer:wait already returned a terminal prediction, pass it through. Otherwise
        poll prediction['urls']['get'] up to MAX_POLLS. Returns the (possibly updated)
        prediction dict, or None on timeout."""
        terminal = ("succeeded", "failed", "canceled")
        if not isinstance(pred, dict):
            return None
        if pred.get("status") in terminal:
            return pred
        get_url = (pred.get("urls") or {}).get("get")
        if not get_url:
            return pred                       # nothing to poll; caller handles non-terminal
        for _ in range(self.MAX_POLLS):
            await asyncio.sleep(self.POLL_DELAY)
            r = await client.get(get_url)
            if r.status_code >= 400:
                return pred
            try:
                pred = r.json()
            except Exception:
                return pred
            if pred.get("status") in terminal:
                return pred
        return None                           # exhausted polls


def _usd(v):
    """A USD valuation -> positive float, or None. <=0 / missing / non-numeric -> None
    (the model does not emit a meaningful $0; treat 0 as no-signal, never a measured zero)."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f > 0 else None
