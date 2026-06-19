"""Ahrefs pre-flight unit-budget check.

Ahrefs API v3 is unit-metered from a depletable monthly pool, and the AhrefsAdapter
spends ~100 units/domain (two 50-unit calls: domain-rating + backlinks-stats; measured
2026-06-18 from x-api-units-cost-total headers). A large batch can therefore exhaust the
monthly allowance mid-run -- the adapter halts cleanly when that happens, but a halt 600
domains into an 800-domain run means a wasted partial run and a re-run later. This module
answers the cheaper question FIRST: "does this batch fit in the units I have left?"

It calls the FREE limits-and-usage endpoint (0 units), computes the batch cost, and
returns a typed verdict the caller can print and/or gate on. Two entry points:
    estimate(...)          pure arithmetic, no network -- testable, reused by preflight()
    async preflight(...)   fetches live remaining units, returns a PreflightResult

The verdict is ADVISORY by design: it never blocks or spends; the caller decides whether
to proceed, prompt, or abort. (A CLI helper at the bottom prints a human summary and
returns an exit code, for use before a manual run.)

Worst-case budgeting (deliberately conservative -- never under-warn):
  - 100 units/domain assumes BOTH calls are cache misses. Cache hits cost 0 (x-api-cache
    header), so real spend is <= estimate; we budget the ceiling so the check never says
    "fits" when it doesn't.
  - "remaining" uses the WORKSPACE pool (units_limit_workspace - units_usage_workspace),
    the binding constraint. If a per-API-key cap is set (units_limit_api_key non-null),
    the tighter of the two governs -- a key cap can bite before the workspace pool does.

Field names verified against a live limits-and-usage response 2026-06-19:
    limits_and_usage.units_limit_workspace      int    (e.g. 400000)
    limits_and_usage.units_usage_workspace      int    (e.g. 50)
    limits_and_usage.units_limit_api_key        int|null (null = no per-key cap)
    limits_and_usage.units_usage_api_key        int
    limits_and_usage.usage_reset_date           ISO8601 (when the pool refills)
    limits_and_usage.subscription               str
"""
import httpx

# Measured per-domain cost: two 50-unit calls (domain-rating + backlinks-stats), each
# floored at the 50-unit base. Confirmed from x-api-units-cost-total response headers.
# If the adapter ever adds/removes an endpoint, update this in lockstep.
UNITS_PER_DOMAIN = 100

LIMITS_URL = "https://api.ahrefs.com/v3/subscription-info/limits-and-usage"
TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0)


class PreflightResult:
    """The advisory verdict. `fits` is the go/no-go; everything else supports a message
    or a caller's own decision. `remaining` is None when the live fetch failed (in which
    case fits defaults to True with fetch_error set -- a usage-endpoint outage must not
    block a run; the adapter's own in-run halt remains the real backstop)."""

    def __init__(self, *, domains, cost, remaining, limit, used, reset_date,
                 cap_source, fits, wall_at=None, fetch_error=None):
        self.domains = domains
        self.cost = cost
        self.remaining = remaining
        self.limit = limit
        self.used = used
        self.reset_date = reset_date
        self.cap_source = cap_source          # "workspace" | "api_key" | None
        self.fits = fits
        self.wall_at = wall_at                # est. domain index where units run out, or None
        self.fetch_error = fetch_error

    def message(self):
        """A human-readable one-or-two-line summary for terminal output."""
        if self.fetch_error:
            return (f"Ahrefs pre-flight: could not read unit balance ({self.fetch_error}). "
                    f"Estimated cost for {self.domains} domains is {self.cost:,} units. "
                    f"Proceeding without a balance check; the adapter will still halt if "
                    f"units run out mid-run.")
        if self.fits:
            return (f"Ahrefs pre-flight OK: {self.domains} domains need ~{self.cost:,} units; "
                    f"{self.remaining:,} available ({self.cap_source} pool, resets "
                    f"{self.reset_date}).")
        return (f"Ahrefs pre-flight WARNING: {self.domains} domains need ~{self.cost:,} units, "
                f"but only {self.remaining:,} remain ({self.cap_source} pool, resets "
                f"{self.reset_date}). The run will hit the wall around domain "
                f"~{self.wall_at:,} of {self.domains}. Reduce the batch, wait for reset, "
                f"or proceed knowing it will halt partway.")


def estimate(domains, remaining, *, limit=None, used=None, reset_date=None,
             cap_source="workspace", units_per_domain=UNITS_PER_DOMAIN):
    """Pure, network-free budget arithmetic. `domains` is a count (int) or a sized
    iterable. Returns a PreflightResult. wall_at is how many domains fit in `remaining`
    when the batch does NOT fit (so the caller can say 'halts around domain N')."""
    n = domains if isinstance(domains, int) else len(list(domains))
    cost = n * units_per_domain
    fits = cost <= remaining
    wall_at = None if fits else max(0, remaining // units_per_domain)
    return PreflightResult(
        domains=n, cost=cost, remaining=remaining, limit=limit, used=used,
        reset_date=reset_date, cap_source=cap_source, fits=fits, wall_at=wall_at,
    )


def _parse_limits(payload):
    """Extract (remaining, limit, used, reset_date, cap_source) from a limits-and-usage
    body. Uses the WORKSPACE pool, unless a tighter per-API-key cap is set. Raises
    ValueError if the body is missing the expected block, so the caller surfaces a clean
    fetch_error rather than a KeyError."""
    block = (payload or {}).get("limits_and_usage")
    if not isinstance(block, dict):
        raise ValueError("missing limits_and_usage block")

    ws_limit = block.get("units_limit_workspace")
    ws_used = block.get("units_usage_workspace") or 0
    reset_date = block.get("usage_reset_date")

    if not isinstance(ws_limit, int):
        raise ValueError("missing/invalid units_limit_workspace")
    ws_remaining = max(0, ws_limit - ws_used)

    # If a per-API-key cap is set, it can bite before the workspace pool. Take the tighter.
    key_limit = block.get("units_limit_api_key")
    key_used = block.get("units_usage_api_key") or 0
    if isinstance(key_limit, int):
        key_remaining = max(0, key_limit - key_used)
        if key_remaining < ws_remaining:
            return key_remaining, key_limit, key_used, reset_date, "api_key"
    return ws_remaining, ws_limit, ws_used, reset_date, "workspace"


async def preflight(domains, api_key, *, client=None, units_per_domain=UNITS_PER_DOMAIN):
    """Fetch live remaining units and return a PreflightResult for `domains`.

    Advisory and fail-open: if the free limits endpoint is unreachable or malformed, the
    result has fits=True + fetch_error set, because a usage-endpoint outage must not block
    a run -- the AhrefsAdapter's in-run credit halt is the real backstop. `client` lets a
    caller/tests inject an httpx.AsyncClient (or a fake); otherwise one is created."""
    n = domains if isinstance(domains, int) else len(list(domains))
    headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}

    async def _do(c):
        resp = await c.get(LIMITS_URL, headers=headers)
        if resp.status_code >= 400:
            raise ValueError(f"HTTP {resp.status_code}")
        return _parse_limits(resp.json())

    try:
        if client is not None:
            remaining, limit, used, reset_date, cap_source = await _do(client)
        else:
            async with httpx.AsyncClient(timeout=TIMEOUT) as c:
                remaining, limit, used, reset_date, cap_source = await _do(c)
    except Exception as e:
        # Fail-open: advise proceeding, but say we couldn't check.
        return PreflightResult(
            domains=n, cost=n * units_per_domain, remaining=None, limit=None,
            used=None, reset_date=None, cap_source=None, fits=True,
            fetch_error=f"{type(e).__name__}: {e}",
        )

    return estimate(n, remaining, limit=limit, used=used, reset_date=reset_date,
                    cap_source=cap_source, units_per_domain=units_per_domain)
