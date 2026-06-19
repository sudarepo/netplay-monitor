"""Tests for the Ahrefs pre-flight unit-budget check (app/enrichment/preflight_ahrefs.py).

No network: preflight() accepts an injected client, so a fake serving the canned
limits-and-usage body (verified live 2026-06-19) exercises the fetch+parse+verdict path.
estimate() is pure arithmetic and tested directly.

Coverage: a batch that fits; a batch that overflows (+ correct wall_at); the workspace
pool used by default; a tighter per-API-key cap taking precedence; null api_key cap
ignored; fetch failure fails OPEN (fits=True + fetch_error); malformed body -> fetch
error; exact-boundary fit; message() strings for the three states.
"""
import asyncio

from app.enrichment.preflight_ahrefs import (
    estimate, preflight, PreflightResult, UNITS_PER_DOMAIN)


class _FakeResponse:
    def __init__(self, status_code, json_obj=None):
        self.status_code = status_code
        self._json = json_obj

    def json(self):
        if self._json is None:
            raise ValueError("no json body")
        return self._json


class _FakeClient:
    def __init__(self, item):
        self._item = item
        self.calls = 0

    async def get(self, url, headers=None):
        self.calls += 1
        if isinstance(self._item, Exception):
            raise self._item
        return self._item


def _run(coro):
    return asyncio.run(coro)


def _limits_body(ws_limit=400000, ws_used=50, key_limit=None, key_used=50,
                 reset="2026-07-10T00:00:00Z"):
    return {"limits_and_usage": {
        "subscription": "Standard, billed monthly",
        "usage_reset_date": reset,
        "units_limit_workspace": ws_limit,
        "units_usage_workspace": ws_used,
        "units_limit_api_key": key_limit,
        "units_usage_api_key": key_used,
        "api_key_expiration_date": "2027-06-19T13:27:39Z",
    }}


# --- estimate() pure arithmetic ----------------------------------------

def test_estimate_fits():
    r = estimate(800, remaining=399950)        # the real current balance
    assert r.cost == 80000                      # 800 * 100
    assert r.fits is True
    assert r.wall_at is None


def test_estimate_overflows_and_reports_wall():
    r = estimate(800, remaining=62000)
    assert r.cost == 80000
    assert r.fits is False
    assert r.wall_at == 620                     # 62000 // 100
    assert "620" in r.message() and "WARNING" in r.message()


def test_estimate_exact_boundary_fits():
    # cost == remaining is a fit (<=), not an overflow.
    r = estimate(100, remaining=10000)
    assert r.cost == 10000 and r.fits is True


def test_estimate_accepts_iterable():
    r = estimate(["a.com", "b.com", "c.com"], remaining=1000)
    assert r.domains == 3 and r.cost == 300


# --- preflight() with a fake client ------------------------------------

def test_preflight_fits_uses_workspace_pool():
    client = _FakeClient(_FakeResponse(200, _limits_body()))
    r = _run(preflight(800, api_key="k", client=client))
    assert r.fits is True
    assert r.cap_source == "workspace"
    assert r.remaining == 399950                # 400000 - 50
    assert r.cost == 80000
    assert "OK" in r.message()


def test_preflight_overflow_warns():
    # Tight workspace pool: only 5000 units left, 800 domains need 80000.
    client = _FakeClient(_FakeResponse(200, _limits_body(ws_limit=5050, ws_used=50)))
    r = _run(preflight(800, api_key="k", client=client))
    assert r.fits is False
    assert r.remaining == 5000
    assert r.wall_at == 50                       # 5000 // 100
    assert "WARNING" in r.message()


def test_preflight_api_key_cap_takes_precedence_when_tighter():
    # Workspace has plenty, but this key is capped at 3000 used 0 -> 3000 remaining governs.
    body = _limits_body(ws_limit=400000, ws_used=50, key_limit=3000, key_used=0)
    client = _FakeClient(_FakeResponse(200, body))
    r = _run(preflight(800, api_key="k", client=client))
    assert r.cap_source == "api_key"
    assert r.remaining == 3000
    assert r.fits is False
    assert r.wall_at == 30


def test_preflight_null_key_cap_ignored():
    # units_limit_api_key null (the real default) -> workspace pool governs, no crash.
    client = _FakeClient(_FakeResponse(200, _limits_body(key_limit=None)))
    r = _run(preflight(10, api_key="k", client=client))
    assert r.cap_source == "workspace"
    assert r.fits is True


def test_preflight_fetch_failure_fails_open():
    # The free endpoint is down -> advise proceeding (fits=True) but flag we couldn't check.
    client = _FakeClient(RuntimeError("connection refused"))
    r = _run(preflight(800, api_key="k", client=client))
    assert r.fits is True                        # fail-open: never block on a usage outage
    assert r.fetch_error is not None
    assert r.remaining is None
    assert r.cost == 80000                       # still reports the estimate
    assert "could not read unit balance" in r.message()


def test_preflight_http_error_fails_open():
    client = _FakeClient(_FakeResponse(500))
    r = _run(preflight(800, api_key="k", client=client))
    assert r.fits is True
    assert "HTTP 500" in r.fetch_error


def test_preflight_malformed_body_fails_open():
    client = _FakeClient(_FakeResponse(200, {"unexpected": "shape"}))
    r = _run(preflight(800, api_key="k", client=client))
    assert r.fits is True
    assert r.fetch_error is not None


def test_units_per_domain_is_measured_value():
    # Guard the measured constant: two 50-unit calls = 100/domain. If this changes,
    # someone changed the adapter's endpoint set and must update the estimate.
    assert UNITS_PER_DOMAIN == 100
