"""Tests for the HumbleWorth adapter (app/enrichment/humbleworth.py).

No network: a fake httpx client serves canned Replicate prediction envelopes. Covers the
synchronous happy path (Prefer:wait returns a terminal prediction), the poll fallback
(returns 'processing' then 'succeeded' on a polled GET), auth-halt, 429 retry, per-domain
error, failed prediction, empty/zeroed values, and the _usd sentinel.
"""
import asyncio

import httpx

from app.enrichment.humbleworth import HumbleworthAdapter, _usd


class _FakeResponse:
    def __init__(self, status_code, json_obj=None):
        self.status_code = status_code
        self._json = json_obj
        self.headers = {}

    def json(self):
        if self._json is None:
            raise ValueError("no json")
        return self._json


class _FakeClient:
    """Serves queued items in order (repeats last). Exceptions are raised."""
    def __init__(self, items):
        self._items = list(items)
        self.calls = 0

    async def post(self, url, json=None, headers=None):
        return self._next()

    async def get(self, url, headers=None):
        return self._next()

    def _next(self):
        item = self._items[min(self.calls, len(self._items) - 1)]
        self.calls += 1
        if isinstance(item, Exception):
            raise item
        return item


def _run(c):
    return asyncio.run(c)


def _adapter():
    return HumbleworthAdapter(api_key="r8_test")


def _succeeded(auction=1200, marketplace=3500, brokerage=5200, domain="example.com", error=None):
    return _FakeResponse(201, {
        "status": "succeeded",
        "output": {"valuations": [{
            "domain": domain, "auction": auction, "marketplace": marketplace,
            "brokerage": brokerage, "error": error}]},
    })


def _patch_sleep(monkeypatch):
    async def _nosleep(*a, **k):
        return None
    monkeypatch.setattr(asyncio, "sleep", _nosleep)


def test_hw_happy_sync():
    client = _FakeClient([_succeeded()])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True and row["error"] is None
    assert row["adapter"] == "humbleworth"
    assert row["data"] == {"auction": 1200.0, "marketplace": 3500.0, "brokerage": 5200.0}
    assert client.calls == 1                  # one call, no poll (Prefer:wait)


def test_hw_poll_fallback(monkeypatch):
    _patch_sleep(monkeypatch)
    # First POST returns 'processing' with a poll URL; GET then returns succeeded.
    processing = _FakeResponse(201, {"status": "processing",
                                     "urls": {"get": "https://api.replicate.com/v1/predictions/x"}})
    client = _FakeClient([processing, _succeeded()])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True
    assert row["data"]["marketplace"] == 3500.0
    assert client.calls == 2                  # POST + one poll GET


def test_hw_auth_halts_and_short_circuits(monkeypatch):
    adapter = _adapter()
    client = _FakeClient([_FakeResponse(401)])
    first = _run(adapter.enrich_one(client, "a.com"))
    assert first["ok"] is False and "auth_failed" in first["error"]
    assert adapter._halted is True
    n = client.calls
    second = _run(adapter.enrich_one(client, "b.com"))
    assert second["ok"] is False and "skipped" in second["error"]
    assert client.calls == n                  # no HTTP for 2nd domain


def test_hw_429_then_success(monkeypatch):
    _patch_sleep(monkeypatch)
    client = _FakeClient([_FakeResponse(429), _succeeded()])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True
    assert client.calls == 2


def test_hw_per_domain_error_is_ok_false():
    client = _FakeClient([_succeeded(error="could not value")])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "per-domain error" in row["error"]


def test_hw_failed_prediction():
    client = _FakeClient([_FakeResponse(201, {"status": "failed", "error": "model crash"})])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "failed" in row["error"]


def test_hw_all_zero_values_is_ok_false():
    # zeros -> None per sentinel -> no usable values -> ok=False (never fabricate 0).
    client = _FakeClient([_succeeded(auction=0, marketplace=0, brokerage=0)])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "no usable values" in row["error"]


def test_hw_partial_values_kept():
    # auction present, marketplace 0 (->None), brokerage present -> ok=True, marketplace None.
    client = _FakeClient([_succeeded(auction=900, marketplace=0, brokerage=3000)])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True
    assert row["data"]["auction"] == 900.0
    assert row["data"]["marketplace"] is None
    assert row["data"]["brokerage"] == 3000.0


def test_hw_invalid_version_422():
    client = _FakeClient([_FakeResponse(422, {"detail": "version not found"})])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "422" in row["error"]


def test_hw_transport_error_single_attempt():
    client = _FakeClient([httpx.ConnectError("refused")])
    adapter = _adapter()
    row = _run(adapter._guarded(client, "example.com"))
    assert row["ok"] is False
    assert "http error" in row["error"]
    assert client.calls == 1


def test_usd_sentinel():
    assert _usd(3500) == 3500.0
    assert _usd("1200.5") == 1200.5
    assert _usd(0) is None                     # 0 -> no signal
    assert _usd(-5) is None
    assert _usd(None) is None
    assert _usd("nope") is None
