"""Tests for the WhoXY adapter (app/enrichment/whoxy.py).

No network: enrich_one takes the httpx client as an argument, so a fake client
serving canned JSON (or raising) exercises every path. These cover the Live/History
orchestration (gated, degrade-on-history-failure), the status-field branching, the
two independent credit-exhaustion paths (HTTP 402 and status:0 + marker), and the
single-attempt discipline (no retry helper — the deliberate contrast with the free
adapters' 4-attempt retry path).

The fake client serves queued responses in order: call 1 = Live WHOIS, call 2 =
History (when gated on). client.calls lets each test assert whether History was
called. Async is driven with asyncio.run(), matching the other adapter test files.
No asyncio.sleep monkeypatch is needed — whoxy does not use _get_with_retries, so
there are no backoff sleeps.
"""
import asyncio

import httpx

from app.enrichment.whoxy import WhoxyAdapter


class _FakeResponse:
    def __init__(self, status_code, json_obj=None):
        self.status_code = status_code
        self._json = json_obj

    def json(self):
        if self._json is None:
            raise ValueError("no json body")
        return self._json


class _FakeClient:
    """Serves queued items in order; repeats the LAST once exhausted. An Exception
    item is raised instead of returned (to simulate a transport error)."""

    def __init__(self, items):
        self._items = list(items)
        self.calls = 0

    async def get(self, url, params=None, headers=None):
        item = self._items[min(self.calls, len(self._items) - 1)]
        self.calls += 1
        if isinstance(item, Exception):
            raise item
        return item


def _run(coro):
    return asyncio.run(coro)


def _resp(status, json_obj=None):
    return _FakeResponse(status, json_obj)


def _adapter():
    # Pass a dummy key so the keyless-warning guard stays quiet (whoxy is keyed).
    return WhoxyAdapter(api_key="test-key")


def _live_ok(**over):
    body = {
        "status": 1,
        "domain_name": "example.com",
        "domain_registered": "yes",
        "create_date": "2009-04-15",
        "update_date": "2020-01-01",
        "expiry_date": "2026-04-15",
        "domain_registrar": {"registrar_name": "GoDaddy.com, LLC"},
        "registrant_contact": {"full_name": "John Doe", "company_name": "Domain Tech Investments"},
        "domain_status": ["clientTransferProhibited"],
    }
    body.update(over)
    return body


def _history_ok(dates, total=None):
    body = {"status": 1, "whois_records": [{"create_date": d} for d in dates]}
    if total is not None:
        body["total_results"] = total
    return body


def test_whoxy_happy_both():
    # History earliest (1998) beats the current create_date (2009) -> source=history.
    live = _resp(200, _live_ok())
    hist = _resp(200, _history_ok(["2009-04-15", "1998-08-12", "2003-06-01"], total=3))
    client = _FakeClient([live, hist])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True and row["error"] is None
    assert row["adapter"] == "whoxy"
    d = row["data"]
    assert d["registered"] is True
    assert d["first_registered"] == "1998-08-12"        # earliest across history (min)
    assert d["first_registered_source"] == "history"
    assert d["create_date"] == "2009-04-15"
    assert d["registrar"] == "GoDaddy.com, LLC"
    assert d["registrant_org"] == "Domain Tech Investments"
    assert d["history_record_count"] == 3
    assert client.calls == 2


def test_whoxy_history_degrades_to_live_fallback():
    # History call raises -> caught locally -> fall back to live create_date, ok=True.
    live = _resp(200, _live_ok())
    client = _FakeClient([live, httpx.ConnectError("history boom")])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True
    d = row["data"]
    assert d["first_registered"] == "2009-04-15"        # live create_date fallback
    assert d["first_registered_source"] == "live_fallback"
    assert d["history_record_count"] == 0
    assert client.calls == 2                             # history WAS attempted


def test_whoxy_live_status_zero_no_history():
    # Logical failure on Live (HTTP 200, status:0, non-credit reason) -> ok=False,
    # and History is NOT called.
    live = _resp(200, _live_ok(status=0, status_reason="Domain not found"))
    client = _FakeClient([live])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "status 0" in row["error"]
    assert "Domain not found" in row["error"]
    assert client.calls == 1


def test_whoxy_credit_exhausted_http_402():
    # Path A: HTTP 402 -> typed credit_exhausted (lines 117-120). json() never called.
    client = _FakeClient([_resp(402, None)])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "credit_exhausted" in row["error"]
    assert client.calls == 1


def test_whoxy_credit_exhausted_status_zero():
    # Path B: HTTP 200, status:0 with an insufficient-balance reason (line 132).
    live = _resp(200, {"status": 0, "status_reason": "Insufficient API credits"})
    client = _FakeClient([live])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "credit_exhausted" in row["error"]
    assert client.calls == 1


def test_whoxy_not_registered_no_history():
    # Lookup succeeded, domain not currently registered -> known-absent ok=True,
    # History gated off.
    live = _resp(200, _live_ok(domain_registered="no", create_date=None))
    client = _FakeClient([live])
    row = _run(_adapter().enrich_one(client, "available.example"))
    assert row["ok"] is True
    d = row["data"]
    assert d["registered"] is False
    assert d["first_registered"] is None
    assert d["first_registered_source"] is None
    assert client.calls == 1                             # history skipped (gate)


def test_whoxy_live_transport_error_single_attempt():
    # A transport error on Live propagates to _guarded -> clean ok=False, and there
    # is exactly ONE attempt (no retry helper) — the contrast with the free adapters'
    # 4-attempt path.
    client = _FakeClient([httpx.ConnectError("connection refused")])
    adapter = _adapter()
    row = _run(adapter._guarded(client, "example.com"))
    assert row["ok"] is False
    assert row["adapter"] == "whoxy"
    assert "http error" in row["error"]                  # _guarded's httpx.HTTPError branch
    assert client.calls == 1                             # SINGLE attempt, no retry
