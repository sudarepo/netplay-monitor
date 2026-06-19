"""Tests for the Ahrefs adapter (app/enrichment/ahrefs.py).

No network: enrich_one takes the httpx client as an argument, so a fake client serving
canned JSON (or raising) exercises every path. ahrefs makes TWO GETs per enrich_one
(domain-rating then backlinks-stats), so the fake client serves queued responses in order
and client.calls counts attempts. Canned payloads use the CONFIRMED-LIVE response shapes
captured 2026-06-18 against ahrefs.com.

Coverage: happy path (both axes, real shapes); per-axis partial degradation (one 5xx ->
that axis None, row still ok=True); both-axes-unavailable -> ok=False; real DR 0.0 and 0
counts preserved (distinct from missing -> None); auth/units halt via 401 + short-circuit
next domain; halt via 429 (spend-protective); halt via 200 error-envelope naming units;
halt via list-form error body; transport single-attempt; DR out-of-range -> None; _num/_dr
sentinel units.
"""
import asyncio

import httpx

from app.enrichment.ahrefs import AhrefsAdapter, _dr, _num


class _FakeResponse:
    def __init__(self, status_code, json_obj=None, headers=None):
        self.status_code = status_code
        self._json = json_obj
        self.headers = headers or {}

    def json(self):
        if self._json is None:
            raise ValueError("no json body")
        return self._json


class _FakeClient:
    """Serves queued items in order; repeats the LAST once exhausted. An Exception item is
    raised instead of returned (simulating a transport error)."""

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


def _resp(status, json_obj=None, headers=None):
    return _FakeResponse(status, json_obj, headers)


def _adapter():
    return AhrefsAdapter(api_key="test-key")


def _dr_body(domain_rating=91.0, ahrefs_rank=635):
    return {"domain_rating": {"domain_rating": domain_rating, "ahrefs_rank": ahrefs_rank}}


def _bl_body(live=19347349, all_time=296694532, live_refdomains=98410,
             all_time_refdomains=310181):
    return {"metrics": {"live": live, "all_time": all_time,
                        "live_refdomains": live_refdomains,
                        "all_time_refdomains": all_time_refdomains}}


def test_ahrefs_happy_path():
    client = _FakeClient([_resp(200, _dr_body()), _resp(200, _bl_body())])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True and row["error"] is None
    assert row["adapter"] == "ahrefs"
    d = row["data"]
    assert d["domain_rating"] == 91.0
    assert d["ahrefs_rank"] == 635
    assert d["live_backlinks"] == 19347349
    assert d["live_refdomains"] == 98410
    assert d["all_time_refdomains"] == 310181
    assert client.calls == 2                          # two endpoints


def test_ahrefs_partial_degrade_one_axis_5xx():
    # domain-rating ok, backlinks-stats 503 -> backlinks fields None, row still ok=True.
    client = _FakeClient([_resp(200, _dr_body()), _resp(503)])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True
    d = row["data"]
    assert d["domain_rating"] == 91.0                 # the axis that answered
    assert d["live_backlinks"] is None                # the failed axis -> None, NOT 0
    assert d["live_refdomains"] is None


def test_ahrefs_both_axes_unavailable_is_ok_false():
    # Both endpoints 5xx (non-halt) -> nothing measured -> UNKNOWN -> ok=False.
    client = _FakeClient([_resp(503), _resp(500)])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "all signals unavailable" in row["error"]


def test_ahrefs_real_zero_dr_and_counts_preserved():
    # A parked/unlinked domain: DR 0.0, 0 refdomains -> real measurements, kept, ok=True.
    client = _FakeClient([
        _resp(200, _dr_body(domain_rating=0.0, ahrefs_rank=0)),
        _resp(200, _bl_body(live=0, live_refdomains=0, all_time_refdomains=0)),
    ])
    row = _run(_adapter().enrich_one(client, "parked.com"))
    assert row["ok"] is True
    d = row["data"]
    assert d["domain_rating"] == 0.0                  # NOT None
    assert d["ahrefs_rank"] == 0
    assert d["live_backlinks"] == 0
    assert d["live_refdomains"] == 0


def test_ahrefs_auth_401_halts_and_short_circuits_next():
    adapter = _adapter()
    client = _FakeClient([_resp(401)])
    first = _run(adapter.enrich_one(client, "first.com"))
    assert first["ok"] is False
    assert "credit_exhausted" in first["error"]       # whoxy/domscan family token
    assert "auth_or_units" in first["error"]
    assert adapter._halted is True
    calls_after_first = client.calls                  # == 1 (halted on the first call)
    second = _run(adapter.enrich_one(client, "second.com"))
    assert second["ok"] is False
    assert "skipped" in second["error"] and "halted" in second["error"]
    assert client.calls == calls_after_first          # NO HTTP call for the 2nd domain


def test_ahrefs_429_halts_spend_protective():
    # 429 under unit metering most likely = the wall -> HALT (not skip-one), to protect spend.
    adapter = _adapter()
    client = _FakeClient([_resp(429)])
    row = _run(adapter.enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "credit_exhausted" in row["error"]
    assert adapter._halted is True


def test_ahrefs_units_message_on_200_halts():
    # A 200 body that is actually an error naming units -> halt.
    adapter = _adapter()
    client = _FakeClient([_resp(200, {"error": "Not enough API units remaining"})])
    row = _run(adapter.enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "credit_exhausted" in row["error"]
    assert adapter._halted is True


def test_ahrefs_list_form_error_body_halts():
    # Ahrefs returns list-form errors like ["Error","Unauthorized"] -> recognized + halt.
    adapter = _adapter()
    client = _FakeClient([_resp(200, ["Error", "Unauthorized"])])
    row = _run(adapter.enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "credit_exhausted" in row["error"]
    assert adapter._halted is True


def test_ahrefs_transport_error_single_attempt():
    client = _FakeClient([httpx.ConnectError("connection refused")])
    adapter = _adapter()
    row = _run(adapter._guarded(client, "example.com"))
    assert row["ok"] is False
    assert row["adapter"] == "ahrefs"
    assert "http error" in row["error"]
    assert client.calls == 1                          # single attempt, no retry


def test_ahrefs_dr_out_of_range_is_none():
    assert _dr("-1") is None
    assert _dr("150") is None                         # >100 impossible -> None
    assert _dr("0") == 0.0                             # real DR 0 preserved
    assert _dr("91.0") == 91.0
    assert _dr(None) is None
    assert _dr("nope") is None


def test_ahrefs_num_sentinel_and_parsing():
    assert _num("0") == 0                              # real measured 0 preserved
    assert _num("-1") is None
    assert _num("98410") == 98410
    assert _num(635) == 635
    assert _num(None) is None
    assert _num(True) is None                          # bool excluded
    assert _num("nan-ish") is None
