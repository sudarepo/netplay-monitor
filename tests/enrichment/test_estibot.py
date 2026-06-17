"""Tests for the Estibot adapter (app/enrichment/estibot.py).

No network: enrich_one takes the httpx client as an argument, so a fake client serving
canned JSON (or raising) exercises every path. estibot makes ONE GET per enrich_one in
the normal case; the 429-only retry loop issues additional GETs to the same URL, so the
fake client serves queued responses in order (repeating the last once exhausted) and
client.calls counts attempts.

asyncio.sleep is monkeypatched module-wide (on the asyncio module itself) for the retry
tests so the 429 backoff sleeps are no-ops — same convention as test_archiveorg. The
non-retry tests need no patch (a single attempt has no backoff).

Coverage: happy path; -1 sentinel -> None per field; all-(-1) row -> ok=True (Estibot
answered "no value" = measured, distinct from a cache miss); cache_miss (not_found) ->
ok=False; auth 400 -> halt + short-circuit next domain; auth via 200 success:false ->
halt; 429 retry-then-success and 429 exhausted; transport single-attempt; success:false
object-shaped results; real 0 preserved; bracket out-of-range -> None; and the
Retry-After cap + backoff jitter on the inherited base helpers.
"""
import asyncio

import httpx

from app.enrichment.base import EnrichmentAdapter
from app.enrichment.estibot import EstibotAdapter, _usd, _bracket


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
    raised instead of returned (to simulate a transport error)."""

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
    # Dummy key so the keyless-warning guard stays quiet (estibot is keyed).
    return EstibotAdapter(api_key="test-key")


def _row(**over):
    body = {"appraised_value": "2500", "appraised_wholesale_value": "1200",
            "price_range_retail": "12"}
    body.update(over)
    return body


def _success(rows, not_found=None):
    return {"success": True, "message": "", "results": rows,
            "not_found": not_found or [], "cache": True, "result_count": len(rows)}


def _fail(message):
    # success:false -> results is an OBJECT (not an array). The adapter must never index it.
    return {"success": False, "message": message,
            "results": {"total": 0, "count": 0, "start": 0, "end": 0, "data": []}}


def _patch_sleep(monkeypatch):
    """Make every asyncio.sleep a no-op, module-wide — covers the 429 backoff call site."""
    async def _nosleep(*args, **kwargs):
        return None
    monkeypatch.setattr(asyncio, "sleep", _nosleep)


def test_estibot_happy_path():
    client = _FakeClient([_resp(200, _success([_row()]))])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True and row["error"] is None
    assert row["adapter"] == "estibot"
    d = row["data"]
    assert d["estimated_value"] == 2500
    assert d["wholesale_value"] == 1200
    assert d["price_range_retail"] == 12
    assert client.calls == 1


def test_estibot_minus_one_is_none_per_field():
    # appraised_wholesale_value -1 -> None, the other fields still measured -> ok=True.
    client = _FakeClient([_resp(200, _success([_row(appraised_wholesale_value="-1")]))])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True
    d = row["data"]
    assert d["estimated_value"] == 2500
    assert d["wholesale_value"] is None             # -1 sentinel -> None, NOT 0
    assert d["price_range_retail"] == 12


def test_estibot_all_minus_one_row_is_ok_true():
    # The confirmed distinction: a cached row with ALL -1 is Estibot answering "no value"
    # -> a real measurement -> ok=True with fields None (NOT a domscan-style all-None guard).
    client = _FakeClient([_resp(200, _success([
        _row(appraised_value="-1", appraised_wholesale_value="-1.00", price_range_retail="-1")
    ]))])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True                        # reached the source, got an answer
    d = row["data"]
    assert d["estimated_value"] is None
    assert d["wholesale_value"] is None
    assert d["price_range_retail"] is None


def test_estibot_real_zero_preserved():
    # A genuine measured 0 must survive (distinct from the -1 sentinel).
    client = _FakeClient([_resp(200, _success([_row(appraised_value="0")]))])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True
    assert row["data"]["estimated_value"] == 0      # NOT None


def test_estibot_cache_miss_not_found_is_ok_false():
    # Domain listed in TOP-LEVEL not_found[] -> UNKNOWN (retry later), typed cache_miss.
    client = _FakeClient([_resp(200, _success([], not_found=["example.com"]))])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "cache_miss" in row["error"] and "not_found" in row["error"]
    assert client.calls == 1


def test_estibot_auth_400_halts_and_short_circuits_next():
    adapter = _adapter()
    client = _FakeClient([_resp(400, _fail("Invalid API key."))])
    first = _run(adapter.enrich_one(client, "first.com"))
    assert first["ok"] is False
    assert "auth_failed" in first["error"]
    assert adapter._halted is True
    calls_after_first = client.calls                # == 1
    second = _run(adapter.enrich_one(client, "second.com"))
    assert second["ok"] is False
    assert "skipped" in second["error"] and "halted" in second["error"]
    assert client.calls == calls_after_first        # NO HTTP call for the 2nd domain


def test_estibot_auth_via_200_success_false_halts():
    # Defensive: the documented invalid-key message arriving on a 200 success:false body.
    adapter = _adapter()
    client = _FakeClient([_resp(200, _fail("Invalid API key."))])
    row = _run(adapter.enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "auth_failed" in row["error"]
    assert adapter._halted is True


def test_estibot_success_false_object_results_shape():
    # success:false with results as an OBJECT (not array). Non-auth message -> ok=False
    # "request unsuccessful", and the object-shaped results must not crash the adapter.
    client = _FakeClient([_resp(200, _fail("Some transient backend error"))])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "request unsuccessful" in row["error"]
    assert "Some transient backend error" in row["error"]


def test_estibot_429_retry_then_success(monkeypatch):
    _patch_sleep(monkeypatch)
    client = _FakeClient([_resp(429), _resp(200, _success([_row()]))])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True                        # the 429-only retry recovered
    assert row["data"]["estimated_value"] == 2500
    assert client.calls == 2                         # one retry


def test_estibot_429_exhausted(monkeypatch):
    _patch_sleep(monkeypatch)
    client = _FakeClient([_resp(429)])               # repeats -> always 429
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "rate_limited" in row["error"]
    assert client.calls == EstibotAdapter.MAX_429_RETRIES + 1   # 1 + 3 = 4 attempts


def test_estibot_transport_error_single_attempt():
    # Transport error is NOT retried (429-only loop doesn't catch it) -> propagates to
    # _guarded -> clean ok=False, exactly ONE attempt.
    client = _FakeClient([httpx.ConnectError("connection refused")])
    adapter = _adapter()
    row = _run(adapter._guarded(client, "example.com"))
    assert row["ok"] is False
    assert row["adapter"] == "estibot"
    assert "http error" in row["error"]
    assert client.calls == 1                         # single attempt, no retry


def test_estibot_bracket_out_of_range_is_none():
    # price_range_retail valid range is 1-24; anything else (25, 0, -1) -> None.
    assert _bracket("25") is None
    assert _bracket("0") is None
    assert _bracket("-1") is None
    assert _bracket("1") == 1
    assert _bracket("24") == 24
    assert _bracket(None) is None


def test_estibot_usd_sentinel_and_parsing():
    # Direct unit checks on the USD coercion: -1/-1.00 -> None, real 0 preserved.
    assert _usd("-1") is None
    assert _usd("-1.00") is None
    assert _usd("0") == 0
    assert _usd("2500") == 2500
    assert _usd(None) is None
    assert _usd("not-a-number") is None


def test_estibot_retry_after_capped():
    # Inherited base helper: a hostile/huge Retry-After is CAPPED so it can't stall a run.
    a = _adapter()
    assert a.RETRY_AFTER_CAP == 30.0
    assert a._retry_after(_resp(429, headers={"retry-after": "99999"})) == 30.0
    assert a._retry_after(_resp(429, headers={"retry-after": "5"})) == 5.0
    assert a._retry_after(_resp(429, headers={})) is None


def test_estibot_backoff_has_jitter():
    # Inherited base helper: exponential term + additive jitter in [0, base_delay).
    a = _adapter()
    for attempt in range(4):
        base = 1.0 * (2 ** attempt)
        val = a._backoff(attempt, 1.0)
        assert base <= val < base + 1.0             # jitter keeps it within [base, base+1)
