"""Tests for the OpenPageRank adapter (app/enrichment/openpagerank.py).

No network: enrich_one takes the httpx client as an argument, so a fake client serving
canned JSON (or raising) exercises every path. One GET per enrich_one normally; the
429-only retry loop issues additional GETs. Canned payloads use the CONFIRMED response
shape from DomCop's docs (verified 2026-06-19).

Coverage: happy path; the 404-sentinel trap (page_rank 0 on a 404 item -> ok=False
not_found, NOT a measured zero); genuine measured low score on a 200 item preserved;
auth 403 -> halt + short-circuit next; 429 retry-then-success and 429 exhausted;
transport single-attempt; empty/malformed response array; out-of-range score -> None;
_opr_score / _opr_int / _rank sentinel units.
"""
import asyncio

import httpx

from app.enrichment.openpagerank import (
    OpenPageRankAdapter, _opr_score, _opr_int, _rank)


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
    return OpenPageRankAdapter(api_key="test-key")


def _envelope(item):
    return {"status_code": 200, "response": [item], "last_updated": "28th Mar 2026"}


def _hit(page_rank_decimal=7.63, page_rank_integer=8, rank="40", domain="example.com"):
    return _envelope({"status_code": 200, "error": "",
                      "page_rank_integer": page_rank_integer,
                      "page_rank_decimal": page_rank_decimal,
                      "rank": rank, "domain": domain})


def _not_found(domain="unknowndomain.com"):
    # The real 404 shape: zeros + null rank, inner status 404.
    return _envelope({"status_code": 404, "error": "Domain not found",
                      "page_rank_integer": 0, "page_rank_decimal": 0,
                      "rank": None, "domain": domain})


def _patch_sleep(monkeypatch):
    async def _nosleep(*args, **kwargs):
        return None
    monkeypatch.setattr(asyncio, "sleep", _nosleep)


def test_opr_happy_path():
    client = _FakeClient([_resp(200, _hit())])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True and row["error"] is None
    assert row["adapter"] == "openpagerank"
    d = row["data"]
    assert d["page_rank"] == 7.63
    assert d["page_rank_int"] == 8
    assert d["global_rank"] == 40
    assert client.calls == 1


def test_opr_404_sentinel_is_not_found_not_zero():
    # THE trap: a 404 item carries page_rank 0 as a sentinel -> ok=False not_found,
    # NEVER a measured zero. This is the load-bearing test for this adapter.
    client = _FakeClient([_resp(200, _not_found())])
    row = _run(_adapter().enrich_one(client, "unknowndomain.com"))
    assert row["ok"] is False
    assert "not_found" in row["error"]
    assert row["data"] == {}                       # no fabricated zeros


def test_opr_genuine_low_score_on_200_preserved():
    # A real low-authority domain: 200 item, page_rank 0.0 -> KEPT (distinct from the 404
    # sentinel). The discriminator is the inner status_code, not the value.
    client = _FakeClient([_resp(200, _hit(page_rank_decimal=0.0, page_rank_integer=0, rank="9500000"))])
    row = _run(_adapter().enrich_one(client, "tiny.com"))
    assert row["ok"] is True
    assert row["data"]["page_rank"] == 0.0          # measured 0 on a 200 item -> kept
    assert row["data"]["page_rank_int"] == 0
    assert row["data"]["global_rank"] == 9500000


def test_opr_auth_403_halts_and_short_circuits_next():
    adapter = _adapter()
    client = _FakeClient([_resp(403)])
    first = _run(adapter.enrich_one(client, "first.com"))
    assert first["ok"] is False
    assert "auth_failed" in first["error"]
    assert adapter._halted is True
    calls_after_first = client.calls                # == 1
    second = _run(adapter.enrich_one(client, "second.com"))
    assert second["ok"] is False
    assert "skipped" in second["error"] and "halted" in second["error"]
    assert client.calls == calls_after_first        # NO HTTP call for the 2nd domain


def test_opr_429_retry_then_success(monkeypatch):
    _patch_sleep(monkeypatch)
    client = _FakeClient([_resp(429), _resp(200, _hit())])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True
    assert row["data"]["page_rank"] == 7.63
    assert client.calls == 2


def test_opr_429_exhausted(monkeypatch):
    _patch_sleep(monkeypatch)
    client = _FakeClient([_resp(429)])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "rate_limited" in row["error"]
    assert client.calls == OpenPageRankAdapter.MAX_429_RETRIES + 1


def test_opr_transport_error_single_attempt():
    client = _FakeClient([httpx.ConnectError("connection refused")])
    adapter = _adapter()
    row = _run(adapter._guarded(client, "example.com"))
    assert row["ok"] is False
    assert row["adapter"] == "openpagerank"
    assert "http error" in row["error"]
    assert client.calls == 1


def test_opr_empty_response_array_is_ok_false():
    client = _FakeClient([_resp(200, {"status_code": 200, "response": []})])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "empty response array" in row["error"]


def test_opr_non_json_is_ok_false():
    client = _FakeClient([_resp(200, None)])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "non-JSON" in row["error"]


def test_opr_score_sentinel_and_parsing():
    assert _opr_score(7.63) == 7.63
    assert _opr_score("10") == 10.0
    assert _opr_score(0) == 0.0                     # real 0 (caller gates the 404 sentinel)
    assert _opr_score(-1) is None
    assert _opr_score(11) is None                   # >10 impossible -> None
    assert _opr_score(None) is None
    assert _opr_score("nope") is None


def test_opr_int_sentinel_and_parsing():
    assert _opr_int(8) == 8
    assert _opr_int("10") == 10
    assert _opr_int(0) == 0
    assert _opr_int(-1) is None
    assert _opr_int(11) is None
    assert _opr_int(True) is None                   # bool excluded
    assert _opr_int(None) is None


def test_opr_rank_sentinel_and_parsing():
    assert _rank("40") == 40                        # arrives as a string
    assert _rank("6") == 6
    assert _rank(None) is None                      # 404 sends null
    assert _rank("") is None
    assert _rank("0") is None                       # rank >= 1
    assert _rank("-3") is None
    assert _rank("nope") is None
