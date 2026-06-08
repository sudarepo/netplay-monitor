"""Tests for the Wikipedia adapter (app/enrichment/wikipedia.py).

No network: enrich_one takes the httpx client as an argument, so a fake client
serving canned MediaWiki JSON (or raising) exercises every path. These cover the
exturlusage result shaping, the capped-page detection, and the HTTP-200 error
body split (retryable throttle vs our-query-bug) — and, through the base helper,
the maxlag 503->retry and transport-error paths.

asyncio.sleep is monkeypatched module-wide so the backoff sleeps inside the base
helper are no-ops. Async is driven with asyncio.run() inside sync test functions,
matching test_dataset.py / test_archiveorg.py.
"""
import asyncio

import httpx

from app.enrichment.wikipedia import WikipediaAdapter


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
    """Serves queued items in order; repeats the LAST item once exhausted (so an
    always-503 / always-raise client needs only one entry). An Exception item is
    raised instead of returned."""

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


def _patch_sleep(monkeypatch):
    async def _nosleep(*args, **kwargs):
        return None
    monkeypatch.setattr(asyncio, "sleep", _nosleep)


def _resp(status, json_obj=None, headers=None):
    return _FakeResponse(status, json_obj, headers)


def _entries(n):
    return [{"ns": 0, "title": f"Article {i}", "url": "http://example.com/x"} for i in range(n)]


def test_wikipedia_happy_path():
    payload = {"batchcomplete": True, "query": {"exturlusage": _entries(3)}}
    row = _run(WikipediaAdapter().enrich_one(_FakeClient([_resp(200, payload)]), "example.com"))
    assert row["ok"] is True and row["error"] is None
    assert row["adapter"] == "wikipedia"
    d = row["data"]
    assert d["linked"] is True
    assert d["link_count"] == 3
    assert d["link_count_capped"] is False


def test_wikipedia_zero_links():
    payload = {"batchcomplete": True, "query": {"exturlusage": []}}
    row = _run(WikipediaAdapter().enrich_one(_FakeClient([_resp(200, payload)]), "neverused.example"))
    assert row["ok"] is True and row["error"] is None
    assert row["data"] == {"linked": False, "link_count": 0, "link_count_capped": False}


def test_wikipedia_capped():
    # A `continue` token means there are more than one page of hits -> capped.
    payload = {"continue": {"eucontinue": "0|123"}, "query": {"exturlusage": _entries(500)}}
    row = _run(WikipediaAdapter().enrich_one(_FakeClient([_resp(200, payload)]), "bigcite.example"))
    d = row["data"]
    assert d["linked"] is True
    assert d["link_count"] == 500
    assert d["link_count_capped"] is True


def test_wikipedia_maxlag_503_then_200(monkeypatch):
    _patch_sleep(monkeypatch)
    ok_payload = {"query": {"exturlusage": _entries(2)}}
    # First a maxlag 503 with Retry-After (handled+honored by the base helper),
    # then a 200 — the retry should succeed.
    client = _FakeClient([_resp(503, None, {"retry-after": "1"}), _resp(200, ok_payload)])
    row = _run(WikipediaAdapter().enrich_one(client, "example.com"))
    assert row["ok"] is True
    assert row["data"]["link_count"] == 2
    assert client.calls == 2                      # one 503, then the 200


def test_wikipedia_error_body_retryable():
    # HTTP 200 carrying a transient maxlag error -> ok=False, marked "throttled".
    payload = {"error": {"code": "maxlag", "info": "Waiting for a database server: 6 seconds lagged."}}
    row = _run(WikipediaAdapter().enrich_one(_FakeClient([_resp(200, payload)]), "example.com"))
    assert row["ok"] is False
    assert "throttled (maxlag)" in row["error"]   # clearly transient, will retry next run


def test_wikipedia_error_body_fatal():
    # HTTP 200 carrying a non-retryable code (our query is malformed) -> ok=False,
    # surfaced loudly with the info field so a grep of error rows finds the bug.
    payload = {"error": {"code": "badvalue",
                         "info": 'Unrecognized value for parameter "eunamespace": 99.'}}
    row = _run(WikipediaAdapter().enrich_one(_FakeClient([_resp(200, payload)]), "example.com"))
    assert row["ok"] is False
    assert "api error (badvalue)" in row["error"]
    assert "Unrecognized value" in row["error"]   # info included for debugging


def test_wikipedia_transport_error_final_attempt(monkeypatch):
    _patch_sleep(monkeypatch)
    client = _FakeClient([httpx.ConnectError("connection refused")])
    adapter = WikipediaAdapter()
    row = _run(adapter._guarded(client, "example.com"))
    assert row["ok"] is False
    assert row["adapter"] == "wikipedia"
    assert "http error" in row["error"]           # _guarded's httpx.HTTPError branch
    assert client.calls == 4                      # 1 initial + 3 retries, all raised
