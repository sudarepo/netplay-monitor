"""Tests for the Wayback adapter (app/enrichment/archiveorg.py).

No network: enrich_one takes the httpx client as an argument, so a tiny fake
client serving canned responses (or raising) exercises every path. These cover
the archiveorg-specific result shaping AND, critically, the shared
EnrichmentAdapter._get_with_retries helper — which has no other test coverage.
The retry trio (429->200, 429-exhausted, transport-error-on-final-attempt) is
really validating the base helper through its first consumer.

asyncio.sleep is monkeypatched module-wide (on the asyncio module itself, not on
archiveorg's namespace) so the backoff sleeps inside the base helper are no-ops —
otherwise the retry tests would actually sleep. Async is driven with
asyncio.run() inside sync test functions, matching test_dataset.py.
"""
import asyncio
import json

import httpx

from app.enrichment.archiveorg import ArchiveOrgAdapter


class _FakeResponse:
    def __init__(self, status_code, text, headers=None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}

    def json(self):
        return json.loads(self.text)


class _FakeClient:
    """Serves queued items in order; repeats the LAST item once the queue is
    exhausted (so an always-429 / always-raise client needs only one entry). An
    item that is an Exception instance is raised instead of returned."""

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
    """Make every asyncio.sleep a no-op, module-wide — covers the base helper's
    backoff call site, not just archiveorg's namespace."""
    async def _nosleep(*args, **kwargs):
        return None
    monkeypatch.setattr(asyncio, "sleep", _nosleep)


def _resp(status, body, headers=None):
    return _FakeResponse(status, body, headers)


def test_archiveorg_happy_path():
    # Rows deliberately NOT in timestamp order (matchType=domain sorts by urlkey),
    # so this pins first/last as min/max, not positional first/last.
    body = json.dumps([
        ["timestamp", "statuscode"],
        ["20190102000000", "200"],
        ["20040312153000", "200"],
        ["20230615120000", "404"],
    ])
    row = _run(ArchiveOrgAdapter().enrich_one(_FakeClient([_resp(200, body)]), "example.com"))
    assert row["ok"] is True and row["error"] is None
    assert row["adapter"] == "archiveorg"
    d = row["data"]
    assert d["archived"] is True
    assert d["first_capture"] == "2004-03-12"   # min over all rows
    assert d["last_capture"] == "2023-06-15"    # max over all rows
    assert d["capture_days"] == 3
    assert d["live_capture_days"] == 2          # two rows with status 200
    assert d["capture_count_capped"] is False


def test_archiveorg_zero_captures():
    # CDX returns 200 + empty body for "no matches": a successful, authoritative
    # "not archived" finding (ok=True), NOT an error.
    row = _run(ArchiveOrgAdapter().enrich_one(_FakeClient([_resp(200, "")]), "neverused.example"))
    assert row["ok"] is True and row["error"] is None
    assert row["data"] == {
        "archived": False,
        "first_capture": None,
        "last_capture": None,
        "capture_days": 0,
        "live_capture_days": 0,
        "capture_count_capped": False,
    }


def test_archiveorg_capped():
    # Pin the cap threshold at exactly LIMIT (>= self.LIMIT), both directions.
    # LIMIT patched to 3 via an instance attribute (self.LIMIT in _summarize).
    rows3 = json.dumps([
        ["timestamp", "statuscode"],
        ["20040101000000", "200"],
        ["20050101000000", "200"],
        ["20060101000000", "200"],
    ])
    a3 = ArchiveOrgAdapter()
    a3.LIMIT = 3
    capped = _run(a3.enrich_one(_FakeClient([_resp(200, rows3)]), "big.example"))
    assert capped["data"]["capture_days"] == 3
    assert capped["data"]["capture_count_capped"] is True       # 3 >= 3 fires

    rows2 = json.dumps([
        ["timestamp", "statuscode"],
        ["20040101000000", "200"],
        ["20050101000000", "200"],
    ])
    a2 = ArchiveOrgAdapter()
    a2.LIMIT = 3
    uncapped = _run(a2.enrich_one(_FakeClient([_resp(200, rows2)]), "mid.example"))
    assert uncapped["data"]["capture_days"] == 2
    assert uncapped["data"]["capture_count_capped"] is False     # 2 >= 3 does NOT fire


def test_archiveorg_429_then_200(monkeypatch):
    _patch_sleep(monkeypatch)
    body = json.dumps([["timestamp", "statuscode"], ["20100101000000", "200"]])
    client = _FakeClient([_resp(429, ""), _resp(200, body)])
    row = _run(ArchiveOrgAdapter().enrich_one(client, "example.com"))
    assert row["ok"] is True                  # retry-with-backoff succeeded
    assert row["data"]["archived"] is True
    assert client.calls == 2                  # one 429, then the 200


def test_archiveorg_429_exhausted(monkeypatch):
    _patch_sleep(monkeypatch)
    # Always 429: the base helper returns the still-429 response after exhausting
    # retries; enrich_one maps that to a typed ok=False (it must NOT raise).
    client = _FakeClient([_resp(429, "")])
    row = _run(ArchiveOrgAdapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "rate_limited" in row["error"]
    assert client.calls == 4                  # 1 initial + 3 retries


def test_archiveorg_transport_error_final_attempt(monkeypatch):
    _patch_sleep(monkeypatch)
    # A transport error that persists past the last retry propagates out of
    # _get_with_retries and enrich_one, and is caught by _guarded -> clean ok=False.
    client = _FakeClient([httpx.ConnectError("connection refused")])
    adapter = ArchiveOrgAdapter()
    row = _run(adapter._guarded(client, "example.com"))
    assert row["ok"] is False
    assert row["adapter"] == "archiveorg"
    assert "http error" in row["error"]       # _guarded's httpx.HTTPError branch
    assert client.calls == 4                  # 1 initial + 3 retries, all raised
