"""Tests for the SEOkicks adapter (app/enrichment/seokicks.py).

No network: enrich_one takes the httpx client as an argument, so a fake client serving
canned JSON (or raising) exercises every path. seokicks makes ONE GET per enrich_one in
the normal case; the 429-only retry loop issues additional GETs to the same URL, so the
fake client serves queued responses in order (repeating the last once exhausted) and
client.calls counts attempts. Same fakes/conventions as test_estibot.

asyncio.sleep is monkeypatched module-wide for the retry tests so the 429 backoff sleeps
are no-ops. The non-retry tests need no patch (a single attempt has no backoff).

Coverage: happy path (4 pop counts coerced from strings); missing field -> None; real 0
preserved (distinct from missing); all-zero Overview -> ok=True (SEOkicks answered, a
measurement, NOT an all-None guard); no-Overview body -> ok=False; auth/credit halt via
401 + short-circuit next domain; halt via 403; halt via 200 error-envelope message;
non-auth error envelope -> ok=False without halt; 429 retry-then-success and 429
exhausted; transport single-attempt; non-JSON body; and the _count sentinel unit checks.
"""
import asyncio

import httpx

from app.enrichment.seokicks import SeokicksAdapter, _count


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
    # Dummy key so the keyless-warning guard stays quiet (seokicks is keyed).
    return SeokicksAdapter(api_key="test-appid")


def _overview(**over):
    body = {"linkpop": "14853", "domainpop": "546", "ippop": "448", "netpop": "370"}
    body.update(over)
    return {"Overview": body, "Results": []}


def _patch_sleep(monkeypatch):
    """Make every asyncio.sleep a no-op, module-wide — covers the 429 backoff call site."""
    async def _nosleep(*args, **kwargs):
        return None
    monkeypatch.setattr(asyncio, "sleep", _nosleep)


def test_seokicks_happy_path():
    client = _FakeClient([_resp(200, _overview())])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True and row["error"] is None
    assert row["adapter"] == "seokicks"
    d = row["data"]
    assert d["domainpop"] == 546                     # primary authority signal
    assert d["linkpop"] == 14853
    assert d["ippop"] == 448
    assert d["netpop"] == 370
    assert client.calls == 1


def test_seokicks_missing_field_is_none():
    # A field absent from Overview -> None (UNKNOWN), the rest still measured -> ok=True.
    ov = _overview()
    del ov["Overview"]["ippop"]
    client = _FakeClient([_resp(200, ov)])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True
    d = row["data"]
    assert d["domainpop"] == 546
    assert d["ippop"] is None                         # missing -> None, NOT 0
    assert d["netpop"] == 370


def test_seokicks_real_zero_preserved():
    # A genuine measured 0 (domain with no links in the index) must survive, distinct
    # from missing -> None.
    client = _FakeClient([_resp(200, _overview(domainpop="0", linkpop="0"))])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True
    assert row["data"]["domainpop"] == 0              # NOT None
    assert row["data"]["linkpop"] == 0


def test_seokicks_all_zero_overview_is_ok_true():
    # An all-zero Overview is SEOkicks answering "found nothing in the index" -> a real
    # measurement -> ok=True (NOT a domscan-style all-None guard). Discriminator is "did
    # the source answer," not "are the values empty."
    client = _FakeClient([_resp(200, _overview(
        linkpop="0", domainpop="0", ippop="0", netpop="0"))])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True
    assert all(row["data"][k] == 0 for k in ("domainpop", "linkpop", "ippop", "netpop"))


def test_seokicks_no_overview_is_ok_false():
    # 200 but no Overview block -> not a measurement -> UNKNOWN.
    client = _FakeClient([_resp(200, {"Results": []})])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "no Overview" in row["error"]
    assert client.calls == 1


def test_seokicks_non_json_is_ok_false():
    client = _FakeClient([_resp(200, None)])          # .json() raises
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "non-JSON" in row["error"]


def test_seokicks_auth_401_halts_and_short_circuits_next():
    adapter = _adapter()
    client = _FakeClient([_resp(401)])
    first = _run(adapter.enrich_one(client, "first.com"))
    assert first["ok"] is False
    assert "credit_exhausted" in first["error"]       # whoxy/domscan family token
    assert "auth_or_credit" in first["error"]
    assert adapter._halted is True
    calls_after_first = client.calls                  # == 1
    second = _run(adapter.enrich_one(client, "second.com"))
    assert second["ok"] is False
    assert "skipped" in second["error"] and "halted" in second["error"]
    assert client.calls == calls_after_first          # NO HTTP call for the 2nd domain


def test_seokicks_auth_403_halts():
    adapter = _adapter()
    client = _FakeClient([_resp(403)])
    row = _run(adapter.enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "credit_exhausted" in row["error"]
    assert adapter._halted is True


def test_seokicks_credit_message_on_200_halts():
    # Defensive: a 200 body carrying an error message that names a credit/key problem
    # -> unified auth_or_credit halt.
    adapter = _adapter()
    client = _FakeClient([_resp(200, {"error": "API credit limit reached"})])
    row = _run(adapter.enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "credit_exhausted" in row["error"]
    assert adapter._halted is True


def test_seokicks_non_auth_error_envelope_no_halt():
    # A 200 error message that is NOT auth/credit related -> ok=False (UNKNOWN) but does
    # NOT latch the halt (a transient backend message must not stop the whole run).
    adapter = _adapter()
    client = _FakeClient([_resp(200, {"error": "temporary backend failure"})])
    row = _run(adapter.enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert adapter._halted is False                   # not halted
    assert "request unsuccessful" in row["error"]


def test_seokicks_429_retry_then_success(monkeypatch):
    _patch_sleep(monkeypatch)
    client = _FakeClient([_resp(429), _resp(200, _overview())])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True                          # the 429-only retry recovered
    assert row["data"]["domainpop"] == 546
    assert client.calls == 2                           # one retry


def test_seokicks_429_exhausted(monkeypatch):
    _patch_sleep(monkeypatch)
    client = _FakeClient([_resp(429)])                 # repeats -> always 429
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "rate_limited" in row["error"]
    assert client.calls == SeokicksAdapter.MAX_429_RETRIES + 1   # 1 + 3 = 4 attempts


def test_seokicks_transport_error_single_attempt():
    # Transport error is NOT retried (429-only loop doesn't catch it) -> propagates to
    # _guarded -> clean ok=False, exactly ONE attempt.
    client = _FakeClient([httpx.ConnectError("connection refused")])
    adapter = _adapter()
    row = _run(adapter._guarded(client, "example.com"))
    assert row["ok"] is False
    assert row["adapter"] == "seokicks"
    assert "http error" in row["error"]
    assert client.calls == 1                           # single attempt, no retry


def test_seokicks_http_5xx_single_attempt():
    # A 5xx is single-attempt UNKNOWN (not retried by the 429-only loop, not a halt).
    client = _FakeClient([_resp(503)])
    adapter = _adapter()
    row = _run(adapter.enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "HTTP 503" in row["error"]
    assert adapter._halted is False
    assert client.calls == 1


def test_seokicks_count_sentinel_and_parsing():
    # Direct unit checks on the count coercion: missing/blank/negative -> None, real 0 kept.
    assert _count(None) is None
    assert _count("") is None
    assert _count("not-a-number") is None
    assert _count("-1") is None                        # negative sentinel -> None
    assert _count("0") == 0                            # real zero preserved
    assert _count("14853") == 14853
    assert _count(546) == 546                          # already-int passes through
