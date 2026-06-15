"""Tests for the DomScan adapter (app/enrichment/domscan.py).

No network: enrich_one takes the httpx client as an argument, so a fake client
serving canned JSON (or raising) exercises every path. The adapter makes up to three
GETs per enrich_one in order — call 1 = /health, call 2 = /reputation, call 3 =
/status — so the fake client serves queued responses in order and client.calls lets
each test assert exactly how many axes were attempted (the 402/429/transport
short-circuits stop early).

These cover: the three independent axes (no inter-endpoint gating); local degradation
(one axis fails -> its fields None, ok=True); the all-axes-fail guard (-> ok=False, not
an all-None "measurement"); the typed-402 in-adapter HARD-STOP (token + latch + the
no-HTTP short-circuit on the next domain); 429-aborts-whole-domain vs 5xx-degrades;
the single-attempt discipline (no retry helper); the None != 0 discipline; and the two
corrections — tlds_checked falls back to None when absent, and the x-api-key header.

Async is driven with asyncio.run(), matching the other adapter test files. No
asyncio.sleep monkeypatch is needed — domscan does not use _get_with_retries, so there
are no backoff sleeps.
"""
import asyncio

import httpx

from app.enrichment.domscan import DomscanAdapter


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


def _resp(status, json_obj=None, headers=None):
    return _FakeResponse(status, json_obj, headers)


def _adapter():
    # Pass a dummy key so the keyless-warning guard stays quiet (domscan is keyed).
    return DomscanAdapter(api_key="test-key")


def _health_ok(**over):
    body = {"health_score": 82, "health_grade": "B",
            "health_checks": {"dns": "ok", "http": "ok"}}
    body.update(over)
    return body


def _rep_ok(**over):
    body = {"reputation_score": 70, "reputation_grade": "B", "risk_level": "low",
            "grade_capped_by_parking": False, "reputation_factors": {"blacklists": 0}}
    body.update(over)
    return body


def _status_ok(**over):
    body = {"tld_count": 3, "tlds_registered": ["com", "net", "org"], "tlds_checked": 50}
    body.update(over)
    return body


def _credit_402():
    # Typed, machine-readable exhaustion: dedicated HTTP 402 + error.code +
    # credits_remaining/credits_required body + X-Credits-Remaining header.
    body = {"error": {"code": "INSUFFICIENT_CREDITS"},
            "credits_remaining": 0, "credits_required": 5}
    return _resp(402, body, headers={"x-credits-remaining": "0"})


def test_domscan_happy_all_three():
    # All three axes 200 -> every field populated, ok=True, three calls made.
    client = _FakeClient([_resp(200, _health_ok()),
                          _resp(200, _rep_ok()),
                          _resp(200, _status_ok())])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True and row["error"] is None
    assert row["adapter"] == "domscan"
    d = row["data"]
    assert d["health_score"] == 82 and d["health_grade"] == "B"
    assert d["reputation_score"] == 70 and d["risk_level"] == "low"
    assert d["grade_capped_by_parking"] is False
    assert d["tld_count"] == 3 and d["tlds_registered"] == ["com", "net", "org"]
    assert d["tlds_checked"] == 50
    assert client.calls == 3


def test_domscan_health_degrades_local():
    # Health 503 while reputation + status succeed -> health fields None, ok=True
    # (no inter-endpoint gating). All three axes are still ATTEMPTED.
    client = _FakeClient([_resp(503),
                          _resp(200, _rep_ok()),
                          _resp(200, _status_ok())])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True
    d = row["data"]
    assert d["health_score"] is None and d["health_grade"] is None
    assert d["health_checks"] is None
    assert d["reputation_score"] == 70        # sibling axis preserved
    assert d["tld_count"] == 3
    assert client.calls == 3


def test_domscan_reputation_degrades_local():
    client = _FakeClient([_resp(200, _health_ok()),
                          _resp(500),
                          _resp(200, _status_ok())])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True
    d = row["data"]
    assert d["reputation_score"] is None and d["risk_level"] is None
    assert d["grade_capped_by_parking"] is None    # axis failed -> None, not False
    assert d["health_score"] == 82 and d["tld_count"] == 3
    assert client.calls == 3


def test_domscan_status_degrades_local():
    client = _FakeClient([_resp(200, _health_ok()),
                          _resp(200, _rep_ok()),
                          _resp(502)])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True
    d = row["data"]
    assert d["tld_count"] is None and d["tlds_registered"] is None
    assert d["tlds_checked"] is None
    assert d["health_score"] == 82
    assert client.calls == 3


def test_domscan_all_axes_fail_is_ok_false():
    # Nothing measured -> ok=False (UNKNOWN), NOT an all-None "measurement," so
    # domains_missing() re-runs it. All three axes attempted before the guard fires.
    client = _FakeClient([_resp(500), _resp(500), _resp(500)])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "all signals unavailable" in row["error"]
    assert client.calls == 3


def test_domscan_auth_all_fail_is_ok_false():
    # A bad key fails identically on every axis -> all-axes guard -> ok=False. (The
    # Phase 2.5 auth-latch would short-circuit the wasted calls; not built yet, so all
    # three are still attempted.)
    client = _FakeClient([_resp(401), _resp(401), _resp(401)])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "all signals unavailable" in row["error"]
    assert client.calls == 3


def test_domscan_402_halts_first_axis():
    # Typed 402 on the FIRST axis -> immediate ok=False with the credit_exhausted
    # token; no further axes called; the latch is set.
    adapter = _adapter()
    client = _FakeClient([_credit_402()])
    row = _run(adapter.enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "credit_exhausted" in row["error"]
    assert "INSUFFICIENT_CREDITS" in row["error"]
    assert adapter._halted is True
    assert client.calls == 1                  # stopped at axis 1, no reputation/status


def test_domscan_402_midway_discards_partial():
    # Health 200, then 402 on reputation -> the partial health data is DISCARDED and
    # the whole domain is ok=False + credit_exhausted, so the re-run redoes it clean.
    adapter = _adapter()
    client = _FakeClient([_resp(200, _health_ok()), _credit_402()])
    row = _run(adapter.enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "credit_exhausted" in row["error"]
    assert row["data"] == {}                  # partial health NOT surfaced
    assert adapter._halted is True
    assert client.calls == 2                  # status (axis 3) never attempted


def test_domscan_halt_short_circuits_next_domain():
    # After a 402 latches the adapter, the NEXT domain makes NO HTTP call at all.
    adapter = _adapter()
    client = _FakeClient([_credit_402()])
    _run(adapter.enrich_one(client, "first.com"))
    calls_after_first = client.calls
    row = _run(adapter.enrich_one(client, "second.com"))
    assert row["ok"] is False
    assert "skipped" in row["error"] and "halted" in row["error"]
    assert client.calls == calls_after_first  # zero additional calls for the 2nd domain


def test_domscan_429_aborts_whole_domain():
    # 429 on the first axis -> whole-domain ok=False (recovered on re-run), no further
    # axes. Contrast with 5xx, which degrades locally.
    client = _FakeClient([_resp(429)])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "rate_limited" in row["error"]
    assert client.calls == 1                  # aborted, reputation/status not attempted


def test_domscan_429_midway_aborts_whole_domain():
    # Health 200 then 429 on reputation -> whole domain ok=False (the good health axis
    # is discarded; re-run recovers it cleanly). Status never attempted.
    client = _FakeClient([_resp(200, _health_ok()), _resp(429)])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is False
    assert "rate_limited" in row["error"]
    assert client.calls == 2


def test_domscan_transport_error_single_attempt():
    # A transport error propagates to _guarded -> clean ok=False, exactly ONE attempt
    # (no retry helper) — the contrast with the free adapters' multi-attempt path.
    client = _FakeClient([httpx.ConnectError("connection refused")])
    adapter = _adapter()
    row = _run(adapter._guarded(client, "example.com"))
    assert row["ok"] is False
    assert row["adapter"] == "domscan"
    assert "http error" in row["error"]       # _guarded's httpx.HTTPError branch
    assert client.calls == 1                  # SINGLE attempt, no retry


def test_domscan_non_json_degrades_local():
    # 200 but a non-JSON body on health -> that axis None, ok=True (siblings 200).
    client = _FakeClient([_resp(200, None),    # json() raises
                          _resp(200, _rep_ok()),
                          _resp(200, _status_ok())])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["ok"] is True
    assert row["data"]["health_score"] is None
    assert row["data"]["reputation_score"] == 70
    assert client.calls == 3


def test_domscan_none_not_zero_discipline():
    # A real measured 0 stays 0 (health_score 0 = "measured as bad"); a FAILED axis is
    # None. The two must never collapse.
    client = _FakeClient([_resp(200, _health_ok(health_score=0)),
                          _resp(500),                       # reputation axis fails
                          _resp(200, _status_ok(tld_count=0, tlds_registered=[]))])
    row = _run(_adapter().enrich_one(client, "example.com"))
    d = row["data"]
    assert d["health_score"] == 0             # measured zero, preserved
    assert d["reputation_score"] is None      # failed axis, unknown
    assert d["tld_count"] == 0                # measured zero TLDs, preserved
    assert d["tlds_registered"] == []


def test_domscan_tlds_checked_absent_is_none():
    # Correction (B): when tlds_checked is absent, fall back to None — NOT
    # len(CURATED_TLDS). tld_count still derives from the registered list length.
    client = _FakeClient([_resp(200, _health_ok()),
                          _resp(200, _rep_ok()),
                          _resp(200, {"tlds_registered": ["com", "net"]})])
    row = _run(_adapter().enrich_one(client, "example.com"))
    d = row["data"]
    assert d["tlds_checked"] is None
    assert d["tld_count"] == 2                 # len of the registered list
    assert d["tlds_registered"] == ["com", "net"]


def test_domscan_headers_carry_x_api_key():
    # Auth is via the x-api-key header (not a query param like whoxy).
    h = _adapter()._headers()
    assert h["x-api-key"] == "test-key"
    assert "User-Agent" in h                   # base headers still present


def test_domscan_grade_capped_false_preserved():
    # grade_capped_by_parking is a bool: a measured False is preserved (only a FAILED
    # reputation axis makes it None).
    client = _FakeClient([_resp(200, _health_ok()),
                          _resp(200, _rep_ok(grade_capped_by_parking=True)),
                          _resp(200, _status_ok())])
    row = _run(_adapter().enrich_one(client, "example.com"))
    assert row["data"]["grade_capped_by_parking"] is True
