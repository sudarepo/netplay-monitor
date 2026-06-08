"""Tests for the DatasetAdapter base (app/enrichment/dataset.py).

No network and no DB: a DatasetAdapter's behavior is fully exercisable with an
in-memory fake subclass. These cover the decisions baked into the base —

  - present/absent lookups and the uniform `present` stamp (hit -> present=True
    alongside the payload; miss -> {"present": False}, still an ok=True row);
  - the refresh-failure posture: a clean ok=False batch, every row tagged
    non-persistable (`refresh_error`), and adapter.refresh_error exposed for the
    orchestrator's halt-vs-skip decision;
  - the guarded progress callback (a buggy callback never kills the run);
  - the empty-domains short-circuit (no corpus download when there's nothing to do).

Async is driven with asyncio.run() inside sync test functions, so no
pytest-asyncio dependency is needed.
"""
import asyncio

from app.enrichment.dataset import DatasetAdapter, DatasetUnavailable


class _FakeDataset(DatasetAdapter):
    """A tiny in-memory corpus; refresh() is a no-op counter, never touches the
    network."""
    name = "fake_dataset"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.refresh_calls = 0

    async def refresh(self):
        self.refresh_calls += 1

    def lookup(self, domain):
        return {"example.com": {"rank": 42}}.get(domain)  # dict on hit, None on miss


class _DownDataset(DatasetAdapter):
    """Source unreachable: refresh() raises DatasetUnavailable."""
    name = "down_dataset"

    async def refresh(self):
        raise DatasetUnavailable("down_dataset: cannot reach https://example.invalid")

    def lookup(self, domain):  # pragma: no cover - must never run after a failed refresh
        raise AssertionError("lookup must not run after a failed refresh")


def _run(coro):
    return asyncio.run(coro)


def test_hit_stamps_present_true_alongside_payload():
    (row,) = _run(_FakeDataset().enrich_many(["example.com"]))
    assert row["ok"] is True and row["error"] is None
    assert row["adapter"] == "fake_dataset"
    assert row["data"] == {"rank": 42, "present": True}
    assert "refresh_error" not in row


def test_miss_stamps_present_false_and_is_ok():
    (row,) = _run(_FakeDataset().enrich_many(["missing.com"]))
    assert row["ok"] is True  # a successful lookup that authoritatively found nothing
    assert row["data"] == {"present": False}


def test_refresh_failure_is_clean_and_non_persistable():
    adapter = _DownDataset()
    results = _run(adapter.enrich_many(["a.com", "b.com"]))
    assert len(results) == 2
    assert all(r["ok"] is False for r in results)
    assert all("dataset unavailable" in r["error"] for r in results)
    # Every row tagged non-persistable, and the dataset-level error is exposed so
    # the orchestrator can decide halt-vs-skip.
    assert all(r.get("refresh_error") for r in results)
    assert adapter.refresh_error is not None


def test_progress_callback_fires_once_and_is_guarded():
    seen = []

    def on_progress(done, total):
        seen.append((done, total))
        raise RuntimeError("a buggy callback must not kill the run")

    (row,) = _run(_FakeDataset().enrich_many(["example.com"], on_progress=on_progress))
    assert row["ok"] is True
    assert seen == [(1, 1)]  # fired exactly once despite raising


def test_empty_domains_short_circuits_without_refresh():
    adapter = _FakeDataset()
    assert _run(adapter.enrich_many([])) == []
    assert _run(adapter.enrich_many(["", None])) == []  # falsy entries filtered out
    assert adapter.refresh_calls == 0  # no corpus download when there's nothing to do
