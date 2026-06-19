"""Tests for the Majestic Million adapter (app/enrichment/majestic_million.py).

No network, no DB. Two seams keep it offline:
  - the CSV PARSER is exercised directly via the real _build_index_from_text (canned CSV
    string), mirroring how the per-domain adapter tests feed canned payloads;
  - the END-TO-END refresh/lookup path is exercised by monkeypatching refresh() to load
    a canned index, so enrich_many's present/absent stamping is covered without a download.

Async is driven with asyncio.run() inside sync tests (no pytest-asyncio), same as
test_dataset. Coverage: header-keyed parse of the documented columns; GlobalRank ordering
(lower=stronger) preserved; case-insensitive match; miss -> present=False ok=True row;
hit -> fields + present=True; rank sentinel (0/blank/negative -> None, never 0); count
sentinel (real 0 preserved, blank/negative -> None); malformed rows skipped; empty/HTML
parse -> DatasetUnavailable -> clean ok=False batch; _is_fresh gating skips re-download.
"""
import asyncio

from app.enrichment.dataset import DatasetUnavailable
from app.enrichment.majestic_million import MajesticMillionAdapter, _rank, _count


# A small canned CSV with the real Majestic column header. Includes: two clean rows,
# a subdomain row (distinct from any apex -> exercises exact-match), a malformed short
# row, and a row with a blank GlobalRank (skipped).
_CSV = (
    "GlobalRank,TldRank,Domain,TLD,RefSubNets,RefIPs,IDN_Domain,IDN_TLD,"
    "PrevGlobalRank,PrevTldRank,PrevRefSubNets,PrevRefIPs\n"
    "1,1,google.com,com,500000,600000,google.com,com,1,1,499000,599000\n"
    "42,7,example.com,com,1234,1500,example.com,com,40,7,1200,1480\n"
    "100,30,en.wikipedia.org,org,9000,9500,en.wikipedia.org,org,101,31,8900,9400\n"
    "malformed,row,with,too,few\n"
    ",5,blankrank.com,com,10,12,blankrank.com,com,,5,9,11\n"
)


def _run(coro):
    return asyncio.run(coro)


def _adapter_with_index(csv_text=_CSV):
    """Build an adapter whose refresh() loads a canned index from csv_text — no network."""
    a = MajesticMillionAdapter()

    async def _fake_refresh():
        a._index = a._build_index_from_text(csv_text)

    a.refresh = _fake_refresh        # type: ignore[assignment]
    return a


# --- parser-level tests (real _build_index_from_text) -------------------

def test_parser_keys_by_header_and_extracts_fields():
    a = MajesticMillionAdapter()
    idx = a._build_index_from_text(_CSV)
    assert idx["example.com"] == {
        "global_rank": 42, "tld_rank": 7, "ref_subnets": 1234, "ref_ips": 1500}
    assert idx["google.com"]["global_rank"] == 1


def test_parser_lower_rank_is_stronger_ordering_preserved():
    a = MajesticMillionAdapter()
    idx = a._build_index_from_text(_CSV)
    # google.com (1) is stronger than example.com (42): the raw ints are preserved so the
    # scorer can apply "lower is better" itself; the adapter does not invert.
    assert idx["google.com"]["global_rank"] < idx["example.com"]["global_rank"]


def test_parser_skips_malformed_and_blank_rank_rows():
    a = MajesticMillionAdapter()
    idx = a._build_index_from_text(_CSV)
    # The "malformed,row,with,too,few" line has no GlobalRank/Domain mapping -> skipped.
    assert "with" not in idx and "malformed" not in idx
    # blankrank.com has an empty GlobalRank -> skipped (no usable authority signal).
    assert "blankrank.com" not in idx


def test_parser_indexes_subdomain_rows_as_their_own_key():
    # The CSV is hostname-keyed: en.wikipedia.org is its own row. v1 exact-match means
    # apex "wikipedia.org" is NOT synthesized from it (documented limitation).
    a = MajesticMillionAdapter()
    idx = a._build_index_from_text(_CSV)
    assert "en.wikipedia.org" in idx
    assert "wikipedia.org" not in idx


# --- end-to-end enrich_many tests (canned index via fake refresh) -------

def test_hit_returns_fields_and_present_true():
    (row,) = _run(_adapter_with_index().enrich_many(["example.com"]))
    assert row["ok"] is True and row["error"] is None
    assert row["adapter"] == "majestic_million"
    assert row["data"] == {
        "global_rank": 42, "tld_rank": 7, "ref_subnets": 1234, "ref_ips": 1500,
        "present": True}


def test_miss_is_present_false_ok_true():
    # A domain not in the top ~1M -> a real measurement ("low global authority"), ok=True.
    (row,) = _run(_adapter_with_index().enrich_many(["nowhere-unranked.com"]))
    assert row["ok"] is True
    assert row["data"] == {"present": False}


def test_lookup_is_case_insensitive():
    (row,) = _run(_adapter_with_index().enrich_many(["Example.COM"]))
    assert row["ok"] is True
    assert row["data"]["global_rank"] == 42 and row["data"]["present"] is True


def test_subdomain_apex_is_a_miss_v1_limitation():
    # wikipedia.org is absent (only en.wikipedia.org is in the corpus) -> present=False.
    (row,) = _run(_adapter_with_index().enrich_many(["wikipedia.org"]))
    assert row["ok"] is True
    assert row["data"] == {"present": False}


def test_empty_parse_degrades_to_clean_ok_false_batch(monkeypatch, tmp_path):
    # A 200 that is actually an HTML error page -> the REAL refresh()/_build_index parses
    # 0 usable rows -> DatasetUnavailable -> base marks the whole batch ok=False,
    # non-persistable. Drives the production path (not the _build_index_from_text seam):
    # _download is stubbed to drop an HTML body where the CSV should be.
    a = MajesticMillionAdapter(scratch_dir=tmp_path)

    async def _fake_download(url, d, headers=None):
        from pathlib import Path
        Path(d).write_text("<html>error</html>", encoding="utf-8")
        return Path(d)

    monkeypatch.setattr(a, "_download", _fake_download)
    results = _run(a.enrich_many(["example.com", "google.com"]))
    assert len(results) == 2
    assert all(r["ok"] is False for r in results)
    assert all("dataset unavailable" in r["error"] for r in results)
    assert all(r.get("refresh_error") for r in results)
    assert a.refresh_error is not None


def test_empty_index_raises_dataset_unavailable_directly():
    a = MajesticMillionAdapter()
    try:
        a._build_index_from_text("GlobalRank,Domain\n")   # header only, no data rows
        # _build_index_from_text itself does not raise (it's the seam); the production
        # _build_index does. Assert the production guard instead:
    except DatasetUnavailable:
        pass
    # Production guard: _build_index raises on a zero-row parse.
    import tempfile, os
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as tf:
        tf.write("GlobalRank,TldRank,Domain,TLD,RefSubNets,RefIPs\n")  # header only
        path = tf.name
    try:
        raised = False
        try:
            a._build_index(path)
        except DatasetUnavailable:
            raised = True
        assert raised
    finally:
        os.unlink(path)


# --- _is_fresh download-skip gating -------------------------------------

def test_refresh_skips_download_when_fresh(monkeypatch, tmp_path):
    # If a same-day copy exists, refresh() must NOT call _download — it reuses the file
    # and builds the index from it.
    a = MajesticMillionAdapter(scratch_dir=tmp_path)
    dest = a._scratch_path(a.CSV_FILENAME)
    dest.write_text(_CSV, encoding="utf-8")            # a "fresh" on-disk corpus

    called = {"download": False}

    async def _no_download(*args, **kwargs):
        called["download"] = True
        raise AssertionError("must not download when a fresh copy exists")

    monkeypatch.setattr(a, "_download", _no_download)
    _run(a.refresh())
    assert called["download"] is False
    assert a._index["example.com"]["global_rank"] == 42


def test_refresh_downloads_when_stale(monkeypatch, tmp_path):
    # No on-disk copy -> refresh() calls _download (which we stub to drop the CSV in place).
    a = MajesticMillionAdapter(scratch_dir=tmp_path)
    dest = a._scratch_path(a.CSV_FILENAME)

    called = {"download": False}

    async def _fake_download(url, d, headers=None):
        called["download"] = True
        from pathlib import Path
        Path(d).write_text(_CSV, encoding="utf-8")
        return Path(d)

    monkeypatch.setattr(a, "_download", _fake_download)
    _run(a.refresh())
    assert called["download"] is True
    assert a._index["google.com"]["global_rank"] == 1


# --- sentinel unit checks ----------------------------------------------

def test_rank_sentinel_and_parsing():
    assert _rank("1") == 1
    assert _rank("42") == 42
    assert _rank("0") is None             # 0 is not a real rank -> None, NOT 0
    assert _rank("-3") is None
    assert _rank("") is None
    assert _rank(None) is None
    assert _rank("not-a-number") is None
    assert _rank(7) == 7
    assert _rank("12.0") == 12            # tolerant of a float-looking string


def test_count_sentinel_and_parsing():
    assert _count("1234") == 1234
    assert _count("0") == 0               # a real measured 0 is preserved (distinct from None)
    assert _count("-1") is None
    assert _count("") is None
    assert _count(None) is None
    assert _count("nope") is None
