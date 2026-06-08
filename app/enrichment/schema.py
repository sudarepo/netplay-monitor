"""Enrichment persistence layer.

Stores one current row per (domain, adapter) with the adapter's normalized
payload as a JSON TEXT column. Mirrors db.py: same get_conn() context manager,
same idempotent CREATE TABLE IF NOT EXISTS + try/except ALTER migration style,
same json.dumps for structured columns, same ISO8601 UTC timestamps.

This module reuses db.get_conn() so it shares the same database file, WAL mode,
and connection settings as the rest of netplay-monitor. Import path assumes this
lives at app/enrichment/schema.py alongside app/db.py.
"""
import json
from datetime import datetime

# Reuse the existing connection manager so we share one DB + WAL config.
from app.db import get_conn


def init_enrichment_schema():
    """Create the enrichment table and indexes. Idempotent; safe to call on every
    startup, exactly like db.init_db()."""
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS enrichment (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            domain      TEXT NOT NULL,
            adapter     TEXT NOT NULL,
            fetched_at  TEXT NOT NULL,
            ok          INTEGER NOT NULL DEFAULT 0,
            error       TEXT,
            data        TEXT NOT NULL DEFAULT '{}',
            UNIQUE(domain, adapter)
        );
        CREATE INDEX IF NOT EXISTS idx_enrichment_domain ON enrichment(domain);
        CREATE INDEX IF NOT EXISTS idx_enrichment_adapter ON enrichment(adapter, fetched_at DESC);
        """)
        # Future column additions go here, each in its own try/except so they are
        # independent and idempotent — same convention as db.init_db().
        for ddl in (
            # "ALTER TABLE enrichment ADD COLUMN credits_used INTEGER",
        ):
            try:
                conn.execute(ddl)
            except Exception:
                pass


def save_enrichment(result):
    """Upsert one adapter result dict (the shape produced by EnrichmentAdapter).

    Last-write-wins on (domain, adapter): re-enriching a domain replaces its prior
    row for that adapter. Keep history out of this table; if we ever want history,
    drop the UNIQUE constraint and add a 'latest' flag — but current need is a
    single current value per pair.

    A result tagged with `refresh_error` (a row from a failed DatasetAdapter
    refresh) is refused: it reports a source outage, not real per-domain data, and
    persisting it would clobber a prior good row under last-write-wins. This is the
    persistence-side guard; the orchestrator should also not call save on a failed
    batch (belt-and-suspenders).
    """
    if result.get("refresh_error"):
        return
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO enrichment (domain, adapter, fetched_at, ok, error, data)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(domain, adapter) DO UPDATE SET
                fetched_at = excluded.fetched_at,
                ok         = excluded.ok,
                error      = excluded.error,
                data       = excluded.data
        """, (
            result.get("domain", ""),
            result.get("adapter", ""),
            result.get("fetched_at", datetime.utcnow().isoformat()),
            1 if result.get("ok") else 0,
            result.get("error"),
            json.dumps(result.get("data") or {}),
        ))


def save_enrichment_batch(results):
    """Save a list of adapter result dicts in one transaction.

    Rows tagged with `refresh_error` (from a failed DatasetAdapter refresh) are
    refused: they report a source outage, not per-domain data, and persisting them
    would clobber prior good rows under last-write-wins. This backs up the
    orchestrator, which also should not call save on a failed batch.
    """
    skipped = 0
    with get_conn() as conn:
        for result in results:
            if result.get("refresh_error"):
                skipped += 1
                continue
            conn.execute("""
                INSERT INTO enrichment (domain, adapter, fetched_at, ok, error, data)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(domain, adapter) DO UPDATE SET
                    fetched_at = excluded.fetched_at,
                    ok         = excluded.ok,
                    error      = excluded.error,
                    data       = excluded.data
            """, (
                result.get("domain", ""),
                result.get("adapter", ""),
                result.get("fetched_at", datetime.utcnow().isoformat()),
                1 if result.get("ok") else 0,
                result.get("error"),
                json.dumps(result.get("data") or {}),
            ))
    if skipped:
        print(f"[enrichment] refused to persist {skipped} row(s) from a failed dataset refresh")


def get_enrichment(domain, adapter=None):
    """Return enrichment row(s) for a domain.

    If adapter is given, return that single row (or None). Otherwise return a dict
    keyed by adapter name. JSON `data` is decoded back into a dict.
    """
    with get_conn() as conn:
        if adapter:
            row = conn.execute(
                "SELECT * FROM enrichment WHERE domain = ? AND adapter = ?",
                (domain, adapter),
            ).fetchone()
            return _decode(row) if row else None
        rows = conn.execute(
            "SELECT * FROM enrichment WHERE domain = ?", (domain,)
        ).fetchall()
    return {r["adapter"]: _decode(r) for r in rows}


def get_all_enrichment(adapter=None):
    """Return all enrichment rows, optionally filtered to one adapter.

    Returns a list of decoded row dicts ordered by domain.
    """
    with get_conn() as conn:
        if adapter:
            rows = conn.execute(
                "SELECT * FROM enrichment WHERE adapter = ? ORDER BY domain",
                (adapter,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM enrichment ORDER BY domain, adapter"
            ).fetchall()
    return [_decode(r) for r in rows]


def domains_missing(adapter, candidate_domains, include_absent=False):
    """Given a list of domains, return those with no successful row for `adapter`.

    Useful for resumable runs: enrich only what hasn't already succeeded, so a
    re-run after a credit exhaustion or crash doesn't re-spend on done domains.

    Dataset "miss" rows count as enriched. A DatasetAdapter records a domain that
    is absent from its corpus as a SUCCESSFUL row (ok=1) with data
    {"present": False} — the lookup ran and authoritatively found nothing. So by
    default such a domain is treated as done and is NOT re-queried; this is correct
    for resumability and cost control.

    Re-checking against a fresher corpus: a domain that gets listed AFTER the last
    refresh would, by the rule above, never be picked up again. Pass
    include_absent=True to treat absent rows (ok=1, present=False) as missing so
    they are re-queried against the new corpus. (Hits are still considered done; to
    also re-pull changed hit values — e.g. a moved Majestic rank — run the adapter
    over the full domain list directly rather than filtering through this helper.)
    """
    candidate = {d for d in candidate_domains if d}
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT domain, data FROM enrichment WHERE adapter = ? AND ok = 1",
            (adapter,),
        ).fetchall()
    done = set()
    for r in rows:
        if include_absent:
            # An explicit present=False (a dataset miss) is treated as still-missing
            # so it gets re-queried; anything else with ok=1 counts as done.
            try:
                present = json.loads(r["data"] or "{}").get("present")
            except (ValueError, TypeError):
                present = None
            if present is False:
                continue
        done.add(r["domain"])
    return sorted(candidate - done)


def _decode(row):
    """sqlite3.Row -> dict, decoding the JSON data column and ok flag."""
    d = dict(row)
    try:
        d["data"] = json.loads(d.get("data") or "{}")
    except (ValueError, TypeError):
        d["data"] = {}
    d["ok"] = bool(d.get("ok"))
    return d
