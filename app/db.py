"""SQLite persistence layer.

Three tables:
  domains  - the list of domains being monitored (one row per apex domain)
  checks   - history of every check run (one row per domain+target combo per run)
  settings - key/value config (schedule interval, last run, etc.)
"""
import sqlite3
import json
import os
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

DB_PATH = Path(os.environ.get("DOMAIN_MONITOR_DB", Path(__file__).parent.parent / "data" / "monitor.db"))


def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS domains (
            domain TEXT PRIMARY KEY,
            added_at TEXT NOT NULL,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT NOT NULL,
            target TEXT NOT NULL,            -- 'apex' or 'www'
            checked_at TEXT NOT NULL,
            status TEXT NOT NULL,            -- 'operational' | 'down' | 'redirect' | 'error'
            http_code INTEGER,
            final_url TEXT,
            response_ms INTEGER,
            redirect_chain TEXT,             -- JSON array
            dns_a_records TEXT,              -- JSON array
            ssl_issuer TEXT,
            ssl_expires_at TEXT,
            ssl_days_left INTEGER,
            error TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_checks_domain ON checks(domain, checked_at DESC);
        CREATE INDEX IF NOT EXISTS idx_checks_recent ON checks(checked_at DESC);

        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            kind TEXT NOT NULL,              -- 'manual' | 'scheduled'
            total INTEGER,
            operational INTEGER,
            down INTEGER,
            errored INTEGER
        );
        """)

        # Migrate legacy data: PROTECTED status is gone — reclassify as operational.
        # By definition, those rows had a server respond, so they're OPERATIONAL under the new rules.
        conn.execute("UPDATE checks SET status = 'operational' WHERE status = 'protected'")


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


# --- Domains ---

def add_domains(domain_list):
    """Add multiple domains. Returns (added, skipped) counts."""
    added = 0
    skipped = 0
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        for d in domain_list:
            d = normalize_domain(d)
            if not d:
                skipped += 1
                continue
            try:
                conn.execute("INSERT INTO domains (domain, added_at) VALUES (?, ?)", (d, now))
                added += 1
            except sqlite3.IntegrityError:
                skipped += 1
    return added, skipped


def get_all_domains():
    with get_conn() as conn:
        rows = conn.execute("SELECT domain, added_at, notes FROM domains ORDER BY domain").fetchall()
    return [dict(r) for r in rows]


def delete_domain(domain):
    with get_conn() as conn:
        conn.execute("DELETE FROM domains WHERE domain = ?", (domain,))
        conn.execute("DELETE FROM checks WHERE domain = ?", (domain,))


def clear_all_domains():
    with get_conn() as conn:
        conn.execute("DELETE FROM domains")
        conn.execute("DELETE FROM checks")


def normalize_domain(s):
    """Strip schemes, www, trailing slashes, whitespace. Return apex domain or empty string."""
    if not s:
        return ""
    s = s.strip().lower()
    # remove scheme
    for prefix in ("https://", "http://"):
        if s.startswith(prefix):
            s = s[len(prefix):]
    # remove path
    s = s.split("/")[0].split("?")[0]
    # remove leading www.
    if s.startswith("www."):
        s = s[4:]
    # basic validity
    if not s or " " in s or "." not in s:
        return ""
    return s


# --- Checks ---

def save_check(result):
    """Persist one check result dict."""
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO checks (
                domain, target, checked_at, status, http_code, final_url,
                response_ms, redirect_chain, dns_a_records,
                ssl_issuer, ssl_expires_at, ssl_days_left, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            result["domain"],
            result["target"],
            result["checked_at"],
            result["status"],
            result.get("http_code"),
            result.get("final_url"),
            result.get("response_ms"),
            json.dumps(result.get("redirect_chain") or []),
            json.dumps(result.get("dns_a_records") or []),
            result.get("ssl_issuer"),
            result.get("ssl_expires_at"),
            result.get("ssl_days_left"),
            result.get("error"),
        ))


def save_run(run):
    """Persist run summary."""
    with get_conn() as conn:
        cur = conn.execute("""
            INSERT INTO runs (started_at, finished_at, kind, total, operational, down, errored)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            run["started_at"], run["finished_at"], run["kind"],
            run["total"], run["operational"], run["down"], run["errored"]
        ))
        return cur.lastrowid


def get_latest_results():
    """For each domain+target combo, return the most recent check."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT c.* FROM checks c
            INNER JOIN (
                SELECT domain, target, MAX(id) AS max_id
                FROM checks
                GROUP BY domain, target
            ) latest ON c.id = latest.max_id
            ORDER BY c.domain, c.target
        """).fetchall()
    return [_row_to_check_dict(r) for r in rows]


def get_history_for_domain(domain, limit=50):
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM checks
            WHERE domain = ?
            ORDER BY checked_at DESC
            LIMIT ?
        """, (domain, limit)).fetchall()
    return [_row_to_check_dict(r) for r in rows]


def get_recent_runs(limit=20):
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM runs ORDER BY id DESC LIMIT ?
        """, (limit,)).fetchall()
    return [dict(r) for r in rows]


def _row_to_check_dict(row):
    d = dict(row)
    d["redirect_chain"] = json.loads(d.get("redirect_chain") or "[]")
    d["dns_a_records"] = json.loads(d.get("dns_a_records") or "[]")
    return d


# --- Settings ---

def get_setting(key, default=None):
    with get_conn() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key, value):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO settings (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (key, str(value)))
