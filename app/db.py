"""SQLite persistence layer for Domain Monitor."""
import sqlite3
import json
import os
from contextlib import contextmanager
from datetime import datetime, date, timedelta
from pathlib import Path

DB_PATH = Path(os.environ.get("DOMAIN_MONITOR_DB", Path(__file__).parent.parent / "data" / "monitor.db"))


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


def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS domains (
            domain TEXT PRIMARY KEY,
            added_at TEXT NOT NULL,
            expiry_date TEXT,
            domain_status TEXT DEFAULT 'ACTIVE',
            renewed_at TEXT,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT NOT NULL,
            target TEXT NOT NULL,
            checked_at TEXT NOT NULL,
            status TEXT NOT NULL,
            http_code INTEGER,
            final_url TEXT,
            response_ms INTEGER,
            redirect_chain TEXT,
            dns_a_records TEXT,
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
            kind TEXT NOT NULL,
            total INTEGER,
            operational INTEGER,
            down INTEGER,
            errored INTEGER
        );
        """)
        # Migrate: add expiry_date column if missing
        try:
            conn.execute("ALTER TABLE domains ADD COLUMN expiry_date TEXT")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE domains ADD COLUMN domain_status TEXT DEFAULT 'ACTIVE'")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE domains ADD COLUMN renewed_at TEXT")
        except Exception:
            pass
        # Reclassify legacy PROTECTED status
        conn.execute("UPDATE checks SET status = 'operational' WHERE status = 'protected'")


# --- Domain management ---

def add_domain(domain):
    """Add a single domain. Returns True if new, False if already existed."""
    domain = _normalize(domain)
    if not domain:
        return False
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        try:
            conn.execute("INSERT INTO domains (domain, added_at) VALUES (?, ?)", (domain, now))
            return True
        except sqlite3.IntegrityError:
            return False


def get_all_domains():
    with get_conn() as conn:
        rows = conn.execute("SELECT domain, added_at, expiry_date, domain_status, notes FROM domains ORDER BY domain").fetchall()
    return [dict(r) for r in rows]


def get_active_domains():
    """Return domains that are not expired (active or expiring-soon). Exclude expired ones."""
    today = date.today().isoformat()
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT domain FROM domains
            WHERE domain_status IN ('ACTIVE', 'EXPIRING SOON') OR expiry_date IS NULL
            ORDER BY domain
        """, (today,)).fetchall()
    return [r["domain"] for r in rows]


def delete_domain(domain):
    with get_conn() as conn:
        conn.execute("DELETE FROM domains WHERE domain = ?", (domain,))
        conn.execute("DELETE FROM checks WHERE domain = ?", (domain,))


def clear_domains():
    with get_conn() as conn:
        conn.execute("DELETE FROM domains")
        conn.execute("DELETE FROM checks")


def set_expiry_date(domain, expiry_date):
    """Set expiry date (YYYY-MM-DD string) and recalculate domain_status."""
    today = date.today()
    status = "ACTIVE"
    if expiry_date:
        try:
            exp = date.fromisoformat(expiry_date[:10])
            days_past = (today - exp).days
            if days_past > 60:
                status = "DELETED"  # will be purged on next run
            elif days_past >= 31:
                status = "DELETED"
            elif days_past >= 3:
                status = "REDEMPTION"
            elif days_past >= 1:
                status = "GRACE PERIOD"
            elif days_past == 0:
                status = "EXPIRED"
            elif exp <= today + timedelta(days=30):
                status = "EXPIRING SOON"
        except ValueError:
            pass
    with get_conn() as conn:
        conn.execute(
            "UPDATE domains SET expiry_date = ?, domain_status = ? WHERE domain = ?",
            (expiry_date, status, domain)
        )


def mark_renewed(domain):
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE domains SET domain_status = 'ACTIVE', renewed_at = ? WHERE domain = ?",
            (now, domain)
        )


def purge_expired_domains():
    """Delete domains expired more than 61 days ago (past DELETED stage)."""
    cutoff = (date.today() - timedelta(days=61)).isoformat()
    with get_conn() as conn:
        conn.execute("DELETE FROM domains WHERE expiry_date IS NOT NULL AND expiry_date < ?", (cutoff,))


# --- Check results ---

def save_result(result):
    """Save a flat check result dict from checker.py (one dict per domain+target)."""
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO checks (
                domain, target, checked_at, status, http_code, final_url,
                response_ms, redirect_chain, dns_a_records,
                ssl_issuer, ssl_expires_at, ssl_days_left, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            result.get("domain", ""),
            result.get("target", "apex"),
            result.get("checked_at", datetime.utcnow().isoformat()),
            result.get("status", "error"),
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


def get_all_results():
    """Return latest check result for every domain, merged with domain metadata."""
    with get_conn() as conn:
        domain_rows = conn.execute(
            "SELECT domain, added_at, expiry_date, domain_status FROM domains ORDER BY domain"
        ).fetchall()
        latest = conn.execute("""
            SELECT c.* FROM checks c
            INNER JOIN (
                SELECT domain, target, MAX(id) AS max_id
                FROM checks GROUP BY domain, target
            ) l ON c.id = l.max_id
        """).fetchall()

    checks_by_domain = {}
    for c in latest:
        d = dict(c)
        dom = d["domain"]
        target = d["target"]
        d["redirect_chain"] = json.loads(d.get("redirect_chain") or "[]")
        d["dns_a_records"] = json.loads(d.get("dns_a_records") or "[]")
        if dom not in checks_by_domain:
            checks_by_domain[dom] = {}
        checks_by_domain[dom][target] = d

    results = []
    today = date.today()
    for dr in domain_rows:
        dom = dr["domain"]
        expiry_date = dr["expiry_date"]
        # Compute domain_status dynamically
        ds = "ACTIVE"
        if expiry_date:
            try:
                exp = date.fromisoformat(expiry_date[:10])
                days_past = (today - exp).days
                if days_past > 60:
                    ds = "DELETED"  # will be purged
                elif days_past >= 31:
                    ds = "DELETED"
                elif days_past >= 3:
                    ds = "REDEMPTION"
                elif days_past >= 1:
                    ds = "GRACE PERIOD"
                elif days_past == 0:
                    ds = "EXPIRED"
                elif exp <= today + timedelta(days=30):
                    ds = "EXPIRING SOON"
            except ValueError:
                pass
        c = checks_by_domain.get(dom, {})
        results.append({
            "domain": dom,
            "added_at": dr["added_at"],
            "expiry_date": expiry_date,
            "domain_status": ds,
            "apex": c.get("apex"),
            "www": c.get("www"),
        })
    return results


def get_domain_history(domain, limit=50):
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM checks WHERE domain = ?
            ORDER BY checked_at DESC LIMIT ?
        """, (domain, limit)).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["redirect_chain"] = json.loads(d.get("redirect_chain") or "[]")
        d["dns_a_records"] = json.loads(d.get("dns_a_records") or "[]")
        result.append(d)
    return result


# --- Runs ---

def start_run(kind, total):
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO runs (started_at, kind, total) VALUES (?, ?, ?)",
            (now, kind, total)
        )
        return cur.lastrowid


def finish_run(run_id, operational, down, errored):
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute("""
            UPDATE runs SET finished_at=?, operational=?, down=?, errored=?
            WHERE id=?
        """, (now, operational, down, errored, run_id))


def close_stale_runs():
    """Mark any runs with no finished_at as interrupted (e.g. from a service restart)."""
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE runs SET finished_at=?, operational=0, down=0, errored=0 WHERE finished_at IS NULL",
            (now,)
        )


def get_last_run():
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 1").fetchone()
    return dict(row) if row else None


# --- Helpers ---

def _normalize(s):
    if not s:
        return ""
    s = s.strip().strip('"').strip("'").lower()
    for prefix in ("https://", "http://"):
        if s.startswith(prefix):
            s = s[len(prefix):]
    s = s.split("/")[0].split("?")[0]
    if s.startswith("www."):
        s = s[4:]
    if not s or " " in s or "." not in s:
        return ""
    return s
