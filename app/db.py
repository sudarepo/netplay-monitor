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
        CREATE TABLE IF NOT EXISTS engagements (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            slug        TEXT UNIQUE,
            client      TEXT,
            status      TEXT NOT NULL DEFAULT 'ACTIVE',
            created_at  TEXT NOT NULL,
            notes       TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_engagements_status ON engagements(status, created_at DESC);

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
        # Reclassify legacy PROTECTED status
        conn.execute("UPDATE checks SET status = 'operational' WHERE status = 'protected'")
    # The `domains` table is created fresh / rebuilt+backfilled by its own helper,
    # which manages foreign_keys + transactions itself (see its docstring). This is
    # the single source of truth for the domains DDL. Must run AFTER `engagements`
    # exists, since the new domains schema has an FK into it.
    _migrate_domains_to_engagements()


# --- Engagements & schema migration ---

def _domains_ddl(table):
    """Return the CREATE statement for the engagement-scoped `domains` table.

    One source of truth for the schema, used both to create it fresh and to build
    the rebuild target (`domains_new`). `table` is an internal, controlled name
    ("domains" / "domains_new"), so the f-string is not an injection surface.
    """
    return f"""
        CREATE TABLE {table} (
            engagement_id  INTEGER NOT NULL REFERENCES engagements(id) ON DELETE CASCADE,
            domain         TEXT NOT NULL,
            added_at       TEXT NOT NULL,
            expiry_date    TEXT,
            domain_status  TEXT DEFAULT 'ACTIVE',
            renewal_status TEXT,
            renewed_at     TEXT,
            notes          TEXT,
            PRIMARY KEY (engagement_id, domain)
        )
    """


def _ensure_legacy_engagement(conn):
    """Return the id of the 'legacy' engagement, creating it if absent.

    Used only by the migration to backfill pre-engagement domains. Operates on the
    passed-in (autocommit) connection.
    """
    row = conn.execute("SELECT id FROM engagements WHERE slug = 'legacy'").fetchone()
    if row:
        return row["id"]
    now = datetime.utcnow().isoformat()
    cur = conn.execute(
        "INSERT INTO engagements (name, slug, client, status, created_at, notes) "
        "VALUES (?, 'legacy', NULL, 'ACTIVE', ?, ?)",
        (
            "Legacy (pre-engagement portfolio)",
            now,
            "Auto-created by the engagement-scoping migration to hold domains that "
            "predate engagements.",
        ),
    )
    return cur.lastrowid


def _migrate_domains_to_engagements():
    """Create or migrate `domains` to the engagement-scoped schema. Idempotent;
    safe to call on every init_db(). Returns one of "created" / "migrated" /
    "already-migrated" (handy for tests/logging). Three cases:

      * fresh DB (no `domains` table)        -> create the new composite-key schema
      * legacy DB (no `engagement_id` column) -> rebuild + backfill every row into an
                                                 auto-created "legacy" engagement
      * already migrated (`engagement_id`)    -> no-op

    SQLite cannot change a primary key in place, so the legacy case needs a full
    table rebuild (create new -> copy -> drop -> rename). That procedure must run
    with `PRAGMA foreign_keys=OFF` around the swap, and that pragma CANNOT be
    toggled inside a transaction. So this runs on its OWN connection in autocommit
    mode (isolation_level=None), managing BEGIN/COMMIT and the pragma explicitly —
    deliberately NOT reusing get_conn() (which sets foreign_keys=ON and wraps a
    transaction). After the swap we run `PRAGMA foreign_key_check` before
    re-enabling enforcement. This is the template for future constraint-changing
    migrations.
    """
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None  # autocommit: we manage BEGIN/COMMIT + pragmas ourselves
    try:
        cols = [r["name"] for r in conn.execute("PRAGMA table_info(domains)").fetchall()]

        if not cols:
            # Fresh DB: no `domains` table yet. Create the new schema directly.
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute(_domains_ddl("domains"))
            conn.execute("CREATE INDEX IF NOT EXISTS idx_domains_engagement ON domains(engagement_id)")
            return "created"

        if "engagement_id" in cols:
            return "already-migrated"  # no-op

        # Legacy rebuild + backfill into the 'legacy' engagement.
        legacy_id = _ensure_legacy_engagement(conn)

        # Copy the intersection of the old table's columns with the new schema, so
        # this is robust to older column variants (defaults fill anything absent).
        candidate_cols = ["domain", "added_at", "expiry_date", "domain_status",
                          "renewal_status", "renewed_at", "notes"]
        copy_cols = [c for c in candidate_cols if c in cols]
        collist = ", ".join(copy_cols)

        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("BEGIN")
        try:
            conn.execute(_domains_ddl("domains_new"))
            conn.execute(
                f"INSERT INTO domains_new (engagement_id, {collist}) "
                f"SELECT ?, {collist} FROM domains",
                (legacy_id,),
            )
            conn.execute("DROP TABLE domains")
            conn.execute("ALTER TABLE domains_new RENAME TO domains")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_domains_engagement ON domains(engagement_id)")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        # Verify referential integrity before re-enabling enforcement.
        problems = conn.execute("PRAGMA foreign_key_check").fetchall()
        if problems:
            raise RuntimeError(
                f"foreign_key_check failed after domains rebuild: {[dict(p) for p in problems]}"
            )
        conn.execute("PRAGMA foreign_keys=ON")
        return "migrated"
    finally:
        conn.close()


def create_engagement(name, slug=None, client=None, notes=None):
    """Create an engagement; return its new id. `slug`, if given, must be unique."""
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO engagements (name, slug, client, status, created_at, notes) "
            "VALUES (?, ?, ?, 'ACTIVE', ?, ?)",
            (name, slug, client, now, notes),
        )
        return cur.lastrowid


def list_engagements(include_archived=True):
    """Return engagement dicts, newest first. include_archived=False hides ARCHIVED."""
    with get_conn() as conn:
        if include_archived:
            rows = conn.execute(
                "SELECT * FROM engagements ORDER BY created_at DESC, id DESC"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM engagements WHERE status = 'ACTIVE' "
                "ORDER BY created_at DESC, id DESC"
            ).fetchall()
    return [dict(r) for r in rows]


def get_engagement(engagement_id):
    """Return one engagement dict, or None."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM engagements WHERE id = ?", (engagement_id,)
        ).fetchone()
    return dict(row) if row else None


# --- Domain management ---

def add_domain(engagement_id, domain):
    """Add a single domain to an engagement's portfolio. Returns True if newly
    added, False if that (engagement, domain) pair already existed.

    Uses ON CONFLICT DO NOTHING so a duplicate is detected via rowcount rather than
    by catching IntegrityError — that keeps a genuine FK violation (a non-existent
    engagement_id) loud instead of silently swallowing it as a 'duplicate'.
    """
    engagement_id = _coerce_engagement_id(engagement_id)
    domain = _normalize(domain)
    if not domain:
        return False
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO domains (engagement_id, domain, added_at) VALUES (?, ?, ?) "
            "ON CONFLICT(engagement_id, domain) DO NOTHING",
            (engagement_id, domain, now),
        )
        return cur.rowcount > 0


def get_all_domains(engagement_id):
    engagement_id = _coerce_engagement_id(engagement_id)
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT domain, added_at, expiry_date, domain_status, renewal_status, notes "
            "FROM domains WHERE engagement_id = ? ORDER BY domain",
            (engagement_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_active_domains(engagement_id):
    """Return an engagement's domains that are not expired (active or expiring-soon).

    Scoped to engagement_id. Note the parentheses around the status OR — it must be
    AND-ed with the engagement filter, not left to bind loosely. (Also drops a
    stray, unused `today` binding the single-portfolio version carried.)
    """
    engagement_id = _coerce_engagement_id(engagement_id)
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT domain FROM domains
            WHERE engagement_id = ?
              AND (domain_status IN ('ACTIVE', 'EXPIRING SOON') OR expiry_date IS NULL)
            ORDER BY domain
        """, (engagement_id,)).fetchall()
    return [r["domain"] for r in rows]


def delete_domain(engagement_id, domain):
    """Remove a domain from one engagement's portfolio.

    Deliberately does NOT touch `checks`: checks are domain-shared and may still
    belong to another engagement holding the same domain. Orphaned checks are
    reclaimed separately by delete_orphan_checks().
    """
    engagement_id = _coerce_engagement_id(engagement_id)
    with get_conn() as conn:
        conn.execute(
            "DELETE FROM domains WHERE engagement_id = ? AND domain = ?",
            (engagement_id, domain),
        )


def clear_domains(engagement_id):
    """Clear one engagement's portfolio. Does NOT touch shared `checks` (see
    delete_domain); use delete_orphan_checks() to reclaim orphans."""
    engagement_id = _coerce_engagement_id(engagement_id)
    with get_conn() as conn:
        conn.execute("DELETE FROM domains WHERE engagement_id = ?", (engagement_id,))


def set_expiry_date(engagement_id, domain, expiry_date):
    """Set expiry date (YYYY-MM-DD string) and recalculate domain_status, scoped to
    one engagement's portfolio row."""
    engagement_id = _coerce_engagement_id(engagement_id)
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
            "UPDATE domains SET expiry_date = ?, domain_status = ? "
            "WHERE engagement_id = ? AND domain = ?",
            (expiry_date, status, engagement_id, domain)
        )


def set_renewal_status(engagement_id, domain, status):
    """Set renewal_status to 'RENEW', 'NON-RENEW', or None, scoped to one
    engagement's portfolio row.

    Anything else is silently ignored to keep the column safe from garbage.
    Pass an empty string or None to clear the value.
    """
    engagement_id = _coerce_engagement_id(engagement_id)
    if status not in ("RENEW", "NON-RENEW", None, ""):
        return
    value = status if status else None
    with get_conn() as conn:
        conn.execute(
            "UPDATE domains SET renewal_status = ? WHERE engagement_id = ? AND domain = ?",
            (value, engagement_id, domain)
        )


def mark_renewed(engagement_id, domain):
    engagement_id = _coerce_engagement_id(engagement_id)
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE domains SET domain_status = 'ACTIVE', renewed_at = ? "
            "WHERE engagement_id = ? AND domain = ?",
            (now, engagement_id, domain)
        )


def purge_expired_domains(engagement_id):
    """Delete one engagement's domains expired more than 61 days ago (past the
    DELETED stage). Per-engagement by design — there is no global purge across all
    clients' portfolios (that would be a foot-gun); iterate engagements explicitly
    if a sweep is ever truly needed. Leaves shared `checks` alone (see
    delete_orphan_checks)."""
    engagement_id = _coerce_engagement_id(engagement_id)
    cutoff = (date.today() - timedelta(days=61)).isoformat()
    with get_conn() as conn:
        conn.execute(
            "DELETE FROM domains WHERE engagement_id = ? "
            "AND expiry_date IS NOT NULL AND expiry_date < ?",
            (engagement_id, cutoff),
        )


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


def get_all_results(engagement_id):
    """Return the latest check result for every domain in one engagement's
    portfolio, merged with that engagement's per-domain metadata.

    Engagement scoping comes from filtering `domains` by engagement_id. Check data
    is domain-shared: the latest-check subquery is global (MAX(id) per
    domain+target across all checks), then JOINed to this engagement's domains on
    the BARE domain string (not engagement_id). So each engagement sees one row per
    domain it owns, carrying the latest shared check data.
    """
    engagement_id = _coerce_engagement_id(engagement_id)
    with get_conn() as conn:
        domain_rows = conn.execute(
            "SELECT domain, added_at, expiry_date, domain_status, renewal_status "
            "FROM domains WHERE engagement_id = ? ORDER BY domain",
            (engagement_id,),
        ).fetchall()
        latest = conn.execute("""
            SELECT c.* FROM checks c
            INNER JOIN (
                SELECT domain, target, MAX(id) AS max_id
                FROM checks GROUP BY domain, target
            ) l ON c.id = l.max_id
            INNER JOIN domains d ON d.domain = c.domain AND d.engagement_id = ?
        """, (engagement_id,)).fetchall()

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
            "renewal_status": dr["renewal_status"],
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


def delete_orphan_checks():
    """Delete checks for domains no longer present in ANY engagement's portfolio.

    Separate sweep by design: delete_domain / clear_domains / purge deliberately
    leave shared `checks` alone (a domain may still belong to another engagement),
    so orphan reclamation lives here rather than coupling portfolio ops to the
    shared-data layer. Returns the number of check rows deleted.
    """
    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM checks WHERE domain NOT IN (SELECT DISTINCT domain FROM domains)"
        )
        return cur.rowcount


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

def _coerce_engagement_id(engagement_id):
    """Validate and coerce a REQUIRED engagement_id. This is the validation
    template every engagement-aware fn (and, in turn, every endpoint) follows.

    Portfolio operations are inherently engagement-scoped, so a missing scope is a
    programming error, not a "span all engagements" wildcard — we raise rather than
    silently cross engagement boundaries. None / "" -> ValueError; non-int-coercible
    -> ValueError; otherwise the int.

    Existence is intentionally NOT checked here: writes are guarded by the FK
    (domains.engagement_id REFERENCES engagements(id), with foreign_keys=ON), and
    reads against a non-existent id simply return empty.
    """
    if engagement_id is None or engagement_id == "":
        raise ValueError("engagement_id is required for portfolio operations")
    try:
        return int(engagement_id)
    except (TypeError, ValueError):
        raise ValueError(f"engagement_id must be an integer, got {engagement_id!r}") from None


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
