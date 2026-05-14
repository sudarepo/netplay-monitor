"""FastAPI app -- web UI + REST endpoints + APScheduler for background runs."""
import asyncio
import csv
import io
import re
from contextlib import asynccontextmanager
from typing import Optional
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from . import db
from .checker import run_checks


# --- CSV parsing helpers ---------------------------------------------------

_DATE_FMTS_NO_TIME = ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y")
_DATE_FMTS_WITH_TIME = (
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
    "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M",
)

_HEADER_DOMAIN = {
    "domain", "domain name", "name", "url", "website",
    "domain (punycode)", "domain(punycode)",
}
_HEADER_EXPIRY = {
    "expiry", "expire", "expires", "expiration",
    "expiry date", "expiration date",
    "expires on", "expires at",
    "expire date",
}
_HEADER_RENEWAL = {
    "renew", "renewal", "renewal status", "renew status",
    "auto renew", "autorenew",
}

_RENEWAL_AFFIRMATIVE = {
    "auto renew", "auto-renew", "autorenew", "auto_renew",
    "yes", "y", "true", "t", "1", "on",
    "renew", "renewing", "enabled", "enable", "active",
}
_RENEWAL_NEGATIVE = {
    "manual renew", "manual-renew", "manualrenew", "manual_renew",
    "manual", "no", "n", "false", "f", "0", "off",
    "non-renew", "non renew", "nonrenew", "non_renew",
    "do not renew", "don't renew", "dont renew",
    "expire", "let expire", "drop",
    "disabled", "disable", "inactive", "not renewing",
}


def _clean_cell(s):
    if s is None:
        return ""
    return s.strip().strip('"').strip("'")


def _parse_date_cell(s):
    s = _clean_cell(s)
    if not s:
        return None
    # Strip time portion if present (anything after first whitespace) and try date-only first.
    date_part = s.split()[0] if " " in s else s
    for fmt in _DATE_FMTS_NO_TIME:
        try:
            return datetime.strptime(date_part, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Fallback: try formats that include time.
    for fmt in _DATE_FMTS_WITH_TIME:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _looks_like_date(s):
    s = _clean_cell(s)
    return bool(re.search(r"\b(19|20)\d{2}\b", s))


def _normalize_domain(s):
    s = _clean_cell(s).lower()
    if not s:
        return ""
    for prefix in ("https://", "http://"):
        if s.startswith(prefix):
            s = s[len(prefix):]
    s = s.split("/")[0].split("?")[0]
    if s.startswith("www."):
        s = s[4:]
    if not s or " " in s or "." not in s:
        return ""
    return s


def _normalize_renewal(s):
    s = _clean_cell(s).lower()
    if not s:
        return None
    if s in _RENEWAL_AFFIRMATIVE:
        return "RENEW"
    if s in _RENEWAL_NEGATIVE:
        return "NON-RENEW"
    return None


def _header_key(h):
    return _clean_cell(h).lower().replace("-", " ").replace("_", " ")


def _identify_columns(headers):
    """Return dict with column indices for domain/expiry/renewal, or None if no domain column."""
    out = {"domain": None, "expiry": None, "renewal": None}
    for i, h in enumerate(headers):
        hn = _header_key(h)
        if out["domain"] is None and hn in _HEADER_DOMAIN:
            out["domain"] = i
        elif out["expiry"] is None and hn in _HEADER_EXPIRY:
            out["expiry"] = i
        elif out["renewal"] is None and hn in _HEADER_RENEWAL:
            out["renewal"] = i
    if out["domain"] is None:
        return None
    return out


def _detect_delimiter(line):
    """Prefer tab when present (PanaNames). Then semicolon. Default comma."""
    tabs = line.count("\t")
    semis = line.count(";")
    commas = line.count(",")
    if tabs > 0 and tabs >= semis and tabs >= commas:
        return "\t"
    if semis > commas:
        return ";"
    return ","


# --- Run state -------------------------------------------------------------

class RunState:
    def __init__(self):
        self.running = False
        self.done = 0
        self.total = 0

run_state = RunState()
_schedule_minutes: int = 0
_scheduler: Optional[AsyncIOScheduler] = None


async def execute_run(kind: str = "manual"):
    if run_state.running:
        return
    run_state.running = True
    run_state.done = 0
    active_domains = db.get_active_domains()
    run_state.total = len(active_domains)
    run_id = db.start_run(kind, run_state.total)
    try:
        results = await run_checks(active_domains)
        for result in results:
            db.save_result(result)
            run_state.done += 1
        db.purge_expired_domains()
        op = sum(1 for r in results if r.get("target") == "apex" and r.get("status") == "operational")
        down = sum(1 for r in results if r.get("target") == "apex" and r.get("status") == "down")
        err = run_state.total - op - down
        db.finish_run(run_id, op, down, err)
    finally:
        run_state.running = False


def reschedule(interval_minutes: int):
    global _schedule_minutes, _scheduler
    _schedule_minutes = interval_minutes
    if _scheduler is None:
        return
    _scheduler.remove_all_jobs()
    if interval_minutes > 0:
        _scheduler.add_job(
            execute_run,
            IntervalTrigger(minutes=interval_minutes),
            id="auto_run",
            kwargs={"kind": "scheduled"},
            replace_existing=True,
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    db.init_db()
    db.close_stale_runs()  # close any runs left open by a previous restart
    _scheduler = AsyncIOScheduler()
    _scheduler.start()
    yield
    _scheduler.shutdown(wait=False)


BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


# --- Routes ----------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/results")
async def api_results():
    return db.get_all_results()


@app.post("/api/run")
async def api_run():
    if run_state.running:
        return {"ok": False, "msg": "Already running"}
    asyncio.create_task(execute_run("manual"))
    return {"ok": True}


@app.get("/api/progress")
async def api_progress():
    return {"running": run_state.running, "done": run_state.done, "total": run_state.total}


@app.post("/api/domains")
async def api_add_domains(request: Request):
    payload = await request.json()
    domains = [d.strip().strip('"').strip("'").lower() for d in payload.get("domains", []) if d.strip()]
    added = 0
    for d in domains:
        if d:
            db.add_domain(d)
            added += 1
    return {"ok": True, "added": added}


@app.delete("/api/domains")
async def api_clear_domains():
    db.clear_domains()
    return {"ok": True}


@app.delete("/api/domains/{domain}")
async def api_delete_domain(domain: str):
    db.delete_domain(domain)
    return {"ok": True}


@app.post("/api/domains/upload")
async def api_upload(file: UploadFile):
    raw = await file.read()
    text = raw.decode("utf-8-sig", errors="replace")
    added = 0
    updated = 0
    renewal_set = 0

    # Auto-detect delimiter from first non-empty line (tab / semicolon / comma).
    first_line = ""
    for line in text.split("\n"):
        if line.strip():
            first_line = line
            break
    delimiter = _detect_delimiter(first_line)

    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    rows = list(reader)
    if not rows:
        return {"ok": True, "added": 0, "updated": 0, "renewal_set": 0}

    # Try header-based parsing. If first row identifies a Domain column,
    # use header positions for everything and skip row 0 as data.
    header_map = _identify_columns(rows[0]) if rows else None
    data_rows = rows[1:] if header_map else rows

    for row in data_rows:
        if not row:
            continue

        # Resolve the domain cell.
        if header_map:
            di = header_map["domain"]
            if di >= len(row):
                continue
            domain = _normalize_domain(row[di])
        else:
            domain = _normalize_domain(row[0]) if row else ""

            # Legacy fallback: combine col0 + col1 when TLD lives in a separate column.
            if not domain and len(row) > 1:
                col0 = _clean_cell(row[0]).lower()
                col1 = _clean_cell(row[1]).lower()
                if col0 and col1 and (col1.startswith(".") or (len(col1) <= 6 and "." not in col1 and col1.isalpha())):
                    tld = col1.lstrip(".")
                    if tld and "." not in col0:
                        domain = _normalize_domain(col0 + "." + tld)

        if not domain:
            continue

        # Without headers, skip any row whose first cell is a known header label
        # (defends against multi-section CSVs or stray header echoes).
        if not header_map:
            first_cell = _clean_cell(row[0]).lower()
            if first_cell in _HEADER_DOMAIN:
                continue

        # Resolve expiry.
        expiry_date = None
        if header_map and header_map["expiry"] is not None:
            ei = header_map["expiry"]
            if ei < len(row):
                expiry_date = _parse_date_cell(row[ei])
        else:
            for cell in row[1:]:
                if _looks_like_date(cell):
                    parsed = _parse_date_cell(cell)
                    if parsed:
                        expiry_date = parsed
                        break

        # Resolve renewal_status.
        renewal_status = None
        if header_map and header_map["renewal"] is not None:
            ri = header_map["renewal"]
            if ri < len(row):
                renewal_status = _normalize_renewal(row[ri])

        # Apply.
        db.add_domain(domain)
        if expiry_date:
            db.set_expiry_date(domain, expiry_date)
            updated += 1
        else:
            added += 1
        if renewal_status:
            db.set_renewal_status(domain, renewal_status)
            renewal_set += 1

    return {"ok": True, "added": added, "updated": updated, "renewal_set": renewal_set}


@app.post("/api/domains/{domain}/expiry")
async def api_set_expiry(domain: str, request: Request):
    body = await request.json()
    expiry_date = body.get("expiry_date", "")
    db.set_expiry_date(domain, expiry_date)
    return {"ok": True}


@app.post("/api/domains/{domain}/renewal")
async def api_set_renewal(domain: str, request: Request):
    body = await request.json()
    status = body.get("renewal_status")
    db.set_renewal_status(domain, status)
    return {"ok": True}


@app.post("/api/domains/{domain}/renew")
async def api_renew_domain(domain: str):
    db.mark_renewed(domain)
    return {"ok": True}


@app.get("/api/export")
async def api_export():
    return db.get_all_results()


@app.get("/api/domains/{domain}/history")
async def api_history(domain: str):
    return db.get_domain_history(domain)


@app.get("/api/schedule")
async def api_get_schedule():
    return {"interval_minutes": _schedule_minutes}


@app.post("/api/schedule")
async def api_schedule(request: Request):
    payload = await request.json()
    minutes = int(payload.get("interval_minutes", 0))
    reschedule(minutes)
    return {"ok": True, "interval_minutes": minutes}


@app.get("/api/stats")
async def api_stats():
    last_run = db.get_last_run()
    return {"last_run": last_run}


# --- Helpers ---------------------------------------------------------------

def _parse_domain_text(text: str):
    """Parse pasted text or uploaded csv/txt content into a list of domain strings."""
    if not text:
        return []
    text = text.replace("\r", "\n").replace("\t", ",").replace(";", ",")
    parts = []
    for line in text.split("\n"):
        for piece in line.split(","):
            piece = piece.strip().strip('"').strip("'")
            if piece:
                parts.append(piece)
    return parts


@app.post("/api/deploy")
async def api_deploy(request: Request):
    import subprocess
    auth = request.headers.get("authorization", "")
    if auth != "Bearer deploy-k0ma-2024":
        return {"ok": False, "error": "unauthorized"}
    try:
        result = subprocess.run(
            "cd /opt/k0ma-monitor && git fetch origin && git reset --hard origin/main && systemctl restart k0ma-monitor",
            shell=True, capture_output=True, text=True, timeout=60
        )
        return {"ok": True, "stdout": result.stdout, "stderr": result.stderr, "returncode": result.returncode}
    except Exception as e:
        return {"ok": False, "error": str(e)}
