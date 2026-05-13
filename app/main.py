"""FastAPI app Ã¢ÂÂ web UI + REST endpoints + APScheduler for background runs."""
import asyncio
import csv
import io
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


# --- Run state ---

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


# --- Routes ---

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

    # Auto-detect delimiter: use semicolon if more semicolons than commas in first line
    first_line = text.split("\n")[0] if text else ""
    delimiter = ";" if first_line.count(";") >= first_line.count(",") else ","

    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    date_fmts = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y")

    def parse_date(s):
        s = s.strip().strip('"').strip("'")
        if not s:
            return None
        for fmt in date_fmts:
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def looks_like_date(s):
        s = s.strip().strip('"').strip("'")
        # Must contain digits and separators, and year must be 4 digits
        import re
        return bool(re.search(r'\b(19|20)\d{2}\b', s))

    for row in reader:
        if not row:
            continue
        col0 = row[0].strip().strip('"').strip("'").lower()
        if not col0 or col0.startswith("#"):
            continue
        # Skip header rows
        if col0 in ("domain", "name", "domain (punycode)", "domain(punycode)"):
            continue

        # Build domain name:
        # If col1 exists and looks like a TLD (starts with . or is short extension), combine col0+col1
        domain = col0
        col1 = row[1].strip().strip('"').strip("'").lower() if len(row) > 1 else ""
        if col1 and (col1.startswith(".") or (len(col1) <= 6 and "." not in col1 and col1.isalpha())):
            tld = col1.lstrip(".")
            if tld and "." not in col0:
                domain = col0 + "." + tld

        # Normalize domain
        domain = domain.strip().strip('"').strip("'")
        for prefix in ("https://", "http://"):
            if domain.startswith(prefix):
                domain = domain[len(prefix):]
        domain = domain.split("/")[0].split("?")[0]
        if domain.startswith("www."):
            domain = domain[4:]
        if not domain or " " in domain or "." not in domain:
            continue

        # Find expiry date: scan all columns for a valid date with 4-digit year
        expiry_date = None
        for i, cell in enumerate(row[1:], 1):
            if looks_like_date(cell):
                parsed = parse_date(cell)
                if parsed:
                    expiry_date = parsed
                    break

        db.add_domain(domain)
        if expiry_date:
            db.set_expiry_date(domain, expiry_date)
            updated += 1
        else:
            added += 1

    return {"ok": True, "added": added, "updated": updated}


@app.post("/api/domains/{domain}/expiry")
async def api_set_expiry(domain: str, request: Request):
    body = await request.json()
    expiry_date = body.get("expiry_date", "")
    db.set_expiry_date(domain, expiry_date)
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


# --- Helpers ---

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



@app.get("/api/test-eurodns")
async def api_test_eurodns():
    import httpx, base64
    login = "api_prod_mediainternational"
    password = "ba4831e1"
    auth = base64.b64encode(f"{login}:{password}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}", "Accept": "application/json"}
    results = {}
    endpoints = [
        "https://agent.api.eurodns.com/domain/list",
        "https://api.eurodns.com/domains",
        "https://api.eurodns.com/v1/domains",
        "https://api.eurodns.com/v2/domains",
        "https://rest-api.eurodns.com/v1/domains",
        "https://agent.api.eurodns.com/user/domain/list",
    ]
    async with httpx.AsyncClient(timeout=10.0, verify=False) as client:
        for url in endpoints:
            try:
                r = await client.get(url, headers=headers)
                results[url] = {"status": r.status_code, "body": r.text[:300]}
            except Exception as e:
                results[url] = {"error": str(e)}
    return results
