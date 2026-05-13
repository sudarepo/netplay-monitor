"""FastAPI app — web UI + REST endpoints + APScheduler for background runs."""
import asyncio
import csv
import io
from contextlib import asynccontextmanager
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
_scheduler: AsyncIOScheduler | None = None


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
        op = sum(1 for r in results if r.get("apex", {}) and r["apex"].get("status") == "up")
        down = sum(1 for r in results if r.get("apex", {}) and r["apex"].get("status") == "down")
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
    reader = csv.reader(io.StringIO(text), delimiter=";")
    for row in reader:
        if not row:
            continue
        domain = row[0].strip().strip('"').strip("'").lower()
        if not domain or domain.startswith("#"):
            continue
        expiry_date = None
        if len(row) >= 6:
            expiry_str = row[5].strip().strip('"').strip("'")
            if expiry_str:
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
                    try:
                        expiry_date = datetime.strptime(expiry_str, fmt).strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        continue
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
