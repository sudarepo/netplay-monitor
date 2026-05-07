"""FastAPI app — web UI + REST endpoints + APScheduler for background runs."""
import asyncio
import csv
import io
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request, UploadFile, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from . import db
from .checker import run_checks


# --- Run state ---
# We track the current run in memory so the UI can poll progress without
# hammering the DB. Only one run can be active at a time.
class RunState:
    def __init__(self):
        self.active = False
        self.kind = None
        self.started_at = None
        self.done = 0
        self.total = 0
        self.last_run_id = None

run_state = RunState()
run_lock = asyncio.Lock()
scheduler = None  # type: ignore[var-annotated]


async def execute_run(kind: str = "manual"):
    """Run all checks against the current domain list and persist results.

    Held under run_lock so manual + scheduled can't collide.
    """
    async with run_lock:
        if run_state.active:
            return None
        domains = [d["domain"] for d in db.get_all_domains()]
        if not domains:
            return None

        run_state.active = True
        run_state.kind = kind
        run_state.started_at = datetime.now(timezone.utc).isoformat()
        run_state.done = 0
        run_state.total = len(domains) * 2

        try:
            def progress(done, total):
                run_state.done = done
                run_state.total = total

            results = await run_checks(domains, on_progress=progress)

            # Persist each check result.
            for r in results:
                db.save_check(r)

            # Tally and write run summary.
            operational = sum(1 for r in results if r["status"] == "operational")
            down = sum(1 for r in results if r["status"] == "down")
            errored = sum(1 for r in results if r["status"] == "error")

            run_id = db.save_run({
                "started_at": run_state.started_at,
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "kind": kind,
                "total": len(results),
                "operational": operational,
                "down": down,
                "errored": errored,
            })
            run_state.last_run_id = run_id
            return run_id
        finally:
            run_state.active = False


# --- Scheduler wiring ---

def reschedule(interval_minutes):
    """Set up or tear down the scheduled job. interval_minutes=None disables it."""
    if scheduler is None:
        return
    # Remove existing job if present
    if scheduler.get_job("periodic_check"):
        scheduler.remove_job("periodic_check")

    if interval_minutes and interval_minutes > 0:
        scheduler.add_job(
            execute_run,
            trigger=IntervalTrigger(minutes=interval_minutes),
            id="periodic_check",
            kwargs={"kind": "scheduled"},
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        db.set_setting("schedule_minutes", interval_minutes)
    else:
        db.set_setting("schedule_minutes", 0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    global scheduler
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.start()
    # Restore last schedule setting if any.
    # If this is a fresh deployment with no setting yet, default to 60 minutes
    # so the monitor actively does its job out of the box.
    sm = db.get_setting("schedule_minutes")
    if sm is None:
        # First-ever boot: install a sensible default schedule.
        reschedule(60)
    elif sm:
        try:
            mins = int(sm)
            if mins > 0:
                reschedule(mins)
        except ValueError:
            pass
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="Domain Monitor", lifespan=lifespan)

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


# --- Routes ---

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/domains")
async def api_domains():
    return db.get_all_domains()


@app.post("/api/domains/add")
async def api_add_domains(payload: dict):
    text = payload.get("text", "")
    domains = _parse_domain_text(text)
    added, skipped = db.add_domains(domains)
    return {"added": added, "skipped": skipped, "total_input": len(domains)}


@app.post("/api/domains/upload")
async def api_upload_domains(file: UploadFile):
    raw = await file.read()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("latin-1", errors="ignore")
    domains = _parse_domain_text(text)
    added, skipped = db.add_domains(domains)
    return {"added": added, "skipped": skipped, "total_input": len(domains)}


@app.delete("/api/domains/{domain}")
async def api_delete_domain(domain: str):
    db.delete_domain(domain)
    return {"ok": True}


@app.post("/api/domains/clear")
async def api_clear_domains():
    db.clear_all_domains()
    return {"ok": True}


@app.post("/api/run")
async def api_run():
    if run_state.active:
        return JSONResponse({"error": "a check run is already in progress"}, status_code=409)
    # Fire and forget — UI polls /api/status for progress.
    asyncio.create_task(execute_run(kind="manual"))
    return {"started": True}


@app.get("/api/status")
async def api_status():
    sm = db.get_setting("schedule_minutes", "0")
    return {
        "active": run_state.active,
        "kind": run_state.kind,
        "started_at": run_state.started_at,
        "done": run_state.done,
        "total": run_state.total,
        "schedule_minutes": int(sm or 0),
    }


@app.get("/api/results")
async def api_results():
    """Latest result per domain+target."""
    rows = db.get_latest_results()
    # Group by domain so the UI renders one row per domain with apex+www columns.
    by_domain = {}
    for r in rows:
        by_domain.setdefault(r["domain"], {})[r["target"]] = r
    grouped = []
    for domain in sorted(by_domain.keys()):
        grouped.append({
            "domain": domain,
            "apex": by_domain[domain].get("apex"),
            "www": by_domain[domain].get("www"),
        })
    return grouped


@app.get("/api/history/{domain}")
async def api_history(domain: str, limit: int = 50):
    return db.get_history_for_domain(domain, limit=limit)


@app.get("/api/runs")
async def api_runs():
    return db.get_recent_runs(20)


@app.post("/api/schedule")
async def api_schedule(payload: dict):
    minutes = int(payload.get("minutes") or 0)
    reschedule(minutes if minutes > 0 else None)
    return {"schedule_minutes": minutes}


@app.get("/api/export/csv")
async def api_export_csv():
    """Export the latest results as CSV."""
    rows = db.get_latest_results()
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "domain", "target", "checked_at", "status", "http_code", "final_url",
        "response_ms", "dns_a_records", "ssl_issuer", "ssl_expires_at", "ssl_days_left",
        "redirect_chain", "error",
    ])
    for r in rows:
        writer.writerow([
            r["domain"], r["target"], r["checked_at"], r["status"], r.get("http_code") or "",
            r.get("final_url") or "", r.get("response_ms") or "",
            ";".join(r.get("dns_a_records") or []),
            r.get("ssl_issuer") or "", r.get("ssl_expires_at") or "", r.get("ssl_days_left") or "",
            " -> ".join(f"{h.get('from')} ({h.get('code')})" for h in r.get("redirect_chain") or []),
            r.get("error") or "",
        ])
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=domain_monitor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"}
    )


# --- Helpers ---

def _parse_domain_text(text: str):
    """Parse pasted text or uploaded csv/txt content into a list of domain strings.

    Accepts: one per line, comma-separated, mixed. Handles common pollution like
    schemes, www prefixes, paths, blank lines, BOM.
    """
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
