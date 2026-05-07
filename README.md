# Netplay Monitor

Domain monitoring dashboard. FastAPI + SQLite + APScheduler.

## Stack

- Python 3.9+
- FastAPI / Uvicorn
- SQLite (file-based, at data/monitor.db)
- APScheduler for periodic checks
- Single-page Jinja2 template with vanilla JS frontend

## Production deployment

Deployed at monitor.netplaymedia.com on MojoHost.

- Service: netplay-monitor.service (systemd)
- Code: /opt/netplay-monitor/
- Port: 127.0.0.1:8765 (behind Apache reverse proxy)
- Python: 3.9.25 (in .venv)

## Local development

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    uvicorn app.main:app --reload --port 8765

Then open http://localhost:8765
