from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
import os
import uuid
from datetime import datetime
import logging

from libs.common.db import Base, get_engine, get_session, Job, Source, Event
from libs.common.config import Settings
from libs.common.utils import format_status
from apps.worker.tasks import run_discovery

app = FastAPI(title="Deep Research Starter", version="0.1.0")

# Startup event: print a nice localhost URL in logs
@app.on_event("startup")
async def show_url():
    logging.info("API available at http://localhost:8000")

# Always resolve template dir relative to this file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# Ensure DB tables exist at startup
engine = get_engine()
Base.metadata.create_all(bind=engine)


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/start")
def start_research(person: str = Form(...)):
    settings = Settings()
    job_id = str(uuid.uuid4())
    with get_session() as db:
        job = Job(
            id=job_id,
            person=person,
            status="queued",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db.add(job)
        db.commit()
    # hand off to Celery
    run_discovery.delay(job_id, {"person": person})
    return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)


@app.get("/jobs/{job_id}", response_class=HTMLResponse)
def job_page(request: Request, job_id: str):
    with get_session() as db:
        job = db.get(Job, job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return templates.TemplateResponse("job.html", {"request": request, "job": job})


@app.get("/fragments/jobs/{job_id}/status", response_class=HTMLResponse)
def job_status_fragment(job_id: str):
    with get_session() as db:
        job = db.get(Job, job_id)
        if not job:
            return HTMLResponse("<span class='badge'>unknown</span>")
        return HTMLResponse(format_status(job.status))


@app.get("/fragments/jobs/{job_id}/sources", response_class=HTMLResponse)
def job_sources_fragment(job_id: str):
    with get_session() as db:
        sources = (
            db.query(Source)
            .filter(Source.job_id == job_id)
            .order_by(Source.id.desc())
            .limit(50)
            .all()
        )
    rows = []
    for s in sources:
        rows.append(
            f"""
        <tr>
          <td>{s.source}</td>
          <td>{s.kind}</td>
          <td><a href="{s.url}" target="_blank">{(s.title or s.url)[:90]}</a></td>
          <td>{s.published_at or ""}</td>
        </tr>""".strip()
        )
    table = (
        "<table><thead><tr><th>Source</th><th>Kind</th><th>Title/URL</th><th>Date</th></tr></thead><tbody>"
        + "\n".join(rows)
        + "</tbody></table>"
    )
    return HTMLResponse(table)


@app.get("/fragments/jobs/{job_id}/timeline", response_class=HTMLResponse)
def job_timeline_fragment(job_id: str):
    with get_session() as db:
        events = db.query(Event).filter(Event.job_id == job_id).order_by(Event.date.asc()).all()
    rows = []
    for e in events:
        citations_html = "".join(
            [
                f'<div><a href="{c.get("url","")}" target="_blank">{c.get("label","source")}</a></div>'
                for c in (e.citations or [])
            ]
        )
        rows.append(
            f"""
        <tr>
          <td>{e.date or ""}</td>
          <td>{e.event_text}</td>
          <td>{citations_html}</td>
        </tr>""".strip()
        )
    table = (
        "<table><thead><tr><th>Date</th><th>Event</th><th>Citations</th></tr></thead><tbody>"
        + "\n".join(rows)
        + "</tbody></table>"
    )
    return HTMLResponse(table)


@app.get("/api/health")
def health():
    return {"ok": True}
