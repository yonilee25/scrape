from fastapi import FastAPI, Request, Form, APIRouter, HTTPException
from sqlalchemy import func
from libs.common.db import Base, get_engine, get_session, Job, Source, Document, Event
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func
from libs.common.db import Base, get_engine, get_session, Job, Source, Document, Event
import os
import uuid
from datetime import datetime
import logging
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
templates = Jinja2Templates(directory="/app/apps/api/templates")

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
    run_discovery.delay(job_id, {"person": person})
    return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)

@app.get("/fragments/jobs/{job_id}/status", response_class=HTMLResponse)
def job_status_fragment(job_id: str):
    def _wrap(text: str) -> str:
        # Return a span that keeps polling after each swap
        return (
            f'<span class="badge" '
            f'     hx-get="/fragments/jobs/{job_id}/status" '
            f'     hx-trigger="load, every 2s" '
            f'     hx-swap="outerHTML">{text}</span>'
        )

    with get_session() as db:
        job = db.get(Job, job_id)
        if not job:
            return HTMLResponse(_wrap("unknown"),
                                headers={"Cache-Control": "no-store, max-age=0"})

        total = db.query(func.count(Document.id)).filter(Document.job_id == job_id).scalar() or 0
        finished = db.query(func.count(Document.id)).filter(
            Document.job_id == job_id,
            Document.status.in_(["indexed", "index_failed", "normalize_failed"])
        ).scalar() or 0

        return HTMLResponse(
            _wrap(f"{job.status} ({finished}/{total})"),
            headers={"Cache-Control": "no-store, max-age=0"},
        )

@app.get("/fragments/jobs/{job_id}/sources", response_class=HTMLResponse)
def job_sources_fragment(job_id: str, request: Request):
    with get_session() as db:
        sources = (
            db.query(Source)
            .filter(Source.job_id == job_id)
            .order_by(Source.id.desc())   # <-- use id desc (exists on your model)
            .limit(50)
            .all()
        )

        # Build the HTML NOW (inside the session) to avoid DetachedInstanceError
        rows = []
        for s in sources:
            title = (s.title or s.url or "")[:90]
            src   = getattr(s, "source", getattr(s, "provider", ""))  # support either column name
            kind  = getattr(s, "kind", "")
            date  = getattr(s, "published_at", None)                  # this is a STRING in your model
            date_str = date or ""

            rows.append(
                f"""
            <tr>
              <td>{src}</td>
              <td>{kind}</td>
              <td><a href="{s.url}" target="_blank">{title}</a></td>
              <td>{date_str}</td>
            </tr>""".strip()
            )

        table = (
            "<table><thead><tr>"
            "<th>Source</th><th>Kind</th><th>Title/URL</th><th>Date</th>"
            "</tr></thead><tbody>"
            + "\n".join(rows)
            + "</tbody></table>"
        )
        return HTMLResponse(table, headers={"Cache-Control": "no-store, max-age=0"})

@app.get("/fragments/jobs/{job_id}/timeline", response_class=HTMLResponse)
def job_timeline_fragment(job_id: str):
    with get_session() as db:
        events = (
            db.query(Event)
            .filter(Event.job_id == job_id)
            .order_by(Event.date.asc())
            .all()
        )

        rows = []
        for e in events:
            cits = e.citations or []
            citations_html = "".join(
                f'<div><a href="{c.get("url","")}" target="_blank">{c.get("label","source")}</a></div>'
                for c in cits
            )
            date = e.date
            date_str = date.strftime("%Y-%m-%d %H:%M") if hasattr(date, "strftime") else (date or "")

            rows.append(
                f"""
            <tr>
              <td>{date_str}</td>
              <td>{e.event_text}</td>
              <td>{citations_html}</td>
            </tr>""".strip()
            )

        table = (
            "<table><thead><tr><th>Date</th><th>Event</th><th>Citations</th></tr></thead><tbody>"
            + "\n".join(rows)
            + "</tbody></table>"
        )
        return HTMLResponse(table, headers={"Cache-Control": "no-store, max-age=0"})

@app.get("/jobs/{job_id}", response_class=HTMLResponse)
def job_page(request: Request, job_id: str):
    with get_session() as db:
        job = db.get(Job, job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        html = templates.get_template("job.html").render({"request": request, "job": job})
        return HTMLResponse(html)

@app.get("/api/health")
def health():
    return {"ok": True}
