import os, io, json, time, hashlib
from datetime import datetime
import requests
from celery import states
from apps.worker.worker import celery_app
from libs.common.db import get_session, Job, Source, Document, Event
from libs.contracts.models import DiscoveryItem
from libs.common.utils import robots_allows
from libs.common.storage import put_bytes, put_file
from libs.normalize.text_normalize import html_to_text, pdf_to_text
from libs.normalize.audio_normalize import transcribe
from libs.common.config import Settings
from libs.common.qdrant_utils import upsert_chunk
from libs.analyze.rag import embed, make_timeline

from libs.providers import searxng_provider
from libs.providers import sitemap_provider
from libs.providers import wordpress_provider

settings = Settings()

def _set_status(job_id: str, status: str):
    with get_session() as db:
        job = db.query(Job).get(job_id)
        if job:
            job.status = status
            job.updated_at = datetime.utcnow()
            db.add(job)

def _hash_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

@celery_app.task
def run_discovery(job_id: str, req: dict):
    person = req.get("person")
    _set_status(job_id, "discovering")
    items: list[DiscoveryItem] = []

    # Provider 1: SearXNG (optional, if running)
    try:
        searx_items = searxng_provider.discover(person, max_results=min(settings.DISCOVERY_MAX_RESULTS, 15))
        print(f"[DEBUG] SearXNG returned {len(searx_items)} items for {person}", flush=True)
        items.extend(searx_items)
    except Exception as e:
        print(f"[DEBUG] SearXNG provider failed: {e}", flush=True)

    # Provider 2: Sitemaps (demo)
    demo_domains = ["medium.com", "substack.com", "wordpress.com"]  
    try:
        site_items = sitemap_provider.discover(person, domains=demo_domains, max_results=10)
        print(f"[DEBUG] Sitemap returned {len(site_items)} items for {person}", flush=True)
        items.extend(site_items)
    except Exception as e:
        print(f"[DEBUG] Sitemap provider failed: {e}", flush=True)

    # Provider 3: WordPress JSON (demo)
    try:
        wp_items = wordpress_provider.discover(person, domains=demo_domains, max_results=10)
        print(f"[DEBUG] WordPress returned {len(wp_items)} items for {person}", flush=True)
        items.extend(wp_items)
    except Exception as e:
        print(f"[DEBUG] WordPress provider failed: {e}", flush=True)
    

    print(f"[DEBUG] run_discovery for {person} ({job_id}) got {len(items)} raw items", flush=True)
    for i, it in enumerate(items[:5]):
        print(f"[DEBUG] Raw item {i}: url={it.url}, kind={it.kind}, source={it.source}, title={it.title}", flush=True)

    # Deduplicate by URL
    seen = set()
    unique = []
    for it in items:
        if it.url in seen:
            continue
        seen.add(it.url)
        unique.append(it)

    print(f"[DEBUG] run_discovery unique items: {len(unique)}", flush=True)

    # Store and enqueue fetch
    with get_session() as db:
        for it in unique:
            print(f"[DEBUG] Inserting Source for job {job_id}: {it.url}", flush=True)
            s = Source(job_id=job_id, url=it.url, kind=it.kind, source=it.source, title=it.title,
                       published_at=it.published_at, confidence=it.confidence, status="queued")
            db.add(s)
            db.flush()  # to get s.id
            print(f"[DEBUG] Enqueued fetch_source for source_id={s.id}", flush=True)
            fetch_source.delay(job_id, s.id, it.dict())

    print(f"[DEBUG] Updating job {job_id} status to 'fetching'", flush=True)
    _set_status(job_id, "fetching")

@celery_app.task
def fetch_source(job_id: str, source_id: int, item: dict):
    url = item["url"]
    kind = item.get("kind","webpage")
    # Respect robots.txt
    if not robots_allows(url):
        with get_session() as db:
            print(f"[DEBUG] Blocking fetch due to robots.txt for job {job_id}: {url}", flush=True)
            s = db.query(Source).get(source_id)
            if s:
                s.status = "blocked_by_robots"
            return

    # Simple fetch logic: HTML/PDF only for starter; audio/video would need licenses
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent":"DeepResearchBot"})
        ctype = r.headers.get("Content-Type","").split(";")[0].strip().lower()
        if r.status_code != 200:
            raise RuntimeError(f"status {r.status_code}")
        body = r.content
        key = f"jobs/{job_id}/raw/{_hash_bytes(body)[:12]}"
        if "pdf" in ctype or url.lower().endswith(".pdf"):
            key += ".pdf"; ctype = "application/pdf"
        elif "html" in ctype or ("pdf" not in ctype and "json" not in ctype):
            key += ".html"; ctype = "text/html"
        else:
            key += ".bin"

        file_path = put_bytes(key, body, content_type=ctype)

        with get_session() as db:
            s = db.query(Source).get(source_id)
            if s:
                d = Document(job_id=job_id, source_id=source_id, file_path=file_path, mime_type=ctype, status="fetched")
                db.add(d)
                s.status = "fetched"
                db.flush()
                normalize.delay(job_id, d.id)
    except Exception as e:
        with get_session() as db:
            s = db.query(Source).get(source_id)
            if s:
                s.status = "fetch_failed"

@celery_app.task
def normalize(job_id: str, document_id: int):
    # Download from MinIO not implemented (starter uses stored bytes). We already have content in MinIO.
    # For simplicity, we won't re-download; in a real app you'd implement storage.get_object here.
    # Instead, we mark that we "normalized" by generating text fields inline using the original source URL.
    with get_session() as db:
        d = db.query(Document).get(document_id)
        s = db.query(Source).get(d.source_id)
        if not d or not s:
            return

    # Re-fetch the URL (simplest path) and extract text for indexing
    try:
        r = requests.get(s.url, timeout=30, headers={"User-Agent":"DeepResearchBot"})
        ctype = r.headers.get("Content-Type","").split(";")[0].strip().lower()

        text_out = ""
        transcript_out = None
        if "html" in ctype or s.url.lower().endswith((".html",".htm","/")):
            text_out = html_to_text(r.content, url=s.url)
            ext = "txt"
        elif "pdf" in ctype or s.url.lower().endswith(".pdf"):
            # Save temp PDF to extract
            tmp = f"/tmp/{document_id}.pdf"
            with open(tmp, "wb") as f:
                f.write(r.content)
            text_out = pdf_to_text(tmp)
            ext = "txt"
        else:
            # non supported types for starter
            text_out = ""
            ext = "txt"

        text_key = f"jobs/{job_id}/normalized/{document_id}.{ext}"
        text_path = put_bytes(text_key, text_out.encode("utf-8"), content_type="text/plain")

        with get_session() as db:
            d = db.query(Document).get(document_id)
            d.text_path = text_path
            d.status = "normalized"
            db.add(d)

        index.delay(job_id, document_id)
    except Exception:
        with get_session() as db:
            d = db.query(Document).get(document_id)
            if d:
                d.status = "normalize_failed"

@celery_app.task
def index(job_id: str, document_id: int):
    # Load text from MinIO? (starter: re-fetch via the source URL and re-extract quick again)
    with get_session() as db:
        d = db.query(Document).get(document_id)
        s = db.query(Source).get(d.source_id)
        if not d or not s:
            return

    try:
        r = requests.get(s.url, timeout=30, headers={"User-Agent":"DeepResearchBot"})
        ctype = r.headers.get("Content-Type","").split(";")[0].strip().lower()
        if "html" in ctype or s.url.lower().endswith((".html",".htm","/")):
            text = html_to_text(r.content, url=s.url)
        elif "pdf" in ctype or s.url.lower().endswith(".pdf"):
            tmp = f"/tmp/{document_id}.pdf"
            with open(tmp, "wb") as f:
                f.write(r.content)
            text = pdf_to_text(tmp)
        else:
            text = ""

        if not text or len(text.strip()) < 200:
            return

        # Chunk the text simply by ~1000 chars for starter (improve with token-based later)
        chunk_size = 1500
        chunks = [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]
        for ch in chunks:
            vec = embed(ch[:1000])  # Keep prompt small for embeddings
            payload = {
                "job_id": job_id,
                "person": db.query(Job).get(job_id).person,
                "source_url": s.url,
                "published_at": s.published_at,
                "kind": s.kind,
                "text": ch[:1200],  # store a slice for context
            }
            upsert_chunk(vec, payload)

        with get_session() as db:
            d = db.query(Document).get(document_id)
            d.status = "indexed"
            db.add(d)
    except Exception:
        with get_session() as db:
            d = db.query(Document).get(document_id)
            if d:
                d.status = "index_failed"

    # After some documents, trigger analysis (simple heuristic)
    analyze_timeline.delay(job_id)

@celery_app.task
def analyze_timeline(job_id: str):
    _set_status(job_id, "analyzing")
    with get_session() as db:
        job = db.query(Job).get(job_id)
        if not job:
            return
    try:
        tl = make_timeline(job_id, job.person)
        # store events
        with get_session() as db:
            for e in tl:
                ev = Event(job_id=job_id, date=e.get("date"), event_text=e.get("event",""))
                cits = e.get("citations") or []
                ev.citations = cits
                db.add(ev)
        _set_status(job_id, "complete")
    except Exception:
        _set_status(job_id, "analysis_failed")
