# apps/worker/tasks.py

from __future__ import annotations

import os
import hashlib
import traceback
from datetime import datetime
from urllib.parse import urlsplit                                                   #New

import requests
from sqlalchemy import func

from apps.worker.worker import celery_app
from libs.common.config import Settings
from libs.common.db import get_session, Job, Source, Document, Event
from libs.common.storage import put_bytes
from libs.common.utils import robots_allows
from libs.normalize.text_normalize import html_to_text, pdf_to_text
from libs.analyze.rag import embed, make_timeline
from libs.common.qdrant_utils import upsert_chunk
from libs.providers import searxng_provider, sitemap_provider, wordpress_provider
from libs.contracts.models import DiscoveryItem

# Feature flags (default: skip the slower demo providers)
SKIP_SITEMAPS = os.getenv("SKIP_SITEMAPS", "1") == "1"
SKIP_WORDPRESS = os.getenv("SKIP_WORDPRESS", "1") == "1"

settings = Settings()

##########################################################################################   New   ##############################################################
# Be a polite, identifiable UA
HEADERS = {"User-Agent": "DeepResearch/0.1 (+contact: you@example.com)"}

# Simple signal controls
TRUST_DOMAINS = {
    "en.wikipedia.org", "www.reuters.com", "apnews.com", "www.cnn.com",
    "www.bbc.com", "www.nytimes.com", "www.wired.com", "www.cnbc.com",
    "www.theguardian.com", "www.axios.com", "abcnews.go.com", "www.vox.com"
}
BLOCK_DOMAINS = {
    "icon-icons.com", "producthunt.com", "elonmask.co", "truebluemeandyou.com",
    "madeinatlantis.com", "cybergenica.com", "tuvie.com", "wonderfulengineering.com"
}

def _domain(url: str) -> str:
    try:
        return urlsplit(url).netloc.lower()
    except Exception:
        return ""
##########################################################################################   New   ##############################################################

def _set_status(job_id: str, status: str) -> None:
    """Update job status with a timestamp."""
    with get_session() as db:
        job = db.get(Job, job_id)   
        if job:
            job.status = status
            job.updated_at = datetime.utcnow()
            db.add(job)

def _hash_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _pending_docs_count(job_id: str) -> int:
    """How many docs are not yet terminal (indexed / index_failed / normalize_failed)."""
    with get_session() as db:
        return (
            db.query(func.count(Document.id))
            .filter(
                Document.job_id == job_id,
                Document.status.notin_(["indexed", "index_failed", "normalize_failed"]),
            )
            .scalar()
            or 0
        )

def _maybe_trigger_analysis(job_id: str) -> None:
    """Run analysis only when we actually have docs and none are pending."""
    with get_session() as db:
        total = db.query(func.count(Document.id)).filter(Document.job_id == job_id).scalar() or 0
        if total == 0:
            return  # don't analyze with zero docs

        pending = db.query(func.count(Document.id)).filter(
            Document.job_id == job_id,
            Document.status.notin_(["indexed", "index_failed", "normalize_failed"])
        ).scalar() or 0
        if pending != 0:
            return  # still work to do

        job = db.get(Job, job_id)
        if job and job.status in ("analyzing", "complete", "analysis_failed"):
            return  # avoid duplicates

    analyze_timeline.delay(job_id)

# ---------------------------- DISCOVERY ----------------------------

@celery_app.task
def run_discovery(job_id: str, req: dict):
    person = (req or {}).get("person") or ""
    print(f"[DEBUG] run_discovery start job={job_id} person={person}", flush=True)
    _set_status(job_id, "discovering")

    items: list[DiscoveryItem] = []

    # Provider 1: SearXNG (fast)
    try:
        searx_items = searxng_provider.discover(
            person, max_results=min(settings.DISCOVERY_MAX_RESULTS, 15)
        )
        print(f"[DEBUG] SearXNG returned {len(searx_items)} items for {person}", flush=True)
        items.extend(searx_items)
    except Exception as e:
        print(f"[ERROR] SearXNG provider failed: {e}\n{traceback.format_exc()}", flush=True)

    # Provider 2: Sitemaps (guarded)
    print(f"[DEBUG] SKIP_SITEMAPS={SKIP_SITEMAPS}", flush=True)
    if not SKIP_SITEMAPS:
        try:
            print("[DEBUG] Calling Sitemap provider...", flush=True)
            site_items = sitemap_provider.discover(
                person, domains=["medium.com", "substack.com", "wordpress.com"], max_results=10
            )
            print(f"[DEBUG] Sitemap returned {len(site_items)} items for {person}", flush=True)
            items.extend(site_items)
        except Exception as e:
            print(f"[ERROR] Sitemap provider failed: {e}\n{traceback.format_exc()}", flush=True)

    # Provider 3: WordPress JSON (guarded)
    print(f"[DEBUG] SKIP_WORDPRESS={SKIP_WORDPRESS}", flush=True)
    if not SKIP_WORDPRESS:
        try:
            print("[DEBUG] Calling WordPress provider...", flush=True)
            wp_items = wordpress_provider.discover(
                person, domains=["medium.com", "substack.com", "wordpress.com"], max_results=10
            )
            print(f"[DEBUG] WordPress returned {len(wp_items)} items for {person}", flush=True)
            items.extend(wp_items)
        except Exception as e:
            print(f"[ERROR] WordPress provider failed: {e}\n{traceback.format_exc()}", flush=True)

    print(f"[DEBUG] run_discovery for {person} ({job_id}) got {len(items)} raw items", flush=True)
    for i, it in enumerate(items[:5]):
        print(
            f"[DEBUG] Raw item {i}: url={it.url}, kind={it.kind}, source={it.source}, title={it.title}",
            flush=True
        )

    # Deduplicate by URL
    seen = set()
    unique: list[DiscoveryItem] = []
    for it in items:
        if it.url in seen:
            continue
        seen.add(it.url)
        unique.append(it)

    print(f"[DEBUG] run_discovery unique items: {len(unique)}", flush=True)

    # Store and enqueue fetch
    created_docs = 0
    with get_session() as db:
        for it in unique:
            print(f"[DEBUG] Inserting Source for job {job_id}: {it.url}", flush=True)
            s = Source(
                job_id=job_id,
                url=it.url,
                kind=it.kind,
                source=it.source,
                title=it.title,
                published_at=it.published_at,
                confidence=it.confidence,
                status="queued",
            )
            db.add(s)
            db.flush()  # obtain s.id
            print(f"[DEBUG] Enqueued fetch_source for source_id={s.id}", flush=True)
            fetch_source.delay(job_id, s.id, it.dict())
            created_docs += 1  # we will try to create a doc for this source

    _set_status(job_id, "fetching")
    print(f"[DEBUG] Updating job {job_id} status to 'fetching'", flush=True)

    # Fallback: if no docs get created (robots/errors), try finalization later anyway
    finalize_job.apply_async((job_id,), countdown=45)


# ---------------------------- FETCH ----------------------------

@celery_app.task
def fetch_source(job_id: str, source_id: int, item: dict):
    url = item.get("url")
    kind = item.get("kind", "webpage")
    print(f"[DEBUG] fetch_source received job={job_id} source_id={source_id} url={url}", flush=True)

    # Respect robots.txt
    if not robots_allows(url):
        print(f"[DEBUG] Blocking fetch due to robots.txt for job {job_id}: {url}", flush=True)
        with get_session() as db:
            s = db.get(Source, source_id)
            if s:
                s.status = "blocked_by_robots"
        # Maybe no doc will be created; re-check if we should finalize
        _maybe_trigger_analysis(job_id)
        return

    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": "DeepResearchBot"})
        if r.status_code != 200:
            raise RuntimeError(f"status {r.status_code}")
        ctype = (r.headers.get("Content-Type", "") or "").split(";")[0].strip().lower()
        body = r.content

        key = f"jobs/{job_id}/raw/{_hash_bytes(body)[:12]}"
        if "pdf" in ctype or url.lower().endswith(".pdf"):
            key += ".pdf"; ctype = "application/pdf"
        elif "html" in ctype or ("pdf" not in ctype and "json" not in ctype):
            key += ".html"; ctype = "text/html"
        else:
            key += ".bin"

        file_path = put_bytes(key, body, content_type=ctype)
        print(f"[DEBUG] fetch_source stored to {file_path} ({ctype})", flush=True)

        with get_session() as db:
            s = db.get(Source, source_id)
            if s:
                d = Document(
                    job_id=job_id,
                    source_id=source_id,
                    file_path=file_path,
                    mime_type=ctype,
                    status="fetched",
                )
                db.add(d)
                s.status = "fetched"
                db.flush()
                print(f"[DEBUG] Enqueue normalize for document_id={d.id}", flush=True)
                normalize.delay(job_id, d.id)

    except Exception as e:
        print(f"[ERROR] fetch_source failed: {e}\n{traceback.format_exc()}", flush=True)
        with get_session() as db:
            s = db.get(Source, source_id)
            if s:
                s.status = "fetch_failed"
        _maybe_trigger_analysis(job_id)


# ---------------------------- NORMALIZE ----------------------------

@celery_app.task
def normalize(job_id: str, document_id: int):
    print(f"[DEBUG] normalize received job={job_id} doc_id={document_id}", flush=True)

    # Read primitives while session is OPEN
    with get_session() as db:
        d = db.get(Document, document_id)
        if not d:
            print("[WARN] normalize: missing Document", flush=True)
            return
        s = db.get(Source, d.source_id)
        url = s.url if s else None

        # mark progress
        d.status = "normalizing"
        db.add(d)

    if not url:
        print("[WARN] normalize: missing Source/URL", flush=True)
        _maybe_trigger_analysis(job_id)
        return

    # Network work OUTSIDE the session
    ok = False
    text_out = ""
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": "DeepResearchBot"})
        r.raise_for_status()
        ctype = (r.headers.get("Content-Type", "") or "").split(";")[0].strip().lower()

        if "html" in ctype or url.lower().endswith((".html", ".htm", "/")):
            text_out = html_to_text(r.content, url=url)
        elif "pdf" in ctype or url.lower().endswith(".pdf"):
            tmp = f"/tmp/{document_id}.pdf"
            with open(tmp, "wb") as f:
                f.write(r.content)
            text_out = pdf_to_text(tmp)
        else:
            text_out = ""
        ok = True
    except Exception as e:
        print(f"[ERROR] normalize failed: {e}\n{traceback.format_exc()}", flush=True)
        ok = False

    # Save result in a NEW session
    with get_session() as db:
        d = db.get(Document, document_id)
        if d:
            if ok:
                text_key = f"jobs/{job_id}/normalized/{document_id}.txt"
                text_path = put_bytes(text_key, (text_out or "").encode("utf-8"), content_type="text/plain")
                d.text_path = text_path
                d.status = "normalized"
            else:
                d.status = "normalize_failed"
            db.add(d)

    # Continue
    if ok:
        print(f"[DEBUG] Enqueue index for doc_id={document_id}", flush=True)
        index.delay(job_id, document_id)
    else:
        _maybe_trigger_analysis(job_id)


# ---------------------------- INDEX ----------------------------

@celery_app.task
def index(job_id: str, document_id: int):
    print(f"[DEBUG] index received job={job_id} doc_id={document_id}", flush=True)

    # Read primitives while session is OPEN
    with get_session() as db:
        d = db.get(Document, document_id)
        if not d:
            print("[WARN] index: missing Document", flush=True)
            return
        s = db.get(Source, d.source_id)
        j = db.get(Job, job_id)
        if not s or not j:
            print("[WARN] index: missing Source/Job", flush=True)
            return

        url = s.url
        published_at = s.published_at
        kind = s.kind
        person = j.person

    # Prepare text outside session (re-fetch for simplicity)
    ok = True
    text = ""
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": "DeepResearchBot"})
        r.raise_for_status()
        ctype = (r.headers.get("Content-Type", "") or "").split(";")[0].strip().lower()
        if "html" in ctype or url.lower().endswith((".html", ".htm", "/")):
            text = html_to_text(r.content, url=url)
        elif "pdf" in ctype or url.lower().endswith(".pdf"):
            tmp = f"/tmp/{document_id}.pdf"
            with open(tmp, "wb") as f:
                f.write(r.content)
            text = pdf_to_text(tmp)
        else:
            text = ""
    except Exception as e:
        print(f"[ERROR] index fetch failed: {e}\n{traceback.format_exc()}", flush=True)
        ok = False

    if ok and text and len(text.strip()) >= 200:
        chunk_size = 1500
        chunks = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]
        for ch in chunks:
            vec = embed(ch[:1000])
            payload = {
                "job_id": job_id,
                "person": person,
                "source_url": url,
                "published_at": published_at,
                "kind": kind,
                "text": ch[:1200],
            }
            upsert_chunk(vec, payload)
    else:
        print("[DEBUG] index: too little text or fetch failed, skipping embedding", flush=True)

    # Mark doc terminal status
    with get_session() as db:
        d = db.get(Document, document_id)
        if d:
            d.status = "indexed" if ok and text and len(text.strip()) >= 200 else "index_failed"
            d.updated_at = datetime.utcnow()
            db.add(d)

    # If no pending docs remain, analyze
    _maybe_trigger_analysis(job_id)


# ---------------------------- ANALYZE ----------------------------

@celery_app.task

def analyze_timeline(job_id: str):
    print(f"[DEBUG] analyze_timeline start job={job_id}", flush=True)
    _set_status(job_id, "analyzing")

    # Read person locally
    with get_session() as db:
        j = db.get(Job, job_id)
        if not j:
            print("[WARN] analyze_timeline: missing Job", flush=True)
            _set_status(job_id, "analysis_failed")
            return
        person = j.person

    try:
        # First pass
        tl = make_timeline(job_id, person) or []

        # Optional: if short, try again (e.g., your rag.py can accept k to increase context)
        if len(tl) < 8:
            tl = make_timeline(job_id, person) or []

        # --- de-duplicate & save events ---
        seen, uniq = set(), []
        for e in (tl or []):
            date  = (e.get("date")  or "").strip()
            event = (e.get("event") or "").strip()
            if not event:
                continue
            key = (date, event.lower())
            if key in seen:
                continue
            seen.add(key)
            uniq.append(e)

        with get_session() as db:
            for e in uniq:
                ev = Event(
                    job_id=job_id,
                    date=e.get("date"),
                    event_text=e.get("event", ""),
                )
                ev.citations = e.get("citations") or []
                db.add(ev)

        _set_status(job_id, "complete")
        print(f"[DEBUG] analyze_timeline complete job={job_id} events={len(uniq)}", flush=True)

    except Exception as e:
        print(f"[ERROR] analyze_timeline failed: {e}\n{traceback.format_exc()}", flush=True)
        _set_status(job_id, "analysis_failed")

#def analyze_timeline(job_id: str):
#    print(f"[DEBUG] analyze_timeline start job={job_id}", flush=True)
#    _set_status(job_id, "analyzing")

#    # Read person locally
#    with get_session() as db:
#        j = db.get(Job, job_id)
#        if not j:
#            print("[WARN] analyze_timeline: missing Job", flush=True)
#            _set_status(job_id, "analysis_failed")
#            return
#        person = j.person

#    try:
#        tl = make_timeline(job_id, person) or []
#        
#        with get_session() as db:
#            for e in tl:
#                ev = Event(
#                    job_id=job_id,
#                    date=e.get("date"),
#                    event_text=e.get("event", ""),
#                )
#                ev.citations = e.get("citations") or []
#                db.add(ev)
#        _set_status(job_id, "complete")
#        print(f"[DEBUG] analyze_timeline complete job={job_id} events={len(tl)}", flush=True)
#    except Exception as e:
#        print(f"[ERROR] analyze_timeline failed: {e}\n{traceback.format_exc()}", flush=True)
#        _set_status(job_id, "analysis_failed")


# ---------------------------- FINALIZE (fallback) ----------------------------

@celery_app.task
def finalize_job(job_id: str):
    with get_session() as db:
        job = db.get(Job, job_id)
        if job and job.status in ("analyzing", "complete", "analysis_failed"):
            print(f"[DEBUG] finalize_job: job {job_id} already {job.status}, skipping")
            return

        total = db.query(func.count(Document.id)).filter(Document.job_id == job_id).scalar() or 0
        pending = db.query(func.count(Document.id)).filter(
            Document.job_id == job_id,
            Document.status.notin_(["indexed", "index_failed", "normalize_failed"])
        ).scalar() or 0

    print(f"[DEBUG] finalize_job job={job_id} total_docs={total} pending_docs={pending}")
    if total == 0:
        # try again shortly; maybe every source was blocked and docs haven't materialized
        finalize_job.apply_async((job_id,), countdown=20)
        return

    if pending == 0:
        analyze_timeline.delay(job_id)
    else:
        finalize_job.apply_async((job_id,), countdown=20)
