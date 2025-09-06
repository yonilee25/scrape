# apps/worker/tasks.py

from __future__ import annotations

import os
import hashlib
import traceback
from datetime import datetime

import requests

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


def _set_status(job_id: str, status: str) -> None:
    """Update job status with a timestamp."""
    with get_session() as db:
        job = db.query(Job).get(job_id)
        if job:
            job.status = status
            job.updated_at = datetime.utcnow()
            db.add(job)


def _hash_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


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

    _set_status(job_id, "fetching")
    print(f"[DEBUG] Updating job {job_id} status to 'fetching'", flush=True)


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
            s = db.query(Source).get(source_id)
            if s:
                s.status = "blocked_by_robots"
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
            s = db.query(Source).get(source_id)
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
            s = db.query(Source).get(source_id)
            if s:
                s.status = "fetch_failed"


# ---------------------------- NORMALIZE ----------------------------

@celery_app.task
def normalize(job_id: str, document_id: int):
    print(f"[DEBUG] normalize received job={job_id} doc_id={document_id}", flush=True)
    with get_session() as db:
        d = db.query(Document).get(document_id)
        s = db.query(Source).get(d.source_id) if d else None
        if not d or not s:
            print("[WARN] normalize: missing Document or Source", flush=True)
            return

    try:
        r = requests.get(s.url, timeout=30, headers={"User-Agent": "DeepResearchBot"})
        ctype = (r.headers.get("Content-Type", "") or "").split(";")[0].strip().lower()

        text_out = ""
        if "html" in ctype or s.url.lower().endswith((".html", ".htm", "/")):
            text_out = html_to_text(r.content, url=s.url)
        elif "pdf" in ctype or s.url.lower().endswith(".pdf"):
            tmp = f"/tmp/{document_id}.pdf"
            with open(tmp, "wb") as f:
                f.write(r.content)
            text_out = pdf_to_text(tmp)
        else:
            text_out = ""

        text_key = f"jobs/{job_id}/normalized/{document_id}.txt"
        text_path = put_bytes(text_key, (text_out or "").encode("utf-8"), content_type="text/plain")
        print(f"[DEBUG] normalize wrote {text_path} (len={len(text_out)})", flush=True)

        with get_session() as db:
            d = db.query(Document).get(document_id)
            if d:
                d.text_path = text_path
                d.status = "normalized"
                db.add(d)

        print(f"[DEBUG] Enqueue index for doc_id={document_id}", flush=True)
        index.delay(job_id, document_id)

    except Exception as e:
        print(f"[ERROR] normalize failed: {e}\n{traceback.format_exc()}", flush=True)
        with get_session() as db:
            d = db.query(Document).get(document_id)
            if d:
                d.status = "normalize_failed"


# ---------------------------- INDEX ----------------------------

@celery_app.task
def index(job_id: str, document_id: int):
    print(f"[DEBUG] index received job={job_id} doc_id={document_id}", flush=True)
    with get_session() as db:
        d = db.query(Document).get(document_id)
        s = db.query(Source).get(d.source_id) if d else None
        job = db.query(Job).get(job_id) if d else None
        if not d or not s or not job:
            print("[WARN] index: missing Document/Source/Job", flush=True)
            return

    try:
        r = requests.get(s.url, timeout=30, headers={"User-Agent": "DeepResearchBot"})
        ctype = (r.headers.get("Content-Type", "") or "").split(";")[0].strip().lower()
        if "html" in ctype or s.url.lower().endswith((".html", ".htm", "/")):
            text = html_to_text(r.content, url=s.url)
        elif "pdf" in ctype or s.url.lower().endswith(".pdf"):
            tmp = f"/tmp/{document_id}.pdf"
            with open(tmp, "wb") as f:
                f.write(r.content)
            text = pdf_to_text(tmp)
        else:
            text = ""

        if not text or len(text.strip()) < 200:
            print("[DEBUG] index: too little text, skipping embedding", flush=True)
            return

        chunk_size = 1500
        chunks = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]
        for ch in chunks:
            vec = embed(ch[:1000])
            payload = {
                "job_id": job_id,
                "person": job.person,
                "source_url": s.url,
                "published_at": s.published_at,
                "kind": s.kind,
                "text": ch[:1200],
            }
            upsert_chunk(vec, payload)

        with get_session() as db:
            d = db.query(Document).get(document_id)
            if d:
                d.status = "indexed"
                db.add(d)

        # kick analysis after indexing
        analyze_timeline.delay(job_id)

    except Exception as e:
        print(f"[ERROR] index failed: {e}\n{traceback.format_exc()}", flush=True)
        with get_session() as db:
            d = db.query(Document).get(document_id)
            if d:
                d.status = "index_failed"


# ---------------------------- ANALYZE ----------------------------

@celery_app.task
def analyze_timeline(job_id: str):
    print(f"[DEBUG] analyze_timeline start job={job_id}", flush=True)
    _set_status(job_id, "analyzing")
    with get_session() as db:
        job = db.query(Job).get(job_id)
        if not job:
            print("[WARN] analyze_timeline: missing Job", flush=True)
            return
    try:
        tl = make_timeline(job_id, job.person) or []
        with get_session() as db:
            for e in tl:
                ev = Event(
                    job_id=job_id,
                    date=e.get("date"),
                    event_text=e.get("event", ""),
                )
                ev.citations = e.get("citations") or []
                db.add(ev)
        _set_status(job_id, "complete")
        print(f"[DEBUG] analyze_timeline complete job={job_id} events={len(tl)}", flush=True)
    except Exception as e:
        print(f"[ERROR] analyze_timeline failed: {e}\n{traceback.format_exc()}", flush=True)
        _set_status(job_id, "analysis_failed")
