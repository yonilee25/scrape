# libs/analyze/rag.py
import os, json, re
import httpx
from libs.common.config import Settings
from libs.common.qdrant_utils import search_similar

settings = Settings()

def embed(text: str) -> list[float]:
    # Uses Ollama's embeddings endpoint
    url = settings.OLLAMA_HOST.rstrip("/") + "/api/embeddings"
    payload = {"model": settings.OLLAMA_EMBED, "prompt": text}
    with httpx.Client(timeout=60) as client:
        r = client.post(url, json=payload)
        r.raise_for_status()
        data = r.json()
        return data.get("embedding", [])

def _parse_ndjson(text: str) -> str:
    """Stitch streamed NDJSON chat into a single assistant content string."""
    chunks = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        msg = (obj or {}).get("message") or {}
        part = msg.get("content")
        if part:
            chunks.append(part)
        if obj.get("done"):
            break
    return "".join(chunks).strip()

def chat(system_prompt: str, user_prompt: str) -> str:
    """Call Ollama chat and return assistant message content (robust to streaming)."""
    url = settings.OLLAMA_HOST.rstrip("/") + "/api/chat"
    payload = {
        "model": settings.OLLAMA_LLM,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
        # IMPORTANT: request a single JSON object (not NDJSON stream)
        "stream": False,
        "options": {"temperature": 0.2, "num_ctx": 4096},
    }
    with httpx.Client(timeout=120) as client:
        r = client.post(url, json=payload)
        r.raise_for_status()
        # Preferred path: single JSON object
        try:
            data = r.json()
            msg = (data or {}).get("message") or {}
            return (msg.get("content") or "").strip()
        except ValueError:
            # Fallback: NDJSON body
            return _parse_ndjson(r.text)

def _extract_json_block(text: str):
    """Pull a JSON object from LLM text. Handles ```json ... ``` fences and stray prose."""
    # fenced block first
    m = re.search(r"```json\s*(.*?)\s*```", text, flags=re.S | re.I)
    if m:
        block = m.group(1)
        try:
            return json.loads(block)
        except Exception:
            pass
    # fallback: first {...} span
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(text[start:end + 1])
        except Exception:
            pass
    # some models return a top-level list
    if text.strip().startswith("[") and text.strip().endswith("]"):
        try:
            return json.loads(text)
        except Exception:
            pass
    return None

def make_timeline(job_id: str, person: str) -> list[dict]:
    # Retrieve top chunks relevant to "timeline" for this person
    q = f"Key dated events for {person}. Provide event date and description."
    vec = embed(q)
    hits = search_similar(vec, job_id=job_id, limit=16)

    # Build compact context with citations
    ctx_lines = []
    for i, h in enumerate(hits):
        pl = h.payload or {}
        src = pl.get("source_url", "")
        date = pl.get("published_at", "")
        text = (pl.get("text", "") or "").replace("\n", " ")
        text = text[:600]
        ctx_lines.append(f"[{i+1}] {date} {text} (src: {src})")

    # STRONGLY bias model toward valid JSON (double quotes, exact keys)
    system = (
        "You are a precise analyst. Use ONLY the provided context. "
        "Respond with a single JSON object ONLY, no prose, in this schema: "
        '{"timeline":[{"date":"YYYY-MM-DD or YYYY","event":"...","citations":[{"url":"...","label":"source"}]}]}'
    )
    user = (
        "Context:\n" + "\n".join(ctx_lines) +
        f"\n\nTask: Extract 3–5 key dated events about {person}. "
        "Every event MUST have a date string and citations using the src URLs. "
        "Respond with JSON ONLY matching the schema above."
    )

    raw = chat(system, user).strip()

    # Robust parse
    obj = _extract_json_block(raw)
    timeline = []
    if isinstance(obj, dict):
        timeline = obj.get("timeline") or obj.get("events") or []
    elif isinstance(obj, list):
        timeline = obj  # sometimes the model returns a bare list

    # Normalize to expected structure
    out = []
    for e in (timeline or []):
        if not isinstance(e, dict):
            continue
        date = str(e.get("date", "") or "").strip()
        event = str(e.get("event", "") or e.get("text", "")).strip()
        cits = e.get("citations") or []
        if not isinstance(cits, list):
            cits = []
        norm_cits = []
        for c in cits:
            if isinstance(c, dict):
                url = c.get("url") or c.get("source") or ""
                label = c.get("label") or "source"
            elif isinstance(c, str):
                url = c; label = "source"
            else:
                continue
            if url:
                norm_cits.append({"url": url, "label": label})
        if event:
            out.append({"date": date, "event": event, "citations": norm_cits})
    return out
