import httpx
from libs.common.config import Settings
from libs.common.qdrant_utils import search_similar
import json

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

def chat(system_prompt: str, user_prompt: str) -> str:
    url = settings.OLLAMA_HOST.rstrip("/") + "/api/chat"
    payload = {
        "model": settings.OLLAMA_LLM,
        "messages": [
            {"role":"system", "content": system_prompt},
            {"role":"user", "content": user_prompt},
        ],
        "options": {"temperature": 0.2, "num_ctx": 4096}
    }
    with httpx.Client(timeout=120) as client:
        r = client.post(url, json=payload)
        r.raise_for_status()
        data = r.json()
        return data["message"]["content"]

def make_timeline(job_id: str, person: str) -> list[dict]:
    # Retrieve top chunks relevant to "timeline" for this person
    q = f"Key dated events for {person}. Provide event date and description."
    vec = embed(q)
    hits = search_similar(vec, job_id=job_id, limit=6)
    # Build compact context with citations
    ctx_lines = []
    for i, h in enumerate(hits):
        pl = h.payload or {}
        src = pl.get("source_url","")
        date = pl.get("published_at","")
        text = pl.get("text","")[:600].replace("\n"," ")
        ctx_lines.append(f"[{i+1}] {date} {text} (src: {src})")

    system = "You are a precise analyst. Use ONLY the provided context. Return JSON with fields: timeline=[{date:'YYYY-MM-DD or YYYY', event:'...', citations:[{url:'...', label:'source'}]}]. If you are unsure, return an empty list."
    user = "Context:\n" + "\n".join(ctx_lines) + f"\n\nTask: Extract 3–5 key dated events about {person}. Use citations using the src URLs. Respond with JSON only."
    raw = chat(system, user).strip()
    # Try to parse JSON; if it fails, return empty timeline
    try:
        data = json.loads(raw)
        return data.get("timeline", [])
    except Exception:
        # Very small fallback: empty
        return []
