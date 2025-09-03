# Deep Research Starter (Python-only, local)

All-local starter for a private deep-web/person research pipeline using **Python only**:

- **FastAPI** (API + server-rendered UI with HTMX)
- **Celery + Redis** (background pipeline)
- **PostgreSQL** (metadata)
- **MinIO** (S3-style storage for raw files & transcripts)
- **Qdrant** (vector DB)
- **Ollama** (LLM: `qwen2.5:7b-instruct`; Embeddings: `nomic-embed-text`)
- **Providers**: SearXNG (optional), Sitemaps (basic demo), WordPress JSON (basic demo)
- **Normalization**: `trafilatura` for HTML, `pypdf` for PDFs, `faster-whisper` for audio

> This is a *starter*. It is intentionally lightweight and focuses on the
> architecture and "contracts" between stages. Extend to your needs.

---

## 0) Prereqs (Windows 11 recommended with WSL2)

1. **WSL2** with Ubuntu: open PowerShell (Admin) → `wsl --install -d Ubuntu`
2. **Docker Desktop** → Settings → Use WSL2 backend (enable your Ubuntu distro)
3. **NVIDIA driver** (if you have an RTX GPU) → enable GPU support in Docker Desktop
4. **Clone or unzip** this folder into your WSL home (or any path Docker can see)

## 1) First run (core services)

```bash
cd infra
# Bring up core infra + API + worker (no LLM yet)
docker compose --profile core up -d postgres redis minio qdrant api worker
```

Check health:
- API: http://localhost:8000/ (should show a form)
- MinIO console: http://localhost:9001 (user: admin / pass: admin12345)
- Qdrant: http://localhost:6333/dashboard (basic info)

## 2) Add the LLM & embeddings

```bash
# Start Ollama (LLM server)
docker compose --profile ai up -d ollama

# Pull models (Qwen 7B instruct + Nomic embedding)
docker exec -it ollama ollama pull qwen2.5:7b-instruct
docker exec -it ollama ollama pull nomic-embed-text
```

> If VRAM is tight, keep context small (e.g., 4096) and avoid running diarization at the same time.

## 3) (Optional) Discovery helpers

```bash
# SearXNG metasearch + RSSHub for more leads
docker compose --profile discovery up -d searxng rsshub
```

## 4) Use the app

1. Open http://localhost:8000
2. Enter a person's name and click **Start research**
3. The job page will load with status. It will progressively fill in **Sources** and a **Timeline**.
4. Click a source to open the original link.

## 5) Environment

Duplicate `.env.example` as `.env` and adjust as needed.

## 6) Notes

- This starter respects **robots.txt** and avoids paywalls/DRM/captchas.
- The normalization uses light-weight libs (trafilatura/pypdf). For advanced parsing (tables, Office docs, OCR), consider `unstructured` and `tesseract` later.
- Embeddings default to **Ollama (`nomic-embed-text`)** to avoid large Python model downloads.
- For diarization/speaker labels, add WhisperX/pyannote later (heavier).

## 7) Troubleshooting

- If worker can't reach Ollama, ensure `OLLAMA_HOST=http://ollama:11434` and that the container is running.
- If embeddings dimension mismatch, update `EMBEDDING_DIM` in `.env` to match your embedding model (e.g., nomic-embed-text is 768). Then recreate the collection or let the code create it fresh.
- On Windows, ensure Docker Desktop has file-sharing permission for this folder.

## 8) Shutdown

```bash
docker compose down
```

## 9) License

MIT (for the starter scaffolding).
