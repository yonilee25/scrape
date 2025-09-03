import trafilatura
from pypdf import PdfReader
from pathlib import Path

def html_to_text(html_bytes: bytes, url: str | None = None) -> str:
    # trafilatura expects a string; it can also take URL context to improve extraction
    try:
        downloaded = trafilatura.extract(html_bytes.decode("utf-8", errors="ignore"), include_comments=False, include_tables=False)
        return downloaded or ""
    except Exception:
        return ""

def pdf_to_text(pdf_path: str) -> str:
    try:
        reader = PdfReader(pdf_path)
        texts = []
        for page in reader.pages:
            t = page.extract_text() or ""
            texts.append(t)
        return "\n".join(texts)
    except Exception:
        return ""
