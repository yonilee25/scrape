import os
import json

# Lazy import of faster_whisper
try:
    from faster_whisper import WhisperModel
except ImportError:
    WhisperModel = None

# Default model (can be overridden by WHISPER_MODEL env var)
WHISPER_MODEL = os.getenv("WHISPER_MODEL", "small")

def transcribe(audio_path: str, lang: str | None = None) -> dict:
    if WhisperModel is None:
        raise RuntimeError(
            "faster-whisper is not installed in this container. "
            "Run transcription in the worker service, or install faster-whisper here."
        )

    model = WhisperModel(WHISPER_MODEL, device="auto", compute_type="int8")
    segments, info = model.transcribe(audio_path, language=lang, vad_filter=True)

    return {
        "language": info.language,
        "segments": [
            {"start": seg.start, "end": seg.end, "text": seg.text.strip()}
            for seg in segments
        ]
    }
