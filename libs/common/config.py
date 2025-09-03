from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    ENV: str = Field(default="dev")
    TZ: str = Field(default="UTC")

    DATABASE_URL: str
    REDIS_URL: str

    MINIO_ENDPOINT: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    MINIO_BUCKET: str = "research"

    VECTOR_URL: str

    OLLAMA_HOST: str = "http://ollama:11434"
    OLLAMA_LLM: str = "qwen2.5:7b-instruct"
    OLLAMA_EMBED: str = "nomic-embed-text"
    EMBEDDING_DIM: int = 768

    SEARXNG_URL: str | None = None
    RSSHUB_URL: str | None = None

    DISCOVERY_MAX_RESULTS: int = 25

    class Config:
        env_file = ".env"
