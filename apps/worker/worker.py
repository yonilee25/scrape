import os
from celery import Celery
from libs.common.config import Settings

settings = Settings()

celery_app = Celery(
    "deep_research",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
)
celery_app.conf.task_default_queue = "default"
celry_prefetch = os.getenv("CELERY_PREFETCH_MULTIPLIER", "1")
celery_app.conf.worker_prefetch_multiplier = int(celry_prefetch)

# import tasks so Celery registers them
import apps.worker.tasks  # noqa
