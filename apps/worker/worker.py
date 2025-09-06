import logging
from kombu import Exchange, Queue
import os
from celery import Celery

# Read broker URL from env (default to your docker-compose redis service)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

celery_app = Celery(
    "deep_research",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["apps.worker.tasks"],
)

# Default routing / queue config
celery_app.conf.update(
    task_default_queue="default",
    task_default_exchange="default",
    task_default_exchange_type="direct",
    task_default_routing_key="default",
    task_queues=(
        Queue("default", Exchange("default"), routing_key="default"),
    ),
    task_routes={
        "apps.worker.tasks.run_discovery": {"queue": "default"},
        "apps.worker.tasks.fetch_source": {"queue": "default"},
        "apps.worker.tasks.normalize": {"queue": "default"},
        "apps.worker.tasks.index": {"queue": "default"},
        "apps.worker.tasks.analyze_timeline": {"queue": "default"},
    },
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)


@celery_app.on_after_configure.connect
def startup_log(sender, **kwargs):
    """Log a friendly message once worker is configured."""
    logging.info("🚀 Celery worker started and connected to Redis at %s", REDIS_URL)
    logging.info("✅ Registered tasks: %s", list(sender.tasks.keys()))

