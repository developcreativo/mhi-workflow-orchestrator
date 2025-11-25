import os
from typing import Optional
from celery import Celery


def _get_env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def create_celery_app() -> Celery:
    """
    Create and configure a Celery application.

    Environment variables:
    - CELERY_BROKER_URL: e.g. redis://localhost:6379/0 or amqp://guest:guest@localhost:5672//
    - CELERY_RESULT_BACKEND: e.g. redis://localhost:6379/1 or rpc://
    - CELERY_TASK_DEFAULT_QUEUE: optional, default 'flows_controller'
    """
    
    # Prefer explicit broker URL; if missing and running in GCP, default to Pub/Sub broker
    broker_url = f"gcpubsub://projects/celery-test-471911"
    if not broker_url:
        project_id = os.getenv("_M_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
        if project_id:
            broker_url = f"gcpubsub://projects/celery-test-471911"
        else:
            raise RuntimeError("Missing CELERY_BROKER_URL and no GCP project id available")
    result_backend = os.getenv("CELERY_RESULT_BACKEND", broker_url)
    default_queue = os.getenv("CELERY_TASK_DEFAULT_QUEUE", "flows_controller")
    
    app = Celery(
        "flows_controller",
        broker=broker_url,
        backend=result_backend,
        include=["FlowsController.tasks"],
    )

    app.conf.update(
        task_default_queue=default_queue,
        task_acks_late=True,
        worker_prefetch_multiplier=1,
        task_time_limit=int(os.getenv("CELERY_TASK_TIME_LIMIT", "600")),
        task_soft_time_limit=int(os.getenv("CELERY_TASK_SOFT_TIME_LIMIT", "540")),
        broker_transport_options={
            "ack_deadline_seconds": int(os.getenv("CELERY_GCPUBSUB_ACK_DEADLINE", "60")),
            "polling_interval": float(os.getenv("CELERY_GCPUBSUB_POLLING_INTERVAL", "0.3")),
            "queue_name_prefix": os.getenv("CELERY_GCPUBSUB_QUEUE_PREFIX", "kombu-"),
            "expiration_seconds": int(os.getenv("CELERY_GCPUBSUB_EXPIRATION_SECONDS", "86400")),
        },
    )

    return app


# Create default app for celery -A FlowsController.celery_app.celery
celery = create_celery_app()


