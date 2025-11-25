import json
import logging
from typing import Dict, Any
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)


def publish_message(project_id: str, topic_id: str, payload: Dict[str, Any]) -> str:
    """Publica un mensaje en Pub/Sub y devuelve el message_id."""
    publisher = pubsub_v1.PublisherClient()
    # Permitir topic como nombre simple o full path
    if topic_id.startswith("projects/"):
        topic_path = topic_id
    else:
        topic_path = publisher.topic_path(project_id, topic_id)

    data = json.dumps(payload).encode("utf-8")
    future = publisher.publish(topic=topic_path, data=data)
    message_id = future.result()
    logger.info(f"Mensaje publicado en {topic_id}: {message_id}")
    return message_id


