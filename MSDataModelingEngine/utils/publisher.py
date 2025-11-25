import json
from typing import Any, Dict
from google.cloud import pubsub_v1
import logging

logger = logging.getLogger(__name__)


def publish_message(project_id: str, topic_id: str, payload: Dict[str, Any]) -> str:
    """
    Publica un mensaje JSON en Pub/Sub y devuelve el message_id.
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    data = json.dumps(payload).encode('utf-8')
    future = publisher.publish(topic_path, data)
    message_id = future.result()
    logger.info(f"Published message to {topic_id}: {message_id}")
    return message_id






