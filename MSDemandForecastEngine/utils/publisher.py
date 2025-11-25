"""
Utilidades para publicación de mensajes a Pub/Sub.
"""
import json
import logging
import os
from typing import Dict, Any
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)


def publish_message(topic_id: str, payload: Dict[str, Any]) -> str:
    """
    Publica un mensaje a un topic de Pub/Sub.
    
    Args:
        topic_id: ID del topic
        payload: Datos a publicar
        
    Returns:
        str: ID del mensaje publicado
    """
    try:
        project_id = os.environ.get('GOOGLE_CLOUD_PROJECT', 'mind-hotel-insights-dev')
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)
        
        message_data = json.dumps(payload).encode('utf-8')
        future = publisher.publish(topic_path, message_data)
        message_id = future.result()
        
        logger.info(f"Mensaje publicado en topic {topic_id} con ID: {message_id}")
        return message_id
        
    except Exception as e:
        logger.error(f"Error publicando mensaje en topic {topic_id}: {str(e)}")
        raise
