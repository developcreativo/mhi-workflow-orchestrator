"""
Publisher service implementation using Google Cloud Pub/Sub.
"""
import json
import logging
from typing import Dict, Any
from google.cloud import pubsub_v1
from core.interfaces import PublisherInterface
from core.config import config

logger = logging.getLogger(__name__)

class PubSubPublisher(PublisherInterface):
    """Google Cloud Pub/Sub implementation of PublisherInterface."""
    
    def __init__(self, project_id: str = None):
        self.project_id = project_id or config.PROJECT_ID
        self.publisher = pubsub_v1.PublisherClient()
        
    def publish(self, topic: str, message: Dict[str, Any]) -> str:
        """
        Publishes a message to a Pub/Sub topic.
        
        Args:
            topic: The topic name or ID.
            message: The message dictionary to publish.
            
        Returns:
            str: The message ID of the published message.
        """
        try:
            # Construct full topic path if it's just a topic name
            if '/' not in topic:
                topic_path = self.publisher.topic_path(self.project_id, topic)
            else:
                topic_path = topic
                
            message_bytes = json.dumps(message).encode('utf-8')
            future = self.publisher.publish(topic_path, message_bytes)
            message_id = future.result()
            
            logger.info(f"📤 MESSAGE PUBLISHED - Topic: {topic}, Message ID: {message_id}")
            return message_id
            
        except Exception as e:
            logger.error(f"❌ ERROR PUBLISHING MESSAGE - Topic: {topic}, Error: {str(e)}")
            raise
