"""
Interfaces for the core components of the FlowController.
"""
from typing import Dict, Any, Optional, Protocol

class FlowDefinitionRepositoryInterface(Protocol):
    """Interface for retrieving flow definitions."""
    
    def get_flow_definition(self, account: str, flow_id: str) -> Optional[Dict[str, Any]]:
        ...

class FlowRunStateRepositoryInterface(Protocol):
    """Interface for managing flow execution states."""
    
    def get_flow_run_state(self, flow_id: str, run_id: str) -> Optional[Dict[str, Any]]:
        ...
        
    def save_flow_run_state(self, flow_id: str, run_id: str, state: Dict[str, Any]) -> None:
        ...

class NotificationServiceInterface(Protocol):
    """Interface for sending notifications."""
    
    def send_flow_notification(self, flow_state: Dict[str, Any], notification_type: str) -> None:
        ...

class PublisherInterface(Protocol):
    """Interface for publishing messages to a message broker."""
    
    def publish(self, topic: str, message: Dict[str, Any]) -> str:
        ...

class FlowExecutorInterface(Protocol):
    """Interface for executing flows."""
    
    def execute_flow(self, message_json: Dict[str, Any]) -> Dict[str, Any]:
        ...
