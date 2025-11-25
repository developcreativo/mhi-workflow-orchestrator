"""
Main entry point for the FlowController Cloud Function.
"""
import logging
from functions_framework import cloud_event

from core.utils.message_utils import decode_message_data, extract_flow_identifiers
from core.handlers.flow_handlers import FlowStartHandler, FlowContinuationHandler
from core.handlers.callback_handler import CallbackHandler
from core.services.publisher import PubSubPublisher
from core.services.dynamic_flow_service import DynamicFlowService
from storage.repositories import FlowDefinitionRepository, FlowRunStateRepository
from core.notifications import NotificationService
from worker.flowscontroller import FlowController

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Dependency Injection Wiring ---

# 1. Infrastructure / Repositories
publisher = PubSubPublisher()
flow_repo = FlowDefinitionRepository()
state_repo = FlowRunStateRepository()
notification_service = NotificationService()

# 2. Domain Services
dynamic_flow_service = DynamicFlowService(
    flow_repo=flow_repo,
    state_repo=state_repo,
    publisher=publisher
)
classic_flow_controller = FlowController() # Legacy controller

# 3. Handlers
flow_start_handler = FlowStartHandler(
    dynamic_flow_service=dynamic_flow_service,
    classic_flow_controller=classic_flow_controller
)

flow_continuation_handler = FlowContinuationHandler(
    dynamic_flow_service=dynamic_flow_service
)

callback_handler = CallbackHandler(
    state_repo=state_repo,
    notification_service=notification_service,
    publisher=publisher
)

# -----------------------------------

@cloud_event
def FlowWorker(cloud_event):
    """
    Main entry point for the Cloud Function.
    Evaluates the received event and processes it according to its type.
    
    Args:
        cloud_event: Pub/Sub event with message data.
    """
    try:
        # Log received event
        logger.info(f"Event received: {cloud_event.data}")
        
        # Decode message
        message_json = decode_message_data(cloud_event)
        logger.info(f"Processing normalized message: {message_json}")
        
        # Extract flow identifiers
        flow_id, account, task_id, run_id = extract_flow_identifiers(message_json)
        
        # Determine processing type and execute
        if not task_id and not run_id:
            # New flow start
            logger.info(f"Processing new flow start: {message_json}")
            return flow_start_handler.handle_flow_start(message_json, flow_id, account)
        elif message_json.get('status') in ['completed', 'failed', 'success']:
            logger.info(f"Processing Cloud Function callback: {message_json}")
            # Cloud Function callback (completed or failed)
            source_step = message_json.get('step') or message_json.get('topic') or message_json.get('source') or 'unknown'
            logger.info(f"🔄 Processing Cloud Function callback: status={message_json.get('status')}, step={source_step}")
            return callback_handler.handle_task_callback(message_json)
        else:
            # Existing flow continuation
            logger.info(f"Processing existing flow continuation: {message_json}")
            return flow_continuation_handler.handle_flow_continuation(message_json, flow_id, account, task_id, run_id)
    except Exception as e:
        logger.error(f"Error processing event: {str(e)}", exc_info=True)
        raise