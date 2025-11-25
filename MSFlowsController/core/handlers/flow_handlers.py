"""
Handlers for different flow types.
"""
import logging
from typing import Dict, Any, Optional

from core.interfaces import FlowExecutorInterface
from worker.flowscontroller import FlowController
from core.utils.message_utils import has_dynamic_definition

logger = logging.getLogger(__name__)


class FlowStartHandler:
    """Handler for flow initiation."""
    
    def __init__(
        self, 
        dynamic_flow_service: FlowExecutorInterface,
        classic_flow_controller: Optional[FlowController] = None
    ) -> None:
        self.dynamic_flow_service = dynamic_flow_service
        self.classic_flow_controller = classic_flow_controller or FlowController()
    
    def handle_flow_start(self, message_json: Dict[str, Any], flow_id: str, account: str) -> Dict[str, Any]:
        """
        Handles the start of a flow.
        Determines whether to use the dynamic executor or the classic pipeline.
        """
        
        logger.info(f"🚀 STARTING FLOW - Flow ID: {flow_id}, Account: {account}")
        logger.info(f"📋 FLOW DATA: {message_json}")
        
        if has_dynamic_definition(message_json):
            logger.info("🔄 DYNAMIC FLOW - Embedded definition detected; using dynamic executor")
            steps_count = len(message_json.get('flow_config', {}).get('steps', []))
            logger.info(f"📊 STEPS COUNT: {steps_count}")
            
            result = self.dynamic_flow_service.execute_flow(message_json)
            logger.info(f"✅ DYNAMIC FLOW COMPLETED - Result: {result}")
        else:
            logger.info(f"🔄 CLASSIC FLOW - Start flow detected for flow: {flow_id}, account: {account}")
            result = self.classic_flow_controller.start_flow(message_json)
        
        logger.info(f"Start flow result: {result}")
        
        if result.get('status') in ['success', 'started']:
            logger.info(f"Flow {result.get('flow_id')} started successfully for account: {result.get('account')}")
            return {
                "status": "success",
                "flow_id": result.get('flow_id'),
                "account": result.get('account'),
                "run_id": result.get('run_id'),
                "flow_definition": result.get('flow_definition'),
                "execution_status": result.get('execution_status')
            }
        else:
            error_msg = result.get('error', 'No error details available')
            logger.error(f"Error starting flow {flow_id}: {error_msg}")
            return {
                "status": "error",
                "flow_id": result.get('flow_id'),
                "account": result.get('account'),
                "run_id": result.get('run_id'),
                "flow_definition": result.get('flow_definition'),
                "execution_status": result.get('execution_status'),
                "error": error_msg,
                "task_id": None,
                "run_id": None
            }


class FlowContinuationHandler:
    """Handler for flow continuation."""
    
    def __init__(self, dynamic_flow_service: FlowExecutorInterface) -> None:
        self.dynamic_flow_service = dynamic_flow_service
    
    def handle_flow_continuation(
        self, 
        message_json: Dict[str, Any], 
        flow_id: str, 
        account: str, 
        task_id: str, 
        run_id: str
    ) -> Dict[str, Any]:
        """
        Handles the continuation of a dynamic flow.
        """
        
        logger.info(f"Starting dynamic orchestration for flow: {flow_id}, account: {account}, task: {task_id}, run: {run_id}")
        
        # Execute dynamic flow using injected service
        result = self.dynamic_flow_service.execute_flow(message_json)

        if result['status'] == 'success':
            logger.info(f"Flow {flow_id} continued successfully for account: {account}")
            return {
                "status": "flow_started",
                "flow_id": flow_id,
                "account": account,
                "task_id": task_id,
                "run_id": run_id
            }
        else:
            error_msg = result.get('error', 'No error details available')
            logger.error(f"Error continuing flow {flow_id}: {error_msg}")
            return {
                "status": "error",
                "flow_id": flow_id,
                "error": error_msg,
                "task_id": task_id,
                "run_id": run_id
            }
