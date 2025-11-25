"""
Service for executing dynamic N8N-like flows.
"""
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from core.interfaces import (
    FlowDefinitionRepositoryInterface,
    FlowRunStateRepositoryInterface,
    PublisherInterface,
    FlowExecutorInterface
)
from core.config import config
from core.exceptions import (
    FlowConfigurationError, 
    FlowDefinitionNotFoundError, 
    StateNotFoundError, 
    FlowExecutionError
)
from core.utils.logging_utils import get_flow_logger
from flows.normalization import normalize_steps, is_advanced_flow
from n8n_engine import N8NLikeEngine

# Base logger
logger = logging.getLogger(__name__)

class DynamicFlowService(FlowExecutorInterface):
    """
    Service for executing dynamic flows with N8N-like configuration.
    """
    
    def __init__(
        self,
        flow_repo: FlowDefinitionRepositoryInterface,
        state_repo: FlowRunStateRepositoryInterface,
        publisher: PublisherInterface
    ):
        self.flow_repo = flow_repo
        self.state_repo = state_repo
        self.publisher = publisher
        
    def execute_flow(self, message_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes a dynamic flow.
        """
        flow_id = message_json.get('flow_id')
        run_id = message_json.get('run_id')
        task_id = message_json.get('task_id')
        account = message_json.get('account')
        
        # Create contextual logger
        log = get_flow_logger(__name__, flow_id, run_id, task_id)
        
        try:
            log.info(f"=== STARTING DYNAMIC FLOW SERVICE ===")
            log.info(f"Account: {account}")
            
            # Determine if it's a new flow or continuation
            if not run_id and not task_id:
                return self._handle_new_flow(account, flow_id, message_json, log)
            else:
                return self._handle_continuation(flow_id, run_id, task_id, message_json, log)
                
        except Exception as e:
            log.error(f"Error executing dynamic flow: {str(e)}")
            # Return error dict for now to maintain contract, but could re-raise
            return {
                "status": "error",
                "error": str(e)
            }

    def _handle_new_flow(self, account: str, flow_id: str, message_json: Dict[str, Any], log: logging.LoggerAdapter) -> Dict[str, Any]:
        """Handles the start of a new flow."""
        log.info("🚀 NEW FLOW START - Reading definition")
        
        flow_definition = self.flow_repo.get_flow_definition(account, flow_id)
        
        if not flow_definition:
            log.info("📋 No definition in repo, using message config")
            flow_config = message_json.get('flow_config', message_json)
        else:
            log.info(f"📋 Using definition from repo for account {account}")
            flow_config = flow_definition
            
        run_id = str(uuid.uuid4())
        # Update logger with new run_id
        log.extra['run_id'] = run_id
        log.info(f"🆔 New Run ID generated: {run_id}")
        
        initial_state = {
            "flow_id": flow_id,
            "run_id": run_id,
            "account": account,
            "status": "running",
            "flow_config": flow_config,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "steps": []
        }
        self.state_repo.save_flow_run_state(flow_id, run_id, initial_state)
        log.info("✅ Initial state saved")
        
        return self._execute_engine(flow_config, flow_id, account, run_id, None, message_json, log)

    def _handle_continuation(self, flow_id: str, run_id: str, task_id: str, message_json: Dict[str, Any], log: logging.LoggerAdapter) -> Dict[str, Any]:
        """Handles the continuation of an existing flow."""
        log.info("🔄 FLOW CONTINUATION - Reading state")
        
        if not run_id:
            raise FlowConfigurationError("run_id is required for continuation", flow_id)
            
        flow_state = self.state_repo.get_flow_run_state(flow_id, run_id)
        if not flow_state:
            raise StateNotFoundError(f"Execution state not found for {flow_id}/{run_id}", flow_id, run_id)
            
        flow_config = flow_state.get('flow_config', {})
        if not flow_config:
            raise FlowConfigurationError("No flow configuration found in state", flow_id, run_id)
            
        return self._execute_engine(flow_config, flow_id, flow_state.get('account'), run_id, task_id, message_json, log)

    def _execute_engine(
        self, 
        flow_config: Dict[str, Any], 
        flow_id: str, 
        account: str, 
        run_id: str, 
        task_id: Optional[str], 
        message_json: Dict[str, Any],
        log: logging.LoggerAdapter
    ) -> Dict[str, Any]:
        """Executes the N8N engine logic."""
        
        steps = normalize_steps(flow_config)
        flow_config['steps'] = steps
        
        if not steps:
            raise FlowConfigurationError("No steps defined in flow configuration", flow_id, run_id)
            
        initial_context = {
            'flow_id': flow_id,
            'account': account,
            'task_id': task_id,
            'run_id': run_id,
            **message_json.get('context', {})
        }
        
        if is_advanced_flow(steps):
            log.info("🚀 Advanced flow detected - using N8N-like engine")
            # Inject our publisher into the engine
            engine = N8NLikeEngine(config.PROJECT_ID, self.publisher)
            result = engine.execute_flow(flow_config, initial_context)
            
            # Save updated state
            flow_state = {
                "flow_id": flow_id,
                "run_id": run_id,
                "account": account,
                "status": result.get("status", "success"),
                "flow_config": flow_config,
                "executed_steps": result.get('executed_steps', []),
                "total_steps": len(result.get('executed_steps', [])),
                "engine_type": "n8n_like",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat()
            }
            
            self.state_repo.save_flow_run_state(flow_id, run_id, flow_state)
            
            return {
                "status": result.get("status", "success"),
                "flow_id": flow_id,
                "account": account,
                "task_id": task_id,
                "run_id": run_id,
                "executed_steps": result.get('executed_steps', []),
                "total_steps": len(result.get('executed_steps', [])),
                "engine_type": "n8n_like"
            }
        else:
            log.info("📋 Basic flow detected - using simple engine (placeholder)")
            return {"status": "success"}
