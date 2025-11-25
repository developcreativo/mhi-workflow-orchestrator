"""
Handler for processing Cloud Function callbacks and detecting internal errors.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from core.interfaces import (
    FlowRunStateRepositoryInterface,
    NotificationServiceInterface,
    PublisherInterface
)
from core.config import config

logger = logging.getLogger(__name__)


class CallbackHandler:
    """Handler for processing Cloud Function callbacks."""
    
    def __init__(
        self,
        state_repo: FlowRunStateRepositoryInterface,
        notification_service: NotificationServiceInterface,
        publisher: PublisherInterface
    ):
        self.state_repo = state_repo
        self.notification_service = notification_service
        self.publisher = publisher
        self.project_id = config.PROJECT_ID
    
    def handle_task_callback(self, message_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processes a Cloud Function callback.
        """
        flow_id = message_json.get('flow_id')
        run_id = message_json.get('run_id')
        task_id = message_json.get('task_id')
        account = message_json.get('account')
        
        # Create contextual logger
        from core.utils.logging_utils import get_flow_logger
        log = get_flow_logger(__name__, flow_id, run_id, task_id)
        
        try:
            # Normalize status: 'success' -> 'completed'
            raw_status = message_json.get('status')
            status = 'completed' if raw_status == 'success' else raw_status
            result = message_json.get('result', {})
            timestamp = message_json.get('timestamp')
            source_step = message_json.get('step') or message_json.get('topic') or message_json.get('source')
            
            log.info(f"🔄 CALLBACK RECEIVED - Status: {status}")
            
            if not all([flow_id, run_id, account, status]):
                return {
                    "status": "error",
                    "error": "Incomplete callback: missing flow_id/run_id/account/status"
                }
            
            # Load current flow state
            log.info(f"🔍 FETCHING STATE")
            flow_state = self.state_repo.get_flow_run_state(flow_id, run_id)
            if not flow_state:
                log.error(f"❌ STATE NOT FOUND")
                return {
                    "status": "error", 
                    "error": f"Execution state not found for {flow_id}/{run_id}"
                }
            
            # Infer task_id if missing
            if not task_id:
                task_id = self._infer_task_id(message_json, flow_state)
                if task_id:
                    message_json['task_id'] = task_id
                    # Update logger context with inferred task_id
                    log.extra['task_id'] = task_id
                    log.info(f"🧭 INFERRED TASK_ID: '{task_id}'")
                else:
                    log.warning(f"⚠️ Could not infer task_id for step/topic='{source_step}'")
            
            # Update specific task status
            updated_state = self._update_task_status(
                flow_state, task_id, status, result, timestamp, log
            )
            
            # Determine if flow should continue or stop
            if status == "failed":
                return self._handle_failure(flow_id, run_id, task_id, result, updated_state, log)
            
            elif status == "completed":
                return self._handle_completion(flow_id, run_id, task_id, source_step, updated_state, log)
            
            else:
                log.warning(f"Unknown status: {status}")
                return {
                    "status": "error",
                    "error": f"Unknown status: {status}"
                }
                
        except Exception as e:
            log.error(f"Error processing callback: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    def _handle_failure(self, flow_id: str, run_id: str, task_id: str, result: Dict[str, Any], updated_state: Dict[str, Any], log: logging.LoggerAdapter) -> Dict[str, Any]:
        """Handles task failure."""
        log.error(f"❌ TASK FAILED")
        updated_state["status"] = "error"
        updated_state["error"] = f"Task {task_id} failed: {result.get('message', 'Unknown error')}"
        updated_state["failed_task"] = task_id
        updated_state["completed_at"] = datetime.now(timezone.utc).isoformat()
        
        log.info(f"💾 SAVING ERROR STATE")
        self.state_repo.save_flow_run_state(flow_id, run_id, updated_state)
        
        log.info(f"📧 SENDING FAILURE NOTIFICATION")
        try:
            self.notification_service.send_flow_notification(updated_state, 'fail')
        except Exception as notification_error:
            log.error(f"❌ Error sending failure notification: {str(notification_error)}")
        
        return {
            "status": "error",
            "flow_id": flow_id,
            "run_id": run_id,
            "failed_task": task_id,
            "error": result.get('message', 'Unknown error')
        }

    def _handle_completion(self, flow_id: str, run_id: str, task_id: str, source_step: str, updated_state: Dict[str, Any], log: logging.LoggerAdapter) -> Dict[str, Any]:
        """Handles task completion."""
        log.info(f"✅ Task completed successfully")
        
        next_steps = self._get_next_executable_steps(updated_state)
        self._check_for_timeout_steps(updated_state, log)
        
        if not next_steps:
            # Check for flow completion
            all_steps = updated_state.get('flow_config', {}).get('steps', [])
            all_completed = bool(all_steps) and all(s.get('status') == 'completed' for s in all_steps)
            
            if all_completed:
                log.info("🎉 All steps completed. Marking flow as 'completed'.")
                updated_state["status"] = "completed"
                updated_state["completed_at"] = datetime.now(timezone.utc).isoformat()
                
                self.state_repo.save_flow_run_state(flow_id, run_id, updated_state)
                
                log.info(f"📧 SENDING SUCCESS NOTIFICATION")
                try:
                    self.notification_service.send_flow_notification(updated_state, 'success')
                except Exception as notification_error:
                    log.error(f"❌ Error sending success notification: {str(notification_error)}")
                
                return {
                    "status": "flow_completed",
                    "flow_id": flow_id,
                    "run_id": run_id,
                    "completed_steps": [s.get('id') for s in all_steps],
                    "next_steps": []
                }
        
        if next_steps:
            log.info(f"🚀 Executing next steps: {[s['id'] for s in next_steps]}")
            for step in next_steps:
                try:
                    log.info(f"📤 DISPATCHING STEP - {step['id']} ({step['type']})")
                    topic_id = self._get_topic_for_step_type(step['type'])
                    
                    step_message = {
                        **step['config'],
                        'flow_id': flow_id,
                        'run_id': run_id,
                        'task_id': step['id'],
                        'step_name': step.get('name', step['id']),
                        'step_type': step['type'],
                        'account': updated_state.get('account'),
                        'callback_topic': 'ms-flows-controller',
                        'callback_required': True
                    }
                    
                    message_id = self.publisher.publish(topic_id, step_message)
                    log.info(f"✅ STEP DISPATCHED - {step['id']} Message ID: {message_id}")
                    
                    step['status'] = 'running'
                    step['started_at'] = datetime.now(timezone.utc).isoformat()
                    
                except Exception as step_error:
                    log.error(f"❌ ERROR DISPATCHING STEP - {step['id']}: {step_error}")
                    step['status'] = 'failed'
                    step['error'] = str(step_error)
            
            updated_state["next_executable_steps"] = [s['id'] for s in next_steps]
        
        self.state_repo.save_flow_run_state(flow_id, run_id, updated_state)
        
        return {
            "status": "success",
            "flow_id": flow_id,
            "run_id": run_id,
            "completed_task": task_id,
            "next_steps": [s['id'] for s in next_steps] if next_steps else []
        }

    def _update_task_status(self, flow_state: Dict[str, Any], task_id: str, status: str, result: Dict[str, Any], timestamp: str, log: logging.LoggerAdapter) -> Dict[str, Any]:
        """Updates the status of a specific task in the flow."""
        steps = flow_state.get('flow_config', {}).get('steps', [])
        step_found = False
        
        for step in steps:
            if step.get('id') == task_id:
                step['status'] = status
                step['result'] = result
                step['completed_at'] = timestamp or datetime.now(timezone.utc).isoformat()
                step_found = True
                log.info(f"📝 Updated status of {task_id} to {status}")
                break
        
        if not step_found:
            log.warning(f"Step {task_id} not found in flow configuration")
        
        flow_state['last_updated'] = datetime.now(timezone.utc).isoformat()
        
        completed_steps = [s for s in steps if s.get('status') == 'completed']
        failed_steps = [s for s in steps if s.get('status') == 'failed']
        
        flow_state['completed_steps_count'] = len(completed_steps)
        flow_state['failed_steps_count'] = len(failed_steps)
        flow_state['total_steps_count'] = len(steps)
        
        return flow_state
    
    def _get_next_executable_steps(self, flow_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Determines which steps can be executed now."""
        steps = flow_state.get('flow_config', {}).get('steps', [])
        completed_steps = {s.get('id') for s in steps if s.get('status') == 'completed'}

        next_executable = []

        for step in steps:
            step_status = step.get('status', 'pending')
            if step_status != 'pending':
                continue

            dependencies = step.get('depends_on', []) or []
            unmet = [dep for dep in dependencies if dep not in completed_steps]
            if not unmet:
                next_executable.append(step)
            else:
                logger.debug(f"⏳ Step {step.get('id')} not executable yet; missing deps={unmet}")

        return next_executable

    def _infer_task_id(self, message_json: Dict[str, Any], flow_state: Dict[str, Any]) -> Optional[str]:
        """Infers task_id without hardcoding."""
        try:
            steps = flow_state.get('flow_config', {}).get('steps', [])
            source_step = message_json.get('step') or message_json.get('topic') or message_json.get('source')
            result = message_json.get('result') or {}

            def flatten(d: Dict[str, Any]) -> Dict[str, str]:
                flat = {}
                for k, v in (d or {}).items():
                    if isinstance(v, (str, int, float, bool)) and v is not None:
                        flat[k] = str(v)
                return flat

            cb_fields = flatten({**message_json, **result})

            def score(step: Dict[str, Any]) -> tuple:
                if source_step and step.get('type') != source_step:
                    return (-1, -1)
                cfg = step.get('config', {}) or {}
                cfg_flat = flatten(cfg)
                matches = sum(1 for k, v in cfg_flat.items() if k in cb_fields and cb_fields[k] == str(v))
                status_pref = 1 if step.get('status') == 'running' else (0 if step.get('status', 'pending') == 'pending' else -1)
                return (matches, status_pref)

            scored = [(score(s), s) for s in steps]
            valid = [s for sc, s in scored if sc[0] >= 0 and sc[1] >= 0]
            if not valid:
                return None
            best = max(valid, key=lambda s: score(s))
            return best.get('id')
        except Exception:
            return None
    
    def _get_topic_for_step_type(self, step_type: str) -> str:
        """Determines the Pub/Sub topic based on step type."""
        topic = step_type
        logger.info(f"📋 Using type '{step_type}' as topic '{topic}'")
        return topic
    
    def _check_for_timeout_steps(self, flow_state: Dict[str, Any], log: logging.LoggerAdapter):
        """Checks for steps that have been running for too long."""
        try:
            steps = flow_state.get('flow_config', {}).get('steps', [])
            current_time = datetime.now(timezone.utc)
            timeout_minutes = 30
            
            for step in steps:
                if step.get('status') == 'running':
                    started_at_str = step.get('started_at')
                    if started_at_str:
                        try:
                            if isinstance(started_at_str, str):
                                started_at = datetime.fromisoformat(started_at_str.replace('Z', '+00:00'))
                            else:
                                started_at = started_at_str
                            
                            elapsed = current_time - started_at
                            elapsed_minutes = elapsed.total_seconds() / 60
                            
                            if elapsed_minutes > timeout_minutes:
                                log.warning(f"⏰ TIMEOUT - Step {step['id']} running for {elapsed_minutes:.1f} mins")
                                step['status'] = 'timeout'
                                step['timeout_at'] = current_time.isoformat()
                                step['error'] = f"Timeout after {elapsed_minutes:.1f} minutes"
                                
                                flow_state['failed_steps_count'] = flow_state.get('failed_steps_count', 0) + 1
                                
                        except Exception as e:
                            log.error(f"Error processing timeout for step {step['id']}: {str(e)}")
                            
        except Exception as e:
            log.error(f"Error checking timeouts: {str(e)}")
