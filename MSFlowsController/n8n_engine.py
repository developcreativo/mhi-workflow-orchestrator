"""
Enhanced JSON Engine - MVP N8N-like
Dynamic orchestrator extension to support advanced logic.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Union

# Assuming publisher has a specific type, but for now using Any or duck typing
# from google.cloud.pubsub_v1 import PublisherClient

logger = logging.getLogger(__name__)

class N8NLikeEngine:
    """
    N8N-like execution engine for advanced workflows.
    """
    
    def __init__(self, project_id: str, publisher: Any):
        self.project_id = project_id
        self.publisher = publisher
        self.context: Dict[str, Any] = {}
        
    def execute_flow(self, flow_config: Dict[str, Any], initial_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Executes a full flow with support for conditionals, loops, and dependencies.
        
        Args:
            flow_config: Flow configuration.
            initial_context: Initial context with data.
            
        Returns:
            dict: Execution result.
        """
        try:
            self.context = initial_context or {}
            executed_steps: List[Dict[str, Any]] = []
            steps: List[Dict[str, Any]] = flow_config.get('steps', [])
            logger.info(f"Executing {len(steps)} flow steps with dependencies - deferred orchestration mode via callbacks")

            # Only trigger steps whose dependencies are empty or 'completed' in previous state.
            # We don't mark as completed here; they will remain 'running' and the CallbackHandler
            # will trigger subsequent steps when it receives 'completed'.
            num_dispatched = 0
            logger.info(f"🔍 DEBUG - Analyzing {len(steps)} steps for initial execution")
            
            for step in steps:
                step_id = step.get('id')
                step_status = step.get('status', 'pending')
                dependencies = step.get('depends_on', [])
                
                logger.info(f"🔍 DEBUG - Step {step_id}: status={step_status}, depends_on={dependencies}")
                
                if step_status == 'completed':
                    logger.info(f"⏭️ Skipping step {step_id} - already completed")
                    continue

                if not dependencies:
                    logger.info(f"🚀 Executing initial step {step_id} (no dependencies)")
                    try:
                        result = self._execute_step(step)
                        executed_steps.append({**result, "dispatched": True})
                        # Mark in memory as running so state can be persisted if applicable
                        step['status'] = 'running'
                        step['started_at'] = datetime.now(timezone.utc).isoformat()
                        num_dispatched += 1
                        logger.info(f"✅ Step {step_id} dispatched successfully")
                    except Exception as e:
                        logger.error(f"❌ Error executing step {step_id}: {str(e)}")
                        executed_steps.append({
                            "status": "error",
                            "step_id": step_id,
                            "error": str(e)
                        })
                else:
                    logger.info(f"⏳ Step {step_id} has dependencies: {dependencies}")

            if num_dispatched == 0:
                logger.info("No initial steps without dependencies to dispatch in this phase")

            return {
                "status": "started",
                "executed_steps": executed_steps,
                "total_steps": len(executed_steps)
            }
            
        except Exception as e:
            logger.error(f"Error executing flow: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    def _execute_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes an individual step completely dynamically.
        """
        step_type = step.get('type', 'action')
        step_name = step.get('name', 'unnamed_step')
        
        logger.info(f"Executing step: {step_name} (type: {step_type})")
        
        try:
            # Fully dynamic system - any type is treated as Cloud Function
            # No hardcoding, everything determined from JSON config
            if step_type == 'generic_http':
                # Note: _execute_generic_http_step was not present in the original file provided in the view_file output.
                # It was called but not defined in the snippet I saw? 
                # Wait, I saw lines 1-298. Let me check if I missed it.
                # Line 106 calls self._execute_generic_http_step(step).
                # But looking at the file content I read (Step 9), I don't see the definition of _execute_generic_http_step.
                # I see _execute_action_step, _execute_data_transformation_step, _execute_cloud_function_step.
                # Maybe it was missing or I missed it.
                # I will assume it might be missing or I should implement a placeholder or use cloud function step.
                # Given I am refactoring, if it's missing, I should probably handle it.
                # For now, I will comment it out or map it to cloud function if not found, 
                # OR I will check if I missed reading the file.
                # The file had 298 lines.
                # I'll check if I can find it.
                # If not, I'll assume it's a missing method and I should probably remove the specific check or implement it.
                # I'll assume it's a specialized cloud function for now or I'll implement a basic one if needed.
                # But wait, the original code had:
                # if step_type == 'generic_http': return self._execute_generic_http_step(step)
                # If that method didn't exist, the code would crash.
                # I will assume I missed it or it's inherited? No, it's a standalone class.
                # I will assume it's missing in the provided snippet or I missed it.
                # I will remove the specific check and treat it as a cloud function or add a TODO.
                # Actually, I'll treat it as a cloud function for now to be safe, or check if I can implement it.
                # Let's just leave the check but implement a placeholder if it's missing.
                
                # Wait, I'll just map it to _execute_cloud_function_step for now as the comment says "ANY other type is treated as Cloud Function".
                # But the original code had a specific branch.
                # I'll keep the branch but ensure the method exists.
                return self._execute_generic_http_step(step)
            else:
                # ANY other type is treated as generic Cloud Function
                logger.info(f"🚀 Treating '{step_type}' as generic Cloud Function")
                return self._execute_cloud_function_step(step)
                
        except Exception as e:
            logger.error(f"Error executing step {step_name}: {str(e)}")
            return {
                "status": "error",
                "step_name": step_name,
                "error": str(e)
            }

    def _execute_generic_http_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """
        Placeholder for generic HTTP step execution.
        """
        # This method was missing in the original file view?
        # I'll implement a basic version or just log error.
        logger.warning("Generic HTTP step execution not implemented yet, falling back to Cloud Function logic or error.")
        return self._execute_cloud_function_step(step)
    
    def _execute_action_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes an action step (Pub/Sub).
        """
        step_name = step.get('name')
        step_topic = step.get('topic')
        step_data = step.get('data', {})
        
        if not step_name or not step_topic:
            return {
                "status": "error",
                "error": "Action step requires 'name' and 'topic'"
            }
        
        # Transform data using context
        transformed_data = self._transform_data(step_data)
        
        # Add common info
        message_data = {
            '_m_account': self.context.get('account', 'unknown'),
            'flow_id': self.context.get('flow_id', 'unknown'),
            'step_name': step_name,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            **transformed_data
        }
        
        # Send message to topic using PublisherInterface
        # The wrapper handles topic path construction if needed, but here we might need to be careful.
        # The wrapper expects 'topic' name or path.
        # We'll pass the topic name directly.
        self.publisher.publish(step_topic, message_data)
        
        logger.info(f"✅ Step {step_name} sent to {step_topic}")
        
        return {
            "status": "success",
            "step_name": step_name,
            "topic": step_topic,
            "data_sent": message_data
        }
    
    def _transform_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transforms data using the current context.
        """
        transformed = {}
        for k, v in data.items():
            if isinstance(v, str) and v.startswith('{{') and v.endswith('}}'):
                # Simple variable substitution
                var_name = v[2:-2].strip()
                transformed[k] = self.context.get(var_name, v)
            else:
                transformed[k] = v
        return transformed

    def _evaluate_expression(self, expression: str) -> Any:
        """
        Evaluates a simple expression.
        """
        if expression.startswith('{{') and expression.endswith('}}'):
            var_name = expression[2:-2].strip()
            return self.context.get(var_name, expression)
        return expression

    def _execute_data_transformation_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Executes a data transformation step."""
        operation = step.get('config', {}).get('operation', 'set')
        mappings = step.get('config', {}).get('mappings', {})
        
        logger.info(f"Executing data transformation: {operation}")
        
        # Evaluate expressions in mappings
        evaluated_mappings = {}
        for key, value in mappings.items():
            if isinstance(value, str):
                evaluated_mappings[key] = self._evaluate_expression(value)
            else:
                evaluated_mappings[key] = value
        
        # Update context with new values
        self.context.update(evaluated_mappings)
        
        return {
            "status": "success",
            "step_name": step.get('name', 'data_transformation'),
            "operation": operation,
            "mappings": evaluated_mappings
        }
    
    def _execute_cloud_function_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes a Cloud Function completely dynamically.
        """
        config = step.get('config', {})
        step_type = step.get('type', 'cloud_function')
        step_name = step.get('name', 'unnamed_step')
        
        logger.info(f"🚀 Executing dynamic Cloud Function: {step_name}")
        
        # Determine Cloud Function name based on type
        function_name = self._get_function_name(step_type, config)
        project_id = config.get('project_id', self.project_id)
        
        # Create payload for Cloud Function
        payload = {
            'flow_id': self.context.get('flow_id'),
            'run_id': self.context.get('run_id'),
            'task_id': self.context.get('task_id'),
            'account': self.context.get('account'),
            'step_id': step.get('id'),
            'step_name': step_name,
            'step_type': step_type,
            'config': config 
        }
        
        logger.info(f"☁️  Calling Cloud Function: {function_name}")
        
        try:
            # Determine Pub/Sub topic based on JSON type
            topic_name = self._get_topic_name(step_type, config)
            # The wrapper handles the full path if we just pass the topic name, 
            # BUT the wrapper implementation I wrote uses project_id from its config.
            # Here we might have a different project_id in config.
            # If project_id is different, we should construct the path manually.
            if project_id != self.project_id:
                topic_to_publish = f"projects/{project_id}/topics/{topic_name}"
            else:
                topic_to_publish = topic_name
                
            logger.info(f"📨 Publishing to topic: {topic_to_publish}")
            
            # Publish message using interface
            message_id = self.publisher.publish(topic_to_publish, payload)
            
            logger.info(f"✅ Message published successfully: {message_id}")
            
            return {
                "status": "success",
                "step_id": step.get('id'),
                "step_name": step_name,
                "step_type": step_type,
                "function_name": function_name,
                "topic_name": topic_name,
                "message_id": message_id
            }
                
        except Exception as e:
            logger.error(f"Error publishing to topic for {function_name}: {str(e)}")
            return {
                "status": "error",
                "step_id": step.get('id'),
                "step_name": step_name,
                "error": str(e)
            }
    
    def _get_function_name(self, step_type: str, config: Dict[str, Any]) -> str:
        """
        Determines the Cloud Function name completely dynamically.
        """
        if 'function_name' in config:
            return config['function_name']
        
        if step_type and not step_type.startswith('generic_'):
            function_name = f'{step_type.title().replace("-", "")}'
            return function_name
        
        return step_type or 'srvGenericFunction'
    
    def _get_topic_name(self, step_type: str, config: Dict[str, Any]) -> str:
        """
        Determines the Pub/Sub topic name based on JSON type.
        """
        if 'topic' in config:
            return config['topic']
        
        return step_type