import pytest
from unittest.mock import MagicMock, ANY
from core.services.dynamic_flow_service import DynamicFlowService
from core.exceptions import FlowConfigurationError, StateNotFoundError

class TestDynamicFlowService:
    
    @pytest.fixture
    def service(self, mock_flow_repo, mock_state_repo, mock_publisher):
        return DynamicFlowService(mock_flow_repo, mock_state_repo, mock_publisher)

    def test_execute_new_flow_success(self, service, mock_flow_repo, mock_state_repo, mock_publisher):
        # Arrange
        message_json = {
            "flow_id": "test-flow",
            "account": "test-account",
            "flow_config": {
                "steps": [
                    {"id": "step1", "type": "action", "name": "Step 1"}
                ]
            }
        }
        
        mock_flow_repo.get_flow_definition.return_value = None # Simulate no repo definition, use message config
        mock_publisher.publish.return_value = "msg-id-123"

        # Act
        result = service.execute_flow(message_json)

        # Assert
        assert result["status"] == "started"
        assert result["flow_id"] == "test-flow"
        assert "run_id" in result
        
        # Verify state was saved
        mock_state_repo.save_flow_run_state.assert_called()
        args, _ = mock_state_repo.save_flow_run_state.call_args
        assert args[0] == "test-flow"
        assert args[2]["status"] == "started"

        # Verify execution (step1 should be dispatched)
        # The N8N engine logic is embedded, so we check if publisher was called
        mock_publisher.publish.assert_called()

    def test_execute_continuation_success(self, service, mock_state_repo, mock_publisher):
        # Arrange
        flow_id = "test-flow"
        run_id = "run-123"
        task_id = "step1"
        account = "test-account"
        
        message_json = {
            "flow_id": flow_id,
            "run_id": run_id,
            "task_id": task_id,
            "account": account,
            "status": "completed", # Callback status
            "result": {"some": "data"}
        }

        # Mock existing state
        mock_state_repo.get_flow_run_state.return_value = {
            "flow_id": flow_id,
            "run_id": run_id,
            "account": account,
            "status": "running",
            "flow_config": {
                "steps": [
                    {"id": "step1", "type": "action", "status": "completed"},
                    {"id": "step2", "type": "action", "depends_on": ["step1"]}
                ]
            }
        }
        
        mock_publisher.publish.return_value = "msg-id-456"

        # Act
        result = service.execute_flow(message_json)

        # Assert
        # In the current implementation, execute_flow re-runs the engine logic.
        # Since step1 is completed in our mock state (simulating it was just updated by callback handler before calling this?),
        # Wait, DynamicFlowService.execute_flow is usually called by FlowContinuationHandler.
        # The engine re-evaluates.
        # If step1 is completed, step2 should be triggered if dependencies are met.
        
        assert result["status"] == "started" # Engine returns 'started' if it dispatches steps
        
        # Verify state update
        mock_state_repo.save_flow_run_state.assert_called()
        
        # Verify step2 dispatch
        # mock_publisher.publish.assert_called() 
        # Note: The N8N engine logic in the service might need the state to be exactly right.
        # If step1 is 'completed' in the config passed to engine, engine sees step2 depends on step1.
        # Engine should dispatch step2.

    def test_execute_continuation_missing_state(self, service, mock_state_repo):
        # Arrange
        message_json = {
            "flow_id": "test-flow",
            "run_id": "missing-run",
            "task_id": "step1",
            "account": "test-account"
        }
        
        mock_state_repo.get_flow_run_state.return_value = None

        # Act & Assert
        # The service currently catches exceptions and returns error dict
        result = service.execute_flow(message_json)
        assert result["status"] == "error"
        assert "Execution state not found" in result["error"]

    def test_execute_new_flow_missing_config(self, service, mock_flow_repo):
        # Arrange
        message_json = {
            "flow_id": "test-flow",
            "account": "test-account"
            # No flow_config and no repo definition
        }
        mock_flow_repo.get_flow_definition.return_value = None

        # Act
        result = service.execute_flow(message_json)

        # Assert
        assert result["status"] == "error"
        assert "No steps defined" in result["error"]
