import pytest
from unittest.mock import MagicMock, ANY
from core.handlers.callback_handler import CallbackHandler

class TestCallbackHandler:
    
    @pytest.fixture
    def handler(self, mock_state_repo, mock_notification_service, mock_publisher):
        return CallbackHandler(mock_state_repo, mock_notification_service, mock_publisher)

    def test_handle_task_completion_success(self, handler, mock_state_repo, mock_publisher):
        # Arrange
        flow_id = "test-flow"
        run_id = "run-123"
        task_id = "step1"
        
        message_json = {
            "flow_id": flow_id,
            "run_id": run_id,
            "task_id": task_id,
            "account": "test-account",
            "status": "success",
            "result": {"output": "ok"}
        }

        # Mock initial state: step1 running, step2 pending depending on step1
        mock_state_repo.get_flow_run_state.return_value = {
            "flow_id": flow_id,
            "run_id": run_id,
            "account": "test-account",
            "status": "running",
            "flow_config": {
                "steps": [
                    {"id": "step1", "type": "action", "status": "running"},
                    {"id": "step2", "type": "action", "status": "pending", "depends_on": ["step1"], "config": {"topic": "topic2"}}
                ]
            }
        }
        
        mock_publisher.publish.return_value = "msg-id-next"

        # Act
        result = handler.handle_task_callback(message_json)

        # Assert
        assert result["status"] == "success"
        assert result["completed_task"] == task_id
        assert "step2" in result["next_steps"]

        # Verify step1 updated to completed
        # Verify step2 dispatched
        mock_publisher.publish.assert_called_with("topic2", ANY)
        
        # Verify state saved
        mock_state_repo.save_flow_run_state.assert_called()
        saved_state = mock_state_repo.save_flow_run_state.call_args[0][2]
        assert saved_state["flow_config"]["steps"][0]["status"] == "completed"
        assert saved_state["flow_config"]["steps"][1]["status"] == "running"

    def test_handle_task_failure(self, handler, mock_state_repo, mock_notification_service):
        # Arrange
        message_json = {
            "flow_id": "test-flow",
            "run_id": "run-123",
            "task_id": "step1",
            "account": "test-account",
            "status": "failed",
            "result": {"message": "Something went wrong"}
        }

        mock_state_repo.get_flow_run_state.return_value = {
            "flow_id": "test-flow",
            "run_id": "run-123",
            "status": "running",
            "flow_config": {
                "steps": [{"id": "step1", "status": "running"}]
            }
        }

        # Act
        result = handler.handle_task_callback(message_json)

        # Assert
        assert result["status"] == "error"
        assert result["failed_task"] == "step1"
        
        # Verify notification sent
        mock_notification_service.send_flow_notification.assert_called_with(ANY, 'fail')
        
        # Verify state saved as error
        saved_state = mock_state_repo.save_flow_run_state.call_args[0][2]
        assert saved_state["status"] == "error"

    def test_handle_flow_completion(self, handler, mock_state_repo, mock_notification_service):
        # Arrange
        # Last step completing
        message_json = {
            "flow_id": "test-flow",
            "run_id": "run-123",
            "task_id": "step2",
            "account": "test-account",
            "status": "success"
        }

        mock_state_repo.get_flow_run_state.return_value = {
            "flow_id": "test-flow",
            "run_id": "run-123",
            "status": "running",
            "flow_config": {
                "steps": [
                    {"id": "step1", "status": "completed"},
                    {"id": "step2", "status": "running"} # This one is completing now
                ]
            }
        }

        # Act
        result = handler.handle_task_callback(message_json)

        # Assert
        assert result["status"] == "flow_completed"
        
        # Verify notification success
        mock_notification_service.send_flow_notification.assert_called_with(ANY, 'success')
        
        # Verify state saved as completed
        saved_state = mock_state_repo.save_flow_run_state.call_args[0][2]
        assert saved_state["status"] == "completed"
