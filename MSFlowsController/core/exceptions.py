"""
Custom exceptions for the FlowController.
"""

class FlowError(Exception):
    """Base exception for all flow-related errors."""
    def __init__(self, message: str, flow_id: str = None, run_id: str = None):
        self.flow_id = flow_id
        self.run_id = run_id
        super().__init__(message)

class FlowConfigurationError(FlowError):
    """Raised when there is an issue with the flow configuration."""
    pass

class FlowExecutionError(FlowError):
    """Raised when an error occurs during flow execution."""
    pass

class StepExecutionError(FlowError):
    """Raised when a specific step fails."""
    def __init__(self, message: str, step_id: str, flow_id: str = None, run_id: str = None):
        self.step_id = step_id
        super().__init__(f"Step {step_id} failed: {message}", flow_id, run_id)

class StateNotFoundError(FlowError):
    """Raised when flow state cannot be found."""
    pass

class FlowDefinitionNotFoundError(FlowError):
    """Raised when flow definition cannot be found."""
    pass
