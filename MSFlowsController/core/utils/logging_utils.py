"""
Logging utilities for structured and contextual logging.
"""
import logging
import json
from typing import Any, Dict, Optional

class ContextLoggerAdapter(logging.LoggerAdapter):
    """
    Logger Adapter to inject flow context (flow_id, run_id, etc.) into logs.
    """
    def __init__(self, logger: logging.Logger, extra: Dict[str, Any] = None):
        super().__init__(logger, extra or {})

    def process(self, msg: str, kwargs: Any) -> tuple[str, Any]:
        """
        Process the logging message and keyword arguments.
        Injects context into the message prefix.
        """
        context_str = ""
        if self.extra:
            items = []
            if 'flow_id' in self.extra:
                items.append(f"Flow={self.extra['flow_id']}")
            if 'run_id' in self.extra:
                items.append(f"Run={self.extra['run_id']}")
            if 'task_id' in self.extra:
                items.append(f"Task={self.extra['task_id']}")
            
            if items:
                context_str = f"[{', '.join(items)}] "

        return f"{context_str}{msg}", kwargs

def get_flow_logger(name: str, flow_id: str = None, run_id: str = None, task_id: str = None) -> logging.LoggerAdapter:
    """
    Factory function to get a logger with context.
    """
    logger = logging.getLogger(name)
    extra = {}
    if flow_id: extra['flow_id'] = flow_id
    if run_id: extra['run_id'] = run_id
    if task_id: extra['task_id'] = task_id
    
    return ContextLoggerAdapter(logger, extra)
