import pytest
from unittest.mock import MagicMock
from core.interfaces import (
    FlowDefinitionRepositoryInterface,
    FlowRunStateRepositoryInterface,
    PublisherInterface,
    NotificationServiceInterface
)

@pytest.fixture
def mock_flow_repo():
    return MagicMock(spec=FlowDefinitionRepositoryInterface)

@pytest.fixture
def mock_state_repo():
    return MagicMock(spec=FlowRunStateRepositoryInterface)

@pytest.fixture
def mock_publisher():
    return MagicMock(spec=PublisherInterface)

@pytest.fixture
def mock_notification_service():
    return MagicMock(spec=NotificationServiceInterface)
