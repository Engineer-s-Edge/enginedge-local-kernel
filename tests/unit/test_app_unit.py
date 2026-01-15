import sys
from unittest.mock import MagicMock

import pytest

# --- 1. Pre-import mocking ---
# We must mock external dependencies that are used at module level in app.py
# app.py tries to connect to Kafka and Wolfram on import.

# Mock Kafka
mock_kafka_module = MagicMock()
mock_kafka_producer = MagicMock()
mock_kafka_consumer = MagicMock()
mock_kafka_module.KafkaProducer = mock_kafka_producer
mock_kafka_module.KafkaConsumer = mock_kafka_consumer
sys.modules["kafka"] = mock_kafka_module

# Mock Wolfram
mock_wolfram_eval = MagicMock()
mock_wolfram_ex = MagicMock()
mock_wolfram_session = MagicMock()
mock_wolfram_eval.WolframLanguageSession = mock_wolfram_session


# We need a proper exception class or mock for the except block
class MockWolframException(Exception):
    pass


mock_wolfram_ex.WolframEvaluationException = MockWolframException

sys.modules["wolframclient"] = MagicMock()
sys.modules["wolframclient.evaluation"] = mock_wolfram_eval
sys.modules["wolframclient.exception"] = mock_wolfram_ex

# Mock os.path.exists to simulate Wolfram Kernel presence
import os  # noqa: E402

original_exists = os.path.exists


def mock_exists(path):
    if "WolframKernel" in path:
        return True
    return original_exists(path)


os.path.exists = mock_exists

# --- 2. Import app ---
# This will execute top-level code using the mocks above
from app import app  # noqa: E402

# Undo mock
os.path.exists = original_exists


@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


@pytest.mark.unit
def test_health_check(client):
    """Test the /health endpoint."""
    rv = client.get("/health")
    assert rv.status_code == 200
    data = rv.get_json()
    assert "status" in data
    # Based on app.py, health might return status: ok
    assert data["status"] == "ok"


@pytest.mark.unit
def test_status_endpoint(client):
    """Test the /status endpoint."""
    rv = client.get("/status")
    assert rv.status_code == 200
    data = rv.get_json()
    # It checks kafka and wolfram status
    # Since we mocked them effectively, they might show as ready or not depending on mock behavior
    # initialize_kafka() returns boolean based on successful initialization
    # initialize_wolfram_session() returns boolean
    assert "wolfram" in data
    assert "kafka" in data
