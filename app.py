import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime

from flask import Flask, jsonify, request
from kafka import KafkaConsumer, KafkaProducer
from wolframclient.evaluation import WolframLanguageSession
from wolframclient.exception import WolframEvaluationException

# --- Configure Logging ---
SERVICE_NAME = os.environ.get("SERVICE_NAME", "enginedge-local-kernel")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()


class KafkaLogHandler(logging.Handler):
    def __init__(self, service_name: str, level=logging.INFO):
        super().__init__(level)
        self.service_name = service_name
        self.buffer_path = os.path.join(
            os.getcwd(), os.environ.get("LOG_BUFFER_DIR", "logs")
        )
        os.makedirs(self.buffer_path, exist_ok=True)
        self.buffer_file = os.path.join(
            self.buffer_path, f"{self.service_name}-buffer.log"
        )
        self._producer = None

    def ensure_producer(self):
        if producer:
            self._producer = producer
        return self._producer is not None

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "level": record.levelname.lower(),
                "message": msg,
                "service": self.service_name,
            }
            topic = f"enginedge.logs.worker.{self.service_name}"
            if self.ensure_producer():
                try:
                    self._producer.send(topic, value=entry, key=str(uuid.uuid4()))
                except Exception:
                    self._buffer(entry)
            else:
                self._buffer(entry)
        except Exception:
            # Never throw from logging
            pass

    def _buffer(self, entry: dict):
        try:
            with open(self.buffer_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception:
            pass


root_logger = logging.getLogger()
root_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
root_console = logging.StreamHandler()
root_console.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
root_logger.addHandler(root_console)
root_logger.addHandler(
    KafkaLogHandler(SERVICE_NAME, level=getattr(logging, LOG_LEVEL, logging.INFO))
)
logger = logging.getLogger(__name__)

# --- Flask App Initialization ---
app = Flask(__name__)

# --- Configuration ---
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092").split(",")
COMPUTE_REQUESTS_TOPIC = "wolfram-compute-requests"
COMPUTE_RESPONSES_TOPIC = "wolfram-compute-responses"
CONSUMER_GROUP = "wolfram-kernel-group"

# --- Kafka Components ---
producer = None
consumer = None
consumer_thread = None
shutdown_event = threading.Event()

# --- Wolfram Language Session ---
session = None

# --- Initialization Functions ---


def initialize_wolfram_session():
    """Initialize the Wolfram Language session."""
    global session
    try:
        # Prefer env var path, then fall back to common versions
        candidate_paths = []
        env_kernel_path = os.environ.get("WOLFRAM_KERNEL_PATH")
        if env_kernel_path:
            candidate_paths.append(env_kernel_path)
        # Add typical install locations for recent versions
        candidate_paths.extend(
            [
                "/usr/local/bin/WolframKernel",  # Standard location in official Docker image
                "/opt/Wolfram/WolframEngine/14.3/Executables/WolframKernel",
                "/opt/Wolfram/WolframEngine/14.2/Executables/WolframKernel",
                "/opt/Wolfram/WolframEngine/14.1/Executables/WolframKernel",
            ]
        )

        kernel_path = None
        for path in candidate_paths:
            if os.path.exists(path):
                kernel_path = path
                break

        if kernel_path:
            session = WolframLanguageSession(kernel_path)
            logger.info(
                f"Successfully started WolframLanguageSession using {kernel_path}."
            )
            return True
        else:
            logger.error("Error: WolframKernel executable not found. Checked paths:")
            for p in candidate_paths:
                logger.error(f" - {p}")
            return False

    except Exception as e:
        logger.error(f"Failed to start WolframLanguageSession: {e}")
        return False


def initialize_kafka():
    """Initialize Kafka producer and consumer."""
    global producer, consumer
    try:
        # Initialize producer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            max_in_flight_requests_per_connection=1,
        )

        # Initialize consumer
        consumer = KafkaConsumer(
            COMPUTE_REQUESTS_TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=CONSUMER_GROUP,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
            consumer_timeout_ms=1000,
        )

        logger.info("Successfully initialized Kafka producer and consumer.")
        return True

    except Exception as e:
        logger.error(f"Failed to initialize Kafka: {e}")
        return False


def kafka_consumer_loop():
    """Background thread to process Kafka messages."""
    logger.info("Starting Kafka consumer loop...")
    while not shutdown_event.is_set():
        try:
            if not consumer:
                time.sleep(1)
                continue

            # Poll for messages with timeout
            message_batch = consumer.poll(timeout_ms=1000, max_records=10)

            for topic_partition, messages in message_batch.items():
                for message in messages:
                    try:
                        process_kafka_message(message.value)
                    except Exception as e:
                        logger.error(f"Error processing Kafka message: {e}")

        except Exception as e:
            logger.error(f"Error in Kafka consumer loop: {e}")
            time.sleep(5)  # Wait before retrying

    logger.info("Kafka consumer loop stopped.")


def process_kafka_message(message):
    """Process a computation request from Kafka."""
    try:
        request_id = message.get("requestId")
        code = message.get("code")
        correlation_id = message.get("correlationId")

        if not request_id or not code:
            logger.error("Invalid message format: missing requestId or code")
            return

        logger.info(f"Processing computation request {request_id}")

        # Perform the computation
        result = perform_computation(code)

        # Send response back via Kafka
        response = {
            "requestId": request_id,
            "correlationId": correlation_id,
            "timestamp": datetime.utcnow().isoformat(),
            "result": result,
        }

        if producer:
            producer.send(COMPUTE_RESPONSES_TOPIC, value=response, key=request_id)
            producer.flush()
            logger.info(f"Sent computation response for request {request_id}")

    except Exception as e:
        logger.error(f"Error processing Kafka message: {e}")


def perform_computation(code):
    """Perform Wolfram computation and return result."""
    if not session:
        return {"success": False, "error": "WolframLanguageSession not available"}

    try:
        # Use session.evaluate to run the code
        result = session.evaluate(code, timeout=30)  # 30-second timeout

        # Convert result to string for JSON serialization
        result_str = str(result)

        return {"success": True, "result": result_str}

    except WolframEvaluationException as e:
        return {"success": False, "error": f"Wolfram evaluation error: {e}"}
    except Exception as e:
        return {"success": False, "error": f"Unexpected error: {e}"}


# --- Initialize Components ---
wolfram_ready = initialize_wolfram_session()
kafka_ready = initialize_kafka()

# Start Kafka consumer thread if Kafka is ready
if kafka_ready:
    consumer_thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
    consumer_thread.start()
    logger.info("Kafka consumer thread started.")
else:
    logger.warning("Kafka not available - running in HTTP-only mode.")


@app.route("/compute", methods=["POST"])
def compute():
    """
    Accepts a POST request with JSON payload: {"code": "Wolfram Language code"}
    Executes the code and returns the result.
    """
    data = request.get_json()
    if not data or "code" not in data:
        return (
            jsonify(
                {
                    "success": False,
                    "error": "Invalid request. 'code' field is required.",
                }
            ),
            400,
        )

    code_to_evaluate = data["code"]
    result = perform_computation(code_to_evaluate)

    if result["success"]:
        return jsonify(result)
    else:
        return jsonify(result), 500


@app.route("/compute/kafka", methods=["POST"])
def compute_via_kafka():
    """
    Accepts a computation request and sends it via Kafka for async processing.
    Returns a request ID for tracking the result.
    """
    if not kafka_ready:
        return jsonify({"success": False, "error": "Kafka not available"}), 503

    data = request.get_json()
    if not data or "code" not in data:
        return (
            jsonify(
                {
                    "success": False,
                    "error": "Invalid request. 'code' field is required.",
                }
            ),
            400,
        )

    request_id = str(uuid.uuid4())
    correlation_id = data.get("correlationId", request_id)

    message = {
        "requestId": request_id,
        "correlationId": correlation_id,
        "code": data["code"],
        "timestamp": datetime.utcnow().isoformat(),
    }

    try:
        if not producer:
            return (
                jsonify({"success": False, "error": "Kafka producer not available"}),
                503,
            )

        producer.send(COMPUTE_REQUESTS_TOPIC, value=message, key=request_id)
        producer.flush()

        logger.info(f"Sent computation request {request_id} via Kafka")
        return jsonify(
            {
                "success": True,
                "requestId": request_id,
                "correlationId": correlation_id,
                "message": "Computation request queued for processing",
            }
        )

    except Exception as e:
        logger.error(f"Failed to send Kafka message: {e}")
        return (
            jsonify({"success": False, "error": f"Failed to queue request: {e}"}),
            500,
        )


@app.route("/status", methods=["GET"])
def status():
    """
    Get comprehensive status of the service including Wolfram and Kafka status.
    """
    status_info = {
        "service": "wolfram-local-kernel",
        "version": "2.0.0",
        "wolfram": {"ready": wolfram_ready, "session_available": session is not None},
        "kafka": {
            "ready": kafka_ready,
            "producer_available": producer is not None,
            "consumer_available": consumer is not None,
            "topics": {
                "requests": COMPUTE_REQUESTS_TOPIC,
                "responses": COMPUTE_RESPONSES_TOPIC,
            },
        },
        "timestamp": datetime.utcnow().isoformat(),
    }

    # Determine overall health
    if wolfram_ready and kafka_ready:
        status_info["status"] = "healthy"
        status_code = 200
    elif wolfram_ready:
        status_info["status"] = "degraded"
        status_code = 200  # Still operational with HTTP
    else:
        status_info["status"] = "unhealthy"
        status_code = 503

    return jsonify(status_info), status_code


@app.route("/health", methods=["GET"])
def health_check():
    """
    A simple health check endpoint.
    """
    try:
        if not session:
            return jsonify({"status": "error", "wolfram_session": "not started"}), 503
        # Perform a fast, safe evaluation to verify the kernel is responsive
        try:
            res = session.evaluate("1+1", timeout=3)
            return (
                jsonify(
                    {
                        "status": "ok",
                        "wolfram_session": "running",
                        "eval": "1+1",
                        "result": str(res),
                    }
                ),
                200,
            )
        except WolframEvaluationException as we:
            return (
                jsonify(
                    {
                        "status": "error",
                        "wolfram_session": "evaluation failed",
                        "details": str(we),
                    }
                ),
                503,
            )
        except Exception as e:
            return (
                jsonify(
                    {
                        "status": "error",
                        "wolfram_session": "unreachable",
                        "details": str(e),
                    }
                ),
                503,
            )
    except Exception as outer:
        # Defensive: never throw from health
        return (
            jsonify(
                {"status": "error", "wolfram_session": "unknown", "details": str(outer)}
            ),
            503,
        )


def shutdown_handler():
    """Cleanup function called on shutdown."""
    logger.info("Shutting down Wolfram Local Kernel...")
    shutdown_event.set()

    if consumer_thread and consumer_thread.is_alive():
        consumer_thread.join(timeout=5)

    if producer:
        try:
            producer.close()
        except Exception:
            pass

    if consumer:
        try:
            consumer.close()
        except Exception:
            pass

    if session:
        try:
            session.terminate()
        except Exception:
            pass

    logger.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        # Run the Flask app, making it accessible from other Docker containers
        port = int(os.environ.get("PORT", 5000))
        app.run(host="0.0.0.0", port=port)
    finally:
        shutdown_handler()
