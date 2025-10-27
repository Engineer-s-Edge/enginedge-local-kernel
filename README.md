# Wolfram Engine Service v2.0

This directory contains a containerized Wolfram Engine, exposed via a simple Python web service with Kafka integration. It is designed to be used as a backend for other services in the EnginEdge stack that need to perform mathematical computations. This service uses the free **Wolfram Engine for Developers**.

## What's New in v2.0

- **Kafka Integration**: Asynchronous computation requests via Kafka topics
- **Improved Scalability**: Background processing with consumer threads
- **Better Monitoring**: Comprehensive status endpoints
- **Enhanced Error Handling**: Robust error handling and logging
- **Dual Communication**: Support for both HTTP and Kafka-based communication

## How It Works

- A `Dockerfile` defines a container image based on Ubuntu 22.04.
- It automatically downloads and installs the Wolfram Engine during the Docker build process.
- A Python Flask application (`app.py`) starts a `WolframLanguageSession`.
- The Flask app exposes HTTP endpoints for computation and status.
- Kafka integration provides asynchronous processing capabilities.
- Background consumer threads process Kafka messages for scalable computation.

## Setup and Configuration

### Environment Variables

- `KAFKA_BROKERS`: Comma-separated list of Kafka brokers (default: `localhost:9092`)
- `WOLFRAM_KERNEL_PATH`: Path to Wolfram kernel executable (auto-detected if not set)

### 1. Get the Wolfram Engine Download URL

This project does not require you to manually download the installer. Instead, it is downloaded automatically during the Docker build. However, you must provide the temporary download URL.

1.  Go to the [Wolfram Engine for Developers page](https://www.wolfram.com/engine/).
2.  Select the Linux version and click the download button.
3.  When the download starts in your browser, **copy the download link**. You can usually do this by right-clicking on the download in your browser's "Downloads" list and selecting "Copy Link Address".

### 2. Build the Image

This service is designed to be built and run as part of the main `docker-compose.yml` file. You must pass the download URL you copied as a build argument.

Run the build command from the root of the repository:
```bash
docker-compose build --build-arg WOLFRAM_DOWNLOAD_URL="<paste your download link here>"
```

After the initial build, you can start the services normally with `docker-compose up`.

### 3. Wolfram Engine Activation

This setup requires a free developer license from Wolfram.

1.  **Get a License:** If you haven't already, go to the [Wolfram Engine for Developers page](https://www.wolfram.com/engine/) and get a license. This will associate a free developer license with your Wolfram ID.
2.  **Activate in the Container:**
    - Start the services: `docker-compose up`
    - Find the container ID: `docker ps` (look for `enginedge-wolfram-kernel`)
    - Start an interactive shell: `docker exec -it <container_id> /bin/bash`
    - Run `wolframscript` and follow the prompts to sign in with your Wolfram ID and password to activate the engine.

## API Endpoints

The service provides multiple endpoints for different use cases:

### `/health` (GET)

A simple health check endpoint to verify that the service and the Wolfram session are running.

### `/status` (GET)

Comprehensive status information including Wolfram and Kafka connectivity.

**Response:**
```json
{
  "service": "wolfram-local-kernel",
  "version": "2.0.0",
  "wolfram": {
    "ready": true,
    "session_available": true
  },
  "kafka": {
    "ready": true,
    "producer_available": true,
    "consumer_available": true,
    "topics": {
      "requests": "wolfram-compute-requests",
      "responses": "wolfram-compute-responses"
    }
  },
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:45.123456"
}
```

### `/compute` (POST)

Executes Wolfram Language code synchronously via HTTP.

- **URL:** `/compute`
- **Method:** `POST`
- **Headers:** `Content-Type: application/json`
- **Body:**
  ```json
  {
    "code": "N[Zeta[2], 50]"
  }
  ```

**Example `curl` command:**
```bash
curl -X POST -H "Content-Type: application/json" \
-d '{"code": "N[Pi, 50]"}' \
http://localhost:5001/compute
```

### `/compute/kafka` (POST)

Queues Wolfram computation for asynchronous processing via Kafka.

- **URL:** `/compute/kafka`
- **Method:** `POST`
- **Headers:** `Content-Type: application/json`
- **Body:**
  ```json
  {
    "code": "N[Zeta[2], 50]",
    "correlationId": "optional-custom-id"
  }
  ```

**Response:**
```json
{
  "success": true,
  "requestId": "550e8400-e29b-41d4-a716-446655440000",
  "correlationId": "optional-custom-id",
  "message": "Computation request queued for processing"
}
```

## Kafka Topics

### Request Topic: `wolfram-compute-requests`

Messages sent to this topic should have the format:
```json
{
  "requestId": "unique-request-id",
  "correlationId": "optional-correlation-id",
  "code": "Wolfram Language code to execute",
  "timestamp": "ISO-8601-timestamp"
}
```

### Response Topic: `wolfram-compute-responses`

Responses are published to this topic with the format:
```json
{
  "requestId": "original-request-id",
  "correlationId": "original-correlation-id",
  "timestamp": "ISO-8601-timestamp",
  "result": {
    "success": true,
    "result": "computation result string"
  }
}
```

## Architecture

The service runs multiple threads:
- **Main Thread**: Flask HTTP server
- **Consumer Thread**: Processes Kafka computation requests
- **Wolfram Session**: Persistent Wolfram kernel session

This architecture allows for:
- Synchronous HTTP requests for immediate results
- Asynchronous Kafka processing for high-throughput scenarios
- Scalable computation handling
- Fault-tolerant message processing

### `/compute` (POST)

Executes Wolfram Language code.

- **URL:** `/compute`
- **Method:** `POST`
- **Headers:** `Content-Type: application/json`
- **Body:**
  ```json
  {
    "code": "N[Zeta[2], 50]"
  }
  ```

**Example `curl` command:**
```bash
curl -X POST -H "Content-Type: application/json" \
-d '{"code": "N[Pi, 50]"}' \
http://localhost:5001/compute
```
(Note: The port is mapped to `5001` on the host in `docker-compose.yml`)
