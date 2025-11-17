# API Example

This is a simple HTTP server example that demonstrates how to receive and process events from the Kafka consumer.

## Overview

The API server provides a single endpoint `/events` that accepts POST requests with JSON payloads. It's designed to work with the Kafka consumer example to demonstrate end-to-end message processing.

## Features

- HTTP server running on port 8090
- Single `/events` endpoint that accepts POST requests
- Logs received messages to stdout
- Returns a simple JSON response

## API Endpoint

### POST /events

Accepts JSON payloads and returns a success response.

**Request:**
```http
POST /events HTTP/1.1
Content-Type: application/json

{"key": "value"}
```

**Successful Response (200 OK):**
```json
{"status":"ok"}
```

## Running the Example

1. Start the API server:
   ```bash
   go run main.go
   ```

2. The server will start on `http://localhost:8090`

3. Send a test request using curl:
   ```bash
   curl -X POST http://localhost:8090/events \
     -H "Content-Type: application/json" \
     -d '{"message": "test"}'
   ```

## Integration with Kafka Consumer

This API is designed to work with the Kafka consumer example. The consumer will:
1. Read messages from a Kafka topic
2. Forward them to this API endpoint
3. Handle retries and backoff if the API is unavailable
