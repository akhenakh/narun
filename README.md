# narun - HTTP to NATS Gateway

`narun` is a lightweight HTTP gateway designed to bridge incoming HTTP requests to backend services communicating over [NATS](https://nats.io/), including [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream). It allows developers to write backend NATS consumers using the standard Go `http.Handler` interface, simplifying the transition to or integration with a NATS-based microservice architecture.

## Purpose

In many architectures, services expose HTTP APIs. However, leveraging a message bus like NATS offers benefits such as:

*   **Decoupling:** Services don't need direct network visibility of each other.
*   **Resilience:** NATS can handle message persistence (via JetStream) and redelivery.
*   **Scalability:** Multiple instances of a consumer service can subscribe to the same subject (using queue groups) for load balancing.
*   **Location Transparency:** The gateway doesn't need to know the IP/port of backend services, only the NATS subject to send requests to.

`narun` acts as the translation layer:

1.  It listens for standard HTTP requests on a configured address.
2.  Based on the request path and method, it looks up a corresponding NATS subject in its configuration.
3.  It marshals the essential parts of the `http.Request` (method, path, headers, body, query params) into a structured format (CBOR).
4.  It sends this payload as a NATS request message to the configured subject, waiting for a reply.
5.  Backend NATS consumers (built using the `nconsumer` library or similar logic) receive the NATS message.
6.  The `nconsumer` library reconstructs an `http.Request` from the message payload.
7.  This reconstructed request is passed to a standard `http.Handler` implementation within the consumer service.
8.  The handler processes the request and writes a response using a standard `http.ResponseWriter` (internally captured by a shim).
9.  The `nconsumer` library marshals the captured response (status code, headers, body) into CBOR.
10. This response payload is sent back as the NATS reply message.
11. `narun` receives the NATS reply, unmarshals the response data.
12. It writes the status code, headers, and body back to the original HTTP client.

## Key Features

*   **HTTP to NATS Bridging:** Translates synchronous HTTP requests into NATS request-reply interactions.
*   **Standard `http.Handler`:** Backend consumers use the familiar `http.Handler` interface, minimizing the learning curve for NATS integration.
*   **Configuration Driven:** Routing from HTTP paths/methods to NATS subjects is defined in a simple YAML file.
*   **NATS JetStream Support:** Configured streams are automatically declared (if missing) for persistence and reliable delivery. Queue groups are used for consumer load balancing.
*   **Observability:** Exposes Prometheus metrics for HTTP requests and NATS interactions.
*   **Graceful Shutdown:** Handles OS signals for clean termination.
*   **Efficient Serialization:** Uses CBOR for encoding request/response data over NATS.

## Architecture Overview

```
+-------------+      HTTP Request      +---------+      NATS Request (CBOR)     +-----------------+      Handler Logic      +-----------------+      NATS Reply (CBOR)       +---------+      HTTP Response       +-------------+
| HTTP Client | ---------------------> | narun   | ---------------------------> | NATS JetStream  | --------------------->  | Consumer        | ----------------------->     | Consumer| -----------------------> | narun       | ---------------------> | HTTP Client   |
|             |                        | Gateway |      (Subject: task.S.X)     | (Stream: S)     | (Listens on task.S.X)   | (http.Handler)  | (ResponseWriter Shim)        | Gateway |                          |             |
+-------------+                        +---------+                              +-----------------+                         +-----------------+                              +---------+                          +-------------+                        +---------------+
                                          ^                                                                                                                                                                              |
                                          |                                                                                                                                                                              |
                                          +--------------------------------------- Configuration (config.yaml) ----------------------------------------------------------------------------------------------------------+
                                          |
                                          +--------------------------------------- Metrics (/metrics) -------------------------------------------------------------------------------------------------------------------+
```

## Getting Started

### Prerequisites

*   Go 1.18 or later
*   A running NATS server (version 2.2.0+ recommended for JetStream) with JetStream enabled.

### Configuration (`config.yaml`)

The `narun` gateway requires a configuration file (default: `config.yaml`) to define routes and NATS connection details.

```yaml
# config.yaml

nats_url: "nats://localhost:4222" # URL of your NATS server
server_addr: ":8080"              # Address for the main HTTP gateway to listen on
metrics_addr: ":9090"             # Address for the Prometheus metrics endpoint
request_timeout_seconds: 15       # Max time (seconds) to wait for a NATS reply
nats_stream: "TASK"       # JetStream stream the subject belongs to

# Define routes: map HTTP path/method to NATS stream/subject
routes:
  - path: "/hello/"             # Incoming HTTP request path
    method: "POST"              # Incoming HTTP request method
    app: "hello"                # The app name will be used to construct the NATS subject
```

### Running the Gateway

1.  Clone the repository (if you haven't already).
2.  Build the gateway:
    ```bash
    go build ./cmd/narun
    ```
3.  Run the gateway, pointing it to your configuration file:
    ```bash
    ./narun -config config.yaml
    # Or using go run:
    # go run ./cmd/narun -config config.yaml
    ```
    You should see logs indicating connection to NATS and the server starting.

### Running an Example Consumer (`hello`)

The `consumers/cmd/hello` directory contains a simple example consumer service.

1.  Build the consumer:
    ```bash
    go build ./consumers/cmd/hello
    ```
2.  Run the consumer, specifying the NATS connection details and the subject/stream/queue it should listen on (these *must* match a route in `config.yaml`):
    ```bash
    ./hello \
        -nats-url "nats://localhost:4222" \
        -stream "ORDERS" \
        -app "hello" \
    ```
    You can run multiple instances of the consumer with the same queue group name, and NATS will distribute messages among them.

### Making a Request

Now, send an HTTP request to the `narun` gateway that matches a configured route:

```bash
curl -X POST \
     -H "Content-Type: application/json" \
     -d '{"name": "NATS User"}' \
     http://localhost:8080/hello/
```

You should receive a response generated by the `hello` consumer:

```json
{
  "message": "Hello, NATS User!",
  "received_path": "/hello/"
}
```

Check the logs of both `narun` and the `hello` consumer to see the request flow.

## Paradigm Shift: HTTP vs. NATS Microservices

*   **Traditional HTTP Microservices:** Often involve an API Gateway that routes HTTP requests directly to other HTTP-based microservices. Services need to know each other's network addresses (often via service discovery). Scaling involves running more HTTP server instances behind a load balancer. Communication is typically synchronous request-response.

*   **NATS-based Microservices (via `narun`):**
    *   **Decoupling:** The `narun` gateway only needs to know the NATS subject, not the consumer's location or how many instances exist. Consumers only need to know which subject to listen on.
    *   **Asynchronous Potential:** While `narun` uses NATS request-reply (synchronous pattern), the underlying NATS communication could be adapted for fire-and-forget or event-driven patterns for other use cases.
    *   **Resilience & Load Balancing:** NATS JetStream provides message persistence (if a consumer is down) and queue groups automatically distribute messages across available consumer instances. Scaling is as simple as running more consumer processes listening on the same subject and queue group.
    *   **Simplified Consumer:** Developers focus on the business logic within the `http.Handler`, letting `nconsumer` manage the NATS interactions, message (de)serialization, and request/response reconstruction.

`narun` offers a way to gain the benefits of a NATS-based architecture while retaining the familiar development pattern of `http.Handler` for the backend services themselves.

## Metrics

The gateway exposes Prometheus metrics on the address specified by `metrics_addr` (default `:9090`) at the `/metrics` path.

Key metrics include:

*   `http_gateway_requests_total`: Counter of incoming HTTP requests labeled by method, path, and resulting status code.
*   `http_gateway_request_duration_seconds`: Histogram of HTTP request latency labeled by method and path.
*   `http_gateway_nats_requests_total`: Counter of NATS requests sent by the gateway, labeled by NATS subject and status (`success`, `timeout`, `error`).

## Ideas
- self registering path consumers
- routing to grpc
- proxying to existing Services
- virtual hosts supports
- routing to a file
- auto cert acme
- https support
- gvisor
- firecracker
- caddy plugin
- metrics for inflights
- direct response NAT
- send consumer logs to NATS
