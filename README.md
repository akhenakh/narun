# narun - HTTP/gRPC to NATS Micro Gateway

`narun` is a lightweight gateway that bridges incoming HTTP or gRPC requests to backend services built using the [NATS Micro](https://docs.nats.io/nats-concepts/micro) framework. It allows developers to write backend services using familiar interfaces (`http.Handler` for HTTP, standard service implementations for gRPC) while leveraging the benefits of a NATS-based microservice architecture.

The companion `nconsumer` library simplifies writing these backend NATS Micro services, using familiar interfaces (`http.Handler` for HTTP, standard service implementations for gRPC).

## Why NATS Micro (and narun)?

Traditional microservice architectures often rely on direct HTTP/gRPC calls between services, requiring service discovery, client-side load balancing, and tightly coupled network visibility. NATS Micro offers an alternative approach with several advantages:

*   **Decoupling:** Services communicate via named NATS subjects, not direct IP addresses or ports. The gateway only needs to know the target NATS service name.
*   **Location Transparency:** Backend services can run anywhere the NATS infrastructure reaches, without the gateway needing to know their specific location.
*   **Scalability & Resilience:** NATS handles service discovery and load balances requests across multiple instances of a backend service automatically. If a service instance fails, NATS routes requests to healthy ones.
*   **Simplified Backend:** Developers can focus on business logic using standard Go interfaces, while `nconsumer` handles the NATS communication details.

`narun` acts as the entry point, translating familiar HTTP/gRPC requests into NATS Micro requests, enabling easy integration with existing tools and clients.

To fully understand NATS Servcice  capabilities, you can watch [this introduction video](https://www.youtube.com/watch?v=AiUazlrtgyU).


## Key Features

*   **HTTP & gRPC Gateway:** Bridges both HTTP and gRPC requests to NATS Micro services.
*   **Standard Interfaces:** Backend services use standard Go `http.Handler` or generated gRPC service interfaces.
*   **Configuration Driven:** Simple YAML file maps HTTP paths/methods or gRPC services to NATS Micro service names.
*   **NATS Micro Integration:** Leverages built-in NATS service discovery, load balancing, and observability.
*   **Observability:** Exposes Prometheus metrics for gateway requests and NATS interactions.
*   **Graceful Shutdown:** Handles OS signals for clean termination.
*   **Caddy Plugin:** Can be integrated directly into the Caddy web server (see `caddynarun/`).

## Getting Started

### Prerequisites

*   A running NATS server (with Service/Micro v1 features)

### Configuration (`config.yaml`)

Define routes mapping incoming requests to backend NATS Micro service names.

```yaml
# config.yaml example with HTTP and gRPC routes

nats_url: "nats://localhost:4222" # URL of your NATS server
server_addr: ":8080"              # Address for the main HTTP gateway
grpc_addr: ":8081"                # Address for the gRPC gateway (optional)
metrics_addr: ":9090"             # Address for Prometheus metrics
request_timeout_seconds: 15       # Max time to wait for a NATS reply

# Define routes: map HTTP path/method OR gRPC service to NATS service name
routes:
  # --- HTTP Route Example ---
  # Requests to POST/PUT /hello/* are sent to the NATS service named "hello"
  - path: "/hello/"             # Incoming HTTP request path prefix/exact match
    methods: ["POST", "PUT"]    # Incoming HTTP request methods
    service: "hello"            # Target NATS Micro service name

  # --- gRPC Route Example ---
  # Requests for the gRPC service hello.v1.Greeter are sent to the NATS service named "greeter-service"
  - grpc: "hello.v1.Greeter"      # Fully qualified gRPC service name
    service: "greeter-service"  # Target NATS Micro service name

  # --- Another HTTP Route ---
  - path: "/goodbye"
    methods: ["GET"]
    service: "goodbye"
```
## Caddy Plugin (caddynarun)

Instead of running the standalone narun binary, you can build a custom Caddy web server binary that includes the narun gateway logic directly. This allows Caddy to handle TLS termination, virtual hosting, static file serving, and other web server tasks, while routing specific paths to your NATS Micro services.

See the caddynarun/ directory.

### Building

```sh
xcaddy build --with github.com/akhenakh/narun/caddynarun@latest
```

## Running the Gateway
```sh
./narun -config config.yaml
```

## Running Example Consumers

### HTTP Consumer (hello)
```sh
go build ./consumers/cmd/hello
./hello -nats-url "nats://localhost:4222" -service "hello"
```

### gRPC Consumer (grpc-hello)
```sh
go build ./consumers/cmd/grpc-hello

/grpc-hello -nats-url "nats://localhost:4222" -service "greeter-service"
```

(The -service flag value must match the service in config.yaml)

You can run multiple instances of the same consumer (with the same -service name), and NATS will automatically load-balance requests between them.
Making Requests

HTTP Request
```sh
curl -X POST \
     -H "Content-Type: application/json" \
     -d '{"name": "NATS User"}' \
     http://localhost:8080/hello/
```
```json
{"message":"Hello, NATS User! (processed by POST)","received_path":"/hello/","consumer_host":"your-hostname"}
```

## gRPC Request (using grpcurl)

Assuming you have the .proto file (consumers/cmd/grpc-hello/proto/hello.proto):

```sh
grpcurl -plaintext \
        -proto consumers/cmd/grpc-hello/proto/hello.proto \
        -d '{"name": "gRPC User"}' \
        localhost:8081 \
        hello.v1.Greeter/SayHello
```
```json
{
  "message": "Hello, gRPC User!"
}
```

## Service Stats
narun fully relies on NATS Micro/Service, so you can the nats service tools.

```sh
nats service stats hello
╭──────────────────────────────────────────────────────────────────────────────────────────────────────╮
│                                       hello Service Statistics                                       │
├────────────────────────┬──────────┬──────────┬─────────────┬────────┬─────────────────┬──────────────┤
│ ID                     │ Endpoint │ Requests │ Queue Group │ Errors │ Processing Time │ Average Time │
├────────────────────────┼──────────┼──────────┼─────────────┼────────┼─────────────────┼──────────────┤
│ 30e0bMATlBLySW7HHGMuR1 │ handler  │ 24,697   │ q           │ 0      │ 244ms           │ 10µs         │
├────────────────────────┼──────────┼──────────┼─────────────┼────────┼─────────────────┼──────────────┤
│                        │          │ 24,697   │             │ 0      │ 244ms           │ 10µs         │
╰────────────────────────┴──────────┴──────────┴─────────────┴────────┴─────────────────┴──────────────╯
```

## Developer Experience

narun and nconsumer aim for a familiar development workflow:

### HTTP Consumer Development

Implement http.Handler: Write your business logic just like you would for a standard Go HTTP server. You receive a standard http.Request (reconstructed by nconsumer with original headers, method, path, body) and write your response to a standard http.ResponseWriter.

Use nconsumer.ListenAndServe: In your main function, create an instance of your handler and pass it to nconsumer.ListenAndServe along with configuration (NATS URL, NATS Micro service name).

nconsumer handles connecting to NATS, registering the service, receiving requests, reconstructing the http.Request, capturing the http.ResponseWriter output, and sending the NATS reply.

See `consumers/cmd/hello/main.go` for an example.

### gRPC Consumer Development

Define Service with Protobuf: Define your service, messages, and RPCs in a .proto file as usual.

Generate Go Code: Use protoc with protoc-gen-go and protoc-gen-go-grpc to generate Go interfaces and types.

Implement gRPC Service: Create a struct that implements the generated *Server interface (e.g., GreeterServer). Implement your RPC methods with standard Go types (context.Context, request struct pointer, response struct pointer, error).

Use nconsumer.ListenAndServeGRPC: In your main function, create an instance of your gRPC service implementation. Pass the nconsumer.Options (NATS URL, NATS Micro service name), the generated grpc.ServiceDesc (e.g., &hello_v1.Greeter_ServiceDesc), and your service implementation instance to nconsumer.ListenAndServeGRPC.

nconsumer handles connecting to NATS, registering the service, receiving requests, identifying the target RPC method (using the X-Original-Grpc-Method header set by narun), decoding the protobuf request, calling your service method, encoding the protobuf response (or error), and sending the NATS reply.

See `consumers/cmd/grpc-hello/main.go` for an example.

## Metrics

The narun gateway exposes Prometheus metrics on the address specified by metrics_addr (default :9090) at the /metrics path.

Key metrics include:

- `http_gateway_requests_total`: Counter of incoming HTTP requests (labels: method, path, status_code).

- `http_gateway_request_duration_seconds`: Histogram of HTTP request latency (labels: method, path).

- `grpc_gateway_requests_total`: Counter of incoming gRPC requests (labels: grpc_method, grpc_code).

- `grpc_gateway_request_duration_seconds`: Histogram of gRPC request latency (labels: grpc_method).

- `gateway_nats_requests_total`: Counter of NATS requests sent by the gateway (labels: subject, status ["success", "timeout", "error"]).

The nconsumer library can also expose metrics (e.g., processing time, active workers) if configured - see its implementation for details.

## How it Works

1.  `narun` listens for HTTP requests (on `server_addr`) and/or gRPC requests (on `grpc_addr`).
2.  Based on the incoming request (HTTP path/method or gRPC service/method), it finds a matching route in its configuration (`config.yaml`).
3.  It identifies the target NATS Micro service name associated with the route.
4.  It forwards the request payload and relevant metadata (like HTTP headers or the original gRPC method name) as a NATS request message to the target service subject.
5.  Backend NATS consumers (built using `nconsumer`) receive the NATS message.
6.  `nconsumer` reconstructs the original request context:
    *   For HTTP: Creates an `http.Request` object.
    *   For gRPC: Identifies the target method and prepares to decode the payload.
7.  The reconstructed request is passed to the developer's service logic:
    *   For HTTP: An `http.Handler`'s `ServeHTTP` method is called.
    *   For gRPC: The appropriate method on the gRPC service implementation is invoked.
8.  The service logic processes the request and generates a response:
    *   For HTTP: Writes to an `http.ResponseWriter` (captured by `nconsumer`).
    *   For gRPC: Returns the response object and error.
9.  `nconsumer` marshals the response (status code, headers, body for HTTP; protobuf message for gRPC) and sends it back as the NATS reply.
10. `narun` receives the NATS reply.
11. It reconstructs the HTTP response (status, headers, body) or gRPC response/error and sends it back to the original client.

## TODO
- route grpc from Caddy
- caddy config does not need the path
- global nats url in caddy rather than per path?

## Ideas
- the gw to wait for a consumer to join on a a request, useful for scale to zero
- [X]routing to grpc
- [X] caddy plugin
- metrics for inflights
- direct response NAT
- send consumer logs to NATS
- [X] move to protocol buffers, done them removed for nats micro
- x request id
- enable tracing
- self registering path consumers
- node runner
  - gvisor
  - firecracker
  - exec
- no registry needed for "images", binary can be compiled using ko and uploaded to objectstore

### Won't
Because of caddy providing the feature:
- proxying to existing Services
- auto cert acme
- https support
- routing to a file
- virtual hosts supports
