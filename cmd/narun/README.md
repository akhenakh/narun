## Narun Management CLI

This tool provides commands to manage Narun application deployments, view logs, and inspect the state of binaries and running applications.

**How to Build:**

```bash
# Build the multi-purpose CLI tool
go build -o narun ./cmd/narun
```

**Usage:**

```
narun <command> [options] [arguments...]
```

**Available Commands:**

*   `deploy`: Upload application binaries and configuration.
*   `logs`: Stream logs from node runners.
*   `list-images`: List application binaries stored in NATS Object Store.
*   `list-apps`: List deployed applications and their status on nodes.
*   `help`: Show detailed help.

---

### `narun deploy <binary_path> [<binary_path>...]`

Uploads one or more application binaries along with a configuration file. It automatically detects the OS/Architecture from the binary headers.

**Options:**

*   `-nats <url>`: NATS server URL (default: "nats://localhost:4222").
*   `-config <path>`: Path to the application `ServiceSpec` YAML configuration file (required).
*   `-timeout <duration>`: Timeout for NATS operations (default: 15s).

**Example:**

1.  **Prepare `hello-spec.yaml`:**
    ```yaml
    name: hello-app
    binary_version_tag: hello-app-v1.3 # Base version tag
    args: [ "--listen", ":9001" ]
    nodes:
      - name: node-amd64
        replicas: 1
      - name: node-arm64
        replicas: 2
    ```
2.  **Build binaries:**
    ```bash
    GOOS=linux GOARCH=amd64 go build -o ./hello-linux-amd64 ./my-app-src
    GOOS=linux GOARCH=arm64 go build -o ./hello-linux-arm64 ./my-app-src
    ```
3.  **Run Deploy:**
    ```bash
    ./narun deploy \
        -nats nats://localhost:4222 \
        -config ./hello-spec.yaml \
        ./hello-linux-amd64 \
        ./hello-linux-arm64
    ```

**What it does:**
*   Connects to NATS.
*   Reads `hello-spec.yaml`.
*   For each binary (`./hello-linux-amd64`, `./hello-linux-arm64`):
    *   Detects platform (e.g., linux/amd64, linux/arm64).
    *   Constructs object name (e.g., `hello-app-v1.3-linux-amd64`).
    *   Uploads binary to `app-binaries` OS bucket with metadata.
*   Uploads `hello-spec.yaml` to `app-configs` KV store.

---

### `narun logs`

Streams logs from running application instances via NATS.

**Options:**

*   `-nats <url>`: NATS server URL.
*   `-app <name>`: Filter logs by application name.
*   `-node <id>`: Filter logs by node ID.
*   `-instance <id>`: Filter logs by specific instance ID (requires `-app`).
*   `-f`, `-follow`: Follow the log stream continuously.
*   `-raw`: Output raw JSON log messages.
*   `-timeout <duration>`: NATS connection timeout.

**Example:**

```bash
# Follow logs for all instances of 'hello-app' on 'node-arm64'
./narun logs -nats nats://host:4222 -app hello-app -node node-arm64 -f

# Get raw logs for a specific instance
./narun logs -app hello-app -instance hello-app-1 -raw
```

---

### `narun list-images`

Lists application binaries stored in the NATS Object Store (`app-binaries`).

**Options:**

*   `-nats <url>`: NATS server URL.
*   `-timeout <duration>`: NATS operation timeout.

**Example Output:**

```
$ ./narun list-images
Listing images from NATS Object Store 'app-binaries'...
OBJECT NAME                  SIZE  MODIFIED             VERSION TAG    OS     ARCH   DIGEST
-----------                  ----  --------             -----------    --     ----   ------
hello-app-v1.3-linux-amd64   12345 2023-10-27T10:30:00Z hello-app-v1.3 linux  amd64  SHA-256=...
hello-app-v1.3-linux-arm64   12300 2023-10-27T10:30:05Z hello-app-v1.3 linux  arm64  SHA-256=...
another-app-v0.1-linux-amd64 9876  2023-10-26T15:00:00Z another-app-v0.1 linux  amd64  SHA-256=...
Found 3 image(s).
```

---

### `narun list-apps`

Lists applications configured in NATS KV (`app-configs`) and shows which nodes are running instances based on `node-runner-states`.

**Options:**

*   `-nats <url>`: NATS server URL.
*   `-timeout <duration>`: NATS operation timeout.

**Example Output:**

```
$ ./narun list-apps
Listing deployed applications and node status...
Found 2 active node(s).
APPLICATION  VERSION TAG    NODE        NODE STATUS  NODE PLATFORM  RUNNING INSTANCES
-----------  -----------    ----        -----------  -------------  -----------------
hello-app    hello-app-v1.3 node-amd64  running      linux/amd64    1
hello-app    hello-app-v1.3 node-arm64  running      linux/arm64    2
other-svc    other-svc-v2.0 node-arm64  running      linux/arm64    1
Found 2 application(s).
```
