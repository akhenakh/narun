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

*   `deploy`: Upload application binaries and optionally configuration.
*   `logs`: Stream logs from node runners.
*   `list-images`: List application binaries stored in NATS Object Store.
*   `list-apps`: List deployed applications and their status on nodes.
*   `delete-app`:  Delete an application configuration from NATS KV.

---

### `narun deploy <binary_path> [<binary_path>...]`

Uploads one or more application binaries. It automatically detects the OS/Architecture from the binary headers.

You can either provide a full `ServiceSpec` configuration file (`-config`) or specify basic deployment parameters using flags (`-name`, `-tag`, `-node`, `-replicas`). If `-config` is provided, the other flags are ignored.

**Options:**

*   `-nats <url>`: NATS server URL (default: "nats://localhost:4222").
*   `-timeout <duration>`: Timeout for NATS operations (default: 15s).
*   `-config <path>`: Path to the application `ServiceSpec` YAML configuration file (optional). If provided, this defines the full deployment spec.
*   `-name <app_name>`: Application name (required if `-config` is not used).
*   `-tag <tag>`: Binary tag (required if `-config` is not used).
*   `-node <node_id>`: Target node name (default: "local"; used if `-config` is not used).
*   `-replicas <count>`: Number of replicas on the target node (default: 1; used if `-config` is not used).

**Example 1: Deploy using a Config File (Full Spec)**

1.  **Prepare `hello-spec.yaml`:**
    ```yaml
    name: hello-app
    tag: hello-app-v1.3 # Base tag
    args: [ "--listen", ":9001" ]
    env:
      - name: GREETING
        value: "Hello from NATS!"
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

**What it does (Example 1):**
*   Connects to NATS.
*   Reads `hello-spec.yaml`.
*   For each binary:
    *   Detects platform (e.g., linux/amd64).
    *   Constructs object name using `binary_version_tag` from YAML (e.g., `hello-app-v1.3-linux-amd64`).
    *   Uploads binary to `app-binaries` OS bucket.
*   Uploads `hello-spec.yaml` content to `app-configs` KV store under the key `hello-app`.

**Example 2: Deploy without a Config File (Simple Defaults)**

1.  **Build a binary:**
    ```bash
    GOOS=linux GOARCH=amd64 go build -o ./simple-server ./my-simple-server-src
    ```
2.  **Run Deploy:**
    ```bash
    ./narun deploy \
        -nats nats://host:4222 \
        -name simple-app \
        -tag simple-v0.1 \
        ./simple-server
        # Deploys to default node "local" with 1 replica
    ```
3.  **Run Deploy (Specifying Node/Replicas):**
    ```bash
    ./narun deploy \
        -nats nats://host:4222 \
        -name simple-app \
        -tag simple-v0.1 \
        -node specific-worker \
        -replicas 2 \
        ./simple-server
    ```

**What it does (Example 2):**
*   Connects to NATS.
*   Uses the provided `-name` and `-tag` flags.
*   Uses the default node "local" and 1 replica (or overrides from `-node`/`-replicas` flags).
*   Detects platform for `./simple-server` (e.g., linux/amd64).
*   Constructs object name using the `-tag` flag (e.g., `simple-v0.1-linux-amd64`).
*   Uploads binary to `app-binaries` OS bucket.
*   **Generates a simple `ServiceSpec` in memory:**
    ```yaml
    name: simple-app
    binary_version_tag: simple-v0.1
    nodes:
      - name: local # Or value from -node flag
        replicas: 1 # Or value from -replicas flag
    ```
*   Uploads this generated YAML to `app-configs` KV store under the key `simple-app`.

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
OBJECT NAME                  SIZE  MODIFIED             APP NAME   TAG            OS     ARCH   DIGEST
-----------                  ----  --------             --------   -----------    --     ----   ------
hello-app-v1.3-linux-amd64   12345 2023-10-27T10:30:00Z hello-app  hello-app-v1.3 linux  amd64  SHA-256=...
hello-app-v1.3-linux-arm64   12300 2023-10-27T10:30:05Z hello-app  hello-app-v1.3 linux  arm64  SHA-256=...
simple-v0.1-linux-amd64      9876  2023-10-28T11:00:00Z simple-app simple-v0.1    linux  amd64  SHA-256=...
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
Found 3 active node(s).
APPLICATION  VERSION TAG    NODE               NODE STATUS      NODE PLATFORM  RUNNING INSTANCES  DESIRED REPLICAS
-----------  -----------    ----               -----------      -------------  -----------------  ----------------
hello-app    hello-app-v1.3 node-amd64         running          linux/amd64    1                  1
hello-app    hello-app-v1.3 node-arm64         running          linux/arm64    2                  2
simple-app   simple-v0.1    local              running          linux/amd64    1                  1
simple-app   simple-v0.1    specific-worker    offline/unknown  -/-            0                  2
other-svc    other-svc-v2.0 node-arm64         running          linux/arm64    1                  1
Found 3 configured application(s).
```
