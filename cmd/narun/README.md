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
*   `service`: Manage deployed services (list, info, delete).
*   `logs`: Stream logs from node runners.
*   `list-images`: List application binaries stored in NATS Object Store.
*   `secret`: Manage secrets for applications.
*   `files`: Manage shared files for applications.
*   `help`: Show this help message.

---

### `narun deploy <binary_path> [<binary_path>...]`

Uploads one or more application binaries. It automatically detects the OS/Architecture from the binary headers.

You can either provide a full `ServiceSpec` configuration file (`-config`) or specify basic deployment parameters using flags (`-name`, `-tag`, `-node`, `-replicas`). If `-config` is provided, the other flags are ignored.

**Options:**

*   `-nats <url>`: NATS server URL (default: "nats://localhost:4222").
*   `-timeout <duration>`: Timeout for NATS operations (default: 15s).
*   `-config <path>`: Path to the application `ServiceSpec` YAML configuration file (optional). If provided, this defines the full deployment spec.
*   `-name <app_name>`: Application name (required if `-config` is not used). This will also be the service name.
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
    *   Constructs object name using `tag` from YAML (e.g., `hello-app-v1.3-linux-amd64`).
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

**What it does (Example 2):**
*   Connects to NATS.
*   Uses the provided `-name` and `-tag` flags.
*   Uses the default node "local" and 1 replica (or overrides from `-node`/`-replicas` flags).
*   Detects platform for `./simple-server` (e.g., linux/amd64).
*   Constructs object name using the `-tag` flag (e.g., `simple-v0.1-linux-amd64`).
*   Uploads binary to `app-binaries` OS bucket.
*   Generates a simple `ServiceSpec` in memory and uploads it to `app-configs` KV store under the key `simple-app`.

---

### `narun service list`

Lists deployed services (applications) configured in NATS KV (`app-configs`) and shows their status on nodes based on `node-runner-states`.

**Options:**

*   `-nats <url>`: NATS server URL.
*   `-timeout <duration>`: NATS operation timeout.

**Example Output:**

```
$ ./narun service list
Listing deployed services and node status...
Found 2 active node(s).
SERVICE    TAG              NODE         NODE STATUS      NODE PLATFORM  DESIRED  RUNNING  INSTANCE IDS
-------    ---              ----         -----------      -------------  -------  -------  ------------
hello-app  hello-app-v1.3   node-amd64   running          linux/amd64    1        1        hello-app-0
hello-app  hello-app-v1.3   node-arm64   running          linux/arm64    2        2        hello-app-0, hello-app-1
simple-app simple-v0.1      local        running          linux/amd64    1        1        simple-app-0
simple-app simple-v0.1      other-node   offline/unknown  -/-            1        0        []
```

---

### `narun service info <service_name>`

Displays detailed information for a specific service, including its configuration (`ServiceSpec`) and the status of its instances on each targeted node.

**Arguments:**

*   `<service_name>`: The name of the service to inspect.

**Options:**

*   `-nats <url>`: NATS server URL.
*   `-timeout <duration>`: NATS operation timeout.

**Example Output:**

```
$ ./narun service info hello-app
Service Information for: hello-app (Tag: hello-app-v1.3)
--- Configuration (ServiceSpec) ---
name: hello-app
tag: hello-app-v1.3
args:
    - --listen
    - :9001
env:
    - name: GREETING
      value: Hello from NATS!
nodes:
    - name: node-amd64
      replicas: 1
    - name: node-arm64
      replicas: 2

--- Node Status & Instances ---
NODE ID     STATUS   PLATFORM     DESIRED  RUNNING  INSTANCE IDS
-------     ------   --------     -------  -------  ------------
node-amd64  running  linux/amd64  1        1        hello-app-0
node-arm64  running  linux/arm64  2        2        hello-app-0, hello-app-1

--- Orphaned Instances (Running on nodes not in current spec) ---
(No orphaned instances found)

```

---

### `narun service delete <service_name>`

Deletes a service configuration from NATS KV (`app-configs`). This action will trigger node runners to stop and remove all instances of the specified service.

**Arguments:**

*   `<service_name>`: The name of the service configuration to delete.

**Options:**

*   `-nats <url>`: NATS server URL.
*   `-timeout <duration>`: NATS operation timeout.
*   `-y`: Skip confirmation prompt.

**Example:**

```bash
./narun service delete -nats nats://host:4222 hello-app -y
```

---

### `narun logs`

Streams logs from running application instances via NATS.

**Options:**

*   `-nats <url>`: NATS server URL.
*   `-app <name>`: Filter logs by application name (service name).
*   `-node <id>`: Filter logs by node ID.
*   `-instance <id>`: Filter logs by specific instance ID (requires `-app`).
*   `-f`, `-follow`: Follow the log stream continuously.
*   `-raw`: Output raw JSON log messages.
*   `-timeout <duration>`: NATS connection timeout.

**Example:**

```bash
# Follow logs for all instances of 'hello-app' on 'node-arm64'
./narun logs -nats nats://host:4222 -app hello-app -node node-arm64 -f
```

---

### `narun list-images`

Lists application binaries stored in the NATS Object Store (`app-binaries`).

**Options:**

*   `-nats <url>`: NATS server URL.
*   `-timeout <duration>`: NATS operation timeout.

**Example Output:** (Output format remains the same)

```
$ ./narun list-images
Listing images from NATS Object Store 'app-binaries'...
OBJECT NAME                  SIZE  MODIFIED             TAG              OS     ARCH   DIGEST
-----------                  ----  --------             ---              --     ----   ------
hello-app-v1.3-linux-amd64   12345 2023-10-27T10:30:00Z hello-app-v1.3   linux  amd64  SHA-256=...
hello-app-v1.3-linux-arm64   12300 2023-10-27T10:30:05Z hello-app-v1.3   linux  arm64  SHA-256=...
```

---

### `narun files`

Manages shared, unencrypted files stored in the NATS Object Store bucket `narun-files`.

**(Subcommands and options remain the same: `add`, `list`, `delete`, `help`)**

---

### `narun secret`

Manages encrypted secrets for applications.

**(Subcommands and options remain the same: `set`, `list`, `delete`, `help`)**

---

## Execution Mode

Narun supports several modes of execution for workloads.

- `exec`: Execute the workload directly on the node runner.
- `landlock`: Execute the workload in a landlocked environment on the node runner.

**(Details and YAML example remain the same)**

---

## Environment Variables for Instances

The `node-runner` injects the following environment variables into running application instances:

*   `NARUN_APP_NAME`: The name of the application (from `ServiceSpec.name`).
*   `NARUN_INSTANCE_ID`: The unique ID for this specific replica (e.g., `my-app-0`).
*   `NARUN_RUN_ID`: A unique ID for the current execution (run) of this instance process.
*   `NARUN_REPLICA_INDEX`: The numerical index of this replica (e.g., `0`).
*   `NARUN_NODE_ID`: The ID of the node runner executing this instance.
*   `NARUN_NODE_OS`: The operating system of the node (e.g., `linux`).
*   `NARUN_NODE_ARCH`: The architecture of the node (e.g., `amd64`).
*   `NARUN_INSTANCE_ROOT`: The absolute path to the instance's working directory root.
*   *User-defined variables*: Any variables defined in `ServiceSpec.env`.
```
