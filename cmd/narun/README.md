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
*   `secret`: Manage secrets for applications.
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


### `narun files`

Manages shared, unencrypted files stored in the NATS Object Store bucket `narun-files`. These files can be mounted into the working directory of application instances using the `mounts` section in the `ServiceSpec`.

**Subcommands:**

*   `add <name> <local_path>`: Adds a new file or replaces an existing file with the given logical `<name>` using the content from `<local_path>`. The `<name>` is used as the key in the object store and referenced in `mounts.source.objectStore`.
*   `list`: Lists the logical names, sizes, modification times, and digests of files stored in the `narun-files` bucket.
*   `delete <name> [-y]`: Deletes the file with the specified logical `<name>` from the object store. Use `-y` to skip confirmation.
*   `help`: Shows detailed help for the files command.

**Options (Common):**

*   `-nats <url>`: NATS server URL (default: "nats://localhost:4222").
*   `-timeout <duration>`: Timeout for NATS operations (default: 15s).

**Example Workflow:**

1.  **Create a local configuration file:**
    ```bash
    echo '{"api_endpoint": "https://example.com/api"}' > ./myapp-settings.json
    ```
2.  **Add the file to Narun:**
    ```bash
    ./narun files add -nats nats://localhost:4222 my-app-settings ./myapp-settings.json
    # -> Uploads myapp-settings.json to the 'narun-files' OS bucket
    #    with the object name 'my-app-settings'
    ```
3.  **List Files:**
    ```bash
    ./narun files list -nats nats://localhost:4222
    # NAME             SIZE  MODIFIED             ORIGINAL FILENAME    DIGEST
    # ----             ----  --------             -----------------    ------
    # my-app-settings  42    2023-10-29T14:00:00Z myapp-settings.json SHA-256=...
    ```
4.  **Use in `ServiceSpec`:** (See deploy example above)
    ```yaml
    mounts:
      - path: config/settings.json # Relative path inside instance work dir
        source:
          objectStore: my-app-settings # Reference the name used in 'add'
    # ...
    ```
5.  **Deploy App:** (Deploy command as shown before)
6.  **Run `node-runner`:** (As shown before)
7.  **Verification:** When the `hello-app` instance starts on a node, the `node-runner` will download the `my-app-settings` object from NATS and place its content at `/path/to/narun-data/instances/hello-app-0/work/config/settings.json` (adjusting for actual data dir and instance ID). The application can then read this file.
8.  **Delete File:**
    ```bash
    ./narun files delete -nats nats://localhost:4222 my-app-settings -y
    ```



### `narun secret`

1.  **Generate a Master Key:** Create a strong 32-byte key and base64 encode it.
    ```bash
    # Example using openssl
    openssl rand -base64 32
    # Copy the output
    export NARUN_MASTER_KEY="YOUR_BASE64_ENCODED_KEY_HERE"

    # Or store it in a File and set the environment variable
    export NARUN_MASTER_KEY_PATH="/path/to/master_key.txt"
    ```
1.  **Set a Secret:**
    ```bash
    ./narun secret set -nats nats://localhost:4222 -master-key $NARUN_MASTER_KEY A_SECURE_KEY "this-is-my-super-secret-value" -desc "API key for external service"
    ```
1.  **List Secrets:**
    ```bash
    ./narun secret list -nats nats://localhost:4222
    ```
1.  **Example deployment**
    ```yaml
    name: hello-app
    tag: v2
    args: [ "--listen", ":9001" ]
    env:
      - name: GREETING
        value: "Hello from NATS!"
      - name: MY_SECRET_ENV # App expects this env var
        valueFromSecret: A_SECURE_KEY # Reference the secret name
    nodes:
      - name: node-amd64
        replicas: 1
    ```
1.  **Run Node Runner with Master Key:**
    ```bash
    ./node-runner -nats-url nats://localhost:4222 -data-dir /data/narun -node-id node-amd64 -master-key $NARUN_MASTER_KEY
    ```
1.  **Verification:** The `hello-app` running on `node-amd64` should now have the environment variable `MY_SECRET_ENV` set to `"this-is-my-super-secret-value"`.
1.  **Delete a Secret:**
    ```bash
    ./narun secret delete -nats nats://localhost:4222 A_SECURE_KEY
    ```

## Execution Mode

Narun is supporting several mode of execution for the workloads.

- `exec`: Execute the workload directly on the node runner.
- `landlock`: Execute the workload in a landlocked environment on the node runner.

```yaml
# narun/cmd/narun/hello.yaml example with landlock
name: hello
tag: aaf56fe96c953198aec60280a99c3b227c9c3215
nodes:
  - name: node-linux-1 # Target Linux node runner
    replicas: 1

  - name: node-other # Target node runner without landlock
    replicas: 1

mode: landlock # Enable landlock mode
landlock:
  shared: true  # Allow shared libraries (often needed for non static Go binaries)
  dns: true     # Allow DNS resolution files
  certs: true   # Allow system certificates
  tmp: true     # Allow /tmp access
  paths:        # Custom paths
    - path: /data/config.json # Example: allow reading a config file
      modes: "r"
    - path: /app/logs # Example: allow writing logs to a specific directory
      modes: "wc" # Allow write and create

args:
  - -concurrency=1
env:
  - name: GREETING
    value: "Hello from restricted NATS!"
  - name: SECRET
    valueFromSecret: SECRET # Example secret usage
```

## Environment Variables for Instances

The `node-runner` injects the following environment variables into running application instances:

*   `NARUN_APP_NAME`: The name of the application (from `ServiceSpec.name`).
*   `NARUN_INSTANCE_ID`: The unique ID for this specific replica (e.g., `my-app-0`).
*   `NARUN_REPLICA_INDEX`: The numerical index of this replica (e.g., `0`).
*   `NARUN_NODE_ID`: The ID of the node runner executing this instance.
*   `NARUN_NODE_OS`: The operating system of the node (e.g., `linux`).
*   `NARUN_NODE_ARCH`: The architecture of the node (e.g., `amd64`).
*   `NARUN_INSTANCE_ROOT`: The absolute path to the instance's working directory root. For `exec` and `landlock` modes, this is the `work` directory within the instance's data directory on the host (e.g., `/data/narun/instances/my-app-0/work`). Applications can use this path to reliably locate mounted files (e.g., `$NARUN_INSTANCE_ROOT/config/settings.json`).
*   *User-defined variables*: Any variables defined in `ServiceSpec.env` (including those resolved from secrets).
