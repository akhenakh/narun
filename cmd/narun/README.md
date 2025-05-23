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
*   `-node <node_id>`: Target node name (default: "local"; used if `-config` is not used, for service mode).
*   `-replicas <count>`: Number of replicas on the target node (default: 1; used if `-config` is not used, for service mode).
*   `-runMode <mode>`: Run mode: "service" or "cron" (default: "service"; used if `-config` is not used).
*   `-cronSchedule <expr>`: Cron schedule expression (e.g., "@every 5m", "0 * * * *") (used for cron mode if `-config` is not used).
*   `-jobMaxRetries <count>`: Max retries for a failed cron job (default: 0; used for cron mode if `-config` is not used).
*   `-jobTimeoutSeconds <sec>`: Timeout in seconds for a cron job execution (0 for no timeout; used for cron mode if `-config` is not used).

**Example 1: Deploy using a Config File (Full Spec for a Service)**

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

**Example 2: Deploy a Cron Job using a Config File**

1.  **Prepare `backup-spec.yaml`:**
    ```yaml
    name: my-nightly-backup
    tag: backup-tool-v1.2
    runMode: cron
    cronSchedule: "0 2 * * *" # Runs daily at 2 AM
    jobTimeoutSeconds: 3600 # Allowed to run for 1 hour
    jobMaxRetries: 1
    # 'command' is usually not needed if the binary itself is the command.
    # If your binary is just a script interpreter, you might use:
    # command: /path/to/interpreter 
    # args: [ /app/backup-script.sh, "--all" ] 
    # For a self-contained binary, 'command' can be omitted or be the binary name.
    # The actual binary path will be resolved by the node-runner.
    # args: # Arguments to the binary specified by 'tag'
    #  - "--all"
    env:
      - name: S3_BUCKET
        value: "my-backup-bucket"
    # No 'nodes' or 'replicas' needed for cron scheduling by narun itself.
    # Resource limits (memoryMB, cpuCores, cgroupParent) can still be defined
    # and will apply to each job execution.
    ```
2.  **Build binary (e.g., `backup-tool`):**
    ```bash
    # (Build your backup tool binary, e.g., backup-tool-linux-amd64)
    ```
3.  **Run Deploy:**
    ```bash
    ./narun deploy \
        -nats nats://localhost:4222 \
        -config ./backup-spec.yaml \
        ./backup-tool-linux-amd64 
    ```

**Example 3: Deploy without a Config File (Simple Defaults for a Service)**

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

**What it does (Example 3):**
*   Connects to NATS.
*   Uses the provided `-name` and `-tag` flags for a service.
*   Uses the default node "local" and 1 replica.
*   Detects platform for `./simple-server`.
*   Constructs object name using the `-tag` flag.
*   Uploads binary to `app-binaries` OS bucket.
*   Generates a `ServiceSpec` for `runMode: service` and uploads it to `app-configs` KV store.

**Example 4: Deploy a Cron Job using Flags**
```bash
./narun deploy -name my-nightly-backup -tag backup-tool-v1.2 \
  -runMode cron -cronSchedule "0 2 * * *" \
  -jobTimeoutSeconds 3600 -jobMaxRetries 1 \
  ./backup-tool-linux-amd64
```
**What it does (Example 4):**
*   Connects to NATS.
*   Uses provided flags to define a cron job.
*   Uploads the binary `backup-tool-linux-amd64`.
*   Generates a `ServiceSpec` for `runMode: cron` with the specified cron parameters and uploads it.


---

### `narun service list`

Lists deployed services (applications) configured in NATS KV (`app-configs`) and shows their status on nodes based on `node-runner-states`.
The "MODE" column indicates if the configuration is for a "Service" or a "Cron" job.
The "CRON SCHEDULE" column shows the schedule if the mode is "Cron".
For cron jobs, node-specific details (NODE, NODE STATUS, DESIRED, RUNNING, INSTANCE IDS) are typically "N/A" as scheduling is centralized and executions are transient.

**Options:**

*   `-nats <url>`: NATS server URL.
*   `-timeout <duration>`: NATS operation timeout.

**Example Output:**

```
$ ./narun service list
Listing deployed services and node status...
Found 2 active node(s).
SERVICE            TAG              MODE     CRON SCHEDULE  NODE        NODE STATUS    NODE PLATFORM  DESIRED  RUNNING  INSTANCE IDS
-------            ---              ----     -------------  ----        -----------    -------------  -------  -------  ------------
hello-app          hello-app-v1.3   Service  -              node-amd64  running        linux/amd64    1        1        hello-app-0
hello-app          hello-app-v1.3   Service  -              node-arm64  running        linux/arm64    2        2        hello-app-0, hello-app-1
my-nightly-backup  backup-tool-v1.2 Cron     0 2 * * *      N/A         N/A            N/A            N/A      N/A      N/A (see info)
simple-app         simple-v0.1      Service  -              local       running        linux/amd64    1        1        simple-app-0
simple-app         simple-v0.1      Service  -              other-node  offline/unknown -/-           1        0        []
```

---

### `narun service info <service_name>`

Displays detailed information for a specific service, including its configuration (`ServiceSpec`).
For services with `runMode: service`, it shows the status of its instances on each targeted node.
For services with `runMode: cron`, it displays cron-specific parameters (`CronSchedule`, `JobMaxRetries`, `JobTimeoutSeconds`) and the recent execution history.

**Arguments:**

*   `<service_name>`: The name of the service to inspect.

**Options:**

*   `-nats <url>`: NATS server URL.
*   `-timeout <duration>`: NATS operation timeout.

**Example Output (Service):** 
(Similar to existing example, shows Node Status & Instances)

**Example Output (Cron Job):**
```
$ ./narun service info my-nightly-backup
Service Information for: my-nightly-backup (Tag: backup-tool-v1.2)
--- Configuration (ServiceSpec) ---
name: my-nightly-backup
tag: backup-tool-v1.2
runMode: cron
cronSchedule: 0 2 * * *
jobTimeoutSeconds: 3600
jobMaxRetries: 1
env:
    - name: S3_BUCKET
      value: my-backup-bucket

--- Node Status & Instances ---
(Service is configured as a cron job, instances are transient. See execution history below.)

--- Cron Job Execution History (Last 10) ---
RUN ID              SCHEDULED AT          STARTED AT            ENDED AT              STATUS    EXIT CODE  ATTEMPTS  ERROR
------              ------------          ----------            --------              ------    ---------  --------  -----
1678886400000000123 2023-03-15T02:00:00Z  2023-03-15T02:00:01Z  2023-03-15T02:00:35Z  success   0          0/1       -
1678800000000000456 2023-03-14T02:00:00Z  2023-03-14T02:00:02Z  2023-03-14T02:01:00Z  failure   1          1/1       Job script error
1678713600000000789 2023-03-13T02:00:00Z  2023-03-13T02:00:00Z  2023-03-13T03:00:00Z  timeout   -1         0/1       Job exceeded timeout of 3600 seconds
```
**Cron Job Execution History Columns:**
*   `RUN ID`: Unique ID for each execution attempt.
*   `SCHEDULED AT`: The time the job was scheduled to start by the cron expression.
*   `STARTED AT`: Actual start time of the execution.
*   `ENDED AT`: End time of the execution.
*   `STATUS`: Result of the execution (e.g., "success", "failure", "timeout", "retrying", "launcher_failure", "failure_to_start").
*   `EXIT CODE`: Exit code of the process if it ran.
*   `ATTEMPTS`: For jobs that retried, shows `current_attempt/max_retries`.
*   `ERROR`: Any error message if the job failed or timed out (can be truncated).

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

## Execution Mode & ServiceSpec

Narun supports several modes of execution for workloads, configured via the `ServiceSpec` YAML.

**Common `ServiceSpec` Fields:**

*   `name: string`: The unique name for your application/service.
*   `tag: string`: A version tag for the binary to be deployed (e.g., `my-app-v1.2.3`). The node runner will look for binaries in the object store named `<tag>-<os>-<arch>`.
*   `command: string` (optional): Command to run. Defaults to the binary name itself. Useful if the binary is an interpreter (e.g., `/usr/bin/python3`) and `args` contains the script.
*   `args: []string` (optional): Arguments to pass to the command/binary.
*   `env: []EnvVar` (optional): Environment variables to set. Each `EnvVar` has `name` and either `value` or `valueFromSecret`.
*   `mounts: []MountSpec` (optional): Files to mount from the `narun-files` object store into the instance's working directory.
*   `user: string` (optional): User to run the process as.
*   `memoryMB: uint64` (optional): Memory soft limit in MiB.
*   `memoryMaxMB: uint64` (optional): Memory hard limit in MiB.
*   `cpuCores: float64` (optional): CPU bandwidth (e.g., 0.5 for 50%).
*   `cgroupParent: string` (optional): Cgroup parent path for resource limits.
*   `networkNamespacePath: string` (optional): Path to an existing network namespace.
*   `mode: string` (optional): Execution mode. Can be:
    *   `"exec"` (default): Execute the workload directly on the node runner.
    *   `"landlock"`: Execute the workload in a Landlock-sandboxed environment.
*   `landlock: LandlockSpec` (optional): Configuration for Landlock if `mode: landlock`.

**Service-Specific `ServiceSpec` Fields:**

*   `nodes: []NodeSelectorSpec`: Defines which nodes should run this service and how many replicas.
    *   `name: string`: Node ID.
    *   `replicas: int`: Number of instances on this node.

**Cron Job-Specific `ServiceSpec` Fields:**

*   `runMode: string`: Must be set to `"cron"` to enable cron scheduling. If omitted or set to `"service"`, it's treated as a long-running service.
*   `cronSchedule: string`: A standard cron expression (e.g., `"@every 1h30m"`, `"0 0 * * WED"`, `"*/5 * * * *"` for every 5 minutes). Required if `runMode` is `"cron"`.
*   `jobMaxRetries: int` (optional): Number of times to retry a failed job execution (default: 0). This does not apply to jobs that time out or have launcher failures.
*   `jobTimeoutSeconds: int` (optional): Maximum duration in seconds a single job execution is allowed to run (default: 0, meaning no timeout). If a job exceeds this, it's terminated.

**(Details for LandlockSpec, MountSpec etc. would follow or be in separate docs)**

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
*   **For Cron Jobs Only:**
    *   `NARUN_JOB_ATTEMPT`: The current attempt number for this execution (e.g., "0" for the first try, "1" for the first retry).
    *   `NARUN_JOB_SCHEDULED_TIME`: The timestamp (RFC3339Nano) when this job was originally scheduled to fire.
```
