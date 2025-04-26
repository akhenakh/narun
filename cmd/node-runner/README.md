**Design Considerations:**

1.  **Core Responsibility:** The `node-runner` manages application processes on a specific node based on configurations stored centrally in NATS.
2.  **Configuration Source:** NATS KV Store (`app-configs`) will hold the desired state (`ServiceSpec`) for each application. The key will be the application name (e.g., `my-service`).
3.  **Binary Source:** NATS Object Store (`app-binaries`) will store the application executables. The object name should ideally correspond to the application name, perhaps with a version suffix if needed (e.g., `my-service` or `my-service-v1.0.0`). For simplicity, we'll start with just the app name.
4.  **State Management:** Each `node-runner` instance needs to maintain the *actual* state of the applications it's managing locally (e.g., running PID, associated config hash, process handle). An in-memory map protected by a mutex is suitable.
5.  **Process Management:** We'll use `os/exec` to start, monitor, and stop application processes. We need to capture stdout/stderr.
6.  **Log Forwarding:** Captured stdout/stderr will be published line-by-line to dedicated NATS subjects (e.g., `logs.<app-name>.<node-id>`).
7.  **Status Reporting:** The runner will publish status updates (running, exited, failed) to NATS subjects (e.g., `status.<app-name>.<node-id>`).
8.  **Failure Handling:** If a managed process exits unexpectedly, the runner should attempt to restart it (basic restart initially, could add backoff).
9.  **Configuration Changes:** The runner will use a NATS KV Watcher on the `app-configs` bucket to react to changes (new apps, updates, deletions).
10. **ELF Config:** While the prompt mentions embedding config in the ELF and reading it, this adds complexity *for the runner*. A more typical pattern is for the runner to use the KV config (`ServiceSpec`) to know *how* to run the binary (args, env), and the binary *itself* might read its own embedded config or separate config files/env vars. Let's simplify: the node runner **will not** read the `.appconfig` section. It will rely solely on the `ServiceSpec` from the NATS KV store to configure the execution environment (command, args, env). The binary name will be derived from `ServiceSpec.Name`.
11. **Data Directory:** The runner needs a local directory to store downloaded binaries.
12. **Node ID:** Each runner needs a unique identifier (could be hostname, UUID, etc.) to distinguish logs/status from different nodes.
