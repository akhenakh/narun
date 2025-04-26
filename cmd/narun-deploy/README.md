
**How to Build and Use:**

1.  **Build:**
    ```bash
    go build -o narun-deploy ./cmd/narun-deploy
    ```

2.  **Prepare Files:**
    *   Create your application binary (e.g., `my-hello-app`).
    *   Create a `ServiceSpec` YAML file (e.g., `hello-spec.yaml`):
        ```yaml
        # hello-spec.yaml
        name: hello-app        # This will be the KV key
        binary_object: hello-app-v1.2 # This MUST match the object name in the Object Store
        args:
          - --listen
          - ":9000"
        env:
          - name: GREETING
            value: "Hello from NATS!"
        ```

3.  **Run:**
    ```bash
    ./narun-deploy \
        -nats nats://localhost:4222 \
        -config ./hello-spec.yaml \
        -binary ./my-hello-app
    ```

**What it does:**

1.  Connects to NATS.
2.  Reads `hello-spec.yaml`.
3.  Uploads the contents of the `./my-hello-app` file to the `app-binaries` object store with the object name `hello-app-v1.2`.
4.  Uploads the content of `hello-spec.yaml` to the `app-configs` key-value store with the key `hello-app`.

The `node-runner` instances watching the `app-configs` KV store will then see the update for the `hello-app` key, read the spec, download the `hello-app-v1.2` binary from the object store (if needed), and start/restart the process according to the spec.
