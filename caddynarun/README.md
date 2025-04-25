## Build Caddy with the Plugin



```sh
xcaddy build --with github.com/akhenakh/narun/caddy@latest
```

This will create a custom `caddy` binary in your current directory.

## Example Caddyfile

Create a `Caddyfile` like this:

```caddyfile
{
	# Optional: Global NATS settings if needed elsewhere, but narun config is self-contained
	# admin off # Disable admin API if not needed
	# log {
	#  	level DEBUG # For development
	# }
}

http://localhost:2080 {
	# Optional: Enable debug logging for this site
	log {
	 	output stderr
	 	level DEBUG
	}

	# Route specific paths through narun
	route /hello/* {
		narun {
			nats_url localhost:4222 # Or nats://localhost:4222
			request_timeout 15s
			nats_stream TASK      # Matches the stream name from narun config/consumer

			# Define routes for this narun instance
			# route <path> <app> [METHOD...]
			route /hello/ hello POST PUT # Handle POST/PUT for /hello/ -> app 'hello'
			# Add more routes here if this instance handles multiple apps/paths
		}
	}

	# Another route example
	route /goodbye {
		narun {
			nats_url localhost:4222
			request_timeout 10s
			nats_stream TASK # Could potentially be a different stream if needed

			route /goodbye goodbye GET # Handle GET for /goodbye -> app 'goodbye'
		}
	}

	# Fallback or other handlers
	route /* {
		respond "Not handled by narun" 404
	}
}

```

## Run

1.  Ensure your NATS server
2.  Run your `narun` consumer(s):
    ```bash
    # Assuming narun/consumers/cmd/hello is built
    ./narun/consumers/cmd/hello/hello -nats-url "nats://localhost:4222" -stream "TASK" -app "hello"
    # Add other consumers (e.g., for 'goodbye' if you implement one)
    ```
3.  Run the custom Caddy build with your Caddyfile:
    ```bash
    ./caddy run
    ```
4.  Send requests:
    ```bash
    # Should hit the 'hello' consumer via narun handler
    curl -v -X POST -H "Content-Type: application/json" -d '{"name":"Caddy User"}' http://localhost:8080/hello/

    # Should hit the 'goodbye' consumer via narun handler (if you run one)
    curl -v http://localhost:8080/goodbye

    # Should hit the fallback handler
    curl -v http://localhost:8080/other/path
    ```

You should see logs in Caddy, the NATS server, and your consumer indicating the request flow. The response from the consumer should be returned to the `curl` client.
