# Quick Start

This guide gets Pylon running locally and verifies it works end-to-end using a Pusher client SDK
and a Pusher server SDK.

## 1. Configure your app

Pylon reads app definitions from a JSON file (default: `apps.json` in the working directory).
Copy the example and edit it:

```sh
cp apps.example.json apps.json
```

Open `apps.json` and replace the placeholder values with your own `id`, `key`, and `secret`:

```json
[
  {
    "name": "My App",
    "id": "my-app-id",
    "key": "my-app-key",
    "secret": "my-app-secret",
    "client_messages_enabled": true,
    "capacity": 10000,
    "webhooks": []
  }
]
```

The `id`, `key`, and `secret` are arbitrary strings you choose — they are used to authenticate
your client and server SDKs. Keep `secret` private.

## 2. Start Pylon

=== "From source"

    ```sh
    cargo run --release
    ```

=== "Binary"

    ```sh
    ./pylon
    ```

=== "Docker"

    ```sh
    docker run -d --name pylon \
      -p 7000:7000 \
      -v "$PWD/apps.json:/etc/pylon/apps.json:ro" \
      -e PYLON_APPS_PATH=/etc/pylon/apps.json \
      --ulimit nofile=1048576:1048576 \
      ghcr.io/i-rocky/pylon:latest
    ```

Pylon listens on `0.0.0.0:7000` by default. Both the WebSocket endpoint (`ws://`) and the
Pusher REST API are served on the same port.

## 3. Connect a client

Install [pusher-js](https://github.com/pusher/pusher-js) and point it at your local Pylon
instance:

```js
import Pusher from "pusher-js";

const pusher = new Pusher("my-app-key", {
  wsHost: "127.0.0.1",
  wsPort: 7000,
  forceTLS: false,
  enabledTransports: ["ws"],
  cluster: "",
});

const channel = pusher.subscribe("my-channel");

channel.bind("my-event", (data) => {
  console.log("Received:", data);
});
```

The `cluster: ""` option prevents pusher-js from appending a cluster subdomain to the host.

## 4. Trigger an event from the server

Install [pusher-http-node](https://github.com/pusher/pusher-http-node) (or any compatible server
SDK) and publish an event:

```js
const Pusher = require("pusher");

const pusher = new Pusher({
  appId: "my-app-id",
  key: "my-app-key",
  secret: "my-app-secret",
  host: "127.0.0.1",
  port: "7000",
  useTLS: false,
});

pusher.trigger("my-channel", "my-event", { message: "Hello from Pylon!" });
```

If the client from step 3 is connected and subscribed to `my-channel`, the `my-event` handler
fires with `{ message: "Hello from Pylon!" }`.

## 5. Verify the server is healthy

Pylon exposes standard health endpoints:

```sh
curl http://127.0.0.1:7000/health   # → 200 OK while running
curl http://127.0.0.1:7000/ready    # → 200 OK when ready to serve traffic
curl http://127.0.0.1:7000/metrics  # → Prometheus exposition
```

## Next steps

Now that Pylon is running, explore the rest of the documentation:

- **Configuration** — environment variables, TLS, adapter selection, overload tuning.
- **Channels** — channel types (public, private, presence, encrypted, cache), auth flow, client events.
- **Clustering** — running multiple nodes behind a load balancer with Redis.
- **Deployment** — systemd units, Docker Compose, Kubernetes Helm chart, reverse-proxy TLS examples.
