# Runtime REST API

Manage tunnels dynamically without editing config files or restarting rathole.

## Setup

Add an `[api]` block to your **server** config:

```toml
[server]
bind_addr = "0.0.0.0:2333"
default_token = "my-tunnel-token"

[api]
bind_addr = "127.0.0.1:9090"
token = "my-api-token"          # optional — enables bearer auth
```

The API starts automatically alongside the rathole instance.

## Authentication

If `token` is set in `[api]`, all requests must include:

```
Authorization: Bearer my-api-token
```

Requests without a valid token receive `401 Unauthorized`.

If `token` is not set, the API is open — bind to localhost only in production.

---

## Endpoints

### List all services

```
GET /api/v1/services
```

Returns all services with their current state.

```bash
curl -s -H "Authorization: Bearer my-api-token" \
  http://127.0.0.1:9090/api/v1/services | jq
```

**Response:**

```json
[
  {
    "name": "ssh",
    "bind_addr": "0.0.0.0:5022",
    "service_type": "tcp",
    "state": "Active"
  },
  {
    "name": "web",
    "bind_addr": "0.0.0.0:8080",
    "service_type": "tcp",
    "state": "Registered"
  }
]
```

**Service states:**

| State | Meaning |
|-------|---------|
| `Registered` | Config added, waiting for client to connect |
| `Active` | Client connected, tunnel is live |
| `Disconnected` | Client was connected but dropped off |

---

### Get a single service

```
GET /api/v1/services/:name
```

Returns `404` if the service doesn't exist.

```bash
curl -s -H "Authorization: Bearer my-api-token" \
  http://127.0.0.1:9090/api/v1/services/ssh | jq
```

---

### Add a tunnel

```
PUT /api/v1/services/:name
```

Adds a new tunnel or replaces an existing one. Takes effect immediately — the port binds and the tunnel config is pushed to all connected gateway clients.

```bash
curl -X PUT \
  -H "Authorization: Bearer my-api-token" \
  -H "Content-Type: application/json" \
  -d '{
    "bind_addr": "0.0.0.0:5022",
    "local_addr": "192.168.16.10:22"
  }' \
  http://127.0.0.1:9090/api/v1/services/ssh
```

**Body fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `bind_addr` | string | **yes** | Server-side listen address (e.g. `"0.0.0.0:5022"`) |
| `local_addr` | string | **yes** | Client-side forward target (e.g. `"192.168.16.10:22"`) — pushed to gateway clients |
| `token` | string | no | Per-service auth token. Auto-filled from `default_token` if omitted |
| `type` | string | no | `"tcp"` (default) or `"udp"` |
| `nodelay` | bool | no | Enable `TCP_NODELAY` |

**Response:**

```json
{"status": "added"}
```

---

### Remove a tunnel

```
DELETE /api/v1/services/:name
```

Removes the tunnel. Port is released and the client tears down the forwarding — all pushed automatically.

```bash
curl -X DELETE \
  -H "Authorization: Bearer my-api-token" \
  http://127.0.0.1:9090/api/v1/services/ssh
```

**Response:**

```json
{"status": "deleted"}
```

---

## Error responses

| Status | Body | Cause |
|--------|------|-------|
| `400` | `{"error": "..."}` | Invalid JSON, missing fields, or missing token |
| `401` | `{"error": "unauthorized"}` | Missing or wrong bearer token |
| `404` | `{"error": "not found"}` | Unknown endpoint or service not found |

---

## How it works: gateway mode

The client runs as a **pure gateway** — no per-service configuration needed. The server pushes everything.

### Server config

```toml
[server]
bind_addr = "0.0.0.0:2333"
default_token = "123"

[api]
bind_addr = "127.0.0.1:9090"
```

No `[server.services]` needed. Add tunnels at runtime via the API.

### Client config (gateway mode)

```toml
[client]
remote_addr = "your-server-ip:2333"
default_token = "123"
```

That's it. No services defined — the client auto-enables gateway mode, connects to the server, and waits for tunnel configs to be pushed.

### Workflow

**1. Start the server and client** with the configs above.

**2. Add a tunnel from the server:**

```bash
curl -X PUT \
  -H "Content-Type: application/json" \
  -d '{"bind_addr": "0.0.0.0:5022", "local_addr": "192.168.16.10:22"}' \
  http://127.0.0.1:9090/api/v1/services/ssh
```

The server:
- Binds port `5022`
- Pushes the tunnel config to all connected gateway clients

The client:
- Receives the push
- Starts forwarding traffic from the server's `:5022` to `192.168.16.10:22`

**3. Add more tunnels — no client restart needed:**

```bash
curl -X PUT \
  -H "Content-Type: application/json" \
  -d '{"bind_addr": "0.0.0.0:8080", "local_addr": "192.168.1.50:3000"}' \
  http://127.0.0.1:9090/api/v1/services/web
```

**4. Check status:**

```bash
curl -s http://127.0.0.1:9090/api/v1/services | jq
```

**5. Remove a tunnel:**

```bash
curl -X DELETE http://127.0.0.1:9090/api/v1/services/web
```

### Pre-configured tunnels (TOML)

You can also define tunnels in the server TOML. Include `local_addr` so they get pushed to gateway clients on connect:

```toml
[server]
bind_addr = "0.0.0.0:2333"
default_token = "123"

[server.services.ssh]
bind_addr = "0.0.0.0:5022"
local_addr = "192.168.16.10:22"

[server.services.web]
bind_addr = "0.0.0.0:8080"
local_addr = "192.168.1.50:3000"

[api]
bind_addr = "127.0.0.1:9090"
```

When a gateway client connects, all services with `local_addr` are pushed to it automatically. New services added via API are also pushed in real time.
