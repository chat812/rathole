# Runtime REST API

The runtime REST API allows you to manage services dynamically without editing config files or restarting rathole.

## Setup

Add an `[api]` block to your config file:

```toml
[api]
bind_addr = "127.0.0.1:9090"    # Address the API listens on
token = "my-secret-token"        # Optional. Enables bearer token authentication
```

The API server starts automatically alongside the rathole instance.

## Authentication

If `token` is set in the config, all requests must include a bearer token:

```
Authorization: Bearer my-secret-token
```

Requests without a valid token receive `401 Unauthorized`.

If `token` is not set, the API is open (bind to localhost only in production).

## Endpoints

### List All Services

```
GET /api/v1/services
```

Returns all services with their current state.

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

**Service States:**

| State | Description |
|-------|-------------|
| `Registered` | Config present, waiting for client to connect |
| `Active` | Client connected, tunnel is live |
| `Disconnected` | Client was connected but dropped off |

**Example:**

```bash
curl -s -H "Authorization: Bearer my-secret-token" \
  http://127.0.0.1:9090/api/v1/services | jq
```

---

### Get a Single Service

```
GET /api/v1/services/:name
```

Returns info about one service. Returns `404` if the service doesn't exist.

**Example:**

```bash
curl -s -H "Authorization: Bearer my-secret-token" \
  http://127.0.0.1:9090/api/v1/services/ssh | jq
```

**Response:**

```json
{
  "name": "ssh",
  "bind_addr": "0.0.0.0:5022",
  "service_type": "tcp",
  "state": "Active"
}
```

---

### Add or Update a Service

```
PUT /api/v1/services/:name
```

Adds a new service or replaces an existing one. The service takes effect immediately — ports bind and tunnels establish without restart.

The request body format depends on whether rathole is running as a **server** or **client**.

#### Server Mode

```bash
curl -X PUT \
  -H "Authorization: Bearer my-secret-token" \
  -H "Content-Type: application/json" \
  -d '{
    "bind_addr": "0.0.0.0:5022",
    "token": "secret123",
    "local_addr": "192.168.1.10:22",
    "type": "tcp",
    "nodelay": true
  }' \
  http://127.0.0.1:9090/api/v1/services/ssh
```

**Body fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `bind_addr` | string | Yes | Address to expose the service (e.g. `"0.0.0.0:5022"`) |
| `token` | string | Yes | Authentication token (must match client) |
| `local_addr` | string | No | Client-side forward address (e.g. `"192.168.1.10:22"`). When set, the server pushes this to clients automatically — **no client-side config needed** |
| `type` | string | No | `"tcp"` (default) or `"udp"` |
| `nodelay` | bool | No | Enable TCP_NODELAY |

#### Client Mode

```bash
curl -X PUT \
  -H "Authorization: Bearer my-secret-token" \
  -H "Content-Type: application/json" \
  -d '{
    "local_addr": "127.0.0.1:22",
    "token": "secret123",
    "type": "tcp",
    "nodelay": true
  }' \
  http://127.0.0.1:9090/api/v1/services/ssh
```

**Body fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `local_addr` | string | Yes | Address of the local service to forward (e.g. `"127.0.0.1:22"`) |
| `token` | string | Yes | Authentication token (must match server) |
| `type` | string | No | `"tcp"` (default) or `"udp"` |
| `nodelay` | bool | No | Enable TCP_NODELAY |
| `retry_interval` | int | No | Retry interval in seconds |

**Response:**

```json
{"status": "added"}
```

---

### Remove a Service

```
DELETE /api/v1/services/:name
```

Removes a service. The port is released and the tunnel torn down immediately.

**Example:**

```bash
curl -X DELETE \
  -H "Authorization: Bearer my-secret-token" \
  http://127.0.0.1:9090/api/v1/services/ssh
```

**Response:**

```json
{"status": "deleted"}
```

---

## Agent Control Channel (Server Push)

When services have `local_addr` set, the server automatically pushes complete service configs to clients:

- **On client connect:** All existing services with `local_addr` are pushed to the newly connected client
- **On AddService (API or hot-reload):** The new service is pushed to all connected clients
- **On RemoveService:** All connected clients tear down the tunnel

The client acts as a **pure gateway** — it receives the full config (including where to forward traffic) from the server. No client-side service configuration is needed for pushed services.

> **Note:** Services without `local_addr` are NOT pushed. Those require traditional client-side config.

## Error Responses

| Status Code | Body | Cause |
|-------------|------|-------|
| `400` | `{"error": "..."}` | Invalid JSON body or missing required fields |
| `401` | `{"error": "unauthorized"}` | Missing or invalid bearer token |
| `404` | `{"error": "not found"}` | Unknown endpoint or service not found |

## Full Example: Gateway Mode (Server-Only Management)

The client acts as a pure gateway — no service config needed on the client side. The server pushes everything.

**Server config (`server.toml`):**

```toml
[server]
bind_addr = "0.0.0.0:2333"
default_token = "123"

# Pre-configured service with local_addr — pushed to clients on connect
[server.services.ssh]
bind_addr = "0.0.0.0:5022"
local_addr = "192.168.16.10:22"    # Where the client forwards traffic

[api]
bind_addr = "127.0.0.1:9090"
token = "admin-token"
```

**Client config (`client.toml`) — minimal, one bootstrap service required:**

```toml
[client]
remote_addr = "myserver.com:2333"
default_token = "123"

# One service is needed to establish the control channel.
# All additional services are pushed from the server automatically.
[client.services.ssh]
local_addr = "192.168.16.10:22"
```

> **Why one bootstrap service?** The client needs at least one service to initiate the control channel connection to the server. Once connected, the server pushes all other services with `local_addr` through that channel.

**Add a new service at runtime — server side only:**

```bash
# Expose a web app. Include local_addr so the client auto-configures.
curl -X PUT \
  -H "Authorization: Bearer admin-token" \
  -H "Content-Type: application/json" \
  -d '{
    "bind_addr": "0.0.0.0:8080",
    "token": "123",
    "local_addr": "192.168.1.50:3000"
  }' \
  http://127.0.0.1:9090/api/v1/services/web

# Done! The server pushes the service to the client.
# The client creates the tunnel automatically. No client-side action needed.
```

**Check service status:**

```bash
curl -s -H "Authorization: Bearer admin-token" \
  http://127.0.0.1:9090/api/v1/services | jq
```

**Remove the service:**

```bash
curl -X DELETE -H "Authorization: Bearer admin-token" \
  http://127.0.0.1:9090/api/v1/services/web
# Client tears down the tunnel automatically.
```

**TOML gateway mode** (no API needed):

```toml
# server.toml — all services defined here with local_addr
[server]
bind_addr = "0.0.0.0:2333"
default_token = "123"

[server.services.ssh]
bind_addr = "0.0.0.0:5022"
local_addr = "192.168.16.10:22"

[server.services.web]
bind_addr = "0.0.0.0:8080"
local_addr = "192.168.1.50:3000"
```

When a client connects, all services with `local_addr` are pushed to it automatically.
