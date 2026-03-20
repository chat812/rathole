# Connection Approval

Hold incoming visitor connections and require explicit approval before forwarding traffic. When enabled, each new TCP connection triggers a webhook notification and waits for an approve/deny decision via the API.

## How it works

```
Visitor connects to tunnel port
        |
        v
Rathole holds the TCP connection
        |
        v
Webhook POST fires to your controller (fire-and-forget)
        |
        v
Controller sends notification (e.g. Telegram message)
        |
        v
Admin clicks Approve or Deny
        |
        v
Controller calls POST /api/v1/pending/:id/approve (or /deny)
        |
        v
Rathole resumes the connection (or drops it)
```

If no decision is made within the timeout, the connection is automatically denied and dropped.

## Configuration

### Server-side (`server.toml`)

```toml
[server]
bind_addr = "0.0.0.0:2333"
default_token = "my-token"

[server.services.ssh]
bind_addr = "0.0.0.0:5022"
local_addr = "192.168.16.10:22"
require_approval = true                 # <-- enable per service

[api]
bind_addr = "127.0.0.1:9090"
approval_webhook = "http://127.0.0.1:8090/webhook/connection"   # controller URL
approval_timeout = 60                   # seconds before auto-deny (default: 60)
```

### Per-service via API

When adding a tunnel at runtime, include `require_approval` in the body:

```bash
curl -X PUT \
  -H "Content-Type: application/json" \
  -d '{
    "bind_addr": "0.0.0.0:5022",
    "local_addr": "192.168.16.10:22",
    "require_approval": true
  }' \
  http://127.0.0.1:9090/api/v1/services/ssh
```

Services without `require_approval` (or with it set to `false`) forward traffic immediately as before. There is zero overhead for services that do not use this feature.

### `[api]` fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `approval_webhook` | string | none | URL to POST when a new connection is pending. If not set, connections still wait for API approval but no notification is sent. |
| `approval_timeout` | integer | `60` | Seconds to wait for a decision before auto-denying. |

## Webhook payload

When a visitor connects to a service with `require_approval = true`, rathole sends a POST request to `approval_webhook`:

```
POST /webhook/connection
Content-Type: application/json

{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "service_name": "ssh",
  "visitor_addr": "203.0.113.50:43210"
}
```

The webhook is fire-and-forget. Rathole does not wait for the HTTP response. If the webhook endpoint is unreachable, the connection still waits for the timeout and is then auto-denied.

## API endpoints

See the full reference in [REST API](rest-api.md). Summary:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/pending` | List all pending connections |
| `POST` | `/api/v1/pending/:id/approve` | Approve — connection is forwarded |
| `POST` | `/api/v1/pending/:id/deny` | Deny — connection is dropped |

## Manual approval (without webhook)

You can skip `approval_webhook` entirely and poll the pending list:

```bash
# Check for pending connections
curl -s http://127.0.0.1:9090/api/v1/pending | jq

# Approve one
curl -X POST http://127.0.0.1:9090/api/v1/pending/<id>/approve

# Deny one
curl -X POST http://127.0.0.1:9090/api/v1/pending/<id>/deny
```

## Integration with qtun-controller

The [qtun-controller](https://github.com/chat812/qtun-controller) provides a ready-made Telegram integration:

### Controller config (`config.toml`)

```toml
telegram_bot_token = "123456:ABC..."
server_ip = "158.178.234.87"
domain = "tun.example.com"
rathole_api = "http://127.0.0.1:9090"
db_path = "tunnels.db"
port_min = 10000
port_max = 14000
log_level = "info"
idle_timeout_secs = 3600
webhook_listen_addr = "0.0.0.0:8090"    # <-- listen for rathole webhooks
admin_chat_id = 123456789                # <-- your Telegram chat ID
```

### What happens

1. Visitor connects to a tunnel port with `require_approval = true`
2. Rathole POSTs to `http://127.0.0.1:8090/webhook/connection`
3. qtun-controller sends a Telegram message to `admin_chat_id`:

   ```
   Connection Request

   Service: ssh
   Source IP: 203.0.113.50:43210
   ID: 550e8400...

   [Approve]  [Deny]
   ```

4. Admin taps **Approve** or **Deny**
5. qtun-controller calls `POST /api/v1/pending/:id/approve` (or `/deny`)
6. Rathole resumes or drops the connection
7. The Telegram message is edited to show the result

### Finding your chat ID

Send any message to your bot, then:

```bash
curl -s "https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates" | jq '.result[-1].message.chat.id'
```

## Edge cases

| Scenario | Behavior |
|----------|----------|
| Webhook endpoint is down | Connection waits for `approval_timeout`, then auto-denied |
| Admin clicks button twice | Second click returns "expired" — the oneshot is consumed on first decision |
| Visitor disconnects while pending | Approved connection fails immediately on forward, cleaned up naturally |
| Rathole restarts while connections pending | All pending connections are lost (in-memory). Visitors can reconnect. |
| UDP services | Not supported yet. Only TCP connections support approval. |

## Security notes

- The pending API endpoints respect the same bearer token auth as all other API endpoints.
- The webhook fires to `approval_webhook` over plain HTTP by default. Use localhost or a private network. For remote controllers, put a reverse proxy with TLS in front.
- The `approval_timeout` prevents connections from being held indefinitely. Keep it reasonable (30-120 seconds).
