# Multi-Agent Support

Run multiple rathole clients from different machines, each managed by a different user via Telegram. Each user registers their own agent, gets a one-time setup code, and only manages tunnels to their own machine.

## Architecture

```
                      Server (this machine)
                 +---------------------------+
                 |  rathole server (:2333)    |
                 |  rathole API    (:9090)    |
                 |  qtun-controller           |
                 |    - Telegram bot          |
                 |    - webhook server (:8090)|
                 +---------------------------+
                    /           |           \
           Agent: home-pc   Agent: office  Agent: lab-server
           (User A)         (User A)       (User B)
           rathole client   rathole client  rathole client
```

## User Flow

### 1. Register an agent

Send `/register home-pc` to the Telegram bot. The bot replies with a one-time setup code:

```
Agent home-pc registered!

Setup code: ABCD-1234
Expires: 10 minutes

On your machine, run:
rathole --setup 158.178.234.87:9090

Then enter the setup code when prompted.
```

### 2. Set up the client

On your machine, download the rathole binary and run:

```bash
./rathole --setup 158.178.234.87:9090
```

It prompts for the setup code:

```
Enter setup code: ABCD-1234
Config written to client.toml. You can now start normally with:
  rathole client.toml
```

The setup code is single-use and expires after 10 minutes.

### 3. Start the client

```bash
./rathole client.toml
```

The client connects to the server and waits for tunnel configurations to be pushed.

### 4. Create tunnels

Back in Telegram:

```
/create home-pc 192.168.1.100:22
```

The server pushes this tunnel only to the `home-pc` agent. Other agents don't see it.

### 5. Multiple agents per user

Register another agent for a different machine:

```
/register office-server
```

Set it up on that machine, then create tunnels targeting it:

```
/create office-server 10.0.0.5:3389
```

### 6. Manage

| Command | Description |
|---------|-------------|
| `/register <name>` | Register a new agent |
| `/agents` | List your agents with connection status |
| `/unregister <name>` | Remove agent and all its tunnels |
| `/create <agent> <target:port> [port]` | Create a tunnel on a specific agent |
| `/list` | List your tunnels (across all agents) |
| `/kill <name>` | Kill one of your tunnels |

## How It Works

### Per-agent gateway services

Each agent gets a unique gateway service name `__gw_<agent_id>__` on the server. This gives each agent a unique identity in the `ControlChannelMap`, allowing multiple clients to connect simultaneously.

### Service isolation

When a tunnel is created, it is tagged with the creator's `agent_id`. The server's push logic only sends `AddService`/`RemoveService` commands to the matching agent's gateway channel. Services without an `agent_id` (legacy) are pushed to all gateways for backward compatibility.

### Setup codes

Setup codes are stored in-memory on the rathole server with a 10-minute expiry. The `GET /api/v1/setup/:code` endpoint requires no authentication (the code itself is the auth) and is single-use -- it is deleted after the first successful claim.

## API Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `PUT` | `/api/v1/agents/:agent_id` | Yes | Register an agent (creates gateway service) |
| `DELETE` | `/api/v1/agents/:agent_id` | Yes | Unregister agent |
| `GET` | `/api/v1/agents` | Yes | List all agent gateway services |
| `POST` | `/api/v1/setup` | Yes | Create a setup code |
| `GET` | `/api/v1/setup/:code` | **No** | Claim setup code, get client config |

### Register agent

```bash
curl -X PUT -H "Content-Type: application/json" \
  -d '{"token": "agent-unique-token"}' \
  http://127.0.0.1:9090/api/v1/agents/abc123
```

### Create setup code

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"agent_id": "abc123", "token": "agent-unique-token", "setup_code": "ABCD-1234"}' \
  http://127.0.0.1:9090/api/v1/setup
```

### Claim setup code (from client)

```bash
curl http://158.178.234.87:9090/api/v1/setup/ABCD-1234
```

Response:
```json
{
  "remote_addr": "0.0.0.0:2333",
  "token": "agent-unique-token",
  "agent_id": "abc123"
}
```

### Create service with agent_id

```bash
curl -X PUT -H "Content-Type: application/json" \
  -d '{"bind_addr": "0.0.0.0:10500", "local_addr": "192.168.1.100:22", "agent_id": "abc123"}' \
  http://127.0.0.1:9090/api/v1/services/my-tunnel
```

## Client Config

The auto-generated `client.toml` looks like:

```toml
[client]
remote_addr = "158.178.234.87:2333"
default_token = "agent-unique-token"
agent_id = "abc123"
```

`gateway = true` is auto-enabled when no services are defined and `agent_id` is set.

## Backward Compatibility

- The legacy `__gateway__` service still works. A client without `agent_id` connects as before.
- Services without `agent_id` are pushed to ALL gateways (existing behavior).
- All new config fields are optional with defaults. Existing TOML files and API calls work unchanged.

## Security Notes

- The `GET /api/v1/setup/:code` endpoint has no auth because the code itself serves as a one-time secret. Keep codes short-lived (default 10 min).
- Each agent gets its own unique token. Tokens are not shared between agents.
- Users can only manage their own agents and tunnels via the Telegram bot (scoped by `chat_id`).
