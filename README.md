# rathole

![rathole-logo](./docs/img/rathole-logo.png)

[![GitHub stars](https://img.shields.io/github/stars/rapiz1/rathole)](https://github.com/rapiz1/rathole/stargazers)
[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/rapiz1/rathole)](https://github.com/rapiz1/rathole/releases)
![GitHub Workflow Status (branch)](https://img.shields.io/github/actions/workflow/status/rapiz1/rathole/rust.yml?branch=main)
[![GitHub all releases](https://img.shields.io/github/downloads/rapiz1/rathole/total)](https://github.com/rapiz1/rathole/releases)
[![Docker Pulls](https://img.shields.io/docker/pulls/rapiz1/rathole)](https://hub.docker.com/r/rapiz1/rathole)

A secure, stable and high-performance reverse proxy for NAT traversal, written in Rust

rathole, like [frp](https://github.com/fatedier/frp) and [ngrok](https://github.com/inconshreveable/ngrok), can help to expose the service on the device behind the NAT to the Internet, via a server with a public IP.

## Features

- **High Performance** Much higher throughput than frp, more stable under heavy connections. See [Benchmark](#benchmark)
- **Low Resource Consumption** Uses far less memory than similar tools. Binary can be **~500KiB** for embedded devices. See [Build Guide](docs/build-guide.md)
- **Security** Mandatory per-service tokens. Optional Noise Protocol or TLS encryption
- **Hot Reload** Add/remove services by editing the config file — no restart needed
- **Runtime API** REST API to add, remove, and inspect services at runtime
- **Agent Control Channel** Server can push service changes to connected clients automatically
- **Dynamic Port Binding** New services bind ports on-the-fly without restarting
- **Service Registry** Track service state (registered, active, disconnected) in real time

## Quickstart

### Install

Download from [releases](https://github.com/rapiz1/rathole/releases), [build from source](docs/build-guide.md), or use [Docker](https://hub.docker.com/r/rapiz1/rathole).

### Basic Setup

You need: a server with a public IP, and a device behind NAT running a service you want to expose.

**Example:** Expose your home NAS SSH to the Internet.

#### Step 1 — Server (public IP)

```toml
# server.toml
[server]
bind_addr = "0.0.0.0:2333"

[server.services.my_nas_ssh]
token = "use_a_secret_that_only_you_know"
bind_addr = "0.0.0.0:5202"
```

```bash
./rathole server.toml
```

#### Step 2 — Client (behind NAT)

```toml
# client.toml
[client]
remote_addr = "myserver.com:2333"

[client.services.my_nas_ssh]
token = "use_a_secret_that_only_you_know"
local_addr = "127.0.0.1:22"
```

```bash
./rathole client.toml
```

#### Step 3 — Connect

```bash
ssh myserver.com -p 5202
```

Traffic to `myserver.com:5202` is forwarded to your NAS port `22`.

## Runtime API

Enable the REST API to manage services without editing config files or restarting.

### Enable

Add an `[api]` block to your config:

```toml
[api]
bind_addr = "127.0.0.1:9090"
token = "my-api-secret"       # optional, enables bearer token auth
```

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/services` | List all services with state |
| `GET` | `/api/v1/services/:name` | Get one service |
| `PUT` | `/api/v1/services/:name` | Add or update a service |
| `DELETE` | `/api/v1/services/:name` | Remove a service |

### Examples

**List services:**

```bash
curl -H "Authorization: Bearer my-api-secret" \
  http://127.0.0.1:9090/api/v1/services
```

**Add a server service:**

```bash
curl -X PUT \
  -H "Authorization: Bearer my-api-secret" \
  -H "Content-Type: application/json" \
  -d '{"bind_addr":"0.0.0.0:5203","token":"secret123"}' \
  http://127.0.0.1:9090/api/v1/services/new_service
```

**Add a client service:**

```bash
curl -X PUT \
  -H "Authorization: Bearer my-api-secret" \
  -H "Content-Type: application/json" \
  -d '{"local_addr":"127.0.0.1:3000","token":"secret123"}' \
  http://127.0.0.1:9090/api/v1/services/new_service
```

**Remove a service:**

```bash
curl -X DELETE \
  -H "Authorization: Bearer my-api-secret" \
  http://127.0.0.1:9090/api/v1/services/new_service
```

Services added via API take effect immediately — ports bind and tunnels establish without restart.

### Agent Control Channel

When a service is added or removed on the server, the change is automatically pushed to all connected clients via the control channel. Clients create or tear down tunnels in response, enabling fully centralized service management from the server side.

## Configuration Reference

`rathole` auto-detects server or client mode based on which block is present. If both exist, use `--server` or `--client` to specify.

See [examples](./examples) and [Transport docs](./docs/transport.md) for more.

```toml
[client]
remote_addr = "example.com:2333"     # Required. Server address
default_token = "token"              # Optional. Default token for services
heartbeat_timeout = 40               # Optional. 0 to disable. Default: 40s
retry_interval = 1                   # Optional. Default: 1s

[client.transport]
type = "tcp"                         # "tcp", "tls", "noise", "websocket"

[client.transport.tcp]
proxy = "socks5://user:pass@127.0.0.1:1080"  # Optional. http or socks5
nodelay = true                       # Optional. Default: true
keepalive_secs = 20                  # Optional. Default: 20
keepalive_interval = 8               # Optional. Default: 8

[client.transport.tls]
trusted_root = "ca.pem"             # Required for tls
hostname = "example.com"            # Optional

[client.transport.noise]
pattern = "Noise_NK_25519_ChaChaPoly_BLAKE2s"
local_private_key = "base64_key"    # Optional
remote_public_key = "base64_key"    # Optional

[client.transport.websocket]
tls = true                          # Use TLS settings if true

[client.services.myservice]
type = "tcp"                        # "tcp" or "udp". Default: "tcp"
token = "secret"                    # Required if no default_token
local_addr = "127.0.0.1:8080"      # Required. Local service address
nodelay = true                      # Optional
retry_interval = 1                  # Optional

[server]
bind_addr = "0.0.0.0:2333"         # Required. Listen address
default_token = "token"             # Optional
heartbeat_interval = 30             # Optional. 0 to disable. Default: 30s

[server.transport]                  # Same options as client.transport
type = "tcp"

[server.transport.tls]
pkcs12 = "identity.pfx"            # Required for tls
pkcs12_password = "password"        # Required for tls

[server.services.myservice]
type = "tcp"                        # "tcp" or "udp". Default: "tcp"
token = "secret"                    # Required if no default_token
bind_addr = "0.0.0.0:8080"         # Required. Exposed address
nodelay = true                      # Optional

[api]                               # Optional. Runtime REST API
bind_addr = "127.0.0.1:9090"       # Required. API listen address
token = "api-secret"                # Optional. Bearer token for auth
```

## Logging

Control log level via `RUST_LOG` environment variable:

```bash
RUST_LOG=debug ./rathole config.toml
```

Levels: `error`, `warn`, `info` (default), `debug`, `trace`

## Tuning

TCP_NODELAY is enabled by default for lower latency (good for RDP, game servers, etc.). Set `nodelay = false` if bandwidth matters more than latency.

## Benchmark

rathole has similar latency to frp but handles more connections with higher bandwidth and less memory.

See [Benchmark details](./docs/benchmark.md).

![http_throughput](./docs/img/http_throughput.svg)
![tcp_bitrate](./docs/img/tcp_bitrate.svg)
![udp_bitrate](./docs/img/udp_bitrate.svg)
![mem](./docs/img/mem-graph.png)

## Build Features

| Feature | Default | Description |
|---------|---------|-------------|
| `server` | Yes | Server mode |
| `client` | Yes | Client mode |
| `native-tls` | Yes | TLS via system library |
| `rustls` | No | TLS via rustls |
| `noise` | Yes | Noise protocol encryption |
| `websocket-native-tls` | Yes | WebSocket transport |
| `hot-reload` | Yes | Config file watching |
| `api` | Yes | Runtime REST API |

Build with minimal features for embedded:

```bash
cargo build --release --no-default-features --features server,client,noise,hot-reload
```

## Systemd

See [systemd examples](./examples/systemd) for running rathole as a background service.

[Out of Scope](./docs/out-of-scope.md) lists features not planned and why.
