# OpenCode bus proxy

A standalone Rust gateway that moves OpenCode event selection and fan-out out of
the Session execution server. Tokio async I/O, an indexed single-owner router,
shared frame bytes, bounded subscriber queues, and structured `tracing`.

- **Unchanged OpenCode V2 clients:** `/api/event` broadcasts public events.
- **Bus-smart clients:** existing controlled GET/interest PUT selects streaming
  events by Session, while other public events remain broad.
- **One ordinary origin stream:** the proxy consumes `GET /api/event`, where
  ordinary events gain authoritative `scope`; it never guesses Session identity
  from arbitrary payload fields.
- **Same server URL:** other HTTP requests stream to origin; WebSocket upgrades
  are tunneled for PTYs. Direct bus-smart connections keep working.

Requires the additive scoped-event extension implemented in the sibling
[`opencode-bus-smart`](../opencode-bus-smart/packages/protocol/src/groups/event.ts)
checkout. Origin must advertise `OpenCode-Event-Scope: 1` on its ordinary stream.
Existing clients can ignore the added field. The daemon does not embed OpenCode
or open its database.

## Run

```sh
cargo build --release --locked
target/release/opencode-bus-proxy \
  --upstream http://127.0.0.1:4096 \
  --listen 127.0.0.1:4097 \
  --password-file /path/to/shared-server-password
```

Use the same password as the origin, either from the file or
`OPENCODE_SERVER_PASSWORD`. Configure clients with the proxy URL and their usual
credentials. Enable the existing `experimental.session_streaming` client setting
to use selective delivery. Without that setting, bus-smart clients also broadcast.

```sh
RUST_LOG=info,bus_proxy_gateway=debug target/release/opencode-bus-proxy \
  --upstream http://127.0.0.1:4096 --log-json
```

The second example reads `OPENCODE_SERVER_PASSWORD`. `--allow-unauthenticated`
explicitly supports an unauthenticated test origin. `--help` lists limits and
timeouts. A pinned stable toolchain is included; an existing `RUSTUP_TOOLCHAIN`
environment override takes precedence (use `cargo +1.98.0` to select it explicitly).

## Inspect

Authenticated endpoints:

| Path | What it answers |
| --- | --- |
| `/_bus/health` | Is the source ready? Returns 503 while disconnected. |
| `/_bus/subscriptions` | Which live subscriptions are legacy/pending/active, which Sessions they follow, and their queue pressure. |
| `/_bus/metrics` | Prometheus counters and gauges for source traffic and routing admissions. |

Source failure closes downstream streams and invalidates subscription IDs.
Clients reconnect and refresh through their existing connection logic. Streams
are live-only; there is no replay. A slow subscriber is disconnected rather than
silently losing frames or holding up other clients.

## Layout and verification

| Crate | Owns |
| --- | --- |
| `crates/protocol` | Ordinary scoped events, shared selection contract, control and SSE decoding |
| `crates/router` | Session index, atomic interest replacement, bounded queues, epochs |
| `crates/gateway` | Authentication, source lifecycle, HTTP and WebSocket transport |
| `crates/daemon` | CLI, tracing, process supervision and shutdown |

```sh
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all -- --check
cargo build --locked
bun run test/compatibility/clients.ts
```

The final check requires the sibling bus-smart checkout and installed Bun
dependencies. It checks the shared contract fixtures match, creates an isolated
in-memory origin, and exercises its real Bus, ordinary HTTP stream, and unchanged
client/controller code against the compiled proxy.

See [operations](doc/event-multiplexer/operations.md),
[verification and measured results](doc/event-multiplexer/verification.md),
[architecture](design/event-multiplexer/architecture.md), and the
[additive-event contract](design/event-multiplexer/additive-events.md).
