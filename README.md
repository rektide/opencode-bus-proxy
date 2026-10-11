# OpenCode bus proxy

A standalone Rust gateway that moves OpenCode event selection and fan-out out of
the Session execution server. Tokio async I/O, an indexed single-owner router,
shared frame bytes, bounded subscriber queues, and structured `tracing`.

The complete system's [canonical architecture](../opencode-bus-smart/doc/bus-smart/architecture.md)
and [development handoff](../opencode-bus-smart/doc/bus-smart/handoff.md) live in
bus-smart. This repository maintains the optional gateway implementation and its
operating instructions; bus-smart also works directly without the gateway.

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

Use the same password as the origin, from a file, `OPENCODE_SERVER_PASSWORD`,
or a systemd credential named with `--credential` (`BUS_PROXY_CREDENTIAL`).
Configure clients with the proxy URL and their usual
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

The daemon is systemd-aware: socket units hand over the listening socket via
socket activation (the socket unit's `ListenStream=` owns the address; an
explicit `--listen` must match the inherited address), and `Type=notify` units get
`READY=1` once serving, `STOPPING=1` at shutdown, and `WatchdogSec=` heartbeats.
Together with `--credential`, a unit file runs the gateway without secrets in
argv or the environment — a hardened production unit preset ships in
[`systemd/`](systemd/) (see
[operations](doc/event-multiplexer/operations.md)).

Opt-in [cooperative SSE handover](doc/event-multiplexer/handover.md) preserves
supported established responses **and their plain-HTTP source** across SIGUSR2
process replacement. Choose exact `buffered` transfer or bounded `boundary`
drain/drop, and adjust waiting-frame targets live, including zero.

## Inspect

Default builds are local-only: no OpenTelemetry SDK, OTLP or exporter HTTP client.
For optional OTLP trace export, build with
`cargo build --release --locked -p opencode-bus-proxy --features otlp`, then set
`OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318`.
`otel` enables reusable composition without an OTLP backend; it does not make
OTLP available. Requesting OTLP in a build without `otlp` fails with a rebuild
instruction, unless explicitly disabled. The library lives in the root
[`telemetry/core`](telemetry/README.md) domain subtree.
Collector headers, resource attributes, sampling, batching, and independent export
filtering are described in [OTLP configuration](doc/event-multiplexer/operations.md#otlp-trace-export).

Authenticated endpoints:

| Path                  | What it answers                                                                                           |
| --------------------- | --------------------------------------------------------------------------------------------------------- |
| `/_bus/health`        | Is the source ready? Returns 503 while disconnected.                                                      |
| `/_bus/subscriptions` | Which live subscriptions are legacy/pending/active, which Sessions they follow, and their queue pressure. |
| `/_bus/metrics`       | Prometheus counters and gauges for source traffic and routing admissions.                                 |
| `/_bus/handover`      | Transfer capabilities, connection/subscription ownership, and last preparation progress.                  |
| `/_bus/queue`        | GET selected waiting-frame target; authenticated PUT `{"frames":0}` (or another u32) changes it live.       |

Source failure aborts downstream SSE responses with a transport error and
invalidates subscription IDs; it does not report successful stream completion.
Clients reconnect and refresh through their existing connection logic. Streams
are live-only; there is no replay. A slow subscriber is disconnected rather than
silently losing frames or holding up other clients.

## Layout and verification

| Crate                 | Owns                                                                        |
| --------------------- | --------------------------------------------------------------------------- |
| `components/protocol` | Ordinary scoped events, shared selection contract, control and SSE decoding |
| `components/router`   | Session index, atomic interest replacement, bounded queues, epochs          |
| `components/gateway`  | Authentication, source lifecycle, HTTP and WebSocket transport              |
| `cli/daemon`          | CLI, tracing, process supervision and shutdown, systemd integration         |
| `telemetry/core`      | Caller-owned typed telemetry configuration, layers and provider composition |

Package and Rust API names are unchanged. Older design and verification records
may retain the former `crates/` coordinates; current source navigation uses the
paths above. The four core packages remain the default workspace members;
telemetry is selected explicitly by package or daemon feature.

```sh
cargo test --locked
cargo clippy --all-targets --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo fmt --all -- --check
cargo build --locked
bun run test/compatibility/clients.ts
```

The final check requires the sibling bus-smart checkout and installed Bun
dependencies. It checks the shared contract fixtures match, creates an isolated
in-memory origin, and exercises its real Bus, ordinary HTTP stream, and unchanged
client/controller code against the compiled proxy.

See [operations](doc/event-multiplexer/operations.md),
[current verification](doc/event-multiplexer/additive-verification.md),
[earlier measurements](doc/event-multiplexer/verification.md),
[architecture](design/event-multiplexer/architecture.md), and the
[additive-event contract](design/event-multiplexer/additive-events.md).
