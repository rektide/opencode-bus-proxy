---
type: OperationsGuide
title: Operating the event gateway
description: Gateway operation with native discovery contribution and opt-in independent OTLP/native ClickHouse completed-span destinations.
resource: /doc/event-multiplexer/operations.md
status: maintained
revised_by: model:openai/gpt-6.1-sol#xhigh
date: 2026-10-11
sources:
  - resource: opencode://ses_ed2d6efafffeJebEhJ6FDRWIeF
    title: Native telemetry build/configuration/table setup, coexistence and bounded lifecycle additions; existing gateway guidance retained
  - resource: /telemetry/arrow/README.md
    title: Versioned collector-compatible table contract and projection losses
  - resource: /telemetry/clickhouse-native/README.md
    title: Verified native protocol/acknowledgment and runtime/TLS constraints
  - resource: /design/event-multiplexer/clickhouse-schema0.glm53fx.md
    title: Independent ClickStack/HyperDX infrastructure/schema research
  - resource: opencode://ses_ed04fa2e4ffe2pYU135EWTc730
    title: Native local discovery sections merged from the native-discovery branch
  - resource: urn:opencode:session:ses_ed3ebd410ffehOKUhZcNU2hHYk
    title: Native discovery, enrichment, stomp and watcher direction
  - resource: opencode://ses_ec367db07ffeXxqPpfAZWBTUkc
    title: Merge of native-discovery-20261012 into main; topology ownership adjusted for deferred service-manager adoption
---

# Operating the event gateway

## Topology

Implementation ownership is described in the
[crate map](../../README.md#layout-and-verification) and
[gateway architecture](../../design/event-multiplexer/architecture.md#crate-boundaries).
Native contribution/restoration and the Effect watcher live in
`integrations/discovery`. `integrations/service-manager` is a standalone
listener/notification policy extraction whose adoption by the daemon is pending;
the daemon still owns its activation, fdstore, watchdog and handover policy
inline, including named-descriptor activation.

Run one proxy against one bus-smart origin whose ordinary `GET /api/event`
advertises `OpenCode-Event-Scope: 1`. The upstream URL is an HTTP(S)
origin, without credentials, query, or a route prefix. Clients use the proxy as
their complete server URL; the gateway owns event GETs and interest PUTs and
forwards other routes. Ordinary HTTP traffic can continue while the event source
reconnects.

The origin sends ordinary OpenCode events plus authoritative `scope`; the proxy
forwards those same events. A normal `server.connected` ID identifies the upstream
epoch. The earlier private source route and `event-source.*` envelopes have been
removed. An older origin lacking the capability is rejected explicitly; ordinary
client compatibility does not imply that every old origin supplies routing facts.

Client interest is exact Session identity. The five native fragment types are
selective; lifecycle events, RPC, permissions, and forms are broadcast. Sessions
moving directory do not change their interest identity. Legacy clients supply no
interest declarations and retain broadcast semantics. The existing client setting
`experimental.session_streaming: true` enables bus-smart's controlled adapter.

## Credentials and browser compatibility

Supply the origin's shared password through `OPENCODE_SERVER_PASSWORD`,
`--password-file` (`BUS_PROXY_PASSWORD_FILE`), or `--credential`
(`BUS_PROXY_CREDENTIAL`); `--password-file` and `--credential` together is
rejected. A file or credential takes precedence over the environment variable,
and trailing CR/LF is removed; other whitespace is significant.
The default username is `opencode`, configurable through
`--username`/`OPENCODE_SERVER_USERNAME`.

`--credential` names a systemd credential: the daemon reads
`$CREDENTIALS_DIRECTORY/<name>`, which any of `LoadCredential=`,
`LoadCredentialEncrypted=`, `SetCredential=`, or `ImportCredential=` provides
(systemd ≥247; ≥250 for encrypted). Encrypted credentials are decrypted before
the daemon sees them, so the same flag covers both. Credential names must be
filename-safe (leading letter or digit, then letters, digits, `.`, `-`, `_`) —
validated before the name is joined to the directory, so a misconfigured name
cannot traverse elsewhere. A system unit can hand over the password without
plaintext argv or environment:

```ini
[Service]
ExecStart=/usr/local/bin/opencode-bus-proxy \
    --upstream http://127.0.0.1:4096 \
    --listen 127.0.0.1:4097 \
    --credential opencode-password
LoadCredential=opencode-password:/etc/opencode/server-password
# Alternatively, encrypted at rest (binds the name; decrypt at start):
# LoadCredentialEncrypted=opencode-password:/etc/opencode/server-password.cred
```

Create the encrypted form with
`systemd-creds encrypt --name=opencode-password /dev/stdin /etc/opencode/server-password.cred`.
`--password-file %d/opencode-password` in `ExecStart=` also works without the
flag, but `--credential` fails with a named cause instead of a generic missing
file when the unit forgets the `LoadCredential=` line.

Local feed and diagnostic routes accept the native Basic/query credentials and
password-signed pairing sessions. Ordinary forwarded requests retain the caller's
credentials. Only the proxy's own source connection gets its configured service
credential. Host and Origin remain the client's external values for forwarding;
this preserves port-scoped pairing cookie names and native same-origin checks.
Pairing routes and ticketed PTY connects remain origin-owned. Additional allowed
browser origins can be supplied with repeatable `--cors` flags.

`--allow-unauthenticated` is available for explicitly unauthenticated origins.
This is one shared OpenCode authorization domain; Session interest is a delivery
preference, not a permission boundary.

## Native local discovery

`--service-file` / `BUS_PROXY_SERVICE_FILE` opts into contribution to **one
existing operator-owned OpenCode registration**, e.g. the local channel's
`$XDG_STATE_HOME/opencode/service-local.json` (normally
`~/.local/state/opencode/service-local.json`). No path/channel is guessed, and
an absent record is never invented. This requires the same user/filesystem
authority as the publisher; the DynamicUser/read-only preset is not suited to
editing another user's private discovery slot without a deliberate policy.

### Modes and contact precedence

- **cooperate** (default `--service-mode` / `BUS_PROXY_SERVICE_MODE`): supplement
  endpoint descriptions; leave the publisher's selected `url` unchanged. New
  server-reg source chooses its contact as explicit `serve --publish-url` >
  nonempty native `OPENCODE_PROXY_URL` > actual listener. Setting that env on
  the publisher routes ordinary clients through this daemon without a wrapper.
- **stomp** (explicit): also replace the eligible origin's selected contact
  with this gateway's URL and reapply it whenever OpenCode publishes again.
  This deliberately beats both native explicit and proxy-default contact
  choices while active; it does **not** overwrite identity or credentials.
  It supports an older publisher that knows only its bind URL. Only the proxy
  stomps; there is no origin watcher continually fighting it.

`--service-url` / `BUS_PROXY_SERVICE_URL` overrides the advertised local HTTP
origin. Otherwise use the actual bound/adopted socket and port, with wildcard
IPv4/IPv6 mapped to loopback. Upstream stays the independently configured
BUS_PROXY_UPSTREAM; never read forwarding targets from the contributed slot.
Native clients authenticate forwarded GET /api/info and require its PID and
version to match the record: therefore top-level PID/version/id/password
remain the origin's. The daemon's own PID is descriptive contribution ownership,
not a pretend OpenCode server identity.

### Default-on markup and eligibility

`--service-metadata true|false` / `BUS_PROXY_SERVICE_METADATA` defaults true
whenever a service file is selected; it is independent of routing mode. Native
publication has the analogous default-on `OPENCODE_SERVICE_METADATA` switch.
Metadata adds no password copy:

```json
{
  "mode": "server",
  "url": "http://127.0.0.1:49372",
  "pid": 123,
  "discovery": {
    "version": 1,
    "source": "proxy",
    "endpoints": {
      "service": { "url": "http://0.0.0.0:49373", "pid": 123 },
      "proxy": { "url": "http://127.0.0.1:49372", "pid": 456 }
    }
  }
}
```

The usual version/id/single password and the private `_busProxy` ownership
marker are omitted from this example. `discovery.source` describes the native
publisher's preference (or `daemon` for added legacy descriptions), not a
second contact election. Endpoint names are extensible: unknown compatible
fields and named endpoints survive contribution. If the publisher describes
a different proxy, retain it and describe this daemon as `busProxy` instead.
Stomp still sets the selected top-level URL to this daemon. Cooperative mode
plus disabled metadata performs no write.

Eligibility requires mode server, the configured local HTTP origin (direct
URL or version-1 `discovery.endpoints.service`), matching credentials, and a
forwarded /api/info PID/version match **through the serving gateway**, followed
by successful /_bus/health. Managed records, unrelated ports/origins, corrupt
records, unsupported markup, and live foreign contributors are not claimed.
Origin restarts publish fresh identity; contribution always uses that new
PID/version/id, not a cached startup record. A changed password is not guessed:
coordinate the daemon's shared credential/start with the origin.

### Reuse the existing Effect watcher

Rust/Tokio cannot directly execute the Effect-TS watcher. The optional bridge
is a Bun child **supervised by the daemon**, emitting only ready/change/error
markers; it never reads or writes record contents. Registration ownership and
the real proxy PID stay in Rust. Explicitly supply:

```sh
opencode-bus-proxy --upstream http://127.0.0.1:49373 \
  --listen 127.0.0.1:49372 --password-file /path/to/shared-password \
  --service-file /absolute/state/opencode/service-local.json \
  --service-mode stomp \
  --service-watch-helper /path/to/opencode-bus-proxy/script/service-watch.ts \
  --service-watch-source /path/to/opencode-working
```

Environment counterparts are BUS_PROXY_SERVICE_WATCH_HELPER and
BUS_PROXY_SERVICE_WATCH_SOURCE. The source root must carry effect-watch's
`packages/util/src/filesystem-watch-client.ts` and its installed dependencies.
The helper calls FileSystemWatchClient.create(FileSystemWatch.fromEnvironment),
watches the **parent directory**, then filters/coalesces the basename. This
survives temp+rename publication and missing-file recreation without replacing
the established watcher or vendoring another backend. Native/Watchman backend
and watcher-disable environment controls remain the existing owner's. No
OpenCode source composition/build occurs merely by using this helper.

Tradeoff: enabling reuse requires Bun and the selected module checkout. With
no helper, or while it fails/has been disabled, the daemon polls once per
second. Failed helpers retry with capped backoff and a fixed warning, without
logging module/record contents. With an active watcher and an owned record,
events drive reconciliation plus a 30-second safety reread; while waiting for
health or a publication, one-second retries continue. Byte-identical own
writes are quiescent, even if they trigger another watch event. SIGTERM closes
the helper's Effect scopes, with bounded forced cleanup if necessary.

### Atomicity, shutdown and the single-slot limitation

One adjacent 0600 lock serializes cooperating proxy daemons. Contributions
use a 0600 same-directory temporary file, sync, final exact-byte reread and
atomic rename; symlinks and nonregular/oversized records are refused. The
marker stores contributing PID/token and public restoration details, never a
credential backup. On graceful shutdown restore only the exact last owned
record to its predecessor contact/markup. This retains a native publisher's
explicit proxy choice, or restores an older publisher's direct URL. A changed
or removed slot is never overwritten on shutdown. A dead contributor can be
reclaimed after the same origin/health checks; a live one is left alone.

**Check+rename is not compare-and-swap.** Native OpenCode does not take the
proxy's lock. A native or foreign rename in the narrow final-check/rename
interval can lose an update. Normal republishing is watched/reapplied; no
cross-process transactional guarantee is claimed. Removing this residual race
would need a common publisher protocol or a separate discovery contract.

Compfuzor exposes this as opt-in OPENCODE_BUS_PROXY_DISCOVERY, with default
cooperative mode and default-on enrichment/watcher reuse. Select stomp
explicitly for older publication or forced contact policy. Generation, build
installation, activation and client restart are separate operator actions.

Related homes: [native publisher adaptation](../../../opencode-server-reg/doc/server-reg/publication.md)
owns OpenCode's selection semantics; [discovery verification](discovery-verification.md)
records tested properties and activation limits. The
[additive-event verification](additive-verification.md) remains evidence for
source delivery; discovery does not replace that transport contract.

## systemd service integration

The daemon runs happily without systemd and picks these up only when the
service manager provides them:

- **Socket activation** — with `LISTEN_PID`/`LISTEN_FDS` set for this process,
  the gateway adopts the single `ListenStream=` descriptor systemd duplicated
  into it and does not bind. An explicit `--listen`/`BUS_PROXY_LISTEN`
  alongside activation must match its actual bound address. Without activation,
  `--listen` defaults to `127.0.0.1:4097`.
- **Readiness notification** — `Type=notify` units receive `READY=1` (with the
  listening address as `STATUS=`) once the gateway accepts connections and
  `STOPPING=1` when shutdown begins. Readiness means serving; upstream source
  connection state remains `/_bus/health`'s to report, including its 503 while
  disconnected.
- **Watchdog** — `WatchdogSec=` schedules `WATCHDOG=1` heartbeats at half the
  configured timeout. The watchdog reflects process liveness, not source
  health; the source supervisor's reconnect backoff can legitimately outlast a
  tight watchdog budget, so size `WatchdogSec=` for that.
- **Descriptor store** — a self-bound listener is offered to the manager via
  `FDSTORE=1`, so restarts keep the listening socket even without a socket
  unit; see the subsection below for the composition rules.

```ini
# opencode-bus-proxy.socket
[Socket]
ListenStream=127.0.0.1:4097

[Install]
WantedBy=sockets.target
```

```ini
# opencode-bus-proxy.service
[Service]
Type=notify
WatchdogSec=30
ExecStart=/usr/local/bin/opencode-bus-proxy \
    --upstream http://127.0.0.1:4096 \
    --credential opencode-password
LoadCredential=opencode-password:/etc/opencode/server-password

[Install]
WantedBy=multi-user.target
```

A socket unit lets the gateway restart without refusing new connections:
systemd holds the listening socket while the service is down, and queued
connections survive the restart. Existing streams are live-only and end with
the process that served them. `systemctl enable --now
opencode-bus-proxy.socket` starts the service on the first connection.

### Hardened unit preset

The inline units above favor readability. For production, the repository
ships a hardened preset — [`systemd/opencode-bus-proxy.socket`](../../systemd/opencode-bus-proxy.socket)
and [`systemd/opencode-bus-proxy.service`](../../systemd/opencode-bus-proxy.service) —
validated with `systemd-analyze verify`: the service runs as a transient
`DynamicUser=` with `NoNewPrivileges=`, a read-only filesystem
(`ProtectSystem=strict`), kernel/interface hardening
(`ProtectKernelTunables=`, `ProtectKernelModules=`, `ProtectKernelLogs=`,
`ProtectHostname=`, `ProtectClock=`, `ProtectControlGroups=`, `ProtectProc=invisible`),
`RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6`, an empty capability
bounding set, and `SystemCallFilter=@system-service` with
`SystemCallArchitectures=native`. `MemoryDenyWriteExecute=yes` is safe here:
nothing in the dependency tree generates native code at runtime (tokio,
hyper, regex, and ring are interpreted or build-time compiled). Install both
units into `/etc/systemd/system/`; the service reads the origin password
through `LoadCredential=` and never sees it in argv or the environment.

The preset also sets `FileDescriptorStoreMax=1`, which pairs with the
gateway's fdstore handoff — see the next section.

### Descriptor store: restart continuity without a socket unit

systemd's fdstore gives a `Type=notify` service the socket unit's restart
continuity without a socket unit. When the gateway binds its listener itself
(no `LISTEN_FDS` handover), it sends `FDSTORE=1` with `FDNAME=listener` to
`NOTIFY_SOCKET` with the descriptor attached; the service manager holds the
socket open across `Restart=` and hands it back through the ordinary
socket-activation handover on the next start. The kernel keeps queueing
connects on the stored socket, so a restarting service — including a crash
under `Restart=on-failure` — never presents a connection-refused window.
fdstore requires `FileDescriptorStoreMax=` to be non-zero: it defaults to
zero, which is why the preset sets `FileDescriptorStoreMax=1`. Stopping (not
restarting) the service discards the store, and the next cold start binds
normally.

Composition rules:

- Only a listener the daemon bound itself is stored. A `LISTEN_FDS`
  handover — from a socket unit or a replayed fdstore — is not re-stored:
  the manager already retains that socket, and duplicate store entries
  would accumulate across restarts.
- A replayed store arrives as named socket activation. An explicit
  `--listen`/`BUS_PROXY_LISTEN` validates the inherited address, so nondefault
  self-bound ports survive restarts without removing their configuration.
- With the socket unit enabled, the manager holds the listening socket
  itself and the daemon never takes the self-bound branch;
  `FileDescriptorStoreMax=1` is then simply unused.

The handoff is best-effort: without a service manager it is a no-op, and a
failed store only means restarts rebind with a brief refused window — logged
at debug, never fatal.

Normal restarts preserve only the listener. Opt-in
[cooperative SSE handover](handover.md) uses SIGUSR2, named client/source sockets,
and a sealed private memfd image to preserve established supported SSE responses
and their plain-HTTP source. It offers exact buffered transfer and bounded
boundary drain/drop, with explicit unsupported-connection failure. Its unit
capacity, bootstrap, signal, ownership, and stale-image rules are different from
ordinary restart. An origin restart still requires client refresh: live-only
missed events cannot be recovered by retaining a downstream socket.

## OTLP trace export

Packaging is opt-in as well as runtime opt-in. Default workspace builds select
the four core packages and the daemon's empty default feature set, without OTel
or reqwest export dependencies. Build a collector-capable binary with
`cargo build --release --locked -p opencode-bus-proxy --features otlp`.
The additive `otel` feature selects composition only; `otlp` selects its backend.
Without `otlp`, explicit exporter selection or endpoint inference fails startup
with an actionable rebuild instruction. `OTEL_SDK_DISABLED=true` and
`OTEL_TRACES_EXPORTER=none` still take precedence. Unknown exporter names fail;
`clickhouse-native` is a separate additive backend feature. Select either sink or
`OTEL_TRACES_EXPORTER=otlp,clickhouse-native`; they use separate bounded processors
with a shared resource/sampler/filter. `none` mixed with other sinks and duplicate
destinations are rejected. Native endpoints alone never infer native activation.

Reusable provider/layer policy lives at `telemetry/core/` (`bus-proxy-telemetry`),
not in gateway/router. Environment and credential resolution, TLS global policy,
subscriber installation, supervision and shutdown remain daemon-owned. Blocking
client/provider initialization and final drain run outside application workers.
The development toolchain is pinned to 1.98.0; no lower MSRV is advertised.

The daemon can batch-export its existing `tracing` spans and their events over
OTLP/HTTP. Export is opt-in: set a collector endpoint or `OTEL_TRACES_EXPORTER=otlp`.
For example, in the service's environment:

```ini
Environment=OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
Environment=OTEL_SERVICE_NAME=opencode-bus-proxy
Environment=OTEL_RESOURCE_ATTRIBUTES=deployment.environment.name=local
Environment=BUS_PROXY_OTEL_FILTER=info,bus_proxy_gateway=debug,bus_proxy_router=debug
```

| Environment | Meaning / default |
| --- | --- |
| `OTEL_TRACES_EXPORTER` | `otlp`, `clickhouse-native`, or their comma-separated pair. `none` alone disables even with endpoints set. Unset infers **only OTLP** from an OTLP endpoint. Unknown/duplicate/mixed-none values fail. |
| `OTEL_SDK_DISABLED` | `true` (case-insensitive) disables export regardless of the other settings. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Collector base URL; the SDK appends `/v1/traces`. HTTP and HTTPS are supported. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Full traces URL, used verbatim; overrides the generic endpoint. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` / `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` | `http/protobuf` (default) or `http/json`; trace-specific wins. gRPC is rejected. |
| `OTEL_EXPORTER_OTLP_HEADERS` / `OTEL_EXPORTER_OTLP_TRACES_HEADERS` | Comma-separated `key=value` headers, e.g. `authorization=Bearer%20token`. Trace-specific replaces generic headers. |
| `OTEL_EXPORTER_OTLP_TIMEOUT` / `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT` | Export HTTP timeout in milliseconds (SDK default 10000); trace-specific wins. |
| `OTEL_SERVICE_NAME` | Service name, default `opencode-bus-proxy`; takes precedence over `service.name` in resource attributes. |
| `OTEL_RESOURCE_ATTRIBUTES` | Comma-separated resource `key=value` attributes. `service.version` is supplied from the daemon package version. |
| `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG` | SDK sampling, default `parentbased_always_on`. Supported samplers: `always_on`, `always_off`, `traceidratio`, `parentbased_always_on`, `parentbased_always_off`, `parentbased_traceidratio`. Ratio argument is 0–1. |
| `OTEL_BSP_SCHEDULE_DELAY` | Batch interval in milliseconds (5000). |
| `OTEL_BSP_MAX_QUEUE_SIZE` | Bounded completed-span queue (2048). |
| `OTEL_BSP_MAX_EXPORT_BATCH_SIZE` | Maximum spans per batch (512), capped by queue size. |
| `BUS_PROXY_OTEL_FILTER` | Export filter using `RUST_LOG` directive syntax (default `info`), independent of local logging. Only proxy-owned tracing targets are exported, preventing exporter feedback loops. |
| `BUS_PROXY_OTEL_SHUTDOWN_TIMEOUT_MS` | Total graceful SDK drain budget (5000; 1..=300000), divided among selected processors. Native cancellation/join adds its unavoidable upstream connect/cleanup allowance; see below. |

`RUST_LOG` and `--log-json` still control local output. Sampling and export
filtering do not suppress local logs. Standalone log events outside spans are
not OTLP logs; metrics remain available at `/_bus/metrics`. There is no incoming
HTTP trace-context extraction or outgoing context injection in this integration.

Spans export when closed: long-lived subscriptions and the router can remain
open until disconnect/shutdown. There are no per-frame spans. Export runs on a
bounded background worker; collector failures do not interrupt gateway traffic.
Shutdown drains completed spans after gateway/router shutdown, with the host's
bounded graceful wait (5 seconds by default). Export is best-effort; queue overflow,
collector failure, or a forced kill can lose telemetry.

HTTPS uses platform certificate verification. Custom OTLP certificate/mTLS env
settings and compression are not configured by this integration. SDK numeric
and sampling settings follow SDK fallback behavior on invalid values; malformed
export filters and unsupported exporter/protocol selections fail startup.

## Native ClickHouse trace export

Build with `cargo build --release --locked -p opencode-bus-proxy --features
clickhouse-native`, or `--features otlp,clickhouse-native` to make both available.
Local-only and OTLP-only builds compile no Arrow/native dependencies. Runtime
selection is explicit; a requested but absent backend fails before listening with
its exact rebuild feature. Disabled/`none` selections do not read native config.

The external **clickhouse-arrow 0.2.1** client takes Arrow 57 RecordBatches but
sends **native TCP/TLS blocks**, not HTTP ArrowStream. Native configuration has a
separate namespace: do not reuse an HTTP endpoint or the preserved draft's config.
Export is completed spans + attached events/links, not standalone logs/live state.

### Explicit table setup and example

Use an existing verified collector/ClickStack trace table, or edit the database /
table in [`telemetry/arrow/schema/traces-v1.sql`](../../telemetry/arrow/schema/traces-v1.sql)
and execute it yourself, for example:

```sh
clickhouse client --host 127.0.0.1 --port 9000 --multiquery < telemetry/arrow/schema/traces-v1.sql
```

This **example** creates `default.otel_traces`; it is not a deployment decision.
The DDL has no implicit TTL, migrations or trace-id lookup materialized view.
Choose table ownership, retention and indexes deliberately. Native insert names
its 22 columns explicitly, so HyperDX MATERIALIZED extras are not overwritten.
The [schema contract](../../telemetry/arrow/README.md) describes standard string
attributes, parallel events/links, nanoseconds, limits and deliberate losses.

Example for a **trusted local plaintext** server (TLS is the default otherwise):

```ini
Environment=OTEL_TRACES_EXPORTER=clickhouse-native
Environment=BUS_PROXY_CLICKHOUSE_NATIVE_HOST=127.0.0.1
Environment=BUS_PROXY_CLICKHOUSE_NATIVE_PORT=9000
Environment=BUS_PROXY_CLICKHOUSE_NATIVE_TLS=false
Environment=BUS_PROXY_CLICKHOUSE_NATIVE_DATABASE=default
Environment=BUS_PROXY_CLICKHOUSE_NATIVE_TABLE=otel_traces
Environment=BUS_PROXY_CLICKHOUSE_NATIVE_USERNAME=default
Environment=BUS_PROXY_CLICKHOUSE_NATIVE_CREDENTIAL=clickhouse-password
LoadCredential=clickhouse-password:/etc/opencode/clickhouse-password
Environment=OTEL_SERVICE_NAME=opencode-bus-proxy
```

Library hosts supply typed config/resource/policy themselves. The daemon reads
environment/files/systemd credentials and selects the global rustls ring provider
before any TLS construction. It does not require a reachable database for readiness.

All following names have prefix **`BUS_PROXY_CLICKHOUSE_NATIVE_`**:

| Suffix | Meaning / default |
| --- | --- |
| `HOST` | **Required** native hostname or IP, not a URL/credentials. Numeric IP avoids residual OS DNS work. |
| `PORT` | 9440 with TLS, 9000 without; nonzero UInt16. |
| `TLS` | Strict `true`/`false` (case-insensitive); **true**. |
| `TLS_SERVER_NAME` | Optional TLS identity override, useful with a numeric host; requires TLS. |
| `DATABASE` / `TABLE` | Database `default`; **table required**. ASCII identifiers, no leading digit, at most 128 bytes; no arbitrary SQL. |
| `USERNAME` / `PASSWORD` | User `default`; empty password if none supplied. Secrets are not placed in endpoints or error strings. |
| `PASSWORD_FILE` / `CREDENTIAL` | File or filename-safe systemd credential (mutually exclusive); overrides password env. Trailing CR/LF removed, other whitespace retained. |
| `TIMEOUT_MS` | Positive DNS/handshake/insert/full-ack deadline, 10000; max 300000. Fixed upstream blocking-connect allowance is additional. |
| `MAX_BATCH_ROWS` / `MAX_BATCH_BYTES` | 4096 / 16777216. At most one active and one queued bounded raw batch; SDK trace queue is independently bounded by OTEL_BSP settings. |
| `MAX_STRING_BYTES` / `MAX_ATTRIBUTES` | 16384 / 1024, per encoded value/string and attribute map. |
| `MAX_EVENTS` / `MAX_LINKS` / `MAX_ARRAY_ITEMS` | 128 / 128 per span; 1024 per attribute array. |
| `REDACT_KEYS` | Additional nonempty comma-separated exact case-insensitive attribute keys. Credential-related defaults apply already; not a general PII/content filter. |

Native numeric/boolean/identifier policy is strict; malformed selected config
fails startup. Oversize projected batches fail whole rather than silently truncate.
Shared BSP settings still follow SDK fallback behavior; configure SDK span/event
limits consistently with native projection limits if increasing them.

The daemon rejects upstream `CLICKHOUSE_NATIVE_DEBUG_ARROW`,
`CONNECTION_READ_BUFFER_SIZE`, and `CONNECTION_WRITE_BUFFER_SIZE` when native is
selected: they bypass typed policy, and debug Arrow can print batch data. Libraries
never unset environment silently; embedded hosts must own these upstream ambient
exceptions. Disabled/`none` export ignores this native-only validation too.

### Acknowledgment and shutdown constraints

Native exports run on a private runtime thread, never gateway/router workers.
Success requires the **entire native response and a same-connection ping** (the
audited upstream can otherwise mistake a fatal-I/O channel close for completion).
No automatic retries: timeout/disconnection can follow a committed insert.
Failures are sanitized, best-effort batch drops and do not stop gateway traffic.
Native and OTLP never silently fall back to one another. Selecting both exports
each sampled span twice to their independent destinations; if a collector also
writes to the same ClickHouse table, that creates duplicate rows.

Upstream TCP connect is non-preemptible with a fixed **30-second** timeout. Add
that allowance to operation/cancellation bounds; private-worker isolation protects
gateway scheduling but cannot erase an upstream blocking call. After the graceful
SDK drain budget, the host cancels/joins native work rather than detaching inserts:
shutdown can add **30 seconds plus one second of DNS cleanup**. OS `getaddrinfo`
may outlive that one-second allowance as a DNS-only task; it cannot resume an
insert. Use numeric host + TLS name if residual DNS work is unacceptable.

Native TLS supports WebPKI roots only, no custom CA/mTLS configuration. Both
aws-lc and ring build because of upstream mandatory features; host preselection
prevents silent TLS policy changes. Wire compression is off: default upstream LZ4
failed on installed 26.9 insert headers. Verified native/table paths used
ClickHouse **25.6.13.41 and 26.9.7.9**; latest floating ClickStack images and a
live HyperDX UI were not tested.

Example query (replace the configured database/table as appropriate):

```sql
SELECT Timestamp, ServiceName, SpanName, Duration / 1e6 AS DurationMs, StatusCode
FROM default.otel_traces ORDER BY Timestamp DESC LIMIT 20;
```

## Limits and failure behavior

Use `--help` for current defaults. The configurable limits are:

- `--queue-frames` and `--queue-bytes`: per-subscriber backlog. The frame target
  is also adjustable through authenticated `PUT /_bus/queue`, including zero's
  one ready-receiver credit; shrink preserves old admissions while they drain.
  See [queue semantics](handover.md#live-queue-dial-including-zero). Logical bytes are
  charged independently for every recipient, although physical frame storage is
  shared. These bounds exclude bytes already yielded to HTTP/kernel buffers.
- `--max-subscribers`: includes inactive subscriptions waiting for their first PUT.
- `--command-capacity`: bounded admission/control mailbox.
- `--max-source-frame-bytes`: incremental ordinary SSE frame limit; decoder counts
  non-newline frame bytes including comments/field prefixes. Origin independently
  limits its complete encoded frame to 16 MiB.
- `--source-idle-seconds`: counts any source bytes, including 15-second heartbeat
  comments. A stalled source cannot hide behind proxy heartbeats indefinitely.
- `--request-timeout-seconds`: upstream connection/source/upgrade setup deadline.
  Ordinary API execution, slow uploads, and response bodies can legitimately take
  longer and have no generic short response-header deadline.
- `--max-connections` and `--header-read-seconds`: bound accepted HTTP connections
  (including upgraded tunnels) and expire incomplete inbound request headers.

The sum of subscriber byte allowances is an upper bound on logical queue budget,
not an exact RSS prediction. Choose subscriber count and byte budgets together.
Interest bodies are capped at 256 KiB, 1024 Locations, and 4096 Session entries
before deduplication, matching native controlled delivery.

Queue overflow invalidates that subscriber, cancels its receiver, and removes all
memberships. Source EOF, bad framing/scope, missing or unsupported capability, and
idle failure invalidate the whole source epoch and all attached subscription IDs.
Queued old-epoch events are discarded. Bytes already handed to the network cannot
be recalled. One reconnect loop establishes a new source epoch with capped backoff.
Retired SSE responses fail at the HTTP body boundary, closing their connections
without a successful chunked-response terminator. This exposes the outage to
error-driven client retry loops rather than silently completing their iterators.
Fresh subscriptions receive 503 until the source is ready; clients must retry
those responses too, re-register interests, and refresh their snapshots. The
proxy stays running through an origin outage.
There is no hidden failover, persistent replay, or exactly-once delivery.

Interest replacement is atomic at **proxy admission**. Already queued downstream
frames keep their old recipient decision, and upstream backlog can arrive after a
PUT acknowledgment. Client snapshot/refresh ownership stays with bus-smart.

SIGINT/SIGTERM cancels the source, routing actor, HTTP connections, and upgrade
tunnels. The host grants tasks a bounded shutdown grace before aborting stragglers.

## Tracing and diagnostics

`RUST_LOG` controls `tracing` filtering; `--log-json` emits JSON. Default lifecycle
logs cover source epochs/retries, gateway startup/shutdown and subscription
failures. Source disconnects include a fixed `reason` category; rejected source HTTP
responses include their status code. Debug tracing adds request/control information without bodies, credentials,
or query strings. There is no per-recipient tracing span in the token hot path.

The protected `/_bus/subscriptions` snapshot is the answer to “who is listening
to what?” It exposes process-local subscription identity, mode, active state,
Session membership, and backlog. Metrics use bounded names, not Session-ID labels.
Admissions/bytes are queue acceptance measurements, not confirmed client receipt.

`/_bus/health` is source readiness, not origin process health. The origin's
`/api/info` remains a forwarded API request. A successful listener startup log does
not by itself mean the source negotiated successfully.

## Verification and performance interpretation

Run the workspace tests, Clippy and formatting commands in the root README.
`test/compatibility/clients.ts` uses the sibling bus-smart checkout's real routing
and generated clients, an in-memory database, isolated listeners, and temporary
configuration. It starts no managed service and executes no model calls. Set
`BUS_PROXY_BINARY` to test a different compiled binary. It also checks the native
and Rust copies of `event-scope-v1.json` are identical, and that ordinary clients
and updated native decoders handle scoped events through the same routes.

The router's release benchmark is in `core/router/examples/routing_bench.rs`:

```sh
cargo run --release --locked -p bus-proxy-router --example routing_bench -- 128 20000 512
```

It counts
received frames and reports selective, broad, and no-follower cases. It measures
in-process router throughput, not origin CPU savings or network end-to-end latency.
The real-client compatibility check demonstrates one source connection serving
multiple clients. Quantifying execution-server relief still requires comparing
the same workload with direct embedded and proxy topologies, measuring origin,
proxy, and client CPU separately.
