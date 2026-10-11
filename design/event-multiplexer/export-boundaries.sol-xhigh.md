---
title: Export boundaries and consumer builds
status: proposal, not acceptance
date: 2026-10-11
sources:
  - resource: urn:opencode:session:ses_ed383667dffe1Sa5TLyVGrTi5g
    title: Human direction — architecture first, external clickhouse_arrow, retain both implementations
  - resource: urn:opencode:session:ses_ed3752ffeffe68wHsEDWxGNfzJ
    title: Sol xhigh architectural pass and versioned source inspection
---

# Export boundaries and consumer builds

**Proposal, not acceptance or implementation authorization.** No exporter is
built by this pass. Preserve the [custom HTTP draft](clickhouse-http-draft.rs.txt)
unchanged as an uncompiled, unfinished experiment; retaining it does not select
its schema, configuration, or runtime design.

## Recommendation

**Use crates for ownership/reuse boundaries and additive Cargo features for
consumer selection. We already are multi-crate.** Keep protocol → router →
gateway → daemon; extract optional telemetry composition, and give native
ClickHouse and custom HTTP Arrow delivery separate library crates when selected
for implementation. Neither exporter belongs in router/gateway or behind a
mutually exclusive switch in one implementation.

Keep one versioned tabular projection shared by the two transports, not one
shared transport implementation. This projection can also serve offline Arrow
and state inspection consumers without importing OpenTelemetry. Local logging
is always available; network telemetry is build-time **and** runtime opt-in.

The recommendation is an eight-package destination, not a request to create
four empty packages now. First make existing OTLP optional and reusable. Add the
three Arrow/ClickHouse packages as their actual contracts and implementations
are commissioned. No new package is named `clickhouse_arrow`: that is the Rust
import name of the external **`clickhouse-arrow`** dependency.

## Present ground

[`Cargo.toml`](../../Cargo.toml) and `cargo metadata --no-deps --locked` confirm
four workspace members, resolver 2, edition 2024, no declared features, and no
declared `rust-version`. [`rust-toolchain.toml`](../../rust-toolchain.toml) pins
1.98.0; that is a development toolchain, not an advertised minimum.

| Current package | Dependency boundary / consumer |
| --- | --- |
| `bus-proxy-protocol` | `bytes`, serde/JSON, validation, UUID/base64; no Tokio or HTTP. Reusable wire/parser library; not currently `no_std`. |
| `bus-proxy-router` | Protocol + Tokio/tokio-util + `tracing`; no HTTP, OTel, or Arrow. Reusable routing actor, snapshots and owner state. |
| `bus-proxy-gateway` | Protocol/router + Hyper/HTTP/Tokio and rustls-based upstream TLS; reusable authenticated gateway, not a binary-only component. |
| `opencode-bus-proxy` | Binary host: CLI/env/files/systemd, global subscriber, supervision and shutdown. OTel 0.33.1, OTLP 0.33.1, tracing-opentelemetry 0.34 and reqwest/rustls are currently **unconditional** dependencies. |

Today OTLP is disabled unless explicitly selected or an endpoint is present,
but its dependencies still compile. Existing instrumentation remains useful
without an OTel subscriber; keep `tracing` as the core libraries' interface.
Workspace Tokio currently requests `full`, and Hyper `full`; separating exporters
does not by itself make the existing library dependency graph minimal. Narrow
workspace dependency features and request required capabilities per package in a
separate reviewed change, with router tests retaining their dev-only `test-util`.

The [maintained operations](../../doc/event-multiplexer/operations.md#otlp-trace-export)
describe **completed spans**, their attached events, bounded batch export and
shutdown. They do not describe standalone OTLP logs or OTel metrics. A retained
subscription span can remain unexported while the subscription is open. The
[state-export exploration](state-exports.sol-medium.md) separately distinguishes
observational snapshots, sequenced reactive changes, and frozen restoration
images. Neither an Arrow stream nor a `SpanExporter` unifies those products.

Concurrent handover work already adds `RouterImage` in
[`router/src/lib.rs`](../../crates/router/src/lib.rs) and image/transport owners in
[`gateway/src/handover.rs`](../../crates/gateway/src/handover.rs). This is a
working-copy observation, not verification or acceptance of that work. Leave its
types, freeze/adoption semantics and edits with their owners. An inspection
adapter must not expose exact image buffers, descriptors, credentials or TLS
material merely because they are serializable.

## What external `clickhouse-arrow` actually provides

Registry and the **published 0.2.1 source archive**, inspected 2026-10-11:

| Finding | Architectural consequence |
| --- | --- |
| Latest non-yanked release observed is **0.2.1**, Apache-2.0, edition 2024; manifest/registry declare no `rust-version`. | Use this as the initial audited baseline; do not invent an MSRV or infer one from README benchmark tooling. |
| `ClientBuilder::with_host_port` / `with_endpoint`, `with_tls`, `build_arrow` produce `Client<ArrowFormat>` (`ArrowClient`). | Native TCP/TLS destination, conventionally 9000/9440; **not an HTTP URL** or HTTP adapter. |
| `ArrowFormat` converts Arrow `RecordBatch` to/from **ClickHouse native blocks**. `insert` / `insert_many` return response streams. | Arrow is the in-memory interface, **not Arrow IPC on the native wire**. Consume responses to completion before claiming success; do not send `FORMAT ArrowStream` through this API. |
| README advertises protocol revision **54479** and benchmarks server 25.5.2.47. | These are not proof of compatibility with every server or a minimum supported server version. Pin representative server versions in integration evidence. |
| Default features: `derive`, `serde`, `pool` (bb8), `inner_pool` (arc-swap). `inner_pool` establishes additional connections, default total four. | Start with `default-features = false` for sequential batched insertion; no derive, serde config or pools needed just to send RecordBatches. Reassess pooling only with evidence. |
| Arrow **57** is mandatory, with its default CSV/IPC/JSON features plus prettyprint, IPC compression and chrono-tz. Tokio, tokio-rustls, webpki-roots, lz4_flex and zstd are also mandatory. | Disabling clickhouse-arrow defaults **does not remove Arrow, TLS or compression**. Even its `NativeFormat` API does not eliminate the Arrow dependency in this release. Expect substantial native build costs. |
| It has no public Cargo TLS-provider selector. Its tokio-rustls 0.26 dependency leaves defaults enabled. | tokio-rustls enables aws-lc-rs, whereas this gateway selects ring. A downstream declaration cannot subtract that transitive feature. Native builds bring both providers/build requirements unless upstream changes. |
| `tcp.rs` creates a WebPKI-rooted rustls config with no client authentication. `cafile` exists in options/builder but is not read by the inspected connection/TLS path. | Do **not** promise custom CA or mTLS support from the builder's documentation alone. Verify/fix upstream before offering those configuration capabilities. |
| Native connection setup calls `socket2::Socket::connect_timeout` inside the async path; client I/O uses Tokio tasks. | Async signatures alone do not guarantee nonblocking connection setup. Isolate native export execution from the data-plane scheduler pending upstream investigation. |

The external crate's `cloud` feature adds a ureq cloud-wakeup helper, not a
second HTTP Arrow insertion transport. We should keep it off initially.

The preserved custom draft is genuinely different: reqwest blocking HTTP,
`INSERT … FORMAT ArrowStream`, Arrow IPC `StreamWriter`, and span projection.
ClickHouse documents ArrowStream input and HTTP insertion separately. HTTP is
useful through HTTP-only endpoints/proxies; native is the intended external-crate
approach. No benchmark or reliability winner is established by this inspection.

## Package destination and alternatives

Three plausible groupings:

1. **Stay at four packages, all adapters as feature-gated daemon modules.**
   Smallest manifest change, sufficient if only this executable will ever use
   them. Poor fit for embedded hosts, raw Arrow/state consumers, and separately
   maintained implementations; daemon becomes the only configuration owner.
2. **One optional `bus-proxy-telemetry` package with native/HTTP modules.**
   Reasonable interim extraction for existing OTLP. Feature isolation works,
   but both ClickHouse clients share its release/API and maintenance surface.
3. **Recommended: retain four core packages; separate composition, projection,
   and the two delivery libraries.** Shared schema tests, independent transports,
   and no OTel requirement for a raw Arrow consumer. More manifests, but each
   corresponds to a real audience or dependency ownership boundary.

Proposed paths/package names (all new paths are proposals):

| Path / package | Owns | Does not own |
| --- | --- | --- |
| Existing `crates/{protocol,router,gateway,daemon}` | Existing wire, routing, network and process responsibilities | Analytics runtimes or vendor exporters in the data plane |
| `crates/telemetry` / `bus-proxy-telemetry` | Typed telemetry configuration, resource/sampler/batch/provider composition, tracing-to-OTel layer; optional OTLP and ClickHouse assembly | Global subscriber installation, env/file reads, routing, CLI/systemd |
| `crates/export/arrow` / `bus-proxy-export-arrow` | Versioned table contracts, redaction/limits, Arrow schema and RecordBatch projection; optional `otel-traces` and `router-state` adapters | Network clients, retries, global subscriber, frozen-image creation/import |
| `crates/export/clickhouse-native` / `bus-proxy-clickhouse-native` | Native destination/config/client, insert acknowledgment/error/timeout handling; optional `otel` SpanExporter adapter | HTTP, env lookup, provider or global subscriber ownership |
| `crates/export/clickhouse-http` / `bus-proxy-clickhouse-http` | HTTP destination/auth/client, Arrow IPC stream encoding, HTTP completion/error/timeout handling; optional `otel` SpanExporter adapter | Native protocol, external clickhouse-arrow dependency, env/global/provider ownership |

Both delivery packages expose a typed **batch insertion** interface without
their `otel` feature. Their OTel adapters reuse the trace projection and implement
SDK `SpanExporter` individually. This permits state table insertion, replay of
offline analytics batches, and other hosts' OTel providers without importing the
daemon or its gateway. Do not invent a general async exporter framework to wrap
the SDK. In SDK 0.33.1 `SpanExporter` uses return-position `impl Future`; do not
assume it can be stored as `Box<dyn SpanExporter>`.

The Arrow package uses `arrow-array` and `arrow-schema`, not a query engine.
`otel-traces` enables OTel types/conversion; `router-state` enables a dependency
on the router and its **observational** projection. Neither is a default feature.
If multiple owners later need transport-free shared state DTOs, extract those
then; do not move runtime owners into a speculative universal `state` crate now.
The existing offline Python/Polars bridge can remain independent tooling. rkyv
remains an optional image codec under handover ownership, not an Arrow or tracing
prerequisite.

## Consumer/build contract

**Recommended defaults:** daemon `default = []`; no network telemetry. New
telemetry/projection library feature defaults are empty. A delivery package's
own selected transport dependencies are necessarily compiled when that package
is selected; `otel` adds only its SDK adapter. A distributor wanting continuity
with today's shipped OTLP-capable binary builds explicitly with `otlp`, while
runtime export remains off unless configured. Document this packaging change.

Proposed daemon feature forwarding (sketch, not applied):

```toml
[features]
default = []
otel = ["dep:bus-proxy-telemetry"]
otlp = ["otel", "bus-proxy-telemetry/otlp"]
clickhouse-native = ["otel", "bus-proxy-telemetry/clickhouse-native"]
clickhouse-http = ["otel", "bus-proxy-telemetry/clickhouse-http"]
```

`bus-proxy-telemetry` makes each vendor/client dependency optional and forwards
`otel` to each ClickHouse delivery library. The delivery library's `otel` feature
enables the SDK and `bus-proxy-export-arrow/otel-traces`. The Arrow package never
depends on either transport. Use `dep:` to avoid accidentally advertising
internal dependency features; use weak `dependency?/feature` forwarding where
the option must not itself enable that dependency.

| Consumer | Proposed selection | Built / excluded |
| --- | --- | --- |
| Wire-only library | `-p bus-proxy-protocol` | No Tokio/HTTP/OTel/Arrow |
| Embedded routing actor | `-p bus-proxy-router` | Tokio + tracing; no HTTP/OTel/Arrow |
| Embedded gateway | `-p bus-proxy-gateway` | Existing HTTP/TLS stack; host owns subscriber; no OTel/Arrow |
| Local-only daemon | `-p opencode-bus-proxy --no-default-features` | Local text/JSON logging and inspections; existing gateway TLS still built; **no OTel, OTLP, reqwest exporter or Arrow** |
| OTLP daemon | same + `--features otlp` | Existing HTTP protobuf/JSON exporter and SDK; no Arrow/ClickHouse |
| Native ClickHouse daemon | same + `--features clickhouse-native` | SDK + Arrow 57 + clickhouse-arrow native TCP/TLS; no OTLP or custom HTTP exporter |
| Custom HTTP Arrow daemon | same + `--features clickhouse-http` | SDK + split Arrow crates/IPC + blocking HTTP client; no clickhouse-arrow |
| All three available | same + `--features otlp,clickhouse-native,clickhouse-http` | All compile; runtime config independently selects zero, one or multiple sinks |
| Offline/state Arrow consumer | `-p bus-proxy-export-arrow` (+ `router-state` if needed) | Arrow projection, optional router; no OTel SDK or exporter/network clients |
| Raw native / raw HTTP batch consumer | respective delivery package, no `otel` | Selected transport + Arrow, no OTel SDK or daemon; native still brings its semantic-conventions constants dependency |
| Embedded OTel host | delivery package with `otel`, or telemetry package with selected sinks | Caller-owned provider/subscriber/runtime; no daemon dependency |

These are **future build commands**, not combinations this pass compiled.
Keep the four core packages as explicit workspace `default-members` when adding
optional packages. Root `cargo build/test` then preserves a light local-only
path; `--workspace` deliberately selects heavy members too. Features control
dependencies, not workspace package selection. Optional packages can be resolved
into the shared lockfile without being compiled. Do not make core consumers run
`--workspace --all-features` as their normal verification command.

### Cargo, Arrow and MSRV constraints

- Features are additive and unify on each package/version. Native + HTTP must
  coexist; no `compile_error!` excluding the pair, `no-otel` feature, or a feature
  changing an existing type's meaning. Resolver 2 does **not** isolate normal
  dependency features across selected workspace members. A crate boundary is
  maintenance/API isolation, not separate Cargo resolution in one binary.
- Declare shared dependency versions with conservative defaults disabled at the
  workspace level; declare optionality and required features in each consumer.
  Workspace dependencies themselves cannot be optional. Do not rely on a child
  `default-features = false` to undo workspace-enabled defaults under Cargo 1.98.
- Initially audit `clickhouse-arrow = "=0.2.1"`, default features off, and align
  the Arrow array/schema/IPC family on **57**. Review native dependency upgrades
  explicitly; the native package's version pin need not pin unrelated consumers.
  Libraries cannot rely on this repository's lockfile in downstream builds.
- Arrow major versions create distinct Rust `RecordBatch`/schema types and
  trait identities. This is **not a stable Rust ABI**; Arrow IPC interoperability
  is a separate wire-format question. Do not pass another Arrow major's batch
  directly into `ArrowClient`; avoid exposing Arrow in core APIs. Sharing 57
  gives the two current adapters direct batch reuse. Independent future majors
  need conversion or a serialized boundary, not unchecked casts.
- The native crate enables extra Arrow features transitively; a co-built HTTP
  adapter inherits those too. The HTTP-only build can remain smaller, with IPC
  compression off initially. Disable unnecessary Polars/rkyv/native pools, but
  do not claim downstream feature flags can strip native's mandatory graph.
- Edition 2024 and Arrow 57.0.0's declared MSRV establish a **1.85 floor**, not a
  verified MSRV for clickhouse-arrow 0.2.1 or today's transitive resolution.
  Advertise 1.98.0 as the initially supported/tested toolchain only after the
  selected builds pass; set package `rust-version` accordingly. Lower independent
  core-library MSRVs require actual CI evidence, not a workspace-wide promise
  inferred from one dependency. Keep resolver 2 during this extraction; a later
  resolver/MSRV-policy change deserves its own verification.
- Both transports share this workspace's normal release/lockfile workflow, but
  keep separate manifests, READMEs, tests and dependency upgrade commits. Neither
  transport imports the other. Independent schema breaking changes are explicitly
  versioned; transport-only fixes need not rewrite a schema or the peer client.

## Configuration, runtime and lifecycle ownership

**The daemon owns ambient configuration.** It reads env/CLI/credential files,
resolves precedence and secrets, validates compiled capabilities, installs the
global subscriber, and shuts down what it started. Libraries accept typed config
and explicit clients/execution context; no `from_env()`, `try_init()`, or hidden
global provider installation in their constructors. An embedded host can supply
its own resource and subscriber. Standard OTel environment compatibility stays
in a deliberate host adapter; SDK builders/resource detectors also read env, so
simply hiding our own `std::env` calls is insufficient. Programmatic construction
must not accidentally inherit sampler, resource or batch policy from ambient env.

Preserve current `OTEL_SDK_DISABLED` and `none` precedence. Proposed explicit
exporter names: `otlp`, `clickhouse-native`, `clickhouse-http`; allow a
comma-separated selection, reject `none` mixed with other names and unknown
names. With no selection, retain the existing OTLP-endpoint inference **only for
OTLP**. ClickHouse endpoints alone do not select an implementation. An explicitly
requested unavailable sink (including inferred OTLP in a local-only build) fails
startup with its required Cargo feature, rather than silently dropping export.
These selection extensions are not today's supported environment interface.

Give native `host`/`port`/TLS-name settings and HTTP endpoint settings distinct
namespaces/types; do not reinterpret the draft's `BUS_PROXY_CLICKHOUSE_ENDPOINT`
as a native socket address. Password/file/systemd resolution remains host-owned;
both libraries receive redacted secret values and validated table identities.
Share credential values only by explicit host configuration, never URLs or logs.
Native TLS is WebPKI-rooted as inspected, not platform/custom-CA/mTLS equivalence
with OTLP. Prefer explicit native host+port over ad hoc endpoint string parsing.

`bus-proxy-telemetry` constructs one tracing-to-OTel bridge/provider and **one
bounded batch processor per selected destination**, not a serial fan-out exporter.
Slow native, HTTP or collector delivery must not hold up another export queue or
the routing actor. Account for multiplied buffers and shared sampling; runtime
selection matters even when all features compile. Reject malformed config/schema
up front; destination outages after startup are best-effort failures, not data
plane failures. Do not make gateway readiness depend on a telemetry database.

SDK 0.33.1's ordinary `with_batch_exporter` runs on a dedicated thread and polls
with `futures_executor::block_on`: it supplies **no Tokio reactor**. Keep the
existing blocking OTLP HTTP client/processor pairing, but supply the client
explicitly during extraction: today's builder relies on enabled dependency
features, and unification can instead select an async client. The HTTP experiment can use
this thread if its blocking client lifecycle and finite timeout are tested.
An unadapted async native client cannot just be substituted into the same worker.

For native trace export, prefer an explicitly owned, export-only Tokio executor
kept alive through flush/shutdown; the adapter marshals each batch onto it and
awaits the **completed native response**, with bounded work and cancellation.
Raw native consumers use their own runtime with the connection-setup caveat.
An injected host runtime is an alternative only if its scheduling/isolation
contract is explicit. Do not use nested `Runtime::block_on` on application workers
or assume `Handle::enter` drives current-thread I/O. Timeout/cancellation must not
detach continuing inserts/retries; resetting an interrupted connection and
reporting uncertain acknowledgment are transport responsibilities. Source-level
blocking connection setup also needs a finite bound/isolation test. The SDK's
experimental async-runtime processor is an alternative with extra feature/API
coupling, not necessary to mandate for all consumers.

Native's default tokio-rustls adds aws-lc while gateway/OTLP currently select ring.
Initialize/validate the daemon's rustls provider independently of OTLP activation
before any TLS client is built; otherwise native-only/coexistence configurations
can expose ambiguous default-provider selection. Installing ring does not remove
aws-lc compilation. Libraries must not silently install a process-global provider;
document caller setup or accept explicit TLS construction where the dependency
allows it. An upstream provider/config injection fix would improve this boundary.

Shutdown order: stop/join gateway and router, close retained application spans,
flush/shut down providers off application workers, then stop export executors.
Use a finite overall shutdown budget and report incomplete drains; `force_flush`
does not finish still-open spans. Preserve the existing target allowlist,
independent local/export filters, no per-frame spans and exporter-feedback
exclusion. Logs/events outside spans and live-state samples need their own
adapters; do not silently route them through `SpanExporter`.

## Schema versus transport, and keeping both useful

The Arrow package owns reviewed column names/types/units/nullability, trace
resource/scope/event/link conversion, field limits/redaction, schema identity and
fixtures. Keep OTel trace tables separate from inspection tables
(`subscriptions`, `memberships`, counters/gauges, etc.). Observations carry their
generation and consistency class; image codecs still own exact-byte restoration.
Never run table construction under queue locks or on the router hot path.

The draft's JSON-string attributes/events/links and timestamp/duration choices
are inputs to schema review, **not an accepted standard schema**. They are not
automatically compatible with an OTel Collector's ClickHouse tables or Grafana
queries. Choose one explicit initial trace schema and independently provisioned
ClickHouse DDL (including retention/order keys); validate both transports against
it. Do not let native `create_table` convenience or HTTP insertion invent/migrate
production tables at startup. Match timestamp precision/timezone, UInt64 counters,
empty/null behavior, strings/binary and nested mappings with real server tests.

Each transport owns encoding, acknowledgment, bounded retry/timeout, credential
handling and sanitized errors. HTTP must check response body/exception signals,
not merely HTTP 200; native must drain its response stream. In either path an
acknowledgment timeout can follow a committed insert: default to bounded
best-effort without automatic uncertain-result retries until a deduplication
contract is selected. Neither is durable or exactly-once by virtue of Arrow.

Retain the experiment at its current design path; when commissioned, develop the
HTTP package from it without deleting the evidence. Both implementations can
target the same reviewed schema, while transport-specific tests remain separate.
For side-by-side comparison use separate tables/endpoints. Enabling both against
one table inserts the same spans twice unless an explicit deduplication policy
exists; neither silently shadows or falls back to the other.

## Verification without burdening every consumer

**This pass verified metadata and inspected source; it did not compile an
exporter, test a proposed feature combination, or run a ClickHouse deployment.**
The existing [operations](../../doc/event-multiplexer/operations.md) and
[state-export experiments](state-exports.sol-medium.md#reproduce-the-experiments)
remain the evidence for current behavior, not for this destination design.

Proposed CI lanes after implementation:

1. **Core/local:** explicit four core packages, local-only daemon, parser/router/
   gateway/compatibility tests and tracing regressions. Inspect `cargo tree -e
   features` to assert absence of OTel/Arrow/exporter clients. No ClickHouse,
   collector, Python, containers or exporter packages required.
2. **OTLP only:** existing loopback exporter test gated with
   `required-features = ["otlp"]`; test selection/disable precedence, standalone
   logging, allowed protocols, credentials redaction, failures and bounded drain.
3. **Each Arrow/delivery library:** default/raw and `otel` builds; projection
   fixtures (empty, Unicode, UInt64 max, timestamps, arrays/non-finite attributes,
   resources/scopes/events/links, redaction/size limits). HTTP IPC round-trip and
   loopback HTTP failures need no server. Native unit tests cover its configuration
   and SDK/runtime bridge without a database. Gate any fixture/helper dependencies
   so selecting raw batch transport does not enable the OTel SDK.
4. **Feature intersections:** local-only, `otel` bridge without vendor clients,
   each singleton, all three pairs, and all-three daemon builds/tests, each with
   `--no-default-features --locked`. Test independent queues/filtering/shutdown,
   no recursive export, and TLS-provider initialization without OTLP. Check
   `cargo tree -d` and reverse feature trees for Arrow/rustls/reqwest; a lone
   `--all-features` job cannot detect unwanted minimal-build dependencies.
5. **Exporter-maintainer integration:** pinned disposable ClickHouse server(s),
   both transports writing the same schema to distinct tables; query-back content,
   schema errors, auth, TLS, disconnects/timeouts, native completion errors, HTTP
   200-with-error, and slow-sink isolation. Containers/`test-utils` are explicit
   opt-in test features/jobs, not normal dependencies. `clickhouse-local` can check
   IPC/type decoding but cannot prove TCP/TLS, authentication or acknowledgments.

Run format/Clippy in the corresponding lanes. Apply MSRV/toolchain checks to each
advertised consumer profile and test downstream fixture crates to catch public
Arrow/OTel version coupling and path-only packaging mistakes. A full-workspace
all-feature check is a maintainer union check, not the only support definition.

## Defensible landing sequence, if selected

1. Agree the consumer/default matrix and package boundaries; keep this proposal
   and the HTTP draft as evidence, not live instructions.
2. Gate existing OTLP dependencies, tests and host startup/shutdown code; extract
   reusable typed telemetry composition without changing instrumentation semantics.
   Update packaging/operations to distinguish compiled availability from activation.
3. Specify and fixture the common trace table/DDL independently of transport;
   keep optional state projection and frozen images separate.
4. Commission native and HTTP library implementations separately, verify the
   external-crate runtime/TLS caveats, then add host feature/config forwarding and
   the focused CI lanes. Preserve and compare both; no implicit winner/fallback.

## External evidence

Retrieved 2026-10-11; versioned sources govern package claims, not upstream `main`:

- [Registry: clickhouse-arrow](https://crates.io/api/v1/crates/clickhouse-arrow),
  [0.2.1 source archive](https://crates.io/api/v1/crates/clickhouse-arrow/0.2.1/download),
  [manifest](https://docs.rs/crate/clickhouse-arrow/0.2.1/source/Cargo.toml.orig).
  Archive `.cargo_vcs_info.json` identifies upstream revision
  `ed461b028c5d709848cba164772676552ed52fc4`.
- Versioned native source:
  [`ArrowFormat`](https://docs.rs/crate/clickhouse-arrow/0.2.1/source/src/formats/arrow.rs),
  [`Client::insert`/`insert_many`](https://docs.rs/crate/clickhouse-arrow/0.2.1/source/src/client.rs),
  [builder](https://docs.rs/crate/clickhouse-arrow/0.2.1/source/src/client/builder.rs),
  [connection](https://docs.rs/crate/clickhouse-arrow/0.2.1/source/src/client/connection.rs),
  [TCP/TLS implementation](https://docs.rs/crate/clickhouse-arrow/0.2.1/source/src/client/tcp.rs).
- [tokio-rustls 0.26.6 manifest/default providers](https://docs.rs/crate/tokio-rustls/0.26.6/source/Cargo.toml),
  [rustls 0.23.45 CryptoProvider](https://docs.rs/rustls/0.23.45/rustls/crypto/struct.CryptoProvider.html).
- [Arrow 57.0.0 registry metadata / MSRV](https://crates.io/api/v1/crates/arrow/57.0.0).
- [SDK 0.33.1 SpanExporter](https://docs.rs/opentelemetry_sdk/0.33.1/opentelemetry_sdk/trace/trait.SpanExporter.html),
  [batch processor implementation/runtime and HTTP-client requirements](https://docs.rs/crate/opentelemetry_sdk/0.33.1/source/src/trace/span_processor.rs).
- [ClickHouse ArrowStream](https://clickhouse.com/docs/interfaces/formats/ArrowStream),
  [HTTP insertion, auth and response caveats](https://clickhouse.com/docs/interfaces/http).
  Live server docs are capability references; verify against selected server versions.
- [Cargo additive features/unification/resolver 2](https://doc.rust-lang.org/cargo/reference/features.html#feature-unification),
  [workspace defaults and inherited dependency features](https://doc.rust-lang.org/cargo/reference/workspaces.html).
