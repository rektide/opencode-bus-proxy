---
type: VerificationReport
title: Native ClickHouse Arrow implementation and verification
description: Completed native transport/projection/host integration, independent consumer checks and bounded upstream caveats.
resource: /telemetry/native-verification.sol61x.md
status: implemented and verified in isolated local probes; not deployment acceptance
author: model:openai/gpt-6.1-sol#xhigh
date: 2026-10-11
sources:
  - resource: opencode://ses_ed2d6efafffeJebEhJ6FDRWIeF
    title: Native implementation, source audit, feature matrix, isolated consumer and real-server verification
  - resource: /telemetry/arrow/README.md
    title: Exact versioned collector-compatible projection and deliberate semantic losses
  - resource: /telemetry/clickhouse-native/README.md
    title: Native execution, acknowledgment and upstream constraints
  - resource: /design/event-multiplexer/clickhouse-schema0.glm53fx.md
    title: Independent FF/ClickStack/HyperDX/collector schema research informing implementation
  - resource: /doc/event-multiplexer/operations.md#native-clickhouse-trace-export
    title: Maintained host environment, explicit table setup and lifecycle contract
---

# Native ClickHouse Arrow implementation and verification

Implemented directly in the primary checkout, preserving concurrent research.
No deployment, push, unrelated service/data modification, HTTP implementation,
router-state projection, or other telemetry signal was added.

## Landed responsibility boundaries

- `telemetry/arrow` / **bus-proxy-export-arrow**: Arrow 57 schema/projection
  `otel-clickhouse-traces-v1`; raw schema has no SDK/network. Optional
  `otel-traces` converts completed spans to the collector/HyperDX 22-column shape.
- `telemetry/clickhouse-native` / **bus-proxy-clickhouse-native**: typed
  `Config`, validated `Table`, bounded `BatchClient::insert(RecordBatch)` and
  explicit shutdown; optional `otel::Exporter`. Published **clickhouse-arrow
  = 0.2.1**, default features off, sends native blocks over TCP/TLS, not Arrow IPC.
- `telemetry/core`: optional additive native feature/reexport and caller-owned
  shared provider policy with independent processors. No global subscriber,
  TLS setup or environment mutation.
- `cli/daemon`: additive **clickhouse-native** build feature; runtime
  `OTEL_TRACES_EXPORTER=otlp,clickhouse-native` (or either, or none). Host resolves
  native env/files/systemd secrets, supplies resource/sampling/filter/batches,
  selects TLS provider, installs subscriber, and cancels/joins native work after
  a finite graceful SDK drain budget. Existing OTLP blocking HTTP client remains
  explicit under feature unification.

The four light default members and existing package/API identities remain.
Native HOST/TABLE selection is explicit; database defaults to `default` without
assigning production ownership. Table provisioning, retention and migration are
operator-owned. The HTTP draft is unchanged (verified against `e78964f2`).

## Verification evidence

Rust **1.98.0**, locked workspace dependency resolution. Commands are in the
maintained guides; test counts here identify the runs, not a claim of blanket
production correctness.

| Check | Result |
| --- | --- |
| Local-only daemon nextest | 24 passed; 3 opt-in systemd-manager tests skipped |
| `otel` composition-only daemon | 24 passed; unavailable sink requests remain actionable |
| OTLP-only daemon | 24 passed, including real loopback HTTP export and SIGTERM flush |
| Native-only daemon | 26 passed, including disposable real-server request-span flush; late upstream-env/disable test passed separately |
| Native + OTLP daemon | 27 passed, including loopback OTLP, real native flush with an unavailable OTLP destination, upstream-env rejection and disable precedence |
| Arrow/native/core optional-library union | 13 passed; disposable database test deliberately selected separately |
| Clippy workspace/all targets/all features | Passed, warnings denied |
| Clippy local daemon and raw Arrow/native profiles | Passed, warnings denied |
| `cargo fmt --all -- --check` | Passed |
| Independent raw consumer | Compiled outside workspace feature unification; Arrow batch types directly compatible, no SDK/daemon |
| Independent embedded OTel consumer | Compiled and ran provider/native lifecycle without daemon or global setup |
| Normal dependency trees | Local daemon: no OTel/reqwest/Arrow/ClickHouse; OTLP-only: no Arrow/native; Arrow raw: no SDK/Tokio/network; native raw: no SDK/daemon/core/reqwest (upstream semconv constants remain) |
| Package file inventory | Native library contains self-contained source/README; cross-package maintainer integration fixtures are excluded from its distribution |

Real disposable ClickHouse **25.6.13.41** and **26.9.7.9** probes passed native
handshake and insertion; Unicode and UInt64 maximum; server-default columns;
post-send constraint rejection; fresh-connection recovery; resource/scope and
event/link values; LowCardinality map keys from Utf8; List(Map); UTC nanoseconds;
empty arrays/IDs; redaction; and insertion omitting a HyperDX-style MATERIALIZED
extra. A proxy deliberately withheld acknowledgment after observing a committed
row, then disconnected: the wrapper returned failure, not false success. The SDK
ordinary batch worker flushed native spans without a Tokio reactor. The older
version came from the official static archive; its checksum is recorded in the
native guide. Each test owns/kills/waits only its disposable server/TempDir.

Normal no-database tests also exercised stalled handshakes, timeout, dropped
futures, socket cleanup, bounded raw admission, pending cancellation, repeated
shutdown and unavailable destinations. A core test holds a slow exporter while
the other destination exports, demonstrating non-serial queue ownership.

The new dependency graph was not used as justification to rerun unrelated full
router/gateway handover suites; their earlier integration verification is retained.

## Constraints not hidden by the wrapper

1. Upstream fixed **30-second blocking TCP connect** can overrun cooperative
   operation/cancellation deadlines. Private-worker isolation protects gateway
   scheduling. Native shutdown joins, with that allowance plus one second of DNS
   cleanup; residual OS getaddrinfo cannot continue a canceled insert. Numeric
   host plus TLS name avoids residual DNS work.
2. A drained response channel is not sufficient in upstream 0.2.1: fatal I/O can
   close it silently. Full drain plus a same-connection ping is required. No
   automatic retry of uncertain inserts, durability or exactly-once claim.
3. Compression dependencies are mandatory, but wire compression is **off**:
   upstream default LZ4 failed on 26.9 insert sample headers; uncompressed native
   succeeded on both representative versions.
4. Mandatory native TLS brings aws-lc alongside gateway ring. Reusable TLS config
   requires an already selected host provider, preventing upstream's attempted
   global install from choosing it. Only WebPKI roots; no custom CA/mTLS promise.
5. Upstream reads three buffer/debug environment flags even with typed settings.
   The daemon rejects them when native is selected; embedded hosts own that
   exception. Debug Arrow can expose batch values. No silent env mutation.
6. Upstream leaks a small connection-ID string per established connection;
   successful connection reuse limits this, but repeated reconnects retain the
   audited upstream defect.
7. Standard table omissions (scope attrs/schema URL, flags and drop counts) are
   deliberate and documented, not stuffed into unrelated attrs. Oversize batches
   fail whole; key redaction is not a universal PII filter.

No real certificate-backed native TLS server, live HyperDX UI, newest floating
ClickStack image, lower MSRV, performance benchmark, or production acceptance is
claimed. Database/TTL and deployment choices remain with the operator.

## Checkpoints

- **ba4a87f3** — bounded native batch transport and independent processor assembly.
- **0ffe1792** — shared researched trace projection, optional adapter, additive
  daemon destinations/configuration, tests and maintained guides.
- **bdfe5334** is the independent FX schema-research checkpoint, not authored by
  this implementation session; preserved and used as evidence.

The source/type audit refined the research's lowercase-kind shorthand: actual
pdata spells kinds `Internal`, `Server`, `Client`, `Producer`, `Consumer`.
Status is `Unset`, `Ok`, `Error`. No alternate bespoke schema was silently selected.
