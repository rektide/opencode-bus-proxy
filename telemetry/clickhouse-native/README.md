---
type: ImplementationGuide
title: Native ClickHouse batch transport
description: Caller-owned bounded Arrow batch insertion and its audited upstream constraints.
resource: /telemetry/clickhouse-native/README.md
status: implemented transport; trace schema pending infrastructure research
author: model:openai/gpt-6.1-sol#xhigh
date: 2026-10-11
sources:
  - resource: opencode://ses_ed2d6efafffeJebEhJ6FDRWIeF
    title: Native transport implementation, cancellation and published-source verification
  - resource: /design/event-multiplexer/export-boundaries.sol-xhigh.md
    title: Prior exporter ownership and consumer-build proposal
  - resource: https://docs.rs/crate/clickhouse-arrow/0.2.1/source/src/client/connection.rs
    title: Published connection lifecycle, TLS-provider installation and cancellation
  - resource: https://docs.rs/crate/clickhouse-arrow/0.2.1/source/src/client/internal.rs
    title: Response channel closes on EndOfStream and fatal I/O error
---

# Native ClickHouse batch transport

`bus-proxy-clickhouse-native` uses published **clickhouse-arrow = 0.2.1**, defaults
off, and the **Arrow 57** family (currently locked to 57.3.1). `RecordBatch` is its
in-memory API; TCP/TLS carries **native ClickHouse blocks**, not Arrow IPC or HTTP.
The preserved [HTTP draft](../../design/event-multiplexer/clickhouse-http-draft.rs.txt)
is unchanged and uncompiled. Raw consumers require neither daemon nor OTel SDK.

## Ownership and delivery

Construct `BatchClient::new(Config)` with a validated `Table::new(database, name)`.
Supply explicit secrets, limits and deadlines; no environment or global setup is
performed. Construction is lazy with respect to the network. `insert(batch).await`
uses explicit column names and waits for the entire server response **and a ping
on the same connection**. The ping is necessary: upstream closes the response
channel on fatal I/O errors too, without reliably emitting an error item.

One dedicated thread owns a current-thread Tokio runtime and the sole connection.
Successful inserts reuse it. Interrupted/failed operations discard it before any
subsequent batch. One active and at most one queued bounded batch are allowed;
excess submissions return `Busy`. Dropping an insert future signals cancellation.
Shutdown/last client drop join the thread; do these off application async workers.
The ordinary SDK batch worker does not need a Tokio reactor to await this API.

There are **no retries**, table creation, migrations, retention changes, pruning,
durability or exactly-once guarantees. A failed/timed-out acknowledgment can follow
a committed insert. Transport errors intentionally suppress upstream strings that
could contain credentials or data. Host local debug logging of upstream internals
is a separate confidentiality decision; those targets are never trace-exported.

## Upstream constraints

- The audited TCP connect blocks for a fixed **30 seconds**, not a configurable
  async connect deadline. `operation_timeout` bounds DNS/handshake/insert/drain
  once cooperative polling resumes; add the 30-second non-preemptible allowance.
  This work never executes on gateway/router workers.
- Tokio's upstream hostname resolver uses OS `getaddrinfo`, which cannot be
  preempted. Shutdown allows one second for that DNS-only blocking task, then
  abandons it if necessary; it cannot resume a canceled insert. Supply a numeric
  host (and a TLS server-name override) when no residual DNS work is permissible.
- TLS/compression and Arrow's larger feature graph are mandatory even with crate
  defaults off. Native builds include **aws-lc**, alongside gateway **ring**.
- Transport compression is explicitly **off**. On installed ClickHouse 26.9.7.9,
  upstream's default LZ4 fails while decoding the insert sample header; the
  uncompressed path proceeds correctly. This is not a configurable promise for
  unverified compressed combinations, and does not remove compression dependencies.
- The host must install a rustls `CryptoProvider` before enabling native TLS.
  Validation rejects an absent provider, so upstream's attempted aws-lc global
  installation cannot silently choose process policy. The daemon selects ring.
- Only WebPKI roots and optional TLS server-name override are offered. Upstream's
  `cafile` option is not used in the native path; no custom CA or mTLS promise.
- Upstream leaks a small connection-ID string on each established connection;
  successful connection reuse limits this, but repeated reconnects retain that
  upstream defect. No vendor patch is hidden in this package.

## Verify

```sh
cargo nextest run -p bus-proxy-clickhouse-native --locked
cargo nextest run -p bus-proxy-clickhouse-native --locked --run-ignored only
cargo clippy -p bus-proxy-clickhouse-native --all-targets --locked -- -D warnings
```

The ignored integration test starts its own disposable installed ClickHouse
server in `.test-agent/clickhouse-native/`, inserts Unicode and UInt64 maximum,
checks server-default columns, post-send constraint rejection, recovery and
shutdown. It never touches an existing service/database. Normal lifecycle tests
need no database and exercise a stalled native handshake, timeout, dropped future,
socket cleanup, unavailable destination and operation without a caller reactor.

Transport-stage verification (2026-10-11, Rust 1.98.0): nextest passed all seven
native/core tests, including the disposable **ClickHouse 26.9.7.9** round-trip;
Clippy passed both libraries with all targets and warnings denied. TLS itself is
not yet exercised against a real certificate-backed server. These checks do not
establish a final trace-table contract or HyperDX compatibility.
