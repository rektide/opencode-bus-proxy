---
type: DomainGuide
title: Telemetry domain
description: Reusable policy, Arrow projection and independently selected trace destinations.
resource: /telemetry/README.md
status: implemented
author: model:openai/gpt-6.1-sol#xhigh
date: 2026-10-11
sources:
  - resource: opencode://ses_ed2d6efafffeJebEhJ6FDRWIeF
    title: Native transport/projection integration, additive destinations and feature verification
  - resource: /design/event-multiplexer/export-boundaries.sol-xhigh.md
    title: Export ownership and consumer-build architecture
  - resource: /design/event-multiplexer/clickhouse-schema0.glm53fx.md
    title: Independent ClickStack/HyperDX/collector schema evidence
---

# Telemetry domain

[`core`](core/) is the `bus-proxy-telemetry` composition library. Package names
are independent of directory nesting. [`arrow`](arrow/README.md) owns the versioned
collector-compatible projection; [`clickhouse-native`](clickhouse-native/README.md)
owns published clickhouse-arrow TCP/TLS batch insertion and its optional SDK
adapter. The preserved [HTTP draft](../design/event-multiplexer/clickhouse-http-draft.rs.txt)
remains unchanged/uncompiled in `design/event-multiplexer/`; no empty HTTP crate.

Core accepts an explicit exporter, resource, sampler, span limits and batch
configuration, and returns a caller-owned provider and tracing layer. Use
`Resource::builder_empty()` for environment-isolated resources and
`batch_config` for deterministic batching. No subscriber, TLS crypto provider,
or global OTel provider is installed. Constructors and provider shutdown belong
off async workers for blocking exporters. The SDK batch worker has no Tokio
reactor; async exporters require their own execution arrangement.

`ProviderConfig::builder()` exposes shared resource/sampler/span policy for
several processors; its `batch` field is used by the single-exporter `build`
convenience only. Configure each processor explicitly. Native and OTLP share a
provider/filter/sampling decision, **not a queue**: one bounded SDK processor per
destination prevents a slow sink from serializing another. Native owns an
additional export-only runtime thread, with explicit cancellation/join.

The optional `otlp` feature exposes the upstream exporter builder for deliberate
host adapters. That builder reads OTEL environment variables (including headers)
even with programmatic settings; it is not an environment-isolated constructor.
The daemon owns this adapter and supplies a blocking reqwest client explicitly,
so transitive feature unification cannot switch it to an async client.

Build independently with `cargo check -p bus-proxy-telemetry --locked`, or add
`--features otlp` / `--features clickhouse-native` / both. The four existing core
packages remain workspace default members; `--workspace` deliberately selects
heavy optional domain packages too. Raw native/Arrow consumers require no daemon
or SDK. Arrow adds SDK only with `otel-traces`; native only with `otel`.

Verification lanes:

```sh
cargo +1.98.0 test --locked
cargo +1.98.0 test -p opencode-bus-proxy --no-default-features --features otel --locked
cargo +1.98.0 test -p bus-proxy-telemetry --no-default-features --locked
cargo +1.98.0 nextest run -p bus-proxy-export-arrow --features otel-traces --locked
cargo +1.98.0 nextest run -p bus-proxy-clickhouse-native --features otel --locked
cargo +1.98.0 nextest run -p opencode-bus-proxy --features clickhouse-native --locked
cargo +1.98.0 nextest run -p opencode-bus-proxy --features otlp,clickhouse-native --locked
cargo +1.98.0 clippy --all-targets --locked -- -D warnings
cargo +1.98.0 clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo +1.98.0 fmt --all -- --check
```

Dependency isolation can be inspected with `cargo tree -p opencode-bus-proxy
--no-default-features --locked`: it must contain no opentelemetry, reqwest, Arrow
or ClickHouse packages. Selecting protocol/router/gateway alone must likewise
remain independent of telemetry. Systemd-manager handover tests remain ignored
unless deliberately selected; no deployment is needed for telemetry tests.

The [operations guide](../doc/event-multiplexer/operations.md#native-clickhouse-trace-export)
documents build/selection/configuration and operator-owned table provisioning.
The native library's ignored tests optionally start only their own disposable
ClickHouse process; no Kubernetes/Docker deployment or existing database is used.
