# Telemetry domain

[`core`](core/) is the `bus-proxy-telemetry` composition library. Package names
are independent of directory nesting. Future Arrow/native ClickHouse/HTTP Arrow
implementations can be siblings here; none are implemented by this extraction.
The preserved HTTP draft remains in `design/event-multiplexer/`.

Core accepts an explicit exporter, resource, sampler, span limits and batch
configuration, and returns a caller-owned provider and tracing layer. Use
`Resource::builder_empty()` for environment-isolated resources and
`batch_config` for deterministic batching. No subscriber, TLS crypto provider,
or global OTel provider is installed. Constructors and provider shutdown belong
off async workers for blocking exporters. The SDK batch worker has no Tokio
reactor; async exporters require their own execution arrangement.

The optional `otlp` feature exposes the upstream exporter builder for deliberate
host adapters. That builder reads OTEL environment variables (including headers)
even with programmatic settings; it is not an environment-isolated constructor.
The daemon owns this adapter and supplies a blocking reqwest client explicitly,
so transitive feature unification cannot switch it to an async client.

Build independently with `cargo check -p bus-proxy-telemetry --locked`, or add
`--features otlp`. The four existing core packages remain workspace default
members; selecting `--workspace` also selects this optional composition package.

Verification lanes:

```sh
cargo +1.98.0 test --locked
cargo +1.98.0 test -p opencode-bus-proxy --no-default-features --features otel --locked
cargo +1.98.0 test -p bus-proxy-telemetry --no-default-features --locked
cargo +1.98.0 test --workspace --all-features --locked
cargo +1.98.0 clippy --all-targets --locked -- -D warnings
cargo +1.98.0 clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo +1.98.0 fmt --all -- --check
```

Dependency isolation can be inspected with `cargo tree -p opencode-bus-proxy
--no-default-features --locked`: it must contain no opentelemetry, reqwest, Arrow
or ClickHouse packages. Selecting protocol/router/gateway alone must likewise
remain independent of telemetry. Systemd-manager handover tests remain ignored
unless deliberately selected; no deployment is needed for telemetry tests.
