---
type: Report
title: Implementation and verification — first build and layout integration
description: Preserved first-build evidence with later telemetry and directory-layout verification addenda.
resource: /doc/event-multiplexer/verification.md
status: verification record, not deployment or acceptance
updated: 2026-10-11
sources:
  - resource: urn:opencode:session:ses_ed35c6048ffegHqs9lkaJQablf
    title: Sol medium — mechanical layout, navigation, isolated checks and preserving primary reintegration
---

# Implementation and verification — 2026-10-10

**First-build record.** The later
[ordinary scoped-event migration](additive-verification.md) replaces the private
source route and envelopes described here. This record preserves the original
checks, review corrections, and benchmark conditions.

## Outcome

The four-crate Rust gateway and additive bus-smart source export are implemented.
The release binary is `target/release/opencode-bus-proxy`; operating instructions
are in [the root README](../../README.md) and [operations](operations.md).

Sol medium agents researched the existing wire contract and Rust concurrency
choices, then implemented protocol, router, gateway, and origin in owned areas.
Astra led the architecture, crate seams, CLI, real-client integration, and landing.
A separate Sol medium review found and verified corrections to native IDs,
long-running HTTP compatibility, connection bounds, and tracing lifetimes.

Expected help: keep Session execution in bus-smart while moving per-client
selection, queues, and sockets to a separate process. Observed: one routed origin
connection serves multiple proxy clients, exact Session selection works through
real native clients, and direct-origin controlled delivery also works. This
establishes the topology and compatibility, not a measured reduction in deployment
CPU or execution latency.

## Checks

| Check                                                                                      | Result                                       |
| ------------------------------------------------------------------------------------------ | -------------------------------------------- |
| `cargo +1.98.0 test --workspace --locked`                                                  | 51 passed: protocol 20, router 22, gateway 9 |
| Final gateway tests after source-failure logging refinement                                | 9 passed                                     |
| `cargo +1.98.0 clippy --workspace --all-targets --locked -- -D warnings`                   | Passed                                       |
| `cargo +1.98.0 fmt --all -- --check`                                                       | Passed                                       |
| `cargo +1.98.0 build --release --locked`                                                   | Passed                                       |
| `BUS_PROXY_BINARY=target/release/opencode-bus-proxy bun run test/compatibility/clients.ts` | Passed against final release binary          |
| Bus-smart server focused suite                                                             | 32 passed                                    |
| Bus-smart client event/controlled suite                                                    | 20 passed                                    |
| Bus-smart protocol event suite                                                             | 7 passed                                     |
| Bus-smart server/client/protocol typechecks and client regeneration                        | Passed                                       |
| Bus-smart root `bun run check`                                                             | Passed with existing lint warnings           |

The broader client run had 62 passing tests and three failures. The origin agent
reproduced **the same three failures and 19 assertions** in an isolated pre-source
workspace at bus-smart `0de07ec3` (`1a5d9f36-`), using the same installed dependencies:

| Existing test                                                                     | Failure                                                                      |
| --------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `test/promise.test.ts`: “exposes every standard HTTP API group”                   | Expects integration connect keys `["key"]`; receives `["key", "external"]`.  |
| `test/promise.test.ts`: “session methods use the public HTTP contract”            | Expects interrupt query `?continue=true`; actual URL omits it.               |
| `test/effect.test.ts`: “session methods retain decoded Effect inputs and outputs” | Numeric fixture `idle` is supplied where decoded `DateTime.Utc` is required. |

Those test files and session/integration protocol definitions were unchanged by
the source work. The baseline reproduction is retained at
`/home/rektide/tmp-opencode/source-client-baseline`; these failures were not treated
as passing or repaired as part of the gateway.

Rust verification used the pinned stable 1.98.0 explicitly because the host has a
nightly `RUSTUP_TOOLCHAIN` override. The TypeScript origin checks were run by the
origin implementation agent. Tests use isolated listeners/in-memory storage;
the managed OpenCode service was not used as a fixture.

## Real-client evidence

`test/compatibility/clients.ts` builds the sibling checkout's actual HTTP routes
and Core Bus with an in-memory database and isolated configuration. It launches
the compiled Rust binary, then uses the native Promise client and unmodified
bus-smart controlled adapter to verify:

- One legacy and two differently focused proxy clients share **one origin source
  GET**; proxy event GETs/PUTs do not become per-client origin subscriptions.
- All five high-frequency types follow the correct Session recipients; a broad
  RPC barrier reaches all clients.
- A live complete interest replacement changes subsequent selection without
  reopening the source or downstream stream.
- A simultaneous direct-origin smart client retains embedded selection.
- Native `EventFeedReady`, `ControlledFeedItem`, and subscription-not-found schemas
  accept proxy frames/errors, beyond what the Promise client's permissive decoder
  alone establishes.
- A pairing token issued by native TypeScript auth succeeds on local diagnostics
  and forwarded `/api/info`, with the external-port cookie and Origin intact.
  Wrong-Origin cookie use and missing credentials fail.
- Process shutdown completes and collected tracing does not expose the password.

The adapter's zero removal grace still expires on a timer. The test explicitly
lets that timer run, flushes installation, and checks the installed set before
asserting the post-removal recipient cohort.

Rust loopback tests additionally cover source loss/reconnect/stale IDs, malformed
and oversized source input, idle timeout, byte/count overflow, abandoned controls,
queue cleanup, opaque upgrades, upload/response streaming and trailers, long API
execution, connection saturation, header timeout, cancellation, and tracing
redaction. These are transport/contract tests, not a full interactive TUI run.

## Review corrections preserved separately

1. Native controlled IDs require `evsub_`; the initial router used bare UUIDs.
   The native-schema integration assertion now covers that compatibility gap.
2. A short deadline around every forwarded response would truncate model
   generation and slow uploads. Connection/source/upgrade setup timeouts are
   separate from ordinary API execution and response duration.
3. Subscriber bounds alone did not bound unauthenticated idle HTTP sockets.
   HTTP connection admission and header-read deadlines now cover those sockets
   and retain permits across upgraded tunnels.
4. Lifecycle events needed persistent spans. Source attempt/epoch, HTTP, and
   subscription/control spans now carry context without per-publication spans.
   Source failure categories distinguish status, timeout, framing, envelope, and
   transport failures without logging peer-controlled error values.

The independent follow-up review found no remaining blocker in those four areas.

## Performance evidence

The [router benchmark record](../../components/router/README.md#recorded-verification-2026-10-10)
contains the full command, hardware, compiler, trials, and interpretation. For
128 clients, 20,000 inputs, and 590-byte encoded frames, three-trial medians were:

| Case                  | Input events/s | Deliveries/s |
| --------------------- | -------------: | -----------: |
| One indexed follower  |        162,610 |      162,610 |
| Broad / 128 followers |         12,392 |    1,586,221 |
| No followers          |        178,840 |            0 |

Counts, byte totals, payloads, and final cleanup were validated with zero
overflows. These runs used the host's installed nightly and a disabled tracing
collector, unlike the explicitly stable final builds. They measure actor routing
and drains, not SSE parsing, sockets, total CPU, or origin latency. The next
performance evidence needed is a representative same-workload comparison of
embedded and proxy topologies with origin/proxy/client CPU measured separately.

## Revision anchors

- Origin source implementation: bus-smart `1a5d9f36`; single-event drain correction
  `31dde4de`; native source-client test `3b00b49b`.
- Router implementation `9b6062f6`; native-ID/lifecycle correction `0b110157`.
- Gateway HTTP timeout correction `f57a2882`; connection bounds `ad8bb5d2`;
  async lifecycle tracing `1db1dee8`.
- Real-client integration `0d3ab83a`; native schemas/cookies `dc5e1dd5`.
- Final safe source-failure classification and dependency lock: `9e037bef`.

These checkpoints preserve implementation and correction history; they do not
claim production rollout or acceptance based solely on passing tests.

## Layout follow-up verification — 2026-10-11

Session `ses_ed35c6048ffegHqs9lkaJQablf` verified the mechanical relocation at
`c93615f56262` and navigation update at `56cf231cf126`, on telemetry extraction
`6190889cf750` / `5fe62f1a3139` and handover predecessor `a1782536`.
Package/API names and package-relative fixture includes are unchanged.

In isolated workspace `/home/rektide/tmp-opencode/glowing-cactus-reorg`, Rust
1.98.0 passed:

```sh
cargo test --locked
cargo test --workspace --all-features --locked
cargo clippy --all-targets --locked -- -D warnings
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo fmt --all -- --check
cargo build -p opencode-bus-proxy --no-default-features --locked
OTEL_TRACES_EXPORTER=none bun run test/compatibility/clients.ts
```

The two real-systemd-manager tests remain intentionally ignored. Compatibility
used the real `/home/rektide/src/opencode-bus-smart` checkout through a temporary
sibling symlink, removed afterward. Its first invocation inherited an OTLP
endpoint and correctly failed local-only daemon startup; explicitly disabling
export made the full compatibility suite pass.

`cargo metadata --no-deps --locked` resolves all five packages in the requested
layout and retains four default members. Per-package `cargo tree --locked
--no-default-features` for protocol, router, gateway and daemon contains no
OpenTelemetry, OTLP, reqwest, Arrow or ClickHouse dependencies. All local Markdown
file links resolve; sibling-checkout links require that sibling. Wrapper build
and install scripts use Cargo metadata/package selection, not old crate paths.
The HTTP draft is byte-unchanged and the architecture proposal retains its
original proposed-path table with an explicit present-layout navigation note.

This verifies the isolated branch, not integration or deployment: primary was
still actively editing handover files, so its working copy was left untouched.

## Primary reintegration — 2026-10-11

The later explicit request to reintegrate was completed in primary at
`287d502e`, a merge retaining both `4bb205ed` (the committed handover/queue-policy
line) and `f46b85da` (telemetry extraction and layout follow-up). Primary was clean
before integration; no other author's uncommitted work needed checkpointing.

Eleven delete-versus-edit conflicts were resolved by carrying the latest primary
versions into `components/` and `cli/daemon`. A byte comparison against the
primary predecessor confirmed that every relocated file retains that version,
except the intentional telemetry manifest/module and host startup changes from
the telemetry branch. Startup retains the host's explicit ring TLS policy and
blocking telemetry initialization. The HTTP draft remains byte-identical and
the completion-checkpoint proposal remains unmodified. No ClickHouse feature,
exporter, deployment or history squash was introduced.

Integration-specific Rust 1.98.0 checks passed in primary:

```sh
cargo nextest run --locked
cargo nextest run --workspace --all-features --locked
cargo clippy --all-targets --locked -- -D warnings
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo fmt --all -- --check
```

Nextest passed 98 local-only tests and 99 all-feature tests, including exact and
boundary-drain loopback handover, queue-policy shrink/zero-credit restoration,
unavailable-OTLP startup and real OTLP request-span export/flush on SIGTERM.
The primary predecessor now contains three intentionally ignored real-systemd
manager tests; all three remain skipped. The prior isolated compatibility result
is not claimed as a new primary compatibility run.
