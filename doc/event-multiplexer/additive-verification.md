# Ordinary scoped event migration — verification record

2026-10-10. This record follows the
[initial wrapper-based implementation](verification.md). The current decision is
[additive scope on ordinary OpenCode events](../../design/event-multiplexer/additive-events.md).

## Landed direction

- One upstream `GET /api/event`, negotiated by `OpenCode-Event-Scope: 1`.
- Ordinary `server.connected` starts the connection epoch.
- Ordinary business events carry authoritative `scope`; no `event-source.*`
  wrapper or nested `event` payload.
- Existing controlled GET/interest PUT remains the client control interface.
- Native Protocol and Rust share a versioned selection/conformance fixture.

The work is deliberately split across both repositories. Sol medium researched
native simplifications and implemented the shared native producer; another Sol
medium adapted the Rust protocol and gateway. Astra owns the contract, integration
harness, operating documentation, and final verification. A separate Sol medium
review checked the contract and identified the need for raw-JSON shared fixtures
and precise SSE preservation wording.

## Rust verification completed

At migration implementation `a0ee42b7`, cleanup `d20a6dc1`, crate docs
`fd5d8f1b`, shared raw-wire conformance `208ca751`, and browser capability
exposure `f3f204e2`:

| Check                                                                    | Result                                        |
| ------------------------------------------------------------------------ | --------------------------------------------- |
| `cargo +1.98.0 test --workspace --locked`                                | 58 passed: protocol 23, router 22, gateway 13 |
| `cargo +1.98.0 clippy --workspace --all-targets --locked -- -D warnings` | Passed                                        |
| `cargo +1.98.0 fmt --all -- --check`                                     | Passed                                        |
| `cargo +1.98.0 build --release --locked`                                 | Passed                                        |

Tests cover negotiated capability, first/duplicate connected markers, scoped
authority versus payload fields, raw whole-event retention, future broad types,
scope validation and duplicate keys, gap invalidation, native client controls,
HTTP/auth/upgrade compatibility, obvious self-target rejection, and two-proxy
chaining without accumulating wrappers.

## Cross-language evidence

The real-client harness passed against the release binary with an ordinary upstream GET,
the added scope on both direct and proxied delivery, browser-visible capability
headers, a retired private source route, and native/Rust fixture equality. It also validates old
native event-definition decoding (scope ignored) and updated public-schema
decoding (scope retained). One legacy and two controlled proxy readers shared
one origin `/api/event` request, alongside a direct native controlled reader.
Pairing cookies, ordinary API forwarding, all five focused event types, broad
RPC delivery, and in-place interest replacement also passed.

The shared fixture contains **33 raw wire cases: 18 accepted and 15 rejected**,
including duplicate and escaped keys, null versus omission, global/empty/multiple
Locations, workspace identity, every focused type, and multiline data over CRLF
SSE. Rust validates routing and exact decoded-data preservation; the native check
uses JSON.parse, EventScope decoding, and the native classifier. Both copies are
byte-identical. Native fixture revision: `56a72f22`.

## Native verification completed

The native [implementation report](../../../opencode-bus-smart/design/event-multiplexer/additive-events-implementation.sol.md)
records the shared owner, ordered barriers, scope projection, metric accounting,
failure recovery, generated surfaces, and individual implementation checkpoints.

| Check                                                           | Result                                           |
| --------------------------------------------------------------- | ------------------------------------------------ |
| Protocol event tests                                            | 10 passed                                        |
| Server event, HTTP, move, metric, and deterministic queue tests | 46 passed                                        |
| Core Bus routing and Session move tests                         | 29 passed                                        |
| Focused client event-scope and controlled-feed tests            | 15 passed                                        |
| Protocol, Server, Client typechecks                             | Passed                                           |
| Client regeneration and repeat-generation check                 | Passed; generated artifacts idempotent           |
| Final native root `bun run check`                               | Passed; warning-only lint and package typechecks |

Final root-check receipt:
`/home/rektide/tmp-opencode/bus-smart-additive-final-check.log`.

The broader client run reported **253 passed, 6 skipped, 4 failed**. The three
older Session/integration fixture failures retain their baseline evidence in the
[first implementation record](verification.md). The additional import-boundary
assertion expects an eager Protocol import from the Effect service entrypoint;
it finds zero both now and on isolated pre-migration `8f7a9dd8`, using identical
dependencies and invocation. The native report at `4990ce92` records the graph
comparison and baseline reproduction. The full client suite is not claimed green.

## Review and ordering evidence

Independent review found no concrete implementation blocker in queue/control
ordering, generation failure, cancellation, authoritative projection, encoding
sharing, or the public schema extension. It did identify scheduler-dependent
test coverage. Commit `fff758d0` closes that gap using the real Effect 4.0.1
scheduler dispatcher, with no production scheduling hook:

- Hold the pump, queue publication → replacement → publication, and prove the
  acknowledgment waits while the two publications use old/new interests.
- Queue registration behind a publication and prove the earlier input is not replayed.
- Interrupt queued registration and replacement before execution; prove cleanup
  and unchanged prior interests.
- Establish pending callers before encoding, oversized-frame, and ingress
  failure; prove settlement and immediate healthy recovery.

Shared raw-wire fixtures also close the review's cross-language parsing gap, and
the contract explicitly bounds raw preservation at the decoded SSE data boundary.
The final release integration run passed after browser header exposure was aligned.

## Interpretation

This migration simplifies the wire model and removes an infrastructure route.
It preserves the placement of fan-out in the proxy and introduces shared native
production for direct clients. The prior router microbenchmark still describes
the indexed admission engine; it is not a measurement of the new native pump,
added scope bytes, or whole-deployment CPU benefit.

The parent also ran the revised native benchmark with live metrics. It completed
all four scenarios and waits for `flush`, so the measured interval includes the
pump's admission work. Medians of repetitions 1–6 (discarding warm-up repetition 0):

| Scenario   | Publications | Clients |  CPU ms | Wall ms |
| ---------- | -----------: | ------: | ------: | ------: |
| Idle       |      100,000 |       0 | 207.029 | 164.461 |
| Unfollowed |      100,000 |       1 | 217.419 | 175.798 |
| Indexed    |       32,768 |     512 | 219.644 | 176.759 |
| Broad      |        4,096 |      32 |  39.278 |  25.535 |

Receipt: `/home/rektide/tmp-opencode/bus-smart-additive-benchmark.jsonl`.
These are a new synthetic baseline, not an improvement claim: they include shared
queue/pump and revised metric accounting, and idle/unfollowed traffic still pays
that machinery's cost. The predecessor measured a different producer structure.
A comparable production-topology before/after run is still needed for CPU claims.

## Later discovery work (2026-10-11)

[Native discovery verification](discovery-verification.md) complements this
transport record: publisher contact overrides, daemon endpoint enrichment and
explicit stomp route unchanged native clients through the same forwarding
gateway. It re-runs this real-client harness and adds isolated service discovery
checks; it does not replace the above event-delivery or performance evidence.
