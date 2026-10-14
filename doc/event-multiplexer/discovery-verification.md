---
type: Verification
title: Native proxy discovery implementation and verification
description: Source ownership, changed capabilities, verified tests and unactivated deployment boundary.
resource: file:///home/rektide/src/opencode-bus-proxy/doc/event-multiplexer/discovery-verification.md
status: verified-source-only
generated: { by: "model:openai/gpt-6.1-sol", at: 2026-10-11 }
sources:
  - { resource: "urn:opencode:session:ses_ed3553a53ffeOaUVN5FYO8116p", title: lucky-rocket implementation and tool results }
  - { resource: "urn:opencode:session:ses_ed3ebd410ffehOKUhZcNU2hHYk", title: Parent human direction }
  - { resource: "urn:opencode:session:ses_ed34e6bc3ffenVHwvNdLP7POnL", title: Forwarded FX watcher findings }
  - { resource: "file:///home/rektide/ado/patches.md", title: Existing patch manifest and server-reg ownership }
  - { resource: "file:///home/rektide/src/opencode-bus-proxy/test/compatibility/discovery.ts", title: Real daemon and unchanged native-client fixture }
---

# Native proxy discovery — source-only receipt

## Owned checkpoints and preserved predecessors

| Repository | Immutable checkpoint | What it establishes |
|---|---|---|
| opencode-bus-proxy | `60919a44` | Initial unchanged-OpenCode design; retained as provenance after user superseded that constraint |
| opencode-bus-proxy | `c860ad85` | Predecessor daemon contribution, origin health identity and guarded atomic restoration |
| opencode-bus-proxy | `6a9358cb` | Cooperative default markup, explicit proxy stomp, supervised reused Effect watcher, native discovery harness |
| opencode-bus-proxy | `f232d0da` | Compatible edits/partial save recovery retaining our token remain reconcilable |
| opencode-server-reg | `e6c06c7050de` | Existing server-reg baseline on pwfile, before this adaptation |
| opencode-server-reg | `dff6b9dd` | New native OPENCODE_PROXY_URL/--publish-url selection and default-on metadata |
| opencode-server-reg | `13157d2e` | Existing retention test now waits for native readiness; server boot behavior unchanged |
| compfuzor | `705d05fc` | Shared opt-in config, cooperative/stomp selection, Effect bridge wiring, removed shell route, isolated render tests |
| archive/doc | `a9d60c07` | Existing server-reg manifest adapted in place with selective hunk checkpoint; other authors' edits preserved |

The server-reg float advances within its own owner; dated server-reg-20261010
stays on its predecessor. No OpenCode source was edited in opencode-working,
composed onto working, or rebuilt. Proxy compilation produced test binaries
only; no install/bookmark advancement of the deployed proxy main occurred.

## Verification actually run

| Check | Result |
|---|---|
| Proxy `cargo test --workspace --locked` | **88 pass**: protocol 23, router 22, gateway 13, daemon 30 |
| Proxy `cargo clippy --workspace --all-targets --locked -- -D warnings` | Pass |
| Proxy `cargo fmt --all -- --check` | Pass |
| Proxy `cargo build --locked` | Pass, development test binary only |
| `bun run test/compatibility/clients.ts` | Pass: real Bus, one ordinary uplink, legacy/controlled clients, HTTP/auth forwarding |
| `bun run test/compatibility/discovery.ts` | Pass: real daemon + unchanged Promise/Effect service clients + real Effect watcher + native server-reg publisher |
| Native focused CLI service/publication suite | **44 pass**, 193 expects |
| Native full CLI suite with log env unset and approved TMPDIR | **351 pass, 9 skip, 0 fail**, 1320 expects across 50 files |
| Native canonical root `bun run check` | Pass: warning-only lint, **36 successful typecheck tasks** |
| Native CLI `bun typecheck` | Pass |
| Both compfuzor playbook syntax checks | Pass |
| `python3 tests/integration/opencode_proxy_discovery.test.py` | Pass: real local-connector renders, all outputs isolated, no state publication/activation |

The native full CLI command was:
`env -u OPENCODE_LOG_LEVEL -u OPENCODE_PRINT_LOGS TMPDIR=/home/rektide/tmp-opencode bun test --timeout 30000 --only-failures`
from packages/cli, not the repository root. Canonical root-check output was
retained by the harness at
`/home/rektide/.local/share/opencode/shell/2b217d3be908b7aa518f5eb3bd0bd2a40ee7963f/sh_12cb629ee002Sc4sMGMNq5sS8c.out`.

## Specific evidence and boundaries

- Registration retains origin PID/version/id/password. Replacing its PID with
  the real daemon PID makes unchanged native discovery fail as expected.
- Publication metadata is accepted by the unchanged Effect decoder/Promise
  reader, contains one password only, and retains unknown compatible fields.
- Native precedence is --publish-url > OPENCODE_PROXY_URL > listener. Managed
  publication remains direct; enrichment can be disabled separately.
- Cooperative mode enriches without overriding the native contact. Explicit
  stomp really rewrites eligible native publications, including overrides and
  repeated same-file/atomic-replace writes, retaining other known proxy URLs.
- Mock origin restart changes identity; forwarded info and the fresh record
  agree and both native client styles discover the proxy. This is an isolated
  identity/republication oracle, not a live OpenCode restart measurement.
- Partial JSON is untouched; remove/recreate resumes contribution without
  inventing a missing record. Own writes remain mtime-quiescent. Compatible
  edits containing our marker do not strand graceful restoration.
- Source health and forwarded info gate claims. Managed/foreign/corrupt records,
  symlinks, size violations and live foreign contributors are refused.
- Default Effect bridge uses the actual established FileSystemWatchClient,
  parent-directory filtering/coalescing, and native watcher options. Disabled
  and failed helpers demonstrably fall back to polling. No Rust FFI clone or
  new Effect watcher library patch was made.
- Graceful stop restores only the exact last contribution, preserving a native
  proxy override or restoring a legacy direct URL. Changed/deleted slots remain
  untouched; dead-marker recovery is covered by daemon unit tests.

An existing publisher test initially observed HTTP 503 after the registration
appeared. It assumed onListen meant full boot readiness; `13157d2e` fixes the
test to use native discover's wait boundary. No source boot behavior was changed.
Existing older compfuzor instance/status tests have unrelated stale fixtures;
the new scoped proxy render suite does not edit or claim to repair them.

## Remaining operator decisions, not implementation blockers

Compose/accept the new server-reg source and select/install the new daemon build
before generating active opt-in config. Default cooperate then works with native
OPENCODE_PROXY_URL; explicitly select stomp for older/forced publication policy.
The bridge requires Bun and an effect-watch-capable module checkout. Custom
OPENCODE_DISCOVERY_FILE must match the publisher's actual channel/XDG path.
Credential rotation needs coordinated origin/daemon start. Single-slot native
rename does not share the proxy lock: final-check/rename has a documented
residual non-cooperating writer race, not transactional fencing.

No live service, client, binary, registration, or OpenCode build was changed.
The user-requested post-completion work anchor is **ocbp-native-discovery** in
this repository's native proxy tracker (`bd show ocbp-native-discovery`). It is
closed for the verified source scope, with implementation checkpoints, current
homes, all three session IDs and deferred activation decisions. Its selective
export checkpoint is `bbacf23c`; unrelated ticket edits were preserved.
Deployment remains explicitly outside this completed source scope.

## Connected homes

- [Operating contract](operations.md#native-local-discovery): maintained modes,
  ownership, watcher and shutdown behavior supported by this receipt.
- [Native publication](../../../opencode-server-reg/doc/server-reg/publication.md):
  existing server-reg adaptation, not a duplicate OpenCode feature line.
- [Additive event verification](additive-verification.md): complementary source
  delivery evidence, unaffected by discovery/contact selection.
- [Initial design and superseding direction](../../design/event-multiplexer/native-discovery.md):
  preserved reasoning for the identity choice and runtime tradeoff.
