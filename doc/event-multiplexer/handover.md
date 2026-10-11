---
title: Cooperative SSE process handover
sources:
  - resource: urn:opencode:session:ses_ed36d5fdeffeQ2Ks5coMYJDORR
    title: Sol implementation, isolated replacement evidence, and buffered/boundary/queue-zero direction
---

# Cooperative SSE process handover

The gateway can replace its process while retaining **established supported SSE
responses and the same plain-HTTP upstream source**. This is a planned,
cooperative cut, not crash recovery. Do not use ordinary `systemctl restart`,
SIGTERM, or SIGKILL when asking for preservation.

## What transfers

- The listener, upstream TCP socket, and eligible downstream TCP sockets, by
  generation-scoped descriptor names rather than old FD numbers.
- Existing controlled/legacy subscription IDs, pending/active state, effective
  Session interests, queue order, opening frames, counters, and runtime queue target.
- Exact upstream HTTP chunk/length/close framing, unread userspace bytes, partial
  SSE decoder state, source epoch, and same-boot inactivity deadline.
- In buffered mode, dequeued downstream output and its exact written offset,
  including a partial response header, chunk header, payload, or chunk terminator.

The downstream adapter supports an authorized **first HTTP/1.1 GET** of
`/api/event` or `/api/experimental/event` on its connection, without a request
body, upgrade, or pipelined read-ahead. Ordinary HTTP, tunnels, HTTP/1.0, and SSE
responses reached through an already Hyper-owned/reused connection are explicitly
unsupported for transfer and are closed at the cut. A request-side half-close is
supported. HTTPS upstream state is **not** transferable: cooperative preparation
is rejected while the old process continues serving. An offline source is also
rejected. A failed source during preparation/import retires its epoch; it must
not masquerade as gap-free continuation.
The source adapter expects an uncompressed HTTP/1.1 SSE response with the existing
scope capability; chunked, content-length, and close-delimited bodies are handled.

Authenticated `GET /_bus/handover` lists connection-to-subscription associations,
capabilities/reasons, source capability, the observational routing snapshot, and
handover progress. It exposes no private payload bytes or credentials. Inspect it
before selecting a reset; not every SSE response is transferable.

## Two modes, one signal

Both stop accepting new work while leaving the listener open. TCP connects can
queue in the kernel, subject to its backlog and peer deadlines. Source intake and
all existing transport owners park; already-decoded publications and accepted
control commands settle before the router/image cut.

| Mode | Downstream cut | Tradeoff |
| --- | --- | --- |
| `buffered` (default) | Transfer exact queued and partially written bytes. | Preserves stalled supported clients too, subject to image/resource limits. Private image copies occur at the cut. |
| `boundary` (`unbuffered` alias) | With source/control admission stopped, drain existing output **and all already admitted backlog** to a clean chunk boundary. | A single bounded drain deadline applies to all clients concurrently. Stragglers are closed with an incomplete-response error; their IDs are retired. Surviving clients need no downstream payload image. |

Configure `BUS_PROXY_HANDOVER_MODE=buffered` or `boundary`, or use
`--handover-mode`. `BUS_PROXY_HANDOVER_DRAIN_MS` / `--handover-drain-ms` defaults
to 2000 and accepts 0–30000. Zero keeps only clients already at a clean, empty
boundary. This is not a timer for ordinary API execution. Source parser/read-ahead
state still transfers exactly in boundary mode; discarding it would create a
live-only gap. Pending controlled feeds remain pending after their ready frame
drains; any accepted activation/interest update is represented by the settled
router state. Unsupported control exchanges may fail before their HTTP reply,
even if their operation applied; callers should retry against the surviving ID.

**There is no continuous per-client payload copy in either mode.** Each writer
retains the router's shared immutable `Bytes` frame, a small chunk header, a static
trailer, and a write offset; vectored TCP writes join the segments. Buffered image
creation flattens pending segments only while parked. Boundary mode avoids that
flattening for survivors, but not normal incremental source decoding, kernel
buffers, or selected fan-out queues. Arrow, Polars, and rkyv are not required or
enabled by this mechanism.

## Enable in a systemd service

Requires Linux memfd sealing, `NOTIFY_SOCKET`, `busctl`, and a manager exposing
`org.freedesktop.systemd1.Service.DumpFileDescriptorStore` with permission to
inspect the service's own store. Verified against systemd 262. A barrier orders
notifications; **acceptance is separately verified by name and kernel identity**.

Use a **self-bound listener retained as `FDNAME=listener`**. Socket-unit-only
cooperative handover is not enabled in this first implementation. A nondefault
`BUS_PROXY_LISTEN` is supported: inherited address validation requires an exact
match, not the default port. Disable any conflicting socket unit deliberately.

An opt-in drop-in ships at
[`systemd/opencode-bus-proxy-handover.conf`](../../systemd/opencode-bus-proxy-handover.conf):

```ini
[Service]
Type=notify
NotifyAccess=main
Restart=on-failure
RestartSec=250ms
FileDescriptorStoreMax=2048
LimitNOFILE=8192
Environment=BUS_PROXY_HANDOVER_UNIT=opencode-bus-proxy.service
Environment=BUS_PROXY_HANDOVER_USER=true
Environment=BUS_PROXY_HANDOVER_SUCCESSOR=/usr/local/bin/opencode-bus-proxy
Environment=BUS_PROXY_HANDOVER_MODE=buffered
Environment=BUS_PROXY_HANDOVER_DRAIN_MS=2000
```

For a system service omit `BUS_PROXY_HANDOVER_USER=true`. Preserve your existing
upstream, listen address, username, credential source, and limits. Do not print or
copy passwords into command lines. `Restart=always` also works. Handover commits
by exiting **75**; do not configure a restart-prevent status for 75, or suppress
its on-failure restart with `SuccessExitStatus=75`. Choose a restart delay and
drain budget shorter than source/client deadlines, and account for start limiting.
Capacity needs at least **clients + source + manifest + retained listener**
(1027 slots at the default 1024 connection cap). No overlapping/stale `bp-*`
generation is accepted.

The successor path must be the executable that the unit will actually start.
Install a new binary by atomic rename, not by overwriting the running inode.
Before pausing, the old process probes that executable with
`--check-handover-format 1`; old/incompatible binaries are rejected. Changed
credentials, routing limits, origin, or relevant gateway configuration cause
explicit fresh-source fallback on adoption, not dishonest partial restoration.
Keep settings unchanged across a preservation demonstration. Future incompatible
image schemas must change the handover format version.

**Bootstrap:** an old incapable process cannot export its current clients. After
installing the new binary/unit configuration, one ordinary restart is necessary;
that first bounce disconnects old clients. Only subsequent cooperative resets
demonstrate preservation. Never send SIGUSR2 to an old binary that has not gained
this handler (the default signal disposition may terminate it).

After the deliberate bootstrap and source readiness:

```sh
systemctl --user show opencode-bus-proxy.service \
  -p MainPID -p FileDescriptorStoreMax -p NFileDescriptorStore -p StatusText
# Expect one retained listener before preparation and after successful adoption.
systemctl --user kill --signal=SIGUSR2 --kill-whom=main opencode-bus-proxy.service
systemctl --user show opencode-bus-proxy.service -p MainPID -p NFileDescriptorStore -p StatusText
```

`StatusText` and bounded lifecycle logs expose `quiescing`, `draining`, and `parked`
with kept/dropped/pending counts while HTTP acceptance is stopped. The authenticated
inspection view reports the last handover mode/counts once service resumes. The
main PID must change; a refusal is not a successful reset. Keep the same consuming
client HTTP response open across it, verify its ID/interests and post-cut events,
and verify the source socket did not reconnect. A listener-only PID bounce is not
SSE preservation.

## Live queue dial, including zero

Authenticated `PUT /_bus/queue` accepts exactly `{"frames": <u32>}` in a body of
at most 1024 bytes. Its router-owned update is atomic across existing and future
subscriptions. `GET /_bus/queue` returns the selected target and sampled gauges;
`/_bus/subscriptions` includes the target, actual backlog, direct-delivery slot,
and receive credit. Existing auth, setup-body timeout, and CORS rules apply.

```sh
# Use your existing private curl credential configuration, never a password argv.
curl --config /path/to/private-curl-config -X PUT \
  -H 'Content-Type: application/json' --data '{"frames":0}' \
  http://127.0.0.1:49372/_bus/queue
curl --config /path/to/private-curl-config http://127.0.0.1:49372/_bus/queue
```

Lowering the target **does not clear admitted frames**. Old occupancy can exceed
the target while it drains; new admission that cannot fit retires that subscriber
rather than silently losing events or blocking the source. Growing the target
immediately permits more waiting frames, still bounded by `--queue-bytes`.

At zero there is no new waiting backlog: one armed receiver credit can accept one
direct delivery. It is cancel-safe and byte-bounded. Opening/activation frames
have a single bounded handshake slot so zero does not reject every new feed.
Already dequeued output remains separate from waiting backlog. Another publication
before the credit is consumed/rearmed fails that subscriber explicitly; even a
fast socket can be disconnected by a scheduling burst. Zero is an aggressive
delivery policy, not an unbounded hidden one-frame-per-publication queue. The same
semantics are available at startup with `--queue-frames 0`.

Both image modes preserve the selected runtime target, even if it differs from
the startup default, and restore receive ownership before resuming upstream.
Shrink-in-progress backlog and granted-but-not-dequeued delivery survive buffered
import; boundary drains them before transfer. A later **unplanned crash** gets
startup defaults, not the consumed runtime image; this dial is not persisted
configuration.

## Transaction and failure contract

The private version-1 JSON/base64 envelope has a SHA-256 integrity check, same-boot
identity, compatibility digest, and 256 MiB limit. Fan-out amplification is
budgeted before copying queues/output. The memfd is sealed against writes/growth/
shrinkage; readers use explicit offsets. Checksums detect corruption, not an
untrusted service-manager replacement.

Descriptors are offered individually with unique names and `FDPOLL=0`, manifest
last. The manager's accepted names **and exact device/inode identity** are checked
before the old process exits without socket shutdown, terminal HTTP chunks,
subscription retirement, or telemetry flushing. The successor invalidates all
transient entries and verifies their removal **before any resumed I/O**; a later
crash can inherit only the listener, never replay a stale offset/queue image.

Preparation refusals leave the old generation serving. A canceled/abandoned
preparation lease resumes parked owners because custody remains with the gateway.
Unsupported or boundary-straggler connections already closed during preparation
cannot be resurrected on abort. If transient-store cleanup cannot be verified,
owners stay parked rather than resume against a stale restorable image. Resolve
that exceptional case by an explicit stop (clears the default restart-preserved
store) and cold start, accepting client/source refresh. Do not override store
preservation to `yes` without providing deliberate cleanup.

Missing, corrupt, incompatible, or misnamed adoption material is invalidated,
inherited SSE/source descriptors are closed, and a fresh source epoch is opened.
If invalidation itself cannot be verified, startup refuses resumed I/O. There is
no automatic retry with a stale image and no exactly-once, crash-continuous, or
power-loss recovery claim.

## Isolated verification

```sh
cargo test --workspace --locked
cargo test -p opencode-bus-proxy --test handover_systemd --locked -- --ignored
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all -- --check
cargo build --locked
bun run test/compatibility/clients.ts
```

The explicit systemd suite creates only disposable `obp-handover-test-*` user
units; it never resets an installed live service. Both modes replace the process
twice with the same consuming response, controlled ID/interests, runtime zero
target, and upstream TCP socket. It checks store consumption before I/O and a
subsequent crash's fresh epoch. Capacity refusal retains the old PID/client.
Gateway tests additionally cover multi-MiB partial downstream output, pending and
legacy feeds, every HTTP chunk framing cut, decoder fragments, descriptor
reordering/missing/corrupt state, lease abandonment/cancellation, queued new TCP
connects, unsupported HTTP/tunnels, and boundary timeout/explicit straggler errors.
Router tests cover zero credit/bytes/bursts, shrink/grow, frozen updates, ordered
import above a lowered target, and granted delivery import. See the
[original inventory and transfer reasoning](../../design/event-multiplexer/active-client-handover.md).

### Recorded checks — 2026-10-11

- Workspace: 97 tests passed, three systemd tests deliberately ignored by default;
  the separate explicit systemd run passed all three. Both handover modes reset
  the same binary into a new PID twice; cross-version/live installed upgrades are
  not claimed by this isolated run.
- Clippy (all targets, warnings denied), Rustfmt check, debug build, and optimized
  build passed. Release output is isolated at
  `/home/rektide/tmp-opencode/misty-garden-obp-target/release/opencode-bus-proxy`;
  the live symlink's target was not overwritten or restarted by this work.
- Native unchanged-client compatibility passed against
  `/home/rektide/src/opencode-bus-smart`, using the optimized proxy. The direct
  README command initially failed because this relocated wrapper checkout lacks
  its expected `../opencode-bus-smart` sibling. The **unchanged test script** was
  copied to `/home/rektide/tmp-opencode/misty-garden-compat-6oqozw8d/proxy/test/compatibility/clients.ts`,
  with scratch symlinks for that sibling and the proxy fixtures; run with
  `BUS_PROXY_BINARY` set to the isolated optimized executable. No native code or
  dependencies were changed. It passed real Bus/ordinary source, additive scope,
  all five selective types, broad RPC, interest replacement, direct smart client,
  authentication, HTTP forwarding, and legacy/native client checks.
- `systemd-analyze verify` passed the repository's existing service/socket preset;
  unrelated host-unit deprecation/permission warnings were emitted. The opt-in
  drop-in is an installation example, not a live configuration change.
