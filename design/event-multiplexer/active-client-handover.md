---
title: Active-client upgrade handover registry
status: proposal
sources:
  - resource: urn:opencode:session:ses_ed3b65203ffebXKv17uH4LiZrB
    title: Memfd descriptor manifest and active-client upgrade direction
---

# Active-client upgrade handover registry

**Handover** names the coordinated transfer of live ownership. The **registry**
describes the material and its owners; a **handover manifest** maps named
descriptors and sections; a **state image** holds serialized semantic state and
buffers. rkyv is an optional state-image encoding. Arrow/Polars exports and
reactive tracing adapters can share the registry's data model; their encoding
does not determine the ownership-transfer protocol.

Use a **versioned memfd manifest plus named socket descriptors** to transfer a
quiesced gateway generation. Keep the live registry in memory; write the transfer
image once per planned upgrade, after every participating owner has paused at the
same logical cut. Subscription metadata is mostly slow-changing. Data-plane
buffers change on every read, publication, dequeue, and write.

This is a proposed inventory and transfer contract, not an implemented exporter.
Current fdstore support preserves only the listener. See
[operations](../../doc/event-multiplexer/operations.md#descriptor-store-restart-continuity-without-a-socket-unit).

## Registry of material

Each row declares what the successor needs, how often it changes, and where the
current implementation owns it. “Rebuild” means reconstruct from transferred
semantic state rather than copying Rust objects or memory addresses.

| Material | Handover representation / disposition | Changes when | Current owner / gap |
| --- | --- | --- | --- |
| Manifest envelope | Format version, required features, generation ID, producer version, source/config compatibility identifiers, snapshot clock/boot identity, section lengths and integrity metadata | Once per upgrade | New coordinator; no manifest exists |
| Listening socket | Named FD, socket family/type/listening checks and bound address | Bind/adoption | Daemon `systemd.rs`; already retained |
| Downstream connection identity | Unique connection key, named FD, role (`sse`, HTTP, tunnel), local/peer addresses, protocol, lifecycle/half-close state, subscription or exchange reference | Accept, role transition, close | Gateway `serve`; sockets move into Hyper, no persistent registry |
| Subscription semantics | Existing `evsub_…` ID, source epoch, legacy/controlled mode, pending/active state, effective session interests, connection key | Subscribe, first interest PUT, replacement PUT, source reset, disconnect | Router `Subscriber`; connection association must be added |
| Subscription backlog | Ordered references to complete SSE frame bytes; preserve any opening `ready`/`connected` frame still queued | Every admission/dequeue | Router `Queue`/`Backlog`; inspection reports counts only |
| Shared frame arena | Exact frame bytes, referenced by queue entries; immutable blob ranges can preserve fan-out sharing | Publications and last-reference release | `Bytes` allocations; serialize contents and references, never pointers |
| Downstream SSE output | Response headers/framing phase, frame removed from router but not fully written, encoded output bytes and precise unsent offset; inbound read-ahead if any | Each body poll and socket write/read | Hyper connection plus stream closure; no resumable export/import boundary today |
| Upstream event transport | Named source socket FD, established HTTP response framing/read-ahead, unread body bytes, HTTP state; TLS state if HTTPS | Each read / transport transition | Pooled Hyper/rustls client; no established-stream export/import today |
| Source decoder and admission | Epoch/readiness, partial SSE line/data, BOM/CRLF flags and frame accounting; decoded batch, chunk remainder and any publication in progress | Each source read, decode and router admission | `source_connection` locals and `SseDecoder`; settle in-flight publication or capture its exact disposition |
| Ordinary HTTP exchanges | Both transport sides, request/response framing, buffered body/trailers, operation phase and outcome | Every exchange/read/write | Hyper server/client and `forward`; drain before cut or require an explicit resumable adapter |
| Upgraded tunnels | Downstream and upstream FDs, protocol, unread upgrade bytes, both directional copy buffers/offsets, EOF and half-close progress | Every read/write/half-close | Hyper `Upgraded` and `copy_bidirectional`; explicit buffer ownership needed |
| Timing | Source last activity/deadline, pending setup/header deadlines, reconnect state if offline; SSE heartbeat schedule | Activity, timeout, retry | Local Tokio timers; encode same-boot monotonic deadlines, recreate timers; heartbeat phase may reset |
| Routing/index structures | Rebuild active/legacy sets and session index from subscriptions; restore queue limits and occupancy | Interest/lifecycle/queue changes | Router `Actor`; no need to serialize redundant indexes |
| Admission and task machinery | Rebuild connection permits for every inherited client (including tunnels), queue accounting, cancellation/notification objects and tasks | Connection/task lifecycle | Semaphore, `JoinSet`, `TaskTracker`, router channels; tasks themselves are not transferable |
| Config and authorization context | Compatible routing/limit settings and established-request authorization policy; successor reloads credentials from normal source | Startup/config change; request authorization | Gateway `Config`, daemon credential loading; do not put passwords in inspection output |
| Observability | Optional counters snapshot and generation linkage; rebuild gauges and tracing spans | Every event/request | Gateway metrics and router counters; not delivery-critical |
| Kernel-owned bytes | Remain attached to the socket: receive/send queues, TCP sequence/retransmission state | Continuously | Kernel; retain FD, do not serialize or resend these bytes |

Current routing retains effective session membership, not the whole interest PUT
body. Locations/profile are not separate mutable subscription state in this
implementation. Transfer the actual routing semantics and version their meaning.

The existing `/_bus/subscriptions` response is an observational sample, not a
checkpoint: socket tasks can dequeue while the actor samples queues. A future
registry inspection view should show role, owner, transfer capability, buffer
sizes and blockers. The private transfer image additionally contains exact event
bytes; exposing those through the inspection view is unnecessary.

## Descriptor and section identity

Proposed image layout (field names illustrative, not yet a wire schema):

```text
manifest
  format_version, required_features, generation, producer_version
  boot_id, monotonic_cut, config_compatibility
  descriptors[]
    key, fd_name, kind, connection_key?, expected_socket_properties
  connections[]
    key, role, descriptor_refs, protocol_state_ref, subscription_id?
  source
    epoch, transport_ref, decoder_ref, pending_admission_ref, deadline
  subscriptions[]
    id, connection_key, mode, active, sessions[], ordered_frame_refs[]
  blobs[]
    key, offset, length, encoding, integrity
  optional_metrics
```

Give each passed socket a unique `FDNAME`, such as `bp-g7-down-c42`; give the
manifest `bp-g7-manifest`. Registry references resolve names through
`LISTEN_FDNAMES`, never old numeric FDs. Numbers change on adoption. One notify
message uses the same `FDNAME` for every attached FD, so distinct names require
separate messages. Validate unique names, complete references, descriptor types,
section bounds, compatible protocol versions and limits before starting I/O.

For an initial format, put metadata and a byte arena in one memfd. Create it with
sealing enabled, finish writing at the cut, remove writable mappings and seal
against writes/growth/shrinkage before publishing it. Read via explicit offsets
(`pread`), since passed descriptors share the underlying open-file offset. Larger
images can later use separately named blob memfds without changing logical refs.
This is same-host process-restart state, not persistence through power loss.

## How often to update it

**Recommended first contract: planned upgrade, snapshot on demand.**

- Update the in-memory descriptor registry on accept, role change and close.
- Update semantic subscription records on subscribe/interest change/retirement.
- Keep hot buffers with their current owners during normal service. No periodic
  memfd rewrite is needed for planned handover.
- Pause those owners, establish the cut, and serialize one immutable generation.
  Socket buffers may continue to fill while userspace is paused; TCP backpressure
  applies, so the pause must respect peer timeouts and buffer limits.

**Crash-continuous recovery is a different consistency contract.** Periodic
snapshots can be stale relative to socket reads/writes: replaying an old output
offset duplicates bytes, while omitting bytes consumed from the socket loses
them. Even updating a memfd on every operation leaves a crash window between the
syscall and checkpoint update. That requires a surviving I/O owner or another
explicit crash-consistency mechanism, not just a faster snapshot interval.

## Coordinated cut and adoption

1. Preflight successor compatibility and descriptor capacity. Enter a distinct
   upgrade path: normal shutdown cancels epochs, clears queues and ends HTTP
   bodies, so it cannot produce this image.
2. Stop admitting new work. Settle finite HTTP/control exchanges, including their
   response writes, or identify them as blockers. Stop the source at an owned
   boundary and finish any already-decoded publication batch. Drain accepted
   router commands to a barrier; do not serialize oneshot reply channels.
3. Park every source/client/tunnel I/O owner and the router. An idle peer must
   not prevent reaching the cut; capture a partial frame when necessary. Account
   for bytes already dequeued from a router queue but held by a stream/Hyper.
4. Export named FDs and the sealed manifest for this generation. Publish the
   manifest last and synchronize notification processing. Require complete
   descriptor membership on adoption; a manifest is not proof that all stores
   succeeded.
5. Old owners relinquish without `shutdown(2)`, terminal HTTP chunks, router
   retirement, or writes after the cut. Merely duplicating FDs does not revoke
   old ownership. The successor imports state, rebuilds runtime machinery and
   resumes with exactly one reader/writer per direction.
6. Remove consumed transient store entries and invalidate this image before
   resumed I/O can make it stale. A later crash must not reuse it. Keep listener
   retention separately managed. Preparation failure resumes the old owners;
   failure after their exit uses explicit connection failure/recovery rather
   than guessing at partial state.

Preserve the upstream stream as well as downstream streams for gap-free proxy
upgrades. Reconnecting the live-only origin creates an observation gap even if
every client socket survives. If the origin actually restarts or the source
fails during adoption, retire the old epoch and use the existing bounce/refresh
behavior. Do not carry stale IDs into a newly opened source epoch.

## Concrete implementation blockers and next shape

- **Named multi-FD adoption:** `systemd::requested` currently accepts exactly one
  FD and `activated_fd` always adopts FD 3. Replace this with typed named adoption
  before adding any manifest to the store.
- **Resumable transport ownership:** raw sockets cannot be fed back into a new
  Hyper HTTP parser in the middle of an SSE response. Define explicit established
  SSE transport adapters with serializable framing/buffer state, on both sides.
  HTTPS source/TLS and general HTTP exchange migration remain separate transport
  capabilities until an actual import/export strategy exists.
- **Router freeze/export/import:** add a non-retiring freeze path, queue contents,
  connection associations and reconstruction preserving existing IDs and pending
  activation. Normal `Subscription::drop` closes and clears its queue.
- **Tunnel ownership:** replace opaque copying with explicit per-direction state
  before claiming tunnel handover.
- **Registry inspection:** enumerate every live material owner with
  `transferable`, `drain-required`, or `unsupported` and its reason. This makes
  upgrade readiness observable before committing to the transfer.
- **Manager transaction:** increase `FileDescriptorStoreMax` for all sockets plus
  memfds, including both ends of tunnels and any overlapping generations. Specify
  cleanup, store verification and restart behavior before enabling handover.

Systemd notifications are not a transactional object store: `sendmsg` success
only means enqueue, and `BARRIER=1` confirms processing, not acceptance of every
descriptor. Default fdstore polling removes sockets on HUP/ERR; select deliberate
polling/cleanup behavior for half-closed transferred transports. `FDNAME` is a
label, not an atomic replacement key. Generation names and explicit removal
avoid accidentally mixing old and new material.

Verification should interrupt handover at each ownership transition and exercise
partial chunk writes, source decoder fragments, queued opening frames, interest
PUT races, stalled clients, tunnel half-closes, descriptor reordering/missing
entries, incompatible manifests, repeated upgrades, and origin loss during the
pause. The critical invariant is byte-for-byte continuity with no double reader,
double writer, replayed kernel bytes or lost userspace bytes.

## Sources

- Current implementation: [daemon/systemd](../../crates/daemon/src/systemd.rs),
  [gateway](../../crates/gateway/src/lib.rs),
  [router](../../crates/router/src/lib.rs),
  [SSE decoder](../../crates/protocol/src/sse.rs).
- [systemd sd_notify contract](https://github.com/systemd/systemd/blob/main/man/sd_notify.xml):
  FDSTORE, FDNAME, FDSTOREREMOVE, FDPOLL, BARRIER and notification return semantics;
  retrieved 2026-10-11. It explicitly recommends memfd for application state.
