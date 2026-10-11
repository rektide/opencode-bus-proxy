---
title: Completion signals and application checkpoints for handover
status: proposal — research only
sources:
  - resource: urn:opencode:session:ses_ed3b65203ffebXKv17uH4LiZrB
    title: Human direction — investigate completion checkpoints after frame tuning
  - resource: urn:opencode:session:ses_ed33ccef3ffefNULPTP6v370r9
    title: Astra native-code investigation and protocol design
---

# Completion signals and application checkpoints

**Recommend a small origin-to-proxy publication barrier first, optionally preceded
by a scoped work-admission fence.** Compose that with the existing buffered or
boundary handover. Add negotiated client apply acknowledgments only when the
operator needs proof that particular clients have caught up. Call each result by
its achieved level; never report “all responses back” from session idle alone.

This proposal follows the implemented and isolated-verified
[handover mechanism](../../doc/event-multiplexer/handover.md). It changes no runtime
behavior and does not authorize a live rollout. Exact transport transfer already
allows a proxy handover during an active model response. Application quiescence
improves the chosen moment and its explanation; it is not needed to make the
existing exact-byte transfer correct.

## Grounded native seams

Inspected 2026-10-11: `/home/rektide/src/opencode-bus-smart`, clean Jujutsu working
copy `746a9e3c0b4237f679b3fa491c0a20bc73387658` (parent `d26a070a`). Paths below
are relative to that checkout, not claims about every released OpenCode server.
The published [V2 API reference](https://opencode.ai/v2/docs/api) also lists session
wait, active sessions, session log, and event subscription. The local branch adds
controlled feeds and is the concrete interoperability target here.

| Seam | Observed implementation | Actual meaning |
| --- | --- | --- |
| `packages/core/src/session/execution.ts:111-147` | Coordinator publishes `session.execution.succeeded`, `.failed`, or `.interrupted`, one terminal observation per busy period covering coalesced drains | Execution settlement with an outcome, not necessarily successful model output; not whole-server inactivity |
| `packages/core/src/session/session.ts:265-268`; `run-coordinator.ts:169-175` | `Session.wait` calls `awaitIdle`; it rechecks after a done signal to follow successors | No active execution for that key when observed; fresh admission can immediately invalidate it |
| `packages/core/src/bus.ts:499-549,265-290` | Durable per-aggregate locks; awaited routed observers; live PubSub; routing captured at publication | No single global durable sequence, no socket/client acknowledgment. Observer defects are isolated |
| `packages/server/src/event-feed.ts:49-50,155-179,251-273,312` | `flush` is a control command in the same ingress FIFO as publications; Bus observer only enqueues raw publications | Earlier ingress commands have been processed; not source SSE bytes read by proxy. It cannot cover a producer still upstream of ingress |
| Same file `149-153,180-242` | Overflow retires subscribers; encoding failure invalidates the feed generation | A successful later control cannot retrospectively certify an earlier failed delivery; barrier needs generation/failure tracking |
| `packages/protocol/src/groups/event.ts:125-174` | Typed event union plus ready frame; interest PUT acknowledgment explicitly means ordered admission | Unknown marker variants cannot simply be injected into unchanged clients |
| `packages/client/src/solid/controlled-event-feed.ts:81-85,120-154` | Client `flush` settles installed interests | Interest installation, not flushing event application |
| `packages/client/src/solid/connection.ts:86-95,147-165` | Parsed events enter `pending`; timer batches `onEvent` (default 10 ms) | Parsing a marker is earlier than applying preceding events |
| `packages/core/src/bus.ts:873-915`; `packages/schema/src/event.ts:16-24` | Per-aggregate log replay watermark and `log.synced`; sequence belongs to one aggregate | Useful reconciliation seam, not a global source or delivery cursor |

Important durability limit: `Bus.configured` defaults `persist` to false
(`bus.ts:195-196,221,446,963`). Durable projections/sequences do not themselves
guarantee retained event payloads. Deployment must prove log retention before
offering replay-based recovery. Ephemeral deltas/plugin activity also need their
own recovery story. The source feed remains volatile by contract.

## What exactly completed?

Expose independent facts, not one ambiguous `complete` boolean:

1. **Model step finished:** a provider stream ended. Tools, retries, additional
   steps, compaction, children, or queued user input may remain.
2. **Scoped execution settled:** named busy periods ended with explicit outcomes.
   Background children, shell/PTY processes and detached plugin work are excluded
   unless expressly enrolled. Permission/form waits are blockers, not success.
3. **Work admission fenced:** requests racing the fence are either accepted into
   its cohort or rejected for retry. A list of idle sessions without this fence
   is only an observation.
4. **Operations settled:** admitted HTTP/RPC/control operations finished or have a
   recorded result. Distinguish handler completion, response socket write, and
   caller acknowledgment. SSE and tunnels are not finite HTTP exchanges to drain.
5. **Source published through B:** all enrolled pre-fence publications reached the
   source feed's ordered marker. Seeing an HTTP response from the origin does not
   establish that the proxy has read that marker on its independent SSE socket.
6. **Proxy routed through B:** source prefix processed, each selected publication
   admitted to its eligible subscribers or an explicit retirement recorded.
7. **Socket-written through B:** userspace wrote the relevant prefix to the kernel.
   TCP acknowledgment proves neither parsing nor UI application. Empty router
   queues omit dequeued output and kernel bytes.
8. **Client received/applied through B:** an identified client explicitly attests
   parsing, or completion of its ordered reducer work, respectively. Rendering and
   durable local storage are separate optional levels.
9. **Reconciled/durable checkpoint:** declared state and watermark were committed
   consistently and can be recovered. This exceeds a same-boot memfd handover.

“Known good” must name scope, participant set, achieved level, failures and epoch.
For a proxy-only upgrade, source-prefix plus exact transport handover is usually
the useful contract. For a UI experiment, require applied acknowledgments from
the named test clients. For origin replacement, require separate state recovery.

## Increment 1: a small source-prefix protocol

### Capability and ordering

Add a versioned capability for the **proxy's source subscription**, not a new
unconditional event on every public stream. Illustrative names in this document
are proposed, not existing routes or schemas.

The origin EventFeed should enqueue a barrier command into its existing ingress
FIFO and, in that turn, enqueue a marker onto the selected source subscriber's
ordinary output FIFO. It must follow all earlier admitted business frames. Do not
send it on a heartbeat merge, separate socket, or priority lane: those can overtake
the prefix being certified. The proxy consumes the marker as protocol control and
does not forward an unknown event to legacy clients.

Use a source-stream epoch plus a monotonically increasing **feed ordinal** assigned
at the ordered feed pump. This is new transport metadata, not `evt_…`, wall time,
or `durable.seq`. A proxy-local ordinal alone can name its received prefix but
cannot prove origin completion. One marker per checkpoint is sufficient initially;
per-event wire sequence metadata is optional until gap/replay requirements need it.
Keep IDs and large counters as strings in the wire schema.

```http
POST /_bus/checkpoints
Content-Type: application/json

{"requestID":"req_7","scope":{"sessions":["ses_example"]},
 "goal":"source-prefix","quiesce":false,"timeoutMs":5000}
```

The proxy coordinates a proposed origin request, bound to its currently negotiated
source stream. Idempotency is scoped to request ID, caller, parameters and epoch.
An origin reply says accepted and identifies the barrier; it is not proof of SSE
progress. The source-only, negotiated marker might be:

```text
data: {"type":"event-feed.barrier","data":{"version":1,"checkpointID":"cp_7","originEpoch":"origin_A","streamEpoch":"stream_B","through":"4182","scope":{"sessions":["ses_example"]},"admissionFence":null}}

```

Without a work fence, this means only “the source FIFO prefix before this marker.”
It does **not** mean the named sessions are done. Binding it to an optional
execution observation adds information, not a stronger fence.

At the proxy, handle the marker in the same ordered path as decoded publications.
Reach a router barrier after earlier admissions; record both proxy source epoch
and origin stream epoch. Track retirements and origin feed-generation failures
since checkpoint enrollment. Missing marker, source EOF, source replacement,
encoding/ingress overflow, or incompatible capability fails this attempt explicitly.
Do not carry success into a fresh source epoch.

### Connection to handover

The application coordinator runs **before** current handover closes HTTP
acceptance/parks source intake. Otherwise the source barrier or client ACK route
can deadlock behind the very pause it needs to complete. Once the marker is
observed, choose:

- **Prefix checkpoint + buffered handover:** subsequent traffic is allowed; the
  physical image may include a later prefix. Report application cut and image cut
  separately. Exact transfer preserves outstanding output.
- **Exact application cut:** the source adapter parks after the marker, retaining
  already-read trailing bytes and decoder/chunk state for the existing image.
  Settle corresponding router commands, then proceed to transport handover.
  Heartbeats/other sessions may still exist beyond the scoped marker.

No poll-then-signal gap should claim an exact cut. A future integrated coordinator
must own a prepare token linking checkpoint and handover generation, validate its
epoch/deadline at commit, and release it on abort. An external script using today's
SIGUSR2 can schedule a handover after an observation, but cannot make that compound
operation atomic. Preserve or explicitly invalidate checkpoint records on adoption;
never silently reconstruct proof from observational gauges.

## Increment 2: cooperative work quiescence

Introduce an origin-owned lease with `preparing → fenced → settled → published`,
then `released`, `expired`, or `failed`. Fence first, wait second:

1. Atomically fence admission for the declared scope and capture admitted work IDs.
   Cover direct origin callers, SDK/internal starts, timers and plugin starts, not
   merely requests passing through this proxy. If that coverage is unavailable,
   advertise a narrower `gateway-admission` guarantee.
2. Let enrolled executions finish, including their causally spawned work according
   to a declared policy. Allow necessary tool/form/permission replies and checkpoint
   control while blocking new unrelated starts. Otherwise quiescence can deadlock.
   Avoid an unbounded “queue while fenced”; reject with a retryable typed response.
3. Settle tracked operations and their enrolled publication work. A session terminal
   event is useful evidence, but asynchronous publishers need explicit enrollment
   and a join, not a sleep. Report succeeded/failed/interrupted separately.
4. After those joins, issue the EventFeed marker. Keep the fence held through the
   chosen handover cut; successor explicitly releases it, or origin lease expires.

Use one absolute bounded deadline across phases, with phase/blocker diagnostics.
Lease expiry invalidates the quiescent token even if its marker arrived. Coordinator
loss eventually releases the fence; it must not leave OpenCode permanently paused.
Renewal is authenticated and bounded. An origin restart invalidates volatile leases
and markers. Proxy handover preserves the source epoch; origin restart does not.

Outstanding finite HTTP requests need a parallel exchange registry. Current
handover explicitly permits unsupported exchanges to fail after their operation
applied. A stronger mode must drain them before the cut or list them as blockers.
For reliable retries use operation IDs and bounded retained results; do not blindly
retry prompts because a response disappeared. Interest replacement is naturally
retryable against a surviving subscription, but include interest revision in a
strong checkpoint so a newer replacement cannot be mistaken for an older one.
“Response written” still needs a caller receipt if the goal is “responses back.”

## Increment 3: opt-in client apply acknowledgment

Negotiate a new controlled-feed capability; do not change old typed streams.
Capture a fixed participant cohort and each subscription's interest revision at
checkpoint enrollment. Pending feeds are not active participants. New connections
join after the cut; reconnects get a new generation and cannot ACK the old one.
Freeze interest updates briefly or explicitly version their effect on the cut.
Filtered-out events are not owed to that client.

For each capable participant, enqueue a marker **after** the relevant routed
prefix. A checkpoint nonce plus subscription/generation/interest revision avoids
requiring contiguous global sequence numbers on a selective stream:

```text
data: {"type":"event-feed.checkpoint","data":{"checkpointID":"cp_7","sourceEpoch":"proxy_E","subscriptionID":"evsub_C","interestRevision":"9","through":"4182","ackToken":"opaque-per-participant-token"}}

```

```http
PUT /_bus/checkpoints/cp_7/acks/evsub_C
Content-Type: application/json

{"sourceEpoch":"proxy_E","interestRevision":"9","ackToken":"opaque-per-participant-token","level":"applied"}
```

The client processes the marker through the same application work queue, flushes
the timer batch preceding it, waits for any required asynchronous reducer/refetch
work, then ACKs. Merely receiving the iterator item is `received`, not `applied`.
Define applied as the negotiated reducer contract; it does not mean pixels drawn,
user read, background jobs completed, or local persistence. ACKs are idempotent,
authenticated, bound to the issued token, and rejected for stale/future generations
or stronger unnegotiated levels. A client assertion is trusted-client evidence,
not independent proof of arbitrary client correctness.

Bound ACK storage and cohort size; retain summarized results for an explicit TTL.
Require a named set or `all-capable-at-start`, never a moving “all clients” target.
Unaware clients are reported `unsupported`; disconnected/overflowed participants
remain failures in this cohort rather than disappearing from its denominator.
On timeout choose explicitly: abort the checkpoint, or proceed with transport
handover and report incomplete application proof. Boundary handover may close a
slow client; buffered handover may preserve it. Neither manufactures its ACK.

### Zero queue is a real constraint

`/_bus/queue {"frames":0}` provides armed receive credit, not a payload backlog.
A barrier must not bypass earlier data using a hidden priority/handshake slot.
Source markers consumed at the proxy need no downstream marker admission. For
client markers, use the normal bounded admission policy (overflow can fail the
participant), or design an explicit single pending fence intention that waits for
credit without blocking global routing and cannot be overtaken. The latter is new
state that needs handover export and cancellation semantics. Recommend normal
admission first. Temporarily raising the queue target is an explicit operator
policy change, not an invisible prerequisite or guarantee.

## Stronger recovery checkpoints

A stronger contract requires a state snapshot with per-aggregate revision vector,
retained log coverage, and an atomic subscribe/baseline relationship. Use a
transactional snapshot through W plus replay after W, or subscribe first with a
bounded catch-up buffer and reconcile overlap. Restart or overflow must trigger
reconciliation, not reuse of a stale transport ACK. A separately fetched snapshot
and later subscription can miss changes between them.

Native `log.synced` already marks an aggregate replay watermark. Reuse that
meaning; verify persistence is enabled and covers the interval, and handle unknown
event versions, fork prefixes, retention/deletion and ephemeral state explicitly.
Client applied offsets must be committed with client state to claim durable local
recovery. Cross-session consistency requires an origin snapshot transaction or a
declared vector cut; per-session sequences are not a total order. Files, tools,
external side effects and persistent terminals need their own declared inclusion.
No memfd codec, Arrow export, or socket handover establishes these properties; see
[state exports](state-exports.sol-medium.md#three-different-consistency-products).

## Recommended result shape and rollout

```json
{
  "checkpointID": "cp_7",
  "state": "ready",
  "achieved": ["source-prefix", "proxy-routed"],
  "originEpoch": "origin_A",
  "streamEpoch": "stream_B",
  "sourceEpoch": "proxy_E",
  "through": "4182",
  "quiescence": "not-requested",
  "clients": {"required": 2, "applied": 1, "pending": 1, "unsupported": 3},
  "handoverGeneration": null
}
```

Here `ready` is relative to the requested source-prefix goal, not all-client
completion. Expose requested goal alongside achieved facts in the final schema.
Use separate accepted-operation replies and pollable status so losing the HTTP
reply during handover does not lose checkpoint identity. Protect control endpoints
with existing gateway auth/CORS/body limits; do not expose tokens/private payloads
in general metrics.

1. **Now:** existing wait/terminal events and handover inspection can help choose a
   quiet moment, honestly labeled observational. No protocol deployment required.
2. **First implementation:** negotiated source marker, generation validation,
   router barrier, bounded status, and integrated prepare token. Old downstream
   clients remain unchanged. Keep quiescence optional.
3. **Then:** scoped origin admission leases where execution-complete handoff is
   useful; tracked finite exchanges where losing their replies matters.
4. **Only for selected clients:** application ACK capability and reducer barrier.
5. **Separate undertaking:** durable snapshot/replay recovery across origin crashes.

Open decisions: which work scope (sessions, descendants, location, entire origin);
which internal producers can join; whether failed execution is an acceptable cut;
exact-prefix parking versus a lower-bound checkpoint; timeout/fallback policy;
required client cohort; response receipt versus operation-result recovery; supported
applied-state contract; snapshot/log retention requirements. The smallest practical
default is a source-prefix goal with buffered transport transfer and explicit
incomplete/unsupported application facts.

## Verification contract for implementation

These are proposed tests, not checks performed by this research:

- Hold a producer before ingress; a feed flush must not certify its completion.
  Fence admission concurrently with new prompts/internal wakes; every work ID is
  either enrolled or rejected. Exercise late successor execution and detached work.
- Delay marker serialization, split its SSE/chunk bytes, and read trailing events
  in the same TCP read. HTTP acceptance must not satisfy the source barrier; exact
  cut preserves trailing bytes. Lower-bound mode reports its later image cut.
- Force origin ingress overflow/encoding failure/subscriber retirement before the
  marker. A later marker cannot hide the failed generation or missing participant.
- Race interest PUT, activation, queue shrink, zero-credit bursts and checkpoint.
  Assert marker order, participant revision, and no reserved-slot overtaking.
- Stall a downstream socket, parser, and reducer independently. Only the matching
  level advances. A received ACK cannot satisfy applied; timer-batched events and
  asynchronous refetches must finish before an applied ACK.
- Lose ACK replies, repeat requests, forge/stale tokens, reconnect and join late.
  Cohort membership is stable, retries idempotent, memory/deadlines bounded.
- Keep form/tool replies usable while fenced. Expire/abort lease during every
  phase; no permanent admission pause, no successful expired quiescent token.
- Cut ordinary HTTP after effect commit but before response receipt. Report the
  ambiguity and recover by operation ID; never duplicate a prompt implicitly.
- Repeat both real process handover modes with a checkpoint; preserve source epoch
  and proof metadata where supported. Kill/restart origin and require invalidation.
  A source reconnect is never claimed as continuous merely because clients survive.
- Run unchanged native-client compatibility with source capability enabled; old
  streams never receive new union members. ACK-capable clients get ordered markers.
- For durable mode, race snapshot/subscription, disable payload persistence, truncate
  retention, and crash between local state/offset commits. Require either proven
  reconciliation or explicit unsupported/failure, never guessed exactly-once.

Research verification: read the three requested design/operational documents,
inspected the native files above, and consulted the V2 API reference. No runtime
tests or live resets were performed for this document-only stage.
