# bus-proxy-router

One Tokio actor owns epoch/subscriber state, the active/legacy sets, and the
Session → subscriber index. Selective admission is `O(legacy + followers)`;
broad admission is `O(active)`. Reaping scans subscribers periodically (100 ms),
and during registration/inspection, **not on each publication**.

`Router::spawn(Limits, shutdown)` returns a cloneable handle and supervised task.
Begin a nonempty unique epoch, subscribe legacy or controlled streams, replace
controlled interests, and publish one source frame at a time awaiting each ack.
`Interest` must already obey protocol validation; publication sizes are bounded
by the source decoder. No socket, HTTP, native payload parsing, or replay is here.
`Route` is now an internal decision made by protocol from a negotiated ordinary
event's authoritative scope and the frozen session-streaming type list; it is not
an upstream wire envelope. The actor/index/queue API is otherwise unchanged.

## Guarantees and overload

- Legacy streams start active with `server.connected`; controlled streams start
  with `event-feed.ready` and remain inactive until first replacement. Activation
  admits `server.connected` before adding memberships. Later replacements do not
  create another connected frame or flush already admitted data.
  Subscription IDs use the native `evsub_<32 lowercase hex>` format; a bare UUID
  is not accepted by the native ready-frame schema.
- Admission and complete replacement are actor turns with no internal awaits.
  Concurrent operations are ordered at the actor, not by upstream publication or
  HTTP receipt. A canceled caller may have already committed its command.
- Each queue uses a short-held `std::sync::Mutex<VecDeque<Bytes>>` plus Tokio
  notification/cancellation. Count and logical byte budgets include handshake
  frames. Offers never wait for capacity. Dequeue/drop releases retained bytes;
  one recipient's overflow removes its memberships, discards backlog, and wakes
  its receiver. Shared `Bytes` does not imply kernel zero-copy or an RSS bound.
- Pending clients count toward `max_subscribers`. Aggregate queued logical memory
  is bounded by `max_subscribers × queue_bytes` (plus entry overhead). Limits are
  nonzero and checked for integer/channel capacity overflow before channel creation.
- Commands use bounded `try_send`: `Overloaded` is prompt rejection, not a growing
  population of waiting sends. A command may retain one source-sized frame; the
  source owner must enforce its frame bound and single awaited publisher discipline.
- `end_epoch` cancels the matching token **before** trying the command queue.
  Even an overloaded End immediately makes old receivers unusable. An acknowledged
  End or subsequent Begin retires indexes/queues before a new generation operates.
  Repeating End after retirement returns `Unavailable`; stale names never cancel
  a new epoch. Beginning a different epoch also resets existing subscribers.
- Drop closes its queue immediately and best-effort enqueues removal. Persistent
  cancellation flags plus periodic reaping handle a saturated mailbox. Canceled
  subscribe acknowledgments drop their provisional subscriptions. Actor exit,
  abort, or panic drops an unwind guard that closes every queue.
- Cancellation is checked in receive, offer and replacement. Bytes already
  returned to the consumer cannot be recalled. Socket/TLS/kernel buffering and
  write deadlines belong to the gateway.

Tracing retains one `event.subscription` span in each actor-owned subscriber,
under the `event.router` task span. Registration/removal/reset events use that
lifetime context; complete replacements create `event.subscription.replace`
control spans. There is no per-publication or per-recipient span creation and no
payload/session-name logging. Source and HTTP spans belong to the gateway.

## Snapshot contract

`Snapshot: Serialize` has `epoch: Option<String>`, sorted `subscriptions`,
`counters`, and `gauges`. `SubscriptionInfo` contains `id`, `mode` (`legacy` or
`controlled`), `active`, sorted `sessions`, `backlog_frames`, `backlog_bytes`.

Counters (saturating u64, lifetime aggregates, no per-ID labels): `publications`,
`published_bytes`, `admissions`, `admitted_bytes`, `handshake_frames`,
`no_recipient_publications`, `queue_overflows`, `subscriptions_created`,
`subscriptions_closed`, `epochs_started`, `epochs_ended`. Publications count
accepted current-epoch inputs; admissions count accepted **domain** queue offers,
not socket receipts. Handshake offers are separate. A valid publication can return
success while overflowing/removing some recipients; inspect overflow counters.

Gauges: `subscriptions`, `active`, `pending`, `legacy`, `controlled`, `memberships`,
`backlog_frames`, `backlog_bytes`. Memberships are deduplicated interests, not
Session count. Backlog includes handshakes and excludes dequeued frames. Epoch
and indexes are actor-consistent; readers may concurrently dequeue, so gauges
sum the per-subscription queue samples rather than representing a simultaneous
network snapshot. Snapshot reaps dead subscriptions before reporting.

## Verification

```sh
cargo +1.98.0 test -p bus-proxy-router
cargo +1.98.0 clippy -p bus-proxy-router --all-targets -- -D warnings
cargo +1.98.0 run --release -p bus-proxy-router --example routing_bench -- 128 20000 512
```

Benchmark arguments: clients, events, delta payload bytes. Four Tokio workers;
each client follows a disjoint Session. Cases are round-robin selective, broad,
and unobserved Session routing. Each case validates every payload, exact per-client
and aggregate counts/bytes, no skips except the no-follower case, no overflows,
and final cleanup. A FIFO sentinel drains tails rather than hiding losses behind
source-reset cancellation. Reported duration includes the awaited source admission
hop, concurrent queue drains, and final sentinel, but excludes setup/handshakes.
It is not an SSE decoder, socket, origin-CPU, or whole-deployment benchmark.

### Recorded verification (2026-10-10)

`cargo test -p bus-proxy-router`: 22 tests passed. Coverage includes native ID/
ready shape, activation/replacement cuts, concurrent admission, legacy selection,
count/byte/activation overflow, stale epochs, out-of-band cancellation with full
commands, periodic Drop reaping, abandoned acknowledgments, actor panic/abort,
cancel-safe receive, serializable inspection, and retained/bounded-volume spans.
`cargo clippy -p bus-proxy-router --all-targets -- -D warnings` passed.

Release command above, three repeated trials after compilation: AMD Ryzen 7 5800X,
16 logical CPUs available, four Tokio workers, installed Rust 1.101 nightly;
128 controlled clients, 20,000 source events, 590-byte encoded frames. No CPU
pinning or exclusive host isolation; tracing collector disabled. Shared-host
variation is visible, so these are descriptive local results, not a capacity SLA.

| Case | Median events/s | Median deliveries/s | Events/s range | Validated deliveries per trial |
| --- | ---: | ---: | ---: | ---: |
| Selective (one indexed follower) | 162,610 | 162,610 | 161,714–167,425 | 20,000 |
| Broad (128 followers) | 12,392 | 1,586,221 | 12,350–12,418 | 2,560,000 |
| No followers (clients still attached) | 178,840 | 0 | 169,904–180,497 | 0 |

Every trial verified counters, byte totals and payloads with zero overflows and
zero subscriptions remaining. The final FIFO sentinel's admissions are validated
separately and excluded from the reported domain delivery count.
