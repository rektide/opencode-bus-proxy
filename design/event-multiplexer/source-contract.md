# Routed public source, version 1

**Historical predecessor.** Superseded by
[additive scoped events on ordinary `/api/event`](additive-events.md). The private
route and envelopes below describe the first implementation, not the current
transport contract.

Implementation contract for the bus-smart origin and Rust proxy. The earlier
[Effect mux draft](../../../opencode-bus-smart/design/event-multiplexer/draft0.gpt6a.md)
is background; this source boundary supports a same-origin Rust gateway.

`GET /api/experimental/event/source` uses normal OpenCode server authentication
and SSE (`data: <JSON>\n\n`, comment heartbeats). One proxy opens one eager source
connection to one origin. There is no replay or Last-Event-ID recovery.

The first data frame, emitted after observation is installed, is:

```json
{"type":"event-source.ready","version":1,"epoch":"opaque-random-connection-id"}
```

Subsequent data frames are:

```json
{"type":"event-source.event","route":{"kind":"session","sessionID":"ses_example"},"event":{"id":"evt_example","created":1,"type":"session.text.delta","data":{}}}
{"type":"event-source.event","route":{"kind":"broadcast"},"event":{"id":"evt_other","created":2,"type":"rpc.plugin.progress","location":{"directory":"/workspace"},"data":{}}}
```

- Export only `isOpenCodeEvent` events. The origin owns classification using
  `isStreamingEvent` and `Bus.RoutedEvent.audience.sessionID`.
- The five streaming types use `kind: session`. `sessionID` is optional when Bus
  has no authoritative identity; those events reach legacy subscribers only.
  Never infer identity from event payload data.
- Every other public event uses `kind: broadcast`, even when its data contains
  a Session ID. Public JSON event bytes remain intact through the proxy; its
  parser only decodes the envelope and retains the event as raw JSON.
- Native `server.connected` is transport-local: source observation must omit it.
  Each downstream stream receives its own connection marker.
- The origin observer offers to a bounded queue synchronously without waiting
  on sockets. Encoding runs in the stream drain. Overflow or encoding failure
  fails the source; internal events do not occupy its queue.
- Source EOF, malformed input, unsupported version, repeated ready, oversized
  frame, or transport failure invalidates the proxy epoch and all subscription
  IDs, discards pending downstream frames, and closes downstream streams. One
  proxy-owned reconnect loop establishes the next epoch.
- Queue capacity and maximum encoded source frame size are bounded. The origin
  queue bounds retained event count; the proxy additionally bounds byte backlog
  per subscriber. Neither promises durable delivery.

The proxy exposes existing `/api/event`, `/api/experimental/event`, and interest
PUT paths with their existing bodies and semantics. A successful PUT is a cut
at **proxy admission**, not at origin publication or snapshot capture. Already
admitted frames are not recalled. Upstream backlog can cross that cut.

The source endpoint is infrastructure-facing. The gateway must not forward a
downstream request for it, which would bypass shared fan-out.
