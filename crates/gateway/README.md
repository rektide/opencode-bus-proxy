# bus-proxy-gateway

One source supervisor opens ordinary `GET /api/event` against the configured
fixed HTTP(S) origin. It requires HTTP 200, SSE content type and exactly one
`OpenCode-Event-Scope: 1` response header. First data must be `server.connected`
with an `evt_`-prefixed ID; that ID names the volatile upstream epoch. Another
connected marker, domain-before-connected, invalid scope, malformed/truncated
SSE, oversized frame, transport failure, EOF or timeout retires the generation
and invalidates downstream queues and subscription IDs before reconnection.

The public ordinary and controlled GETs retain their existing local connected/
ready activation sequences and both advertise scope v1. Negotiated domain events
are forwarded with their entire JSON data unchanged (SSE physical line endings
are normalized by the decoder). The protocol crate makes local session-streaming
classification decisions; no payload Session inference or private wrapper exists.
The controlled GET/PUT routes, native auth/error bodies, CORS, bounded queues,
connection budget, cancellation and redacted source/request/subscription spans
retain their existing contracts. Comments provide downstream heartbeats without
concealing upstream idle failure.

All other API/upgrade paths are forwarded to the configured origin, including the
removed `/api/experimental/event/source` path and encoded aliases. The gateway no
longer intercepts that former path; its status/body are the origin's response.
Inbound Host or absolute request authority never selects the uplink origin.

Startup rejects obvious same-port listener self-targets: literal listener address,
loopback aliases (`localhost` or literal loopbacks), and wildcard/loopback overlap.
This is not DNS alias discovery or multihop cycle detection. Configure an actual
upstream origin; proxy chains are supported, origin cycles are not.

## Checks

```sh
cargo +1.98.0 test -p bus-proxy-gateway
cargo +1.98.0 clippy -p bus-proxy-gateway --all-targets -- -D warnings
cargo +1.98.0 fmt -p bus-proxy-gateway --check
```

Loopback coverage includes scope negotiation/sequence rejection, authoritative
selection despite spoofed payload, missing Session scope, malformed scope,
truncation/oversize/idle failures, reconnection and stale IDs, exact raw JSON
through two proxies, both feed capability headers, generic former-route forwarding,
self-target rejection, native auth/CORS/cookies, streaming uploads, cancellation,
upgraded tunnels, connection budgets and bounded redacted tracing.
