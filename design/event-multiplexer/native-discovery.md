# Proxy-owned native discovery contribution — initial predecessor

The initial unchanged-OpenCode constraint below was superseded by user direction
in the same effort. It is preserved as design provenance, not the current
operating contract. Current contract: [operations](../../doc/event-multiplexer/operations.md#native-local-discovery).

Direction: ordinary unchanged OpenCode clients must discover the gateway from
the existing service-local.json slot, without shell URL defaults or OpenCode
source changes. Session: `ses_ed3553a53ffeOaUVN5FYO8116p` (lucky-rocket).

## Ownership and health identity

The current client (`packages/client/src/service-probe.ts` in the sibling
opencode-working checkout) authenticates GET /api/info and requires its PID
and, when recorded, version to equal the registration. The gateway forwards
that endpoint to OpenCode. Therefore the registered PID/version/id/password
remain the **origin's**, not the gateway's. The daemon knows its own PID and
uses it in a separate contribution marker, not as a fictitious OpenCode owner.

Only a valid operator-owned (`mode: server`) record whose URL matches the
independently configured upstream is eligible. Managed services watch their
record and self-evict on replacement; those and foreign endpoints are untouched.
OpenCode's foreground --publish survives slot changes and republishes at each
origin restart. Keep that existing source behavior unchanged.

## Planned daemon contract

- Opt-in --service-file / BUS_PROXY_SERVICE_FILE; no inferred path or channel.
- Advertise the actual bound/adopted socket (wildcard becomes local loopback),
  optionally overridden with --service-url / BUS_PROXY_SERVICE_URL.
- Wait for authenticated forwarded /api/info matching the candidate identity
  and successful /_bus/health before contributing. Upstream is fixed, never
  taken from the contributed file (no self-forwarding loop).
- Poll replacements: a fresh origin record becomes a fresh contribution, with
  its new identity/credentials preserved. Do not invent registration if absent.
- A private marker retains the origin URL, contributing daemon PID, and unique
  instance token. Unknown record fields survive. Dead-daemon contributions can
  be reclaimed after health validation; live/unknown contributors are left alone.
- Hold a proxy-specific adjacent lock to prevent cooperating daemons fighting.
  Write beside the destination with mode 0600 and atomic rename, rereading the
  exact expected bytes immediately before replacement. Do not follow symlinks.
- Graceful shutdown restores only the exact last contributed record to its
  origin URL; a changed slot is never restored over. Crash retains the marker.

## Unavoidable single-slot race

OpenCode's publisher does not cooperate with the proxy lock. A check followed
by rename is not a cross-process compare-and-swap: an origin or foreign writer
can rename in the tiny interval between them. Polling repairs normal origin
republish, but cannot guarantee that an adversarial concurrent writer never
loses an update. Eliminating that race requires a shared publication protocol
or a separate endpoint slot; neither is allowed by this task's unchanged-client
and unchanged-OpenCode constraints. Do not claim transactional ownership.

## Tracking and activation boundary

~/ado/patches.md is the independently maintained OpenCode patch manifest. Its
server-reg and bus-smart work remains useful and must not be replaced by this
Rust daemon feature. Update it with a pointer/disposition, not another OpenCode
feature line. The maintained contract belongs in this repo's operations docs;
compfuzor exposes the opt-in only. Tests use isolated registration files and
mock origins under ~/tmp-opencode. No live registration, service, client, or
OpenCode build is changed by development or verification.

## Subsequent user direction and resulting adaptation

The user explicitly authorized native publishing override, default-on endpoint
markup, and named proxy-owned stomp, then requested existing Effect watcher
reuse. Adapt existing server-reg in its independent owner (not working):
native --publish-url > OPENCODE_PROXY_URL > listener, additive versioned endpoint
descriptions, no identity/auth change, managed registrations stay direct.
Daemon cooperate preserves native contact; explicit stomp reselects the proxy
after origin publication, including native overrides. Markup is a separate
default-on choice, not authorization to enable routing globally.

Effect-TS runs in a daemon-supervised Bun invalidation-only child; Rust retains
all publication/identity/health/restoration ownership. Parent-directory watching
reuses the existing FileSystemWatchClient and handles atomic saves/removal;
bounded polling covers disabled/error/standalone deployments. There is no
origin stomp watcher and no competing owner loop. Finding supplied by FX
`ses_ed34e6bc3ffenVHwvNdLP7POnL`; effect-watch's accepted boundary remains
effect-watch-20261011 `65bca58f`, vendor replacement-following primitive already
available. No new vendor/library patch was required.
