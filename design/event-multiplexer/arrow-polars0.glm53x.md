---
title: Arrow support via Polars — where it should land
description: Assesses the parked Arrow prototype, occhre's actual polars/arrow practice, and recommends a bounded-memory-compatible landing for Polars-facing Arrow support.
status: draft
resource: /design/event-multiplexer/arrow-polars0.glm53x.md
generated: { at: 2026-10-14, by: model:glm-5.3-max }
sources:
  - resource: opencode://ses_ec34f4afaffe0AX1j80TwN20Vu
    title: Independent design wave — prototype/occhre inventory, boundary reconciliation, landing recommendation
  - resource: jj:r zysoxnlt (d14ca9e6)
    title: Prototype commit — typed inspection rows, reactive tracing, Arrow IPC, optional rkyv image (state_exports.rs, arrow_exports.py, rkyv-image/, test_arrow_exports.py read via `jj file show`)
  - resource: jj:r c97ce547
    title: Parking commit — added exactly design/event-multiplexer/clickhouse-http-draft.rs.txt
  - resource: /design/event-multiplexer/clickhouse-http-draft.rs.txt
    title: The parked HTTP Arrow exporter draft (210 lines, uncompiled; read at main)
  - resource: /design/event-multiplexer/export-boundaries.sol-xhigh.md
    title: Recorded export-surface ruling — crates/features, "not a query engine", router-state slot, bounded processors
  - resource: /telemetry/README.md
    title: Telemetry domain layout, per-destination bounded processors, verification lanes
  - resource: /telemetry/arrow/README.md
    title: 22-column otel-clickhouse-traces-v1 contract, Policy bounds, redaction, verification
  - resource: /telemetry/arrow/src/lib.rs
    title: SCHEMA_ID/COLUMNS/schema-metadata conventions, empty_batch
  - resource: /telemetry/arrow/src/traces.rs
    title: Policy/Budget fail-whole projection implementation
  - resource: /telemetry/arrow/schema/traces-v1.sql
    title: Operator-owned DDL example, no TTL
  - resource: /telemetry/arrow/tests/projection.rs
    title: Projection fixture coverage shape
  - resource: /core/router/examples/state_exports.rs
    title: Observational Inspection envelope, reactive View, declared-field tracing test
  - resource: /design/event-multiplexer/examples/arrow_exports.py
    title: Offline Polars adapter — explicit schemas, IPC file+stream, scan_ipc round-trip, manifest
  - resource: /design/event-multiplexer/examples/test_arrow_exports.py
    title: Polars 2.0.0 / PyArrow 26.0.0 interop checks, empty-table schema, u64/Unicode
  - resource: /design/event-multiplexer/examples/rkyv-image/src/main.rs
    title: rkyv image codec experiment (handover-owned, not Arrow prerequisite)
  - resource: /doc/event-multiplexer/operations.md
    title: Bounded-memory limits and failure behavior; OTLP/native export operation; /_bus diagnostics surface
  - resource: /daemon/Cargo.toml
    title: Feature-gating precedent — default=[], otel/otlp/clickhouse-native, required-features tests
  - resource: /daemon/tests/telemetry_availability.rs
    title: Actionable rebuild-instruction failure tests
  - resource: /daemon/src/telemetry.rs
    title: Destination selection/require(), shutdown budget division
  - resource: /Cargo.toml
    title: Workspace members/default-members; arrow-array/arrow-schema 57 pins (read at main)
  - resource: /core/router/src/lib.rs
    title: Snapshot/SubscriptionInfo fields; "IDs/session names are never labels"; RouterImage freeze note
  - resource: file:///home/rektide/src/occhre/crates/interchange/arrow/Cargo.toml
    title: arrow 60 ipc-only pin; cross-major IPC seam; dev-dep arrow-v58 counterparty
  - resource: file:///home/rektide/src/occhre/crates/interchange/arrow/README.md
    title: Bounded versioned envelope; rows-and-bytes budgets; byte-identical replay
  - resource: file:///home/rektide/src/occhre/crates/interchange/arrow/doc/envelope-spec.md
    title: Budgets defaults; arrow version policy small gate
  - resource: file:///home/rektide/src/occhre/crates/sinks/duckdb/Cargo.toml
    title: duckdb-rs pin, vtab-arrow, arrow-58 rename/unification comment
  - resource: file:///home/rektide/src/occhre/doc/research/duckdb-arrow-ipc.md
    title: In-process duckdb-rs ingestion recommendation; Arrow-as-interchange framing
  - resource: file:///home/rektide/src/occhre/design/pipeline/incremental-tmpfs0.gpt61sm.md
    title: "Polars is optional downstream analysis; no Polars dependency needed for the first slice"
  - resource: file:///home/rektide/src/occhre/design/pipeline/batch-outbox0.gpt6a.md
    title: Durable IPC spool profile; LazyFrame limits; Polars scan_ipc consumer role
verification: >-
  Source/document inspection only (jj file show at main txoqoyzp for telemetry-era
  paths; direct reads for occhre and the current design/ tree). No code compiled,
  no Polars installed, no performance measured.
---

# Arrow support via Polars — where it should land

## Situation

The human's lean is "Arrow support via Polars, like `~/src/occhre`". This wave
inventories the parked prototype in this repository, reads what occhre actually
does with polars/arrow, and reconciles both against the recorded export-surface
ruling and the bounded-memory discipline. One structural fact first: the
telemetry-era layout lives on the `main` bookmark (`txoqoyzp`) —
`core/{protocol,router,gateway}`, `daemon`,
`telemetry/{core,arrow,clickhouse-native}` — while at writing time the working
copy was a conflicted merge in progress between that line and the `crates/`-layout
native-discovery line (working-copy parents `txoqoyzp` and `tzstullv`). Path
references below are main-relative; landing happens after that merge resolves.

A caution up front, stated plainly: **occhre does not use Polars in Rust.** An
`rg -il polars` over `~/src/occhre` (excluding `target`/`node_modules`) matches
only design docs; there is no `polars` entry in any of its `Cargo.toml` files
and no Python polars tooling. What occhre actually has is plain arrow-rs
envelopes plus DuckDB as the analytical engine, with Polars named as an
*optional downstream consumer* of Arrow IPC files. If "Arrow via Polars" means
"served Arrow that Polars can consume", occhre's practice transfers directly.
If it means "embed Rust polars in the daemon", it contradicts occhre's own
design reasoning and this repository's recorded ruling. Both readings are
handled below.

## Prototype inventory

### Commit zysoxnlt (d14ca9e6) — "prototype: typed inspection rows, reactive tracing, Arrow IPC and optional rkyv image"

Three distinct artifacts, none of them production code:

1. **`core/router/examples/state_exports.rs`** — the observational
   inspection prototype. `Inspection { schema_version: 1, revision, consistency:
   "observational", subscriptions, memberships, router: Snapshot }` with
   `SubscriptionRow` (id/mode/active/interest_count/backlog_frames/backlog_bytes)
   and `MembershipRow` (id/session). A `View` keeps one `tracing` span per
   subscription with *declared* fields, records changes on diff, and logs
   removals. Its own header says: "no router freeze, import, or production
   hooks" and "A small typed materialized view, NOT the proposed full material
   registry". The embedded test pins the reactive contract: declared fields
   react to `record`, undeclared fields do not.
2. **`design/event-multiplexer/examples/arrow_exports.py`** — the offline
   Polars adapter ("Optional offline Polars adapter for the state_exports Rust
   example"). Builds explicit-schema `pl.DataFrame`s (so empty tables keep
   types), adds the `revision` column lazily, validates the schema before
   collecting, writes both IPC file (`.arrow`) and stream (`.arrows`) formats
   uncompressed, round-trips both through `scan_ipc`/`read_ipc_stream`, runs a
   `group_by("mode")` summary, and emits a `manifest.json` carrying
   schema_version/revision/consistency plus the Polars version. "No payload
   bytes exported."
3. **`design/event-multiplexer/examples/rkyv-image/`** — a self-contained rkyv
   codec experiment for a frozen handover image (`BlobRange` + arena, checked
   archive access, truncation rejection). Header: "Codec experiment only; does
   not freeze or restore gateway runtime objects."

### Commit c97ce547 — "Preserve exploratory HTTP Arrow exporter for separate maintenance pending architecture review"

This commit added exactly one file:
`design/event-multiplexer/clickhouse-http-draft.rs.txt` (verified via
`jj diff -r c97ce547 --stat`). It is a 210-line **uncompiled** draft whose own
header records the parking rationale: "parked when the user requested
architectural review using the clickhouse_arrow crate. Not selected for
implementation." It is an *outbound OTel SpanExporter*: reqwest blocking HTTP
POST of `INSERT … FORMAT ArrowStream` (strict URL/identifier validation,
sensitive headers, 16 KiB+1 bounded response read, feedback-excluded tracing
target), encoding spans into a 28-column schema with JSON-stringified
attributes/events/links via `arrow_ipc::writer::StreamWriter`. Note its schema
is the *alpha* shape — the later reviewed contract is 22 columns with
pdata-faithful stringification.

### What already got built, and what the boundaries doc decided

The prototype was not promoted wholesale; a reviewed subset was:

- `telemetry/arrow` (`bus-proxy-export-arrow`) — transport-free Arrow 57
  projection owning `otel-clickhouse-traces-v1`: exactly 22 non-nullable
  columns (`src/lib.rs` `COLUMNS`), schema identity in Arrow metadata
  (`bus-proxy.schema`), operator-owned DDL example (`schema/traces-v1.sql`,
  no TTL), and an `otel-traces` feature projecting `&[SpanData]` under
  `Policy` bounds (4096 spans, 16 KiB strings, 1024 attributes, 128
  events/links, 1024 array items, 16 MiB batch, fail-whole on oversize,
  default redaction list) — `src/traces.rs` `Budget` uses checked adds and
  rejects rather than truncates.
- `telemetry/clickhouse-native` — the transport consuming those batches.
- `telemetry/core` (`bus-proxy-telemetry`) — caller-owned composition; one
  bounded SDK batch processor per destination, never a serial fan-out
  (`telemetry/README.md`).

`design/event-multiplexer/export-boundaries.sol-xhigh.md` (proposal, not
acceptance — but the implemented crates follow it) already decided the load-
bearing points for this question:

- **"The Arrow package uses `arrow-array` and `arrow-schema`, not a query
  engine. … The existing offline Python/Polars bridge can remain independent
  tooling. rkyv remains an optional image codec under handover ownership, not
  an Arrow or tracing prerequisite."** (Recommendation section.)
- The package table gives `bus-proxy-export-arrow` two optional adapters:
  `otel-traces` **and `router-state`** — the latter "enables a dependency on
  the router and its **observational** projection". So an inspection
  projection crate-slot was already reserved inside the Arrow crate.
- **"Never run table construction under queue locks or on the router hot
  path."** (Schema versus transport section.)
- **"Keep OTel trace tables separate from inspection tables … Observations
  carry their generation and consistency class; image codecs still own exact-
  byte restoration."**
- Daemon owns ambient configuration; features are additive; workspace
  `default-members` stay the four core packages; consumer build matrix and CI
  lanes are spelled out (Consumer/build contract section).

The HTTP draft remains parked and unchanged; `telemetry/README.md` explicitly
notes "no empty HTTP crate".

## What occhre actually does, and what transfers

- **`crates/interchange/arrow` (`fch-arrow`)**: plain `arrow` v60 with
  `default-features = false, features = ["ipc", "ipc_compression"]` plus `sha2`
  for a stable schema-identity digest. "Bounded, versioned Arrow batches …
  Batches are bounded by rows **and** bytes" — `doc/envelope-spec.md` §4 gives
  `Budgets { max_rows: 8192, max_bytes: 32 MiB }`, flushing before a row would
  exceed either bound; replay is deterministic to byte-identical IPC output.
  This is the same fail/flush-bounded posture as `telemetry/arrow`'s
  `Policy`/`Budget`.
- **The version-separated IPC seam** (the sharpest transferable idea): occhre's
  encoder owns arrow 60 while its DuckDB sink consumes via duckdb-rs's arrow
  ^58 — "batches cross as Arrow IPC stream bytes" (`crates/sinks/duckdb/
  Cargo.toml`), proven by `tests/ipc_seam.rs` decoding arrow-60 output with a
  dev-only renamed `arrow-v58` dependency. Arrow majors create distinct Rust
  types (as this repo's boundaries doc also rules); IPC bytes are the
  version-independent contract.
- **Analytical engine**: DuckDB in-process via duckdb-rs `arrow(?)` scans
  (`doc/research/duckdb-arrow-ipc.md` §4 recommendation (a)); Arrow is "the
  in-memory interchange", not the store.
- **Polars' actual role**: `design/pipeline/incremental-tmpfs0.gpt61sm.md`
  (around line 388): "Polars is optional downstream analysis/projection. A
  `LazyFrame` is a query plan, not the storage of pending rows or their
  ownership/durability… **No Polars dependency is needed for this first
  slice.**" And `design/pipeline/batch-outbox0.gpt6a.md` (line 254):
  "laziness alone does not bound joins/sorts/diffs or promise zero-copy
  execution"; its recommended profile (line 207) writes immutable
  *uncompressed* IPC file-format segments precisely because that favors
  mmap-backed reads and Polars `scan_ipc` downstream.

Patterns that transfer here: (1) explicit-schema, rows-and-bytes-bounded
batches; (2) IPC bytes as the only cross-boundary Arrow contract; (3) schema
identity in stable metadata/digests; (4) the query engine — whichever it is —
lives *outside* the producing process. What does *not* transfer: any notion
that occhre justifies a Rust-side polars dependency. It never took one.

## Landing options and tradeoffs

**Option A — extend `telemetry/arrow` in place (recommended).** Add two
optional features to `bus-proxy-export-arrow`, defaults staying empty:

- `router-state`: the projection the boundaries doc already reserved —
  `inspection` module projecting `router::Snapshot` into per-table batches
  (`subscriptions`, `memberships`, plus counters/gauges tables) with its own
  `Policy` and a new schema id (e.g. `bus-proxy-inspection-v1`), carrying
  `revision`/`consistency`/`schema_version` as schema metadata, mirroring the
  Python prototype's manifest semantics.
- `ipc`: bounded Arrow-IPC encoding (arrow-ipc `StreamWriter`) so any consumer
  — HTTP endpoint, file spool, test fixture — shares one writer. Compression
  off initially, matching both the boundaries doc's Cargo note ("the
  HTTP-only build can remain smaller, with IPC compression off initially")
  and occhre's uncompressed-for-mmap/scan_ipc reasoning.

Tradeoff: the crate's audience widens (trace export + inspection consumers).
That is exactly what its README/table-contract structure already
compartmentalizes per schema id, and the boundaries doc chose this slot. The
alternative of splitting inspection into its own crate buys a manifest at the
cost of duplicating schema-metadata, bounds, and fixture conventions while
both still need arrow-ipc.

**Option B — new inspection crate** (`telemetry/inspection`). Cleaner if
inspection grows its own transports (spool files, socket taps), premature
now; the boundaries doc explicitly declined speculative packages ("not a
request to create four empty packages now").

**Option C — fold into `telemetry/core`.** Rejected: core is OTel composition
with zero Arrow dependencies; folding couples projection releases to the
composition crate and makes raw Arrow consumers import OTel-shaped types.
Feature-gating inside core would technically work but dissolves the
crates-as-ownership-boundaries ruling for no gain.

**Where an HTTP Arrow surface belongs.** Two different things must not be
conflated:

- *Outbound trace export* is transport work; `clickhouse-native` already
  ships it, and reviving the parked HTTP draft is the separately-parked
  `clickhouse-http` slot — orthogonal to Polars and not needed for it.
- *Inbound inspection export* belongs on the authenticated `/_bus` surface the
  gateway already owns, as a sibling of `/_bus/subscriptions` and
  `/_bus/metrics` (operations.md "Tracing and diagnostics"): e.g.
  `GET /_bus/inspections/<table>.arrows` returning an IPC stream built from a
  snapshot taken on the request task — never under queue locks, never on the
  router hot path (boundaries ruling). The gateway feature that enables it
  forwards to `bus-proxy-export-arrow/{router-state,ipc}`; the daemon
  advertises availability exactly like OTLP/native.

**Build gating.** Follow the established precedent exactly: daemon
`default = []` with additive features (`daemon/Cargo.toml`:
`otel`/`otlp`/`clickhouse-native`), `[[test]] required-features`,
availability tests asserting actionable rebuild strings
(`daemon/tests/telemetry_availability.rs` checks stderr contains
`--features otlp` / `--features clickhouse-native`), workspace
`default-members` limited to core packages, and the `cargo tree` isolation
assertion (telemetry/README.md: local-only daemon must contain no
opentelemetry/reqwest/Arrow/ClickHouse packages). An
`inspections`/`inspections-arrow` feature slots into all four mechanisms
unchanged.

## Polars versus the bounded-memory discipline

The gateway's contracts (operations.md "Limits and failure behavior"):
per-subscriber queue frames *and* bytes, `--max-subscribers`, bounded command
mailbox, bounded source frame size, bounded interest bodies; queue overflow
invalidates the subscriber; "There is no hidden failover, persistent replay,
or exactly-once delivery"; metrics use bounded names, never Session-ID
labels; the sum of subscriber byte allowances is an explicit upper bound.
`telemetry/arrow`'s own `Policy` extends the same discipline to projection.

Polars is a query engine that wants to own memory. A `LazyFrame` plan's
execution memory is not contractually bounded: joins/sorts/group-bys
materialize at plan discretion, the streaming engine covers only a subset of
operations, and nothing in its API is an admission decision the gateway gets
to make. That is not a flaw — it is the wrong layer. The seam that keeps both
intact, converged on independently by occhre and by this repo's prototype:

1. **Bounded row windows at snapshot grain.** The observational snapshot is
   bounded *by construction* — subscription count ≤ `--max-subscribers`,
   membership ≤ interest caps (256 KiB / 1024 Locations / 4096 Sessions),
   string fields ≤ projection `Policy`. Project `Snapshot → RecordBatch` with
   fail-whole limits, exactly like `traces.rs::project`.
2. **Snapshot-on-demand, off the hot path.** One bounded batch per table per
   request; peak memory is one batch plus IPC buffers. No window of live rows
   is retained between requests.
3. **IPC stream bytes as the boundary.** Encode with `StreamWriter` straight
   into the HTTP response or a spool file. Polars (Python or otherwise)
   consumes the bytes via `scan_ipc`/`read_ipc_stream` — the
   `arrow_exports.py` prototype already demonstrates the full loop including
   empty-table explicit schemas and u64/Unicode values
   (`test_arrow_exports.py`, Polars 2.0.0 / PyArrow 26.0.0).
4. **Cross-major safety for free.** Because the contract is IPC bytes, the
   daemon's arrow-57 pin (tied to clickhouse-arrow 0.2.1's mandatory Arrow 57,
   per the boundaries doc's Cargo/MSRV section) never constrains what a
   downstream polars/duckdb/arrow-60 consumer may use — occhre's seam proof
   is the template if we ever want an in-tree demonstration.

Where Rust-side Polars would *violate* the contract rather than paper over
it — flag these as hard lines:

- **Live/reactive tails.** Feeding the event stream into a growing DataFrame
   has no admission semantics; it recreates an unbounded queue outside the
   queue accounting, invisible to overflow invalidation.
- **Queries over live state in-daemon.** Any `group_by`/`join` whose input is
   not a bounded snapshot executes with unaccounted memory on data-plane-
   adjacent tasks.
- **Session-ID cardinality.** A Polars group-by keyed on session names would
   manufacture the unbounded labels the metrics surface explicitly forbids
   (`router/src/lib.rs`: "IDs/session names are never labels"). Inspection
   tables expose session *strings as data*, which is fine; the moment an
   in-daemon aggregation uses them as keys, the bound is gone.
- **Handover images through Polars.** Frozen restoration is exact-byte and
   rkyv/serde-owned (boundaries ruling; `rkyv-image` experiment). Arrow/Polars
   is observational only; `RouterImage` must never round-trip through a
   dataframe.

## Dependency weight, licensing, build time

- **arrow-ipc 57, default-features off**: the only new always-available
  dependency class; small, Apache-2.0, same major already pinned in the
  workspace for `arrow-array`/`arrow-schema` (main `Cargo.toml`).
- **Polars (Rust)**: MIT-licensed but heavy even feature-minimized
  (smartstring, rayon, its own allocator preferences), and critically its
  Arrow lineage is `polars-arrow` — *not* arrow-rs types. In-process
  interchange with `bus-proxy-export-arrow` batches requires FFI/re-encoding
  rather than a pass-through, and it drags a second Arrow major into a tree
  that must keep 57 for clickhouse-arrow. Over IPC bytes none of this
  applies. If ever taken, it belongs in a non-default, non-data-plane crate
  with its own CI lane so core lanes stay light.
- **arrow-flight**: pulls tonic/prost/gRPC into a hyper-owned gateway for
  zero needed capability — plain chunked HTTP serves IPC streams fine.
  Reject.
- **Licensing**: everything in scope is MIT/Apache-2.0-compatible with this
  workspace (MIT, `Cargo.toml [workspace.package]`); no copyleft exposure.

## Recommended structure and first slice

Structure (Option A), on the main-line layout:

```
telemetry/arrow (bus-proxy-export-arrow)
  features: default=[], otel-traces (existing), router-state (new), ipc (new)
    src/inspection.rs  — Snapshot → {subscriptions,memberships,...} batches
                         under inspection Policy; schema id in metadata
    src/ipc.rs         — bounded StreamWriter encode (via `ipc` feature)
daemon
  features: inspections = ["bus-proxy-export-arrow/router-state",
                           "bus-proxy-export-arrow/ipc"]
  route: GET /_bus/inspections/<table>.arrows  (authenticated, additive)
  tests: required-features; availability failure names the feature
design/event-multiplexer/examples/arrow_exports.py
  unchanged (evidence); optionally gains an HTTP-fetch mode later
```

First incremental slice, in order:

1. `router-state` projection + fixtures in `telemetry/arrow`: empty snapshot
   (explicit schemas survive), Unicode ids, u64 maxima, revision/consistency
   metadata asserted; bounds fail-whole. Mirrors `tests/projection.rs` shape.
2. `ipc` encode + round-trip test using only arrow-rs (decode-what-we-wrote),
   plus an optional cross-major seam test in occhre's style (dev-only newer
   arrow) if the human wants the interop proven in-tree.
3. Daemon route behind `inspections`, built from `router.snapshot().await` on
   the request task; availability test asserting the rebuild string; extend
   the `cargo tree` isolation assertion to prove local-only builds still
   contain no Arrow.
4. Documentation: operations.md gains a short "inspection export" subsection;
   the Python example stays evidence, not product.

## Risks

- **Reading "Polars" as a mandate for in-daemon Rust polars** — the largest
  risk; it would break bounded-memory accounting, double the Arrow lineage,
  and contradict both occhre's practice and this repo's recorded ruling.
- **Layout fork**: resolved after writing — the merge landed as
  `core/{protocol,router,gateway}`, `daemon/`, `telemetry/*`,
  `integrations/{discovery,service-manager}`; paths in this document were
  updated to match.
- **Inspection-scope creep**: sequenced reactive exports and frozen-image
  exports are deliberately *not* in this slice; conflating consistency
  classes (observational / sequenced / frozen, per state-exports and the
  boundaries doc) would reopen solved questions.
- **Endpoint abuse**: an unauthenticated or unbounded inspection endpoint
  would be an information/memory leak; it inherits `/_bus` auth and adds
  per-request snapshot bounds.
- **Schema drift**: inspection schema must be versioned like
  `otel-clickhouse-traces-v1` from day one, or downstream Polars code will
  silently absorb changes.

## Open questions

1. Does "Arrow via Polars" mean *served Arrow consumable by Polars* (occhre's
   actual pattern — this recommendation), or an in-Rust polars integration?
   If the latter: what query, over what data, and where may it run if not in
   the daemon?
2. Should the parked ClickHouse HTTP exporter be revived as the
   `clickhouse-http` transport slot? It is orthogonal to Polars and already
   has a reserved crate boundary — but it is also redundant while
   clickhouse-native works, unless HTTP-only egress paths matter operationally.
3. File-spool profile (occhre batch-outbox style: durable immutable IPC
   segments, publish→drain→ack→unlink) versus HTTP pull only — is any
   consumer expected to poll offline, or is pull sufficient for v1?
4. Which tables belong in inspection v1 beyond subscriptions/memberships —
   counters/gauges from `Snapshot`, epoch identity, queue targets?
5. Where does the layout fork land, and does the merged layout keep
   `telemetry/` as a domain root (affecting every path above)?
