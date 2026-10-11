---
title: Typed material, interoperating state exports, and reactive tracing
status: exploration with runnable prototypes
sources:
  - resource: urn:opencode:session:ses_ed3b65203ffebXKv17uH4LiZrB
    title: Human direction — handover, optional rkyv, Arrow/Polars, reactive attributes
  - resource: urn:opencode:session:ses_ed39358fbffemlbEVXnfxjBMTu
    title: Sol medium export exploration and prototype verification
---

# Typed material, interoperating state exports, and reactive tracing

**Recommendation:** share a typed semantic model and stable material keys, not
one mutable serialized representation. Keep exact bytes with their I/O owners;
feed separate adapters for immutable handover images, Arrow tables, tracing, and
the existing JSON/metrics views. Start with offline Polars exports and a small
allowlisted tracing projection. rkyv is an **optional image codec**, not selected
as a runtime requirement. The [handover registry](active-client-handover.md)
owns the full transfer inventory and coordinated ownership protocol.

The examples below add no production behavior or workspace dependencies. They
prove useful seams, not active-client migration or a complete material registry.

## Ground in the current owners

| Existing material | Available now | What cannot be inferred from it |
| --- | --- | --- |
| Router `Snapshot` | Epoch, sorted subscription rows and sessions, lifetime counters and sampled gauges | Exact frame bytes, connection/descriptor association, globally consistent cut |
| Router `Subscriber` | Mode, active state, effective session set, retained span | Original interest PUT document or arbitrary client profile |
| Router `Queue` / `Backlog` | Ordered `VecDeque<Bytes>`, logical byte count, closed state under a short mutex | Bytes already dequeued into a body/Hyper; kernel send position |
| Gateway `source_connection` | Local decoder, incoming chunks, publication loop | Public snapshot/import API for decoder or established HTTP/TLS transport |
| Gateway inspections | `/_bus/subscriptions` JSON, `/_bus/metrics` flat counters/gauges, `/_bus/health` readiness | Restoration image or exact ordering between gateway atomics and router observations |

[`Snapshot`](../../components/router/src/lib.rs) is one actor turn for indexes and
epoch, but `Queue::recv` reduces individual backlogs concurrently. Its summed
gauges are sums of per-queue samples, not an atomic global measurement. Gateway
source metrics use relaxed atomics outside that turn. Preserve those meanings.
Normal retirement/drop clears queues; reusing shutdown as freeze loses material.

The [tracing test](../../components/router/tests/tracing.rs) deliberately retains one
subscription span, creates child control spans, and creates **no publication
spans**. Export work should not undo that property.

## Several viable combinations

| Combination | Why use it | Cost / boundary |
| --- | --- | --- |
| **A. JSON bridge + offline Python Polars lazy** | Smallest first increment; existing Rust types and serde; Python 2.0 experiments independent of daemon | JSON parsing/copying and explicit table conversion; observational only unless supplied a genuine frozen image |
| **B. Rust Polars lazy exporter** in a separate tool or optional crate | Same Rust DTOs; rich expressions; lazy collect/sink; native Arrow IPC output | Larger build/dependency graph; feature/API/version gating; execute off the router actor |
| **C. Direct Arrow record-batch exporter + external Polars** | Avoid putting a query engine into the gateway; normalized schema with predictable batches | Still adds Arrow writer dependency if in-process; external Polars performs lazy analysis |
| **D. Versioned metadata + raw byte arena** | Simple immutable handover image; explicit lengths/offsets; serde-based metadata possible | Need envelope validation, ownership freeze and transport adapters irrespective of codec |
| **E. Optional rkyv metadata/image sections + byte arena** | Checked archived access and reduced reconstruction for compatible schemas | Explicit format/schema compatibility, alignment and validation; not runtime object cloning |
| **F. Bounded typed deltas + adapters** | Owner changes reactively update spans/current-state views and optionally append Arrow delta batches | Sequencing, removal, overflow/resync, sampling and lifetime handling must be specified |

A + D + selected F is the conservative first path. B/C are alternatives for
scale and deployment constraints, not requirements. E composes with A/B/C/F:
an Arrow consumer need not know whether a handover section uses rkyv, and an image
importer need not load Polars. No reason to serialize on every packet just to
support an on-demand image.

## A reusable typed model, not a second I/O owner

Proposed layers:

```text
owner: semantic state + exact buffers + native runtime machinery
  ├── sample() → InspectionSnapshot → JSON / metrics / Arrow snapshot tables
  ├── committed owner change → MaterialDelta → spans / events / live read model
  └── coordinated freeze(cut) → FrozenMaterial → private image codec + named FDs
```

The registry indexes live owners, identities, capabilities and semantic records.
It does not mirror every buffer into a constantly updated global `HashMap`.
Hot bytes can be borrowed/copied from parked owners only at freeze. A material
descriptor should declare `key`, `kind`, `owner`, `schema_version`, capability
(`transferable`, `drain_required`, `unsupported`), and a bounded reason code.
Inspectable summary includes sizes and blockers, never private payloads/secrets.

Use typed records such as `SubscriptionState`, `ConnectionState`, `SourceState`,
`BufferRef { arena_key, offset: u64, length: u64 }`, `QueueEntry { ordinal,
frame_key }`, and `DescriptorRef { name, role }`. Include explicit mode/phase
enums and distinguish absent/unknown from zero. Portable DTOs use fixed-width
integers, not architecture-sized `usize`; adapters check conversion and limits.
Keys resolve relationships; old FD numbers, Rust pointers and tracing IDs do not.
Redundant indexes rebuild from validated semantic records.

Keep `InspectionSnapshot` and `FrozenImage` as different types. Require a
coordinator-produced cut token for image creation; a boolean `consistent: true`
supplied by any sampling adapter is insufficient. Sampling may project fewer
fields, so do not quietly derive a restorable DTO from inspection output.

### Generated boundaries

An explicit schema declaration may generate DTO derives, fixed Arrow columns,
serde adapters, stable field IDs, and **static** tracing callsite declarations
plus update match arms. Declarations should include field type, unit, nullability,
visibility, supported schema versions, update owner, and exposure policy
(`image`, `inspection`, `table`, `trace`, `metric`).

Example policy, not a generator implemented here:

| Field | Image | Inspection / Arrow | Trace | Metric label |
| --- | --- | --- | --- | --- |
| Subscription ID | yes | yes, authorized | scoped span value | never |
| Effective sessions | yes | membership table, authorized | count only | never |
| Active / phase / mode | yes | yes | fixed fields | only a reviewed finite enum if needed |
| Queue exact bytes | yes, private | length/reference summary only | never | never |
| Backlog byte count | yes/recomputed | yes | sampled/coalesced gauge | numeric value, not label |
| Credentials / TLS secrets | capability-specific private handling | never | never | never |

Do not generate exposure by reflecting every runtime struct or treating every
JSON key as a tracing field/metric label. The gateway's `metric_numbers` currently
walks numeric object fields but does not walk subscription arrays; extending its
input with registry records must not inadvertently expand the metric namespace.
Prefer a separate, fixed numeric projection. Codegen does not generate a freeze
protocol, transport import, authorization, or schema migration correctness.

## Three different consistency products

1. **Observational snapshot:** immutable output after sampling, potentially mixed
   owner instants. Record observation/generation identity, schema version and
   consistency class. Immutable output does not imply a consistent input cut.
2. **Reactive deltas:** bounded `Upsert`, typed `Patch`, and `Remove`, carrying
   generation/key/owner sequence. Commit semantic change before emission. Use a
   single sequence for a single owner, or explicit per-owner watermarks; do not
   invent a global order between independently concurrent queue and actor work.
   Initialize subscription to changes atomically with baseline/watermark, or use
   baseline + ordered replay. Lost delta/lag requires a fresh baseline. A watch
   channel coalesces latest state; broadcast can lag. Neither is a durable log.
3. **Consistent frozen image:** admission closed, commands settled, all relevant
   source/client/tunnel owners parked at the declared cut, exact queue/output/
   decoder material accounted for. Sealed immutable image plus descriptor
   membership; one successor takes ownership. Neither (1) nor (2) replaces it.

Apply bounded backpressure to optional export workers, **not the data plane**.
On telemetry overload drop/coalesce and count losses; on image export failure
abort preparation and resume old owners. Snapshot-derived diff misses transient
states and is not owner-change journaling. Crash continuity still needs a
surviving I/O owner or explicit syscall/state crash-consistency contract.

## Handover image alternatives, including optional rkyv

Start with a small versioned envelope: magic, envelope version, required features,
generation/cut/boot identity, producer/schema compatibility identifiers, section
codec IDs, checked lengths/offsets and integrity metadata. Metadata and bytes may
share a memfd or use separately named blob memfds. Exact frame sharing is explicit
arena references, not `Bytes` pointer identity. Preserve queue ordering and partial
write offsets; do not serialize kernel buffers or arbitrary Tokio/Hyper/rustls
objects. No codec solves their ownership/import blockers.

**Serde metadata + raw arena** is easier to inspect/migrate. **rkyv sections** are
useful when archived traversal or import allocation savings justify them. rkyv
0.8.18 documents compatibility only when schema is unchanged, format-control
features unchanged, and producer version semver-compatible. Pin/record those
choices; support explicit old-schema readers/migrations or refuse adoption.
Adding a Rust field is not transparent schema evolution. Mixed codec sections
allow a future schema to choose independently without forcing Polars consumers
to parse the private archive.

Use checked `rkyv::access::<ArchivedT, rancor::Error>` or
`from_bytes::<T, Error>` with bytecheck. Then separately validate semantic
references, offsets/length addition, limits, descriptor membership and protocol
phases. Checksums do not provide authenticity. Validate envelope before picking
an archived type. Mmap base alignment is not enough: embedded section offsets
must satisfy archived alignment too; keep mapping alive for borrowed archived
values. Remove writable mappings and seal image after freeze. Do not choose
in-place mutation merely because rkyv supports limited archived mutation.

The isolated [rkyv prototype](examples/rkyv-image/src/main.rs) demonstrates derive,
`to_bytes`, checked archived access, checked deserialize, explicit range checks,
exact SSE bytes, and rejection of truncation. It selects little endian, aligned,
64-bit pointer width and pins 0.8.18. Its intentionally incomplete image has no
FDs, transport state, mode/epoch or coordinator; **not a handover wire contract**.

## Arrow tables and Polars lazy execution

Normalize metadata into independently schematized tables. Candidate tables:

| Table | Keys / useful columns |
| --- | --- |
| `materials` | generation, material key, owner/kind/capability/reason |
| `connections` | connection key, role, protocol, phase, descriptor key, subscription key |
| `subscriptions` | ID, mode, active, interest count, backlog frames/bytes |
| `memberships` | subscription ID, session name (one row per membership) |
| `buffers` | buffer key, owner key, kind, byte length, unsent offset; private arena reference only when authorized |
| `queue_entries` | subscription ID, ordinal, frame key, length (optional private diagnostic) |
| `counters` / `gauges` | explicit typed numeric columns or reviewed finite measurement names + units |
| `deltas` | generation, owner sequence, key, operation, typed nullable changed fields |

Use consistent generation/snapshot identity across tables and a manifest listing
their schemas/files/cut class. One Arrow IPC file/stream has one schema; unrelated
tables should not be concatenated into a single stream with schema switches.
IDs/session names are row values, never column names. Preserve UInt64 counters,
null semantics and explicit time units/boot identity. Logical retained bytes are
not RSS or delivered bytes. Arrow `Binary` can carry bytes, but raw protocol
buffers normally belong in private arenas: tabularization is not a replay format.

**IPC file** has footer/block indexing and is appropriate for completed snapshot
artifacts, random access/mmap and `scan_ipc`. **IPC stream** is sequential
schema/dictionaries/batches without the file footer; use stream readers for pipes
or incremental output. Arrow streams do not inherently mean reactive updates:
applications must define delta operation/order/resync semantics. Polars'
*streaming execution engine* is yet another meaning: batched query execution, not
a subscription that recomputes when router state changes.

The [Python adapter](examples/arrow_exports.py) takes the Rust example's typed JSON
rows, pins dtypes including empty tables, wraps in `.lazy()` before expressions,
validates schema, calls `.collect()` once per output query and writes its result
as **both** IPC file and stream. Its group-by reads back with `scan_ipc` and lazy
expressions. A serialized LazyFrame/plan, where supported, is not an Arrow table
and should not be the interoperable export contract. Lazy planning also does not
avoid copying live Rust state into DTOs or allocating initial dataframe columns.

### Native Rust Polars alternative

Rust docs consulted resolve to **0.55.2**; Python prototype uses **2.0.0**.
Those are independently versioned packages, not an asserted release mapping.
This small native sketch is **docs-verified but not compiled here**; an optional
tool would select `lazy` and `ipc` features explicitly:

```rust
use polars::prelude::*;
use std::fs::File;

fn export(df: DataFrame) -> PolarsResult<()> {
    let mut result = df.lazy()
        .group_by([col("mode")])
        .agg([col("backlog_bytes").sum()])
        .collect()?;
    IpcWriter::new(File::create("backlog.arrow")?)
        .finish(&mut result)
}
```

0.55.2 `LazyFrame::sink(destination, format, args)` returns a **LazyFrame**;
`sink_batches(callback, maintain_order, chunk_size)` also returns a LazyFrame.
Execute the returned plan; constructing it is not proof a file was written.
For larger output, evaluate those sinks or batched writers against the selected
engine/version. `scan_ipc` is a lazy input path, not an exporter. `IpcWriter`
provides `with_compat_level` and custom schema metadata; IPC stream writing uses
the separate stream writer/feature path. Don't paste Python sink signatures into
Rust or assume old Rust convenience methods still exist.

Arrow compatibility is broader than package-version equality but not unlimited.
Select writer compatibility settings/types/compression supported by recipients
(new string-view layouts can matter); use simpler strings and uncompressed IPC
as a conservative start. Round-trip through the actual non-Polars consumer. The
included PyArrow test is evidence for the tested pair, not all Arrow versions.
Authentication, retention, redaction and file publication remain adapter duties.

## Can the same data reactively update tracing attributes?

**Yes, selected values can. Field names cannot grow arbitrarily on existing
macro-created tracing spans.** `info_span!(..., active = field::Empty, ...)`
declares fields statically. `span.record("active", new_value)` updates that field;
an undeclared field is silently ignored. Current subscription spans declare only
ID and mode, so active/interest/backlog fields would need explicit declarations
before production owner hooks can update them.

Three interoperating choices:

- **Fixed native fields (recommended):** generated/manual allowlisted callsites
  and typed adapter updates. On subscribe initialize; on committed replacement
  record active/count; on removal emit event then release span. Gauges sampled or
  coalesced. Owner remains authority, tracing is an exposure.
- **Structured change events:** fixed event fields `material_key`, `revision`,
  `kind`, `operation`, plus typed values, or a bounded redacted payload field.
  Better for change history and multi-field atomic observations. A JSON string
  payload is one string field, not automatically flattened structured attributes.
- **Layer-owned dynamic attribute map / backend-specific adapter:** keep typed,
  bounded attributes in registry span extensions or a material-key read model;
  custom `Layer`/formatter/backend merges them on events. That map does not alter
  `tracing::Metadata`; standard formatters/exporters will not automatically read
  it. An OpenTelemetry-specific dynamic attribute adapter is possible, but needs
  its own version/field/cardinality/retention tests and dependency decision.

Rust struct mutation is not observable by tracing automatically. Wire committed
owner changes to adapters, or use snapshot reconciliation and name it as such.
`Span::record` notifies subscriber `on_record`; whether values replace, append,
or export only at span end is a subscriber/backend contract. Already emitted log
events are immutable; update does not revise historical JSON. Disabled spans
record nothing. Do not make tracing the authoritative registry or restore source.

For hot queues, mutation comes from both actor admission and receiver dequeue.
Do not invoke expensive or reentrant tracing subscribers under `Backlog`'s mutex.
Capture a typed change/version while locked, release, then emit with a sequence
or reconcile latest state. Coalesce backlog updates; preserve no-publication-span
behavior. Bound per-material spans by live material limits, cap string lengths,
avoid session lists/payloads/secrets, remove registry entries on close, and bound
export queues. Separate metric cardinality limits from tracing-value policy.

The [Rust prototype](../../components/router/examples/state_exports.rs) creates an
export-only retained span and reconciles successive real snapshots into typed
subscription/membership rows. It emits changed values/events, drops removed rows,
and asserts cleanup. It is **observed-change reactive**, not a production owner
hook and not a complete delta protocol. The unit test uses `Layer::on_record` to
verify updated declared fields and that an undeclared field has no effect.
Changing spans without a new event may produce no new JSON log line; the demo
therefore emits explicit change/removal events. An event's revision is the batch
observation revision; span revision is its last state update, not global atomicity.

## Reproduce the experiments

From repository root, using an approved scratch directory:

```sh
cargo test -p bus-proxy-router --example state_exports
cargo run -p bus-proxy-router --example state_exports > /home/rektide/tmp-opencode/mighty-engine-inspection.json
uv venv /home/rektide/tmp-opencode/mighty-engine-polars
uv pip install --python /home/rektide/tmp-opencode/mighty-engine-polars/bin/python 'polars==2.0.0' 'pyarrow==26.0.0'
/home/rektide/tmp-opencode/mighty-engine-polars/bin/python design/event-multiplexer/examples/arrow_exports.py /home/rektide/tmp-opencode/mighty-engine-inspection.json /home/rektide/tmp-opencode/mighty-engine-arrow
/home/rektide/tmp-opencode/mighty-engine-polars/bin/python design/event-multiplexer/examples/test_arrow_exports.py
cargo run --manifest-path design/event-multiplexer/examples/rkyv-image/Cargo.toml --target-dir /home/rektide/tmp-opencode/mighty-engine-rkyv-target
```

Checks performed 2026-10-11: Rust example test and demo; Polars 2.0.0 IPC
file/stream self-roundtrip; PyArrow 26.0.0 file/stream interoperability with empty
tables, Unicode memberships and UInt64 maximum; rkyv 0.8.18 checked archive and
roundtrip (160-byte demonstration image), range validation and truncated archive
rejection. `cargo test -p bus-proxy-router` also passed all 22 existing tests,
including the retained-span/no-publication-span regression test.
Both Rust prototypes passed Clippy with `-D warnings`; their files were formatted
with Rustfmt (edition 2024).
Python is optional tooling; the isolated rkyv crate has its own lockfile and is
not a production workspace member. No Rust Polars build or migration/import test
is claimed. Artifacts in scratch are disposable experiment outputs.

## Open choices and next defensible moves

- Select export audiences/privacy before broadening schemas. Keep exact-byte
  images private and separate from ordinary authorized inspection.
- Decide whether initial Arrow delivery is offline JSON bridge, optional Rust
  Polars tool, or direct Arrow batches. Measure actual snapshot sizes before
  choosing batched sinks or copying optimizations.
- Specify owner change/watermark/resync contract only if a live subscriber is
  needed; do not mandate a global hot-path journal for periodic analytics.
- Trial allowlisted fields on retained subscription spans with backend replacement
  semantics tested, then consider queue gauge sampling. Dynamic backend attributes
  remain optional, not an excuse to manufacture unbounded callsites.
- Define a stable frozen DTO and import validation independently of codecs.
  Benchmark serde + arena against rkyv + arena on real queues; decide schema
  upgrade/migration policy before enabling fdstore image adoption.
- The resumable upstream/downstream transport and freeze work remains the main
  continuity blocker. Successful exports do not demonstrate successful handover.

## API evidence

Retrieved 2026-10-11:

- [rkyv 0.8.18 format controls and compatibility](https://docs.rs/rkyv/0.8.18/rkyv/),
  [`to_bytes`](https://docs.rs/rkyv/0.8.18/rkyv/fn.to_bytes.html).
- [Rust Polars 0.55.2 LazyFrame](https://docs.rs/polars/0.55.2/polars/prelude/struct.LazyFrame.html),
  [IpcWriter](https://docs.rs/polars/0.55.2/polars/prelude/struct.IpcWriter.html).
- [Python Polars IPC stream writer](https://docs.pola.rs/api/python/stable/reference/api/polars.DataFrame.write_ipc_stream.html)
  (source link identifies `py-2.0.0` when retrieved).
- [Arrow IPC file and streaming specifications](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc).
- [`tracing::Span::record`](https://docs.rs/tracing/0.1.44/tracing/struct.Span.html#method.record):
  static metadata fields, updates and undeclared-field behavior.
- Sourced Polars skill: local
  `/home/rektide/archive/polars-inc/skills/polars/SKILL.md` and
  `references/lazy-api.md`; lazy-first workflow, not evidence of Rust API spelling.
