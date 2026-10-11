---
title: ClickHouse trace schema research — ClickStack/HyperDX ground and a compatible projection
description: Deployed/declarative ClickHouse tracing ground in ~/ff, the schema HyperDX and the OTel Collector ClickHouse exporter expect, clickhouse-arrow 0.2.1 type fit, and a recommended trace-table contract for bus-proxy.
resource: /design/event-multiplexer/clickhouse-schema0.glm53fx.md
status: draft research, not acceptance or implementation authorization
author: model:zai-coding-plan/glm-5.3-flash#max
date: 2026-10-11
sources:
  - resource: opencode://ses_ed2d67072ffeArvM65fh1ZiHZw
    title: FX research wave — inspected ff manifests, HyperDX/contrib sources, clickhouse-arrow crate; no server or deployment exercised
  - resource: /home/rektide/ff/observe/hyperdx/manifests/app.yaml
    title: ff HyperDX app manifest — DEFAULT_SOURCES UI expressions, DEFAULT_CONNECTIONS, Mongo/OpAMP wiring
  - resource: /home/rektide/ff/observe/hyperdx/manifests/otel-collector.yaml
    title: ff ClickStack collector manifest — clickstack-otel-collector:2, CLICKHOUSE_ENDPOINT, CREATE_LEGACY_SCHEMA=true
  - resource: /home/rektide/ff/observe/hyperdx/manifests/ch-server.yaml
    title: ff ClickHouse manifest — server 25.6-alpine, hdx_cluster, 100Gi data PVC, day-partition-friendly config
  - resource: /home/rektide/archive/hyperdxio/hyperdx
    title: HyperDX OSS @ 9ab68432afea86d6e824d6205fe12072c0142885 (2026-02-16, @hyperdx/api 2.16.0-20) — schema seed, goose migrate tool, OpAMP config build, sources model
  - resource: /home/rektide/archive/open-telemetry/opentelemetry-collector-contrib
    title: opentelemetry-collector-contrib @ 60b56a98edd6acf53f976c826f3c84e72c21dfd4 (2026-02-09) — clickhouseexporter DDL templates, JSON alpha variant, trace-id lookup table/MV
  - resource: /home/rektide/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/clickhouse-arrow-0.2.1
    title: clickhouse-arrow 0.2.1 published source — TypedBuilder Arrow↔ClickHouse type mapping and ArrowFormat insert path
  - resource: /design/event-multiplexer/export-boundaries.sol-xhigh.md
    title: Package boundaries, schema-vs-transport ownership, and the rule that DDL provisioning stays outside the daemon
  - resource: /design/event-multiplexer/clickhouse-http-draft.rs.txt
    title: Preserved custom HTTP draft — bespoke JSON-string column choices treated as inputs, not a standard
  - resource: /doc/event-multiplexer/operations.md
    title: Maintained OTLP trace-export behavior — completed spans + events, bounded batches, service-name and filter conventions
  - resource: /telemetry/clickhouse-native/README.md
    title: Sol xhigh native transport (working-copy read for handoff alignment, not accepted) — BatchClient/Table/insert contract, no table creation
---

# ClickHouse trace schema research

**Research only.** This wave answers "what schema actually makes sense" by
reading what exists. It builds nothing, deploys nothing, and benchmarks
nothing. The [export-boundaries proposal](export-boundaries.sol-xhigh.md) still
owns package/build decisions; its rule that the Arrow projection package owns
column names/types/nullability is exactly what this document feeds.

**"hyperdex" = HyperDX (ClickStack).** Local evidence (`~/ff/observe/hyperdx`,
the HyperDX OSS archive, the `hyperdxio` wiki) is entirely the ClickHouse
observability product. No artifact here references HyperDex, the distributed
KV store; the two are not conflated in this document.

## Present ground

**Declared but not running.** `~/ff/observe/hyperdx` holds a Kubernetes
experiment epic: raw manifests plus kustomize/helm/cue/cdk8s/ytt/timoni
renderings of a five-part ClickStack — `app` (hyperdx/hyperdx:2 + Mongo
5.0.32), `otel-collector` (`docker.clickhouse.com/clickstack-otel-collector:2`),
`ch-server` (ClickHouse 25.6-alpine, 100Gi data PVC), namespace. The reachable
cluster (default kubeconfig context) has **no `hyperdx` namespace and no
hyperdx pods** (checked 2026-10-11); nothing here proves a live deployment or a
materialized table schema. Everything below is declarative/source evidence.

**The deployment's own schema contract is in `app.yaml`.** The `DEFAULT_SOURCES`
ConfigMap is how the HyperDX UI finds data, and it names exact ClickHouse
expressions per source kind:

| Kind | Table | Expressions the UI relies on |
| --- | --- | --- |
| trace | `default.otel_traces` | `Timestamp`, `ServiceName`, `SpanName` (implicit), `SpanAttributes`, `ResourceAttributes`, `TraceId`, `SpanId`, `ParentSpanId`, `SpanKind`, `StatusCode`, `StatusMessage`, `Duration` with `durationPrecision: 9` (ns; UI displays `round(Duration/1e6)`) |
| log | `default.otel_logs` | `TimestampTime` (display `Timestamp`), `Body`, `SeverityText`, `ServiceName`, `LogAttributes`, `ResourceAttributes`, `TraceId`/`SpanId` |
| metric | `otel_metrics_{gauge,histogram,sum}` | `TimeUnix`, `ResourceAttributes`, `MetricName`-family columns |
| session | `default.hyperdx_sessions` | log-shaped (rrweb replay), routed by the collector from `rr-web.event` log attributes |

Querying goes through `DEFAULT_CONNECTIONS` → `http://ch-server:8123` as user
`default`. Dashboard accelerators live in a separate **`hyperdx` database** as
app-created `AggregatingMergeTree` materialized views
([hdxMTViews.ts](/home/rektide/archive/hyperdxio/hyperdx/packages/app/src/hdxMTViews.ts));
they are optional and derived — trace lookup itself needs no companion table.

**How tables come into existence.** The ClickStack collector image embeds
goose seed SQL (`docker/otel-collector/schema/seed/0000*.sql` in the OSS repo)
plus a small migrate tool (`packages/otel-collector/cmd/migrate`,
`HYPERDX_OTEL_EXPORTER_TABLES_TTL` default `720h`). `entrypoint.sh` runs the
seed **unless** `HYPERDX_OTEL_EXPORTER_CREATE_LEGACY_SCHEMA=true`, in which
case the collector's `clickhouse` exporter `create_schema` flag makes the
exporter run its own (intended-equivalent) DDL. The ff collector manifest sets
the legacy flag `true` and never runs the seed path. Either way the created
shape is the well-known OTel-ClickHouse schema, in database
`HYPERDX_OTEL_EXPORTER_CLICKHOUSE_DATABASE` (ff: `default`), TTL 720h.

## The trace table both writers target

`seed/00005_otel_traces.sql` (HyperDX @ 9ab68432) and upstream contrib
`clickhouseexporter/internal/sqltemplates/traces_table.sql` (@ 60b56a98) agree
on the core columns; HyperDX adds `__hdx_materialized_rum.sessionId`
(MATERIALIZED) and two extra indexes. Contract-relevant shape:

- `Timestamp DateTime64(9)`; partition `toDate(Timestamp)`; **order key
  `(ServiceName, SpanName, toDateTime(Timestamp))`**; `TTL toDate(Timestamp) +
  ${TABLES_TTL}`; `ttl_only_drop_parts = 1`; `index_granularity = 8192`.
- `TraceId`/`SpanId`/`ParentSpanId`/`TraceState`: plain `String`.
  Writer behavior ([exporter_traces.go](/home/rektide/archive/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/exporter_traces.go)):
  lowercase **hex, empty string when zero**; `Duration` =
  `end − start` in **nanoseconds, UInt64**; `StatusCode` = `Unset|Ok|Error`
  strings; `SpanKind` = `span.Kind().String()` (`internal`, `server`, …);
  `ServiceName` from the `service.name` resource attribute (empty when absent —
  bus-proxy already defaults `OTEL_SERVICE_NAME=opencode-bus-proxy`).
- Attributes are `Map(LowCardinality(String), String)` — **stringified
  values, not typed**; scope contributes `ScopeName`/`ScopeVersion` **only**
  (no `ScopeAttributes`, no `TraceFlags`, no dropped-counts columns in traces).
- Events/Links are **parallel arrays**: `Events.Timestamp Array(DateTime64(9))`,
  `Events.Name Array(LowCardinality(String))`,
  `Events.Attributes Array(Map(...))`; `Links.TraceId/SpanId/TraceState
  Array(String)`, `Links.Attributes Array(Map(...))` — flat dotted column
  names, not Arrow structs.

Consumers read exactly these columns: the HyperDX UI via the source
expressions above, and any contrib-compatible tooling via the same names.
Upstream contrib additionally ships an **alpha** JSON-attributes variant
(feature gate `clickhouse.json`, `traces_json_table.sql`: JSON columns +
`SpanAttributesKeys Array(LowCardinality(String))`) and an optional
`otel_traces_trace_id_ts` lookup table + MV (`traces_id_ts_lookup_*.sql`) —
neither is created by the ClickStack seed and neither is needed for HyperDX.

## Options compared

1. **Adopt the collector/ClickStack trace schema (recommended for traces).**
   The bus-proxy projection writes the 22 core columns above into a table with
   exactly this DDL. Any OTel collector, the ClickStack exporter, and the
   HyperDX UI can read or co-write it; no source remapping, no new query
   dialect. Cost: bus-proxy gives up the HTTP draft's JSON-string convenience
   (see below) and must encode Map/Array Arrow columns.
2. **HyperDX-native but bespoke tables.** Possible — the UI is
   expression-configured, so a custom table needs only a new `DEFAULT_SOURCES`
   entry — but every deviation re-buys UI compatibility, service map/session
   features assume the standard shape, and we become the second schema to
   teach every future consumer. Justified only if typed attribute columns
   become a proven need.
3. **Narrowly bespoke bus-proxy schema.** The preserved
   [HTTP draft](clickhouse-http-draft.rs.txt) projects JSON-string
   `ResourceAttributes`/`SpanAttributes`, single JSON `Events`/`Links` columns,
   plus `ScopeAttributes` and `DroppedAttributesCount`. **As written it is not
   insertable into the collector schema** and vice versa: `otel_traces` has no
   JSON-string attribute columns, no `ScopeAttributes`, no dropped-counts, and
   its events/links are parallel arrays. Choosing this path forfeits HyperDX
   unless an adapter/view is added later. Router-state/inspection exports
   ([state-exports.sol-medium.md](state-exports.sol-medium.md)) are a different
   product and get their own tables either way — never merged into
   `otel_traces`.

Logs and metrics are out of scope here: bus-proxy exports only spans +
attached events ([operations.md](../../doc/event-multiplexer/operations.md)).
If OTLP logs ever flow through the ClickStack collector they land in
`otel_logs`/`hyperdx_sessions` per its routing; direct-insert equivalents are a
separate contract.

## Recommended trace schema contract

**One table shape, ClickStack-compatible; provisioning outside the daemon;
both transports write the same projection.**

1. **DDL owner:** deployment configuration, not Rust startup. Reuse the
   collector's own creation path when the ClickStack collector is deployed, or
   vendored seed SQL (the five `0000*.sql` files are idempotent
   `CREATE TABLE IF NOT EXISTS`) for a bus-proxy-only database. The daemon and
   both export libraries never create, migrate, or TTL tables
   (consistent with [export-boundaries](export-boundaries.sol-xhigh.md)).
2. **Columns:** the 22 core columns listed above, non-nullable, empty strings /
   empty arrays for absent values, stringified attribute values, ns duration,
   hex IDs. The writer inserts an explicit column list (MATERIALIZED extras and
   future additive columns are excluded automatically).
3. **Arrow encoding for clickhouse-arrow 0.2.1** (from its published source,
   `src/arrow/builder.rs` tests + `src/formats/arrow.rs`): inserts convert the
   `RecordBatch` into native blocks **against the server-provided column
   header** (name-keyed `TypedBuilderMap`), so batch column order follows the
   batch schema (the transport builds `INSERT` column names from the batch) —
   mappings to use: `DateTime64(9)` ← `Timestamp(Nanosecond)` (prefer
   `Some("UTC")` timezone metadata); `String`/`LowCardinality(String)` ← `Utf8`
   (builder reconciles LowCardinality); `Map(LowCardinality(String), String)` ←
   `DataType::Map` of a `StructArray{key: Utf8, value: Utf8}`; `UInt64` ←
   `UInt64Array`; `Array(X)` ← `ListArray` (including
   `List(Map(...))` for `Events.Attributes`/`Links.Attributes`); dotted names
   (`Events.Timestamp`, …) are plain flat field names.
4. **Database ownership (open choice, see questions):** an own database (e.g.
   `bus_proxy.otel_traces`) with the identical DDL, own TTL, and zero coupling
   to the observability stack's `default` database — adding one HyperDX source
   entry later makes it browsable. Writing straight into `default.otel_traces`
   alongside the collector is the zero-config-UI alternative at the cost of
   shared table ownership and drift exposure to the floating `:2` images.
5. **Retention:** day partitions + `ttl_only_drop_parts=1` make TTL drops
   cheap; pick TTL per database deliberately (ClickStack default 720h) rather
   than inheriting it silently.

## Unanswered questions for the user

1. **Deployment profile:** direct native/Arrow insert only, OTLP-through-
   collector only, or both selectable? Both are schema-compatible; OTLP-to-
   collector needs no Arrow work at all, direct insert removes the collector
   dependency.
2. **Database ownership:** own `bus_proxy` database (isolated TTL/ownership,
   needs a HyperDX source entry) vs inserting into ClickStack's
   `default.otel_traces` (zero UI config, shared ownership)?
3. **Retention/TTL** for bus-proxy-owned tables, if (2) favors an own database.
4. Whether the router-state/inspection tables (separate product) should share
   this database now or stay unallocated until that work is commissioned.

## Handoff for the exporter author (Sol)

The [native transport](../../telemetry/clickhouse-native/README.md) already
provides `BatchClient`/`Table`/`insert(RecordBatch)` with explicit column
names, no retries, and no table creation — the schema work remaining is the
projection, and this document is its evidence base:

- Build the Arrow schema with exactly the 22 columns in §Recommended, per the
  type-mapping table; keep fields non-nullable; hex-encode IDs to `String`,
  `Duration` ns as `UInt64`, `StatusCode`/`SpanKind` as OTel strings,
  attributes stringified into Map entries, events/links as parallel arrays
  including empty-array (not null) when absent.
- Derive `ServiceName` from the resource exactly as the OTLP path does today
  (same `OTEL_SERVICE_NAME` default and precedence), so both exporters produce
  identical rows.
- Verify against a real ClickHouse 25.6 server before committing to the
  encoding: (a) `LowCardinality(String)` **map keys** from plain Utf8 entries,
  (b) `List(Map(...))` encoding, (c) DateTime64 timezone metadata handling,
  (d) native-protocol acceptance of the 22-column subset into a
  collector-created table containing `__hdx_materialized_*` columns, (e)
  protocol-revision handshake with server 25.6 (crate advertises 54479 —
  unverified claims per the [boundaries doc](export-boundaries.sol-xhigh.md)).
  clickhouse-local alone cannot prove the native path.
- Do not adopt the HTTP draft's JSON-string columns, `ScopeAttributes`, or
  `DroppedAttributesCount` in this table; if those values matter, park them in
  span attributes or a separate table.
- The upstream `_trace_id_ts` lookup table/MV is optional infra, not part of
  the writer contract; don't create it from the daemon.

**Honest gaps:** no live deployment or materialized schema was inspected (the
cluster has none); the local HyperDX archive (2026-02-16, API 2.16.0-20)
predates the floating `docker.clickhouse.com/clickstack-otel-collector:2` /
`hyperdx:2` images the ff manifests pull, so the newest ClickStack revision
could have evolved the seed — re-verify seed vs image when a deployment is
actually stood up; all clickhouse-arrow statements come from reading published
source, never executing it; the ff ClickHouse `users.xml` contains plaintext
dev passwords (`api`/`worker`) which were noted structurally and are not
reproduced as usable credentials here.

## Related

- [Export boundaries and consumer builds](export-boundaries.sol-xhigh.md) —
  owns package/feature decisions; this document supplies the schema half of
  its step 3 ("specify and fixture the common trace table/DDL").
- [Additive events](additive-events.md) — the span/event material the
  projection consumes; the router can also retain spans unexported while open.
- [Typed state exports](state-exports.sol-medium.md) — observational
  snapshots/sequenced changes are a different tabular product with its own
  tables; do not fold into trace analytics.
- [OTLP trace export (operations)](../../doc/event-multiplexer/operations.md) —
  today's export semantics (completed spans, bounded batches, filters) that any
  direct-insert path must preserve.
