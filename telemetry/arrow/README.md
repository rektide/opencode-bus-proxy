---
type: SchemaContract
title: Collector-compatible Arrow trace projection v1
description: Transport-free Arrow 57 projection for the ClickHouse collector and HyperDX trace-table shape.
resource: /telemetry/arrow/README.md
status: implemented, database provisioning and retention remain operator-owned
author: model:openai/gpt-6.1-sol#xhigh
date: 2026-10-11
sources:
  - resource: opencode://ses_ed2d6efafffeJebEhJ6FDRWIeF
    title: Research-informed projection, exact pdata casing/stringification, native schema/value verification
  - resource: /design/event-multiplexer/clickhouse-schema0.glm53fx.md
    title: Independent infrastructure/HyperDX/collector schema research
  - resource: https://github.com/hyperdxio/hyperdx/blob/9ab68432afea86d6e824d6205fe12072c0142885/docker/otel-collector/schema/seed/00005_otel_traces.sql
    title: HyperDX seed — 22 inserted columns and extra MATERIALIZED column
  - resource: https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/60b56a98edd6acf53f976c826f3c84e72c21dfd4/exporter/clickhouseexporter/exporter_traces.go
    title: Collector duration, IDs, service, events/links and stringification
  - resource: https://github.com/open-telemetry/opentelemetry-collector/blob/f857018caa62e6f03b13f34da93875ad94c6e912/pdata/ptrace/span_kind.go
    title: Verified pdata SpanKind.String uses Internal/Server/Client/Producer/Consumer (not lowercase)
  - resource: https://github.com/open-telemetry/opentelemetry-collector/blob/f857018caa62e6f03b13f34da93875ad94c6e912/pdata/pcommon/value.go
    title: Verified AsString float formatting, HTML-escaped JSON arrays and non-finite handling
---

# Collector-compatible Arrow trace projection v1

`bus-proxy-export-arrow` owns **`otel-clickhouse-traces-v1`**, not a transport.
Default features expose Arrow 57 `schema()`, `empty_batch()`, `COLUMNS` and the
operator-owned [DDL example](schema/traces-v1.sql), with no OTel SDK or network.
Optional **`otel-traces`** exposes `traces::project(&[SpanData], &Resource, &Policy)`.
The schema identity is Arrow metadata, not an invented extra table column.

## Table contract

Exactly **22 non-nullable columns**, matching the inspected collector/HyperDX
trace shape. This is not the alpha JSON-attributes schema. The native adapter
uses explicit insert columns, omitting server-owned MATERIALIZED/default extras.

| Columns | ClickHouse type / meaning |
| --- | --- |
| `Timestamp` | `DateTime64(9)`; start Unix nanoseconds, Arrow Timestamp(ns, UTC) |
| `TraceId`, `SpanId`, `ParentSpanId` | String, lowercase hex; empty for all-zero IDs |
| `TraceState` | String W3C header; empty when absent |
| `SpanName`, `SpanKind`, `ServiceName` | String/LowCardinality(String); kind **Internal / Server / Client / Producer / Consumer** |
| `ResourceAttributes`, `SpanAttributes` | Map(LowCardinality(String), String), values stringified like pdata `AsString` |
| `ScopeName`, `ScopeVersion` | String; empty missing version |
| `Duration` | UInt64 nanoseconds, checked end minus start |
| `StatusCode`, `StatusMessage` | **Unset / Ok / Error**; message only for Error |
| `Events.Timestamp`, `Events.Name`, `Events.Attributes` | Parallel arrays of DateTime64(9), LowCardinality(String), Map |
| `Links.TraceId`, `Links.SpanId`, `Links.TraceState`, `Links.Attributes` | Parallel arrays of String, String, String, Map |

Empty maps/arrays are real empty values, never null. The Arrow fields for dotted
columns are flat names, not structs. Utf8 map keys and list items are converted
against the server's LowCardinality header by the external native client.
`ServiceName` comes from `service.name`, empty if absent; the daemon supplies its
normal service/resource defaults. Missing/zero IDs become empty, not a zero-filled
hex string. Duplicate attribute keys use their last value, matching pdata maps.

Strings remain strings; booleans and integers stringify normally. Floats follow
pdata's ES6-style representation (including `-0`); non-finite scalars become
`json: unsupported value: NaN|+Inf|-Inf`. Arrays use compact Go-compatible JSON
escaping (`<`, `>`, `&`, U+2028/U+2029 included). Arrays containing a non-finite
float become an empty string, matching the inspected collector's ignored JSON
marshal error. Rust SDK values cannot represent pdata bytes/maps/nested arrays.

## Deliberate losses and bounds

The standard table **does not retain** scope attributes/schema URL, resource
schema URL, span/link flags, remote-parent status, or dropped attribute/event/link
counts. We do not smuggle these into unrelated attributes or add incompatible
columns. Existing attributes, events, links, resource and scope name/version are
preserved subject to the explicit policy. OTLP remains available for fuller SDK
semantics; neither path exports still-open spans or standalone logs/live state.

`Policy` defaults: 4096 spans, 16 KiB per string/encoded attribute value, 1024
attributes per map, 128 events and 128 links per span, 1024 items per attribute
array, and 16 MiB payload budget. Oversize/invalid batches **fail whole**, rather
than quietly truncating or returning falsely complete rows. Timestamp columns
must fit nonnegative signed i64 Unix nanoseconds; reversed times and UInt64
duration overflow are rejected rather than wrapped. Empty batches are valid.

Default case-insensitive **exact key** redaction removes `authorization`,
`password`, `token`, `cookie`, `set-cookie`, `http.request.header.authorization`
and `http.request.header.cookie` from all exported maps. Callers can replace the
typed list; daemon `REDACT_KEYS` adds keys. This is not a universal PII filter:
review instrumentation, names/status text and string contents before export.

## Provisioning and UI fit

Use an existing verified collector table, or **edit then explicitly execute**
the DDL example. No daemon/library creates or migrates it. The example intentionally
chooses **no retention policy** and no companion trace-id lookup MV. Database,
indexes, TTL and table ownership are deployment decisions. A same-shape table in
another database needs a matching HyperDX source configuration, not new encoding.
HyperDX's inspected source expects duration precision 9; neither the latest
floating images nor a running HyperDX UI were verified by this work.

## Verification

Projection fixtures cover scalar/array stringification, escaping, zero IDs,
Unicode, all kind/status spellings, resource/scope, parallel events/links, empty
values, redaction, checked timestamps and limits. The [native integration](../clickhouse-native/README.md#verify)
inserted this exact shape on **ClickHouse 25.6.13.41 and 26.9.7.9**, queried back
nanoseconds and nested maps, and omitted a HyperDX-style MATERIALIZED extra.
Use `cargo nextest run -p bus-proxy-export-arrow --features otel-traces --locked`.

The [schema research](../../design/event-multiplexer/clickhouse-schema0.glm53fx.md)
supplies infrastructure context; the actual pdata source above corrects its
lowercase-kind shorthand. [Operations](../../doc/event-multiplexer/operations.md#native-clickhouse-trace-export)
owns daemon selection and environment configuration. The preserved HTTP draft
does not implement this schema and remains unchanged.
