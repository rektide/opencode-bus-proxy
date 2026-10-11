//! Disposable local-server integration, opt in with nextest --run-ignored only.
use arrow_array::{RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use bus_proxy_clickhouse_native::{BatchClient, Config, Table};
use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

/// Forward the real native protocol, but hide responses once the marker is sent.
/// The test observes the committed row separately, then cuts the client socket.
fn lose_ack(
    server_port: u16,
    marker: &'static str,
) -> (u16, Arc<AtomicBool>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let cut = Arc::new(AtomicBool::new(false));
    let control = cut.clone();
    let task = thread::spawn(move || {
        let (mut client_read, _) = listener.accept().unwrap();
        let mut client_write = client_read.try_clone().unwrap();
        let mut upstream_read = TcpStream::connect(("127.0.0.1", server_port)).unwrap();
        let mut upstream_write = upstream_read.try_clone().unwrap();
        upstream_read
            .set_read_timeout(Some(Duration::from_millis(20)))
            .unwrap();
        let suppress = Arc::new(AtomicBool::new(false));
        let suppressing = suppress.clone();
        let upload = thread::spawn(move || {
            let mut bytes = Vec::new();
            let mut buf = [0; 4096];
            while let Ok(n) = client_read.read(&mut buf) {
                if n == 0 {
                    break;
                }
                bytes.extend_from_slice(&buf[..n]);
                if bytes.windows(marker.len()).any(|w| w == marker.as_bytes()) {
                    suppressing.store(true, Ordering::Release);
                }
                if upstream_write.write_all(&buf[..n]).is_err() {
                    break;
                }
            }
        });
        let mut buf = [0; 4096];
        while !control.load(Ordering::Acquire) {
            match upstream_read.read(&mut buf) {
                Ok(0) => break,
                Ok(n) if !suppress.load(Ordering::Acquire) => {
                    if client_write.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
                Ok(_) => {}
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(_) => break,
            }
        }
        let _ = client_write.shutdown(Shutdown::Both);
        upload.join().unwrap();
    });
    (port, cut, task)
}

#[path = "../../../test/support/clickhouse.rs"]
mod clickhouse;
use clickhouse::Server;
impl Server {
    fn config(&self, table: &str) -> Config {
        Config {
            host: "127.0.0.1".into(),
            port: self.port,
            tls: false,
            tls_server_name: None,
            username: "default".into(),
            password: String::new(),
            table: Table::new("default", table).unwrap(),
            operation_timeout: Duration::from_secs(3),
            max_batch_rows: 1024,
            max_batch_bytes: 16 * 1024 * 1024,
        }
    }
}

#[test]
#[ignore = "requires installed clickhouse server/client; starts an isolated disposable instance"]
fn native_insert_roundtrip_and_post_send_failure() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("clickhouse_arrow=debug")
        .with_test_writer()
        .try_init();
    let server = Server::start();
    server.query("CREATE TABLE raw_test (value String, count UInt64, server_default String DEFAULT 'server', CONSTRAINT valid CHECK value != 'bad') ENGINE=MergeTree ORDER BY tuple()");
    let client = BatchClient::new(server.config("raw_test")).unwrap();
    let batch = |value: &str| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Utf8, false),
                Field::new("count", DataType::UInt64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec![value])),
                Arc::new(UInt64Array::from(vec![u64::MAX])),
            ],
        )
        .unwrap()
    };
    futures_executor::block_on(client.insert(batch("雪"))).unwrap();
    assert_eq!(
        server.query("SELECT value,count,server_default FROM raw_test"),
        "雪\t18446744073709551615\tserver\n"
    );
    // Constraint rejection happens after the data send, in the drained response.
    assert!(futures_executor::block_on(client.insert(batch("bad"))).is_err());
    assert_eq!(server.query("SELECT count() FROM raw_test"), "1\n");
    // Interrupted connections are not reused; next batch establishes a new one.
    futures_executor::block_on(client.insert(batch("recovered"))).unwrap();
    client.shutdown().unwrap();
    assert_eq!(server.query("SELECT count() FROM raw_test"), "2\n");

    // The server committed this insert but its acknowledgment is withheld. A
    // closed upstream response channel must not be misreported as success.
    let marker = "lost_ack_marker_9f3a";
    let (port, cut, proxy) = lose_ack(server.port, marker);
    let mut cfg = server.config("raw_test");
    cfg.port = port;
    let client = BatchClient::new(cfg).unwrap();
    let data = batch(marker);
    let inserting = thread::spawn(move || {
        let result = futures_executor::block_on(client.insert(data));
        client.shutdown().unwrap();
        result
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    while server.query("SELECT count() FROM raw_test WHERE value='lost_ack_marker_9f3a'") != "1\n" {
        assert!(
            Instant::now() < deadline,
            "insert did not commit through proxy"
        );
        thread::sleep(Duration::from_millis(20));
    }
    cut.store(true, Ordering::Release);
    assert!(
        inserting.join().unwrap().is_err(),
        "lost acknowledgment became false success"
    );
    proxy.join().unwrap();
    let mut wrong = server.config("does_not_exist");
    let client = BatchClient::new(wrong.clone()).unwrap();
    assert!(futures_executor::block_on(client.insert(batch("missing table"))).is_err());
    client.shutdown().unwrap();
    wrong.password = "credential-that-must-not-be-in-errors".into();
    let client = BatchClient::new(wrong).unwrap();
    let error = futures_executor::block_on(client.insert(batch("test"))).unwrap_err();
    assert!(!error.to_string().contains("credential-that"));
    client.shutdown().unwrap();
    #[cfg(feature = "otel")]
    traces(&server);
    eprintln!("Verified {}", server.query("SELECT version()").trim());
}

#[cfg(feature = "otel")]
#[path = "../../arrow/tests/common/mod.rs"]
mod common;

#[cfg(feature = "otel")]
fn traces(server: &Server) {
    use bus_proxy_export_arrow::{
        opentelemetry::trace::{SpanContext, Status},
        traces::{Policy, project},
    };
    let ddl =
        bus_proxy_export_arrow::EXAMPLE_DDL.replace("default.otel_traces", "default.native_traces");
    server.query(&ddl);
    server.query("ALTER TABLE native_traces ADD COLUMN `__hdx_materialized_rum.sessionId` String MATERIALIZED ResourceAttributes['rum.sessionId']");
    let (span, resource) = common::fixture();
    let mut empty = span.clone();
    empty.span_context = SpanContext::empty_context();
    empty.name = "empty span".into();
    empty.events = Default::default();
    empty.links = Default::default();
    empty.status = Status::Ok;
    let client = BatchClient::new(server.config("native_traces")).unwrap();
    let batch = project(&[span, empty], &resource, &Policy::default()).unwrap();
    futures_executor::block_on(client.insert(batch)).unwrap();
    client.shutdown().unwrap();
    assert_eq!(server.query("SELECT toUnixTimestamp64Nano(Timestamp),Duration,TraceId,SpanId,ParentSpanId,SpanKind,StatusCode,ScopeName,ScopeVersion,`__hdx_materialized_rum.sessionId` FROM native_traces WHERE SpanName='雪 span'"),
        "1700000000123456780\t120\t00000000000000000000000000abcdef\t0000000000000123\t\tServer\tError\tfixture-scope\t1.2.3\tfixture-session\n");
    assert_eq!(server.query("SELECT ResourceAttributes['resource.number'],SpanAttributes['span.bool'],has(mapKeys(ResourceAttributes),'authorization'),length(`Events.Name`),length(`Links.TraceId`),`Events.Name`[1],toUnixTimestamp64Nano(`Events.Timestamp`[1]),`Events.Attributes`[1]['event.bool'],`Links.Attributes`[1]['link.int'],has(mapKeys(`Events.Attributes`[1]),'Password'),`Links.TraceState`[1] FROM native_traces WHERE SpanName='雪 span'"),
        "1.5\ttrue\t0\t2\t2\t雪 event\t1700000000123456789\ttrue\t-123\t0\tvendor=state\n");
    assert_eq!(server.query("SELECT empty(TraceId),empty(SpanId),length(`Events.Name`),length(`Links.TraceId`),StatusCode FROM native_traces WHERE SpanName='empty span'"), "1\t1\t0\t0\tOk\n");
    // SDK's ordinary batch processor supplies no reactor. Its adapter must still
    // flush and join successfully with resource policy set by the host provider.
    use bus_proxy_export_arrow::{
        opentelemetry::trace::{Span, Tracer, TracerProvider},
        opentelemetry_sdk::trace::{
            BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider, SpanLimits,
        },
    };
    let client = BatchClient::new(server.config("native_traces")).unwrap();
    let provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_sampler(Sampler::AlwaysOn)
        .with_span_limits(SpanLimits::default())
        .with_span_processor(
            BatchSpanProcessor::builder(
                bus_proxy_clickhouse_native::otel::Exporter::new(client.clone(), Policy::default())
                    .unwrap(),
            )
            .with_batch_config(
                BatchConfigBuilder::default()
                    .with_max_queue_size(8)
                    .with_max_export_batch_size(4)
                    .with_scheduled_delay(Duration::from_secs(60))
                    .build(),
            )
            .build(),
        )
        .build();
    provider.tracer("adapter-scope").start("adapter span").end();
    provider.force_flush().unwrap();
    provider.shutdown().unwrap();
    client.shutdown().unwrap();
    assert_eq!(
        server.query(
            "SELECT count(),any(ServiceName) FROM native_traces WHERE SpanName='adapter span'"
        ),
        "1\tfixture-service\n"
    );
}
