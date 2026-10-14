#[path = "../../test/support/clickhouse.rs"]
mod clickhouse;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};
struct Daemon(Child);
impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn clean_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_opencode-bus-proxy"));
    for (key, _) in std::env::vars_os() {
        let name = key.to_string_lossy();
        if name.starts_with("OTEL_")
            || name.starts_with("BUS_PROXY_")
            || name.starts_with("OPENCODE_SERVER_")
            || name.starts_with("LISTEN_")
            || matches!(
                name.as_ref(),
                "NOTIFY_SOCKET"
                    | "WATCHDOG_USEC"
                    | "CLICKHOUSE_NATIVE_DEBUG_ARROW"
                    | "CONNECTION_READ_BUFFER_SIZE"
                    | "CONNECTION_WRITE_BUFFER_SIZE"
            )
        {
            command.env_remove(key);
        }
    }
    command
}

#[test]
fn selected_native_rejects_upstream_ambient_flags_and_disable_wins() {
    for name in [
        "CLICKHOUSE_NATIVE_DEBUG_ARROW",
        "CONNECTION_READ_BUFFER_SIZE",
        "CONNECTION_WRITE_BUFFER_SIZE",
    ] {
        let output = clean_command()
            .args([
                "--listen",
                "127.0.0.1:0",
                "--upstream",
                "http://127.0.0.1:1",
                "--allow-unauthenticated",
            ])
            .env("OTEL_TRACES_EXPORTER", "clickhouse-native")
            .env(name, "true")
            .output()
            .unwrap();
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(
            error.contains(name) && error.contains("unsupported"),
            "{error}"
        );
    }
    for sdk_disabled in [false, true] {
        let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reservation.local_addr().unwrap();
        drop(reservation);
        let mut command = clean_command();
        command
            .args([
                "--listen",
                &address.to_string(),
                "--upstream",
                "http://127.0.0.1:1",
                "--allow-unauthenticated",
            ])
            .env("CLICKHOUSE_NATIVE_DEBUG_ARROW", "true")
            .env("BUS_PROXY_CLICKHOUSE_NATIVE_TLS", "invalid")
            .env("BUS_PROXY_OTEL_FILTER", "[")
            .env("BUS_PROXY_OTEL_SHUTDOWN_TIMEOUT_MS", "invalid")
            .env("RUST_LOG", "off")
            .env(
                "OTEL_TRACES_EXPORTER",
                if sdk_disabled {
                    "clickhouse-native"
                } else {
                    "none"
                },
            );
        if sdk_disabled {
            command.env("OTEL_SDK_DISABLED", "TRUE");
        }
        let mut daemon = Daemon(
            command
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if TcpStream::connect(address).is_ok() {
                break;
            }
            assert!(
                daemon.0.try_wait().unwrap().is_none(),
                "disabled telemetry still validated native config"
            );
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(10));
        }
    }
}

#[test]
#[ignore = "starts an isolated disposable installed ClickHouse server"]
fn native_daemon_flushes_real_request_spans_with_otlp_available() {
    let server = clickhouse::Server::start();
    server.query(include_str!("../../telemetry/arrow/schema/traces-v1.sql"));
    server.query(
        "ALTER TABLE otel_traces ADD COLUMN materialized_service String MATERIALIZED ServiceName",
    );
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let mut command = clean_command();
    let mut daemon = Daemon(
        command
            .args([
                "--listen",
                &address.to_string(),
                "--upstream",
                "http://127.0.0.1:1",
                "--allow-unauthenticated",
            ])
            .env(
                "OTEL_TRACES_EXPORTER",
                if cfg!(feature = "otlp") {
                    "clickhouse-native,otlp"
                } else {
                    "clickhouse-native"
                },
            )
            .env("BUS_PROXY_CLICKHOUSE_NATIVE_HOST", "127.0.0.1")
            .env("BUS_PROXY_CLICKHOUSE_NATIVE_PORT", server.port.to_string())
            .env("BUS_PROXY_CLICKHOUSE_NATIVE_TLS", "false")
            .env("BUS_PROXY_CLICKHOUSE_NATIVE_TABLE", "otel_traces")
            .env("BUS_PROXY_CLICKHOUSE_NATIVE_TIMEOUT_MS", "1000")
            .env("OTEL_SERVICE_NAME", "native-daemon-test")
            .env(
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
                "http://127.0.0.1:1/unavailable-collector",
            )
            .env("OTEL_EXPORTER_OTLP_TIMEOUT", "100")
            .env("OTEL_BSP_SCHEDULE_DELAY", "60000")
            .env("RUST_LOG", "off")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut request = loop {
        if let Ok(stream) = TcpStream::connect(address) {
            break stream;
        }
        assert!(
            daemon.0.try_wait().unwrap().is_none(),
            "daemon exited before listening"
        );
        assert!(Instant::now() < deadline, "daemon did not listen");
        thread::sleep(Duration::from_millis(10));
    };
    request
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    request
        .write_all(b"GET /_bus/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut response = String::new();
    request.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 503"));
    // SAFETY: SIGTERM targets the live owned child and exercises graceful drain.
    assert_eq!(
        unsafe { libc::kill(daemon.0.id() as i32, libc::SIGTERM) },
        0
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = daemon.0.try_wait().unwrap() {
            let mut stderr = String::new();
            daemon
                .0
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            assert!(status.success(), "{status}: {stderr}");
            assert!(
                !stderr.contains("native telemetry shutdown failed"),
                "{stderr}"
            );
            break;
        }
        assert!(Instant::now() < deadline, "daemon shutdown stalled");
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(server.query("SELECT count(),any(ServiceName),any(materialized_service) FROM otel_traces WHERE SpanName='http_request'"), "1\tnative-daemon-test\tnative-daemon-test\n");
}
