use std::{
    io::{BufRead, BufReader, Read, Write},
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

#[test]
fn exports_real_request_spans_and_flushes_on_sigterm() {
    let collector = TcpListener::bind("127.0.0.1:0").unwrap();
    let collector_address = collector.local_addr().unwrap();
    collector.set_nonblocking(true).unwrap();
    let receive = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(15);
        let stream = loop {
            match collector.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "no OTLP export received");
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("collector accept: {error}"),
            }
        };
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut reader = BufReader::new(stream);
        let mut headers = String::new();
        let mut length = None;
        loop {
            let mut line = String::new();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            if line == "\r\n" { break; }
            if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                length = Some(value.trim().parse::<usize>().unwrap());
            }
            headers.push_str(&line);
        }
        let mut body = vec![0; length.expect("content length")];
        reader.read_exact(&mut body).unwrap();
        reader.get_mut().write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").unwrap();
        (headers, serde_json::from_slice::<serde_json::Value>(&body).unwrap())
    });

    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let mut command = Command::new(env!("CARGO_BIN_EXE_opencode-bus-proxy"));
    for (key, _) in std::env::vars_os() {
        let name = key.to_string_lossy();
        if name.starts_with("OTEL_") || name.starts_with("BUS_PROXY_")
            || name.starts_with("OPENCODE_SERVER_") || name.starts_with("LISTEN_")
            || name == "NOTIFY_SOCKET" || name == "WATCHDOG_USEC"
        {
            command.env_remove(key);
        }
    }
    let mut daemon = Daemon(command
        .args(["--listen", &address.to_string(), "--upstream", "http://127.0.0.1:1", "--allow-unauthenticated"])
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1/unused")
        .env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", format!("http://{collector_address}/custom-traces"))
        .env("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
        .env("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/json")
        .env("OTEL_EXPORTER_OTLP_HEADERS", "x-test=generic")
        .env("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "x-test=signal")
        .env("OTEL_SERVICE_NAME", "proxy-test")
        .env("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment.name=test,service.name=overridden")
        // Nothing should be exported until shutdown drains the batch queue.
        .env("OTEL_BSP_SCHEDULE_DELAY", "60000")
        .env("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")
        .env("RUST_LOG", "off")
        .env("BUS_PROXY_OTEL_FILTER", "info")
        .stdout(Stdio::null()).stderr(Stdio::piped()).spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut request = loop {
        if let Ok(stream) = TcpStream::connect(address) { break stream; }
        assert!(daemon.0.try_wait().unwrap().is_none(), "daemon exited before listening");
        assert!(Instant::now() < deadline, "daemon failed to listen");
        thread::sleep(Duration::from_millis(10));
    };
    request.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    request.write_all(b"GET /_bus/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").unwrap();
    let mut response = String::new();
    request.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 503"));
    // SAFETY: the owned child is still alive; SIGTERM exercises graceful shutdown.
    assert_eq!(unsafe { libc::kill(daemon.0.id() as i32, libc::SIGTERM) }, 0);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = daemon.0.try_wait().unwrap() {
            let mut stderr = String::new();
            daemon.0.stderr.take().unwrap().read_to_string(&mut stderr).unwrap();
            assert!(status.success(), "{status}: {stderr}");
            assert!(!stderr.contains("OTLP shutdown failed"), "{stderr}");
            break;
        }
        assert!(Instant::now() < deadline, "daemon shutdown timed out");
        thread::sleep(Duration::from_millis(10));
    }
    let (headers, payload) = receive.join().unwrap();
    assert!(headers.starts_with("POST /custom-traces HTTP/1.1"), "{headers}");
    assert!(headers.contains("x-test: signal"), "{headers}");
    let resources = payload["resourceSpans"].as_array().unwrap();
    assert!(resources.iter().any(|resource| {
        let attributes = resource["resource"]["attributes"].as_array().unwrap();
        attributes.iter().any(|a| a["key"] == "service.name" && a["value"]["stringValue"] == "proxy-test")
            && attributes.iter().any(|a| a["key"] == "deployment.environment.name" && a["value"]["stringValue"] == "test")
            && resource["scopeSpans"].as_array().unwrap().iter().any(|scope| {
                scope["spans"].as_array().unwrap().iter().any(|span| span["name"] == "http_request")
            })
    }), "{payload}");
}
