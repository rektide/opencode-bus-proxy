use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use bus_proxy_gateway::{Config, serve};
use bus_proxy_protocol::SseDecoder;
use bus_proxy_router::{Limits, Router};
use bytes::Bytes;
use hmac::{Hmac, Mac};
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, StreamBody, combinators::UnsyncBoxBody};
use hyper::{
    body::{Frame, Incoming},
    service::service_fn,
};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::{TokioExecutor, TokioIo},
};
use sha2::Sha256;
use std::{convert::Infallible, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Default)]
struct TraceCapture(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
impl std::io::Write for TraceCapture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for TraceCapture {
    type Writer = Self;
    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn lifecycle_tracing_uses_only_bounded_redacted_http_metadata() {
    let capture = TraceCapture::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_env_filter("bus_proxy_gateway=debug")
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .with_writer(capture.clone())
        .finish();
    // One integration-process subscriber covers independently spawned Tokio tasks
    // and concurrent test runtimes; a thread-local default misses task dispatches.
    tracing::subscriber::set_global_default(subscriber).unwrap();
    let mut harness = Harness::new().await;
    let source = harness.sources.recv().await.unwrap();
    harness.ready().await;
    let mut feed = harness.feed(true).await;
    let id = feed.next().await.unwrap()["data"]["subscriptionID"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(harness.interest(&id, "ses_private").await, 204);
    let response = harness
        .client
        .request(harness.request(
            "GET",
            "/private-operation-name?auth_token=private-query-secret",
            full(Bytes::new()),
        ))
        .await
        .unwrap();
    response.into_body().collect().await.unwrap();
    source
        .send(event("broadcast", None, "private-event-payload"))
        .await
        .unwrap();
    assert_eq!(feed.next().await.unwrap()["type"], "server.connected");
    assert_eq!(feed.next().await.unwrap()["id"], "private-event-payload");
    source
        .send(Bytes::from_static(
            b"data: {\"type\":\"future\",\"scope\":{\"sessionID\":\"private-source-value\"}}\n\n",
        ))
        .await
        .unwrap();
    assert!(feed.aborted().await);
    // Reconnect starts after the failure category has been logged.
    let _next_source = harness.sources.recv().await.unwrap();
    drop(feed);
    harness.close().await;
    let captured = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    assert!(captured.contains("source_attempt"));
    assert!(captured.contains("source_epoch"));
    assert!(captured.contains("source epoch retired"));
    assert!(captured.contains("http_request"));
    assert!(captured.contains("interests"));
    assert!(captured.contains("forward"));
    assert!(captured.contains("reason=\"event_metadata\""));
    for sensitive in [
        &id,
        "ses_private",
        "private-operation-name",
        "auth_token",
        "private-query-secret",
        "private-event-payload",
        "private-source-value",
        &STANDARD.encode("opencode:secret"),
    ] {
        assert!(!captured.contains(sensitive), "leaked {sensitive}");
    }
}

#[tokio::test]
async fn connection_limits_must_be_positive() {
    for config in [
        Config {
            max_connections: 0,
            ..Config::default()
        },
        Config {
            header_read_timeout: Duration::ZERO,
            ..Config::default()
        },
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stop = CancellationToken::new();
        let (router, task) = Router::spawn(Limits::default(), stop.clone()).unwrap();
        assert!(matches!(
            serve(listener, config, router, stop.clone()).await,
            Err(bus_proxy_gateway::GatewayError::Configuration(_))
        ));
        stop.cancel();
        task.await.unwrap();
    }
}
type Body = UnsyncBoxBody<Bytes, Error>;

#[tokio::test]
async fn reused_hyper_sse_has_an_explicit_nontransferable_subscription_association() {
    let harness = Harness::new().await;
    harness.ready().await;
    let address = harness.base.strip_prefix("http://").unwrap();
    let socket = TcpStream::connect(address).await.unwrap();
    let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
        .await
        .unwrap();
    tokio::spawn(connection);
    let request = |path| {
        Request::builder()
            .uri(path)
            .header("host", address)
            .header(
                "authorization",
                format!("Basic {}", STANDARD.encode("opencode:secret")),
            )
            .body(Full::new(Bytes::new()))
            .unwrap()
    };
    client
        .send_request(request("/_bus/health"))
        .await
        .unwrap()
        .into_body()
        .collect()
        .await
        .unwrap();
    let mut body = client
        .send_request(request("/api/experimental/event"))
        .await
        .unwrap()
        .into_body();
    let mut decoder = SseDecoder::new(1024 * 1024);
    let id = loop {
        let frame = body.frame().await.unwrap().unwrap();
        if let Ok(data) = frame.into_data()
            && let Some(frame) = decoder.push(&data).unwrap().into_iter().next()
        {
            let ready: serde_json::Value = serde_json::from_slice(&frame).unwrap();
            break ready["data"]["subscriptionID"].as_str().unwrap().to_owned();
        }
    };
    let mut request = harness.request("GET", "/_bus/handover", full(Bytes::new()));
    request
        .headers_mut()
        .insert("origin", "http://localhost:1".parse().unwrap());
    let response = harness.client.request(request).await.unwrap();
    assert_eq!(
        response.headers()["access-control-allow-origin"],
        "http://localhost:1"
    );
    let value: serde_json::Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let connection = value["connections"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["subscription"] == id)
        .unwrap();
    assert_eq!(connection["role"], "opaque_http1_sse");
    assert_eq!(connection["capability"], "unsupported");
    assert!(
        value["routing"]["subscriptions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["id"] == id && s["active"] == false)
    );
    drop(body);
    harness.close().await;
}

#[tokio::test]
async fn queue_policy_control_is_authenticated_bounded_cors_compatible_and_inspectable() {
    let harness = Harness::new().await;
    let mut unauthorized = harness.request("PUT", "/_bus/queue", full("{\"frames\":0}"));
    unauthorized.headers_mut().remove("authorization");
    assert_eq!(
        harness.client.request(unauthorized).await.unwrap().status(),
        401
    );
    for invalid in [
        "{\"frames\":-1}",
        "{\"frames\":1.5}",
        "{\"frames\":\"0\"}",
        "{\"frames\":4294967296}",
        "{\"frames\":0,\"extra\":true}",
        "{}",
        &" ".repeat(1025),
    ] {
        assert_eq!(
            harness
                .client
                .request(harness.request("PUT", "/_bus/queue", full(invalid.to_owned())))
                .await
                .unwrap()
                .status(),
            400
        );
    }
    for frames in [0, 12] {
        let mut request = harness.request(
            "PUT",
            "/_bus/queue",
            full(format!("{{\"frames\":{frames}}}")),
        );
        request
            .headers_mut()
            .insert("origin", "http://localhost:1".parse().unwrap());
        let response = harness.client.request(request).await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.headers()["access-control-allow-origin"],
            "http://localhost:1"
        );
        let response = harness
            .client
            .request(harness.request("GET", "/_bus/queue", full(Bytes::new())))
            .await
            .unwrap();
        let value: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(value["frames"], frames);
        let response = harness
            .client
            .request(harness.request("GET", "/_bus/subscriptions", full(Bytes::new())))
            .await
            .unwrap();
        let value: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(value["queue_frame_target"], frames);
    }
    harness.close().await;
}
fn full(value: impl Into<Bytes>) -> Body {
    Full::new(value.into())
        .map_err(|never| match never {})
        .boxed_unsync()
}
fn client() -> Client<HttpConnector, Body> {
    Client::builder(TokioExecutor::new()).build_http()
}
fn token() -> String {
    let expires = "4102444800";
    let mut derive = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
    derive.update(b"opencode-session-v1");
    let mut mac = Hmac::<Sha256>::new_from_slice(&derive.finalize().into_bytes()).unwrap();
    mac.update(expires.as_bytes());
    format!(
        "{expires}.{}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    )
}
struct Harness {
    base: String,
    client: Client<HttpConnector, Body>,
    stop: CancellationToken,
    gateway: tokio::task::JoinHandle<Result<(), bus_proxy_gateway::GatewayError>>,
    router: tokio::task::JoinHandle<()>,
    origin: tokio::task::JoinHandle<()>,
    sources: mpsc::Receiver<mpsc::Sender<Bytes>>,
    probes: mpsc::Receiver<&'static str>,
}
impl Harness {
    async fn new() -> Self {
        Self::with_config(Config::default()).await
    }
    async fn with_config(config: Config) -> Self {
        Self::with_source(config, Some("1"), None).await
    }
    async fn with_source(
        mut config: Config,
        scope_version: Option<&'static str>,
        first: Option<&'static str>,
    ) -> Self {
        let origin_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = format!("http://{}", origin_listener.local_addr().unwrap());
        let (source_tx, sources) = mpsc::channel(8);
        let (probe_tx, probes) = mpsc::channel(8);
        let stop = CancellationToken::new();
        let origin_stop = stop.clone();
        let origin = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = origin_stop.cancelled() => break,
                    Some(_) = connections.join_next(), if !connections.is_empty() => {},
                    result = origin_listener.accept() => {
                        let (socket, _) = result.unwrap(); let source_tx = source_tx.clone(); let probe_tx = probe_tx.clone();
                        connections.spawn(async move {
                            let service = service_fn(move |mut request: Request<Incoming>| {
                                let source_tx = source_tx.clone();
                                let probe_tx = probe_tx.clone();
                                async move {
                                    let response = match request.uri().path() {
                                         "/api/event" => {
                                            assert_eq!(request.headers()["authorization"], format!("Basic {}", STANDARD.encode("opencode:secret")));
                                            let (tx, rx) = mpsc::channel(32);
                                             tx.send(Bytes::from(first.map(str::to_owned).unwrap_or_else(|| format!("data: {{\"type\":\"server.connected\",\"id\":\"evt_{}\",\"data\":{{}}}}\n\n", rand::random::<u64>())))).await.unwrap();
                                            source_tx.send(tx).await.unwrap();
                                            let stream = futures_util::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|bytes| (Ok::<_, Error>(Frame::data(bytes)), rx)) });
                                             let mut response = Response::builder().header("content-type", "text/event-stream");
                                             if let Some(version) = scope_version { response = response.header("OpenCode-Event-Scope", version); }
                                             response.body(StreamBody::new(Box::pin(stream)).boxed_unsync()).unwrap()
                                        },
                                        "/pair" => {
                                            let port = request.headers()["host"].to_str().unwrap().rsplit(':').next().unwrap();
                                            Response::builder().status(302).header("location", "/").header("set-cookie", format!("opencode_session_{port}={}; Path=/; HttpOnly; SameSite=Lax", token())).body(full("paired")).unwrap()
                                        },
                                        "/delayed" => {
                                            tokio::time::sleep(Duration::from_millis(250)).await;
                                            Response::new(full("completed"))
                                        },
                                        "/upload" | "/cancel-upload" => {
                                            let cancel_probe = request.uri().path() == "/cancel-upload";
                                            if cancel_probe { probe_tx.send("upload-started").await.unwrap(); }
                                            let collected = request.into_body().collect().await;
                                            if cancel_probe { let _ = probe_tx.send(if collected.is_err() { "upload-canceled" } else { "upload-completed" }).await; }
                                            Response::new(full(collected.map(|v| v.to_bytes()).unwrap_or_default()))
                                        },
                                        "/upgrade" => {
                                            let upgrade = hyper::upgrade::on(&mut request);
                                            tokio::spawn(async move {
                                                let mut io = TokioIo::new(upgrade.await.unwrap());
                                                let mut data = [0; 512];
                                                while let Ok(size) = io.read(&mut data).await {
                                                    if size == 0 || io.write_all(&data[..size]).await.is_err() { break; }
                                                }
                                            });
                                            Response::builder().status(101).header("connection", "upgrade").header("upgrade", "echo").body(full(Bytes::new())).unwrap()
                                        },
                                        _ => {
                                            assert!(!request.headers().contains_key("x-remove"));
                                            let target = request.uri().path_and_query().unwrap().as_str().to_owned();
                                            let host = request.headers()["host"].clone();
                                            let origin = request.headers().get("origin").cloned().unwrap_or_else(|| "absent".parse().unwrap());
                                            let auth = request.headers().get("authorization").cloned().unwrap_or_else(|| "absent".parse().unwrap());
                                            let trailer = request.headers().get("trailer").cloned().unwrap_or_else(|| "x-no-trailer".parse().unwrap());
                                            Response::builder().status(202).header("x-target", target).header("x-host", host).header("x-origin", origin).header("x-auth", auth)
                                                .header("trailer", trailer)
                                                .header("connection", "x-remove-response").header("x-remove-response", "private").header("x-preserve", "yes")
                                                .body(request.into_body().map_err(|e| -> Error { Box::new(e) }).boxed_unsync()).unwrap()
                                        }
                                    };
                                    Ok::<_, Infallible>(response)
                                }
                            });
                            let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(socket), service).with_upgrades().await;
                        });
                    }
                }
            }
            connections.abort_all();
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (router_handle, router) = Router::spawn(Limits::default(), stop.clone()).unwrap();
        config.upstream = upstream;
        config.password = Some("secret".into());
        config.source_idle_timeout = Duration::from_secs(2);
        config.max_source_frame_bytes = 1024;
        let gateway = tokio::spawn(serve(listener, config, router_handle, stop.clone()));
        Self {
            base,
            client: client(),
            stop,
            gateway,
            router,
            origin,
            sources,
            probes,
        }
    }
    fn request(&self, method: &str, path: &str, body: Body) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(format!("{}{path}", self.base))
            .header(
                "authorization",
                format!("Basic {}", STANDARD.encode("opencode:secret")),
            )
            .body(body)
            .unwrap()
    }
    async fn ready(&self) {
        for _ in 0..100 {
            let response = self
                .client
                .request(self.request("GET", "/_bus/health", full(Bytes::new())))
                .await
                .unwrap();
            if response.status() == StatusCode::OK {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("source not ready");
    }
    async fn feed(&self, controlled: bool) -> Feed {
        let response = self
            .client
            .request(self.request(
                "GET",
                if controlled {
                    "/api/experimental/event"
                } else {
                    "/api/event"
                },
                full(Bytes::new()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["x-accel-buffering"], "no");
        assert_eq!(response.headers()["OpenCode-Event-Scope"], "1");
        assert_eq!(
            response.headers()["access-control-expose-headers"],
            "OpenCode-Event-Scope"
        );
        Feed {
            body: response.into_body(),
            decoder: SseDecoder::new(1024 * 1024),
            pending: std::collections::VecDeque::new(),
        }
    }
    async fn interest(&self, id: &str, session: &str) -> StatusCode {
        self.client.request(self.request("PUT", &format!("/api/experimental/event/subscriptions/{id}/interests"), full(serde_json::json!({"locations":[],"sessions":[session],"profile":"session-streaming"}).to_string()))).await.unwrap().status()
    }
    async fn close(self) {
        self.stop.cancel();
        tokio::time::timeout(Duration::from_secs(7), self.gateway)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        self.router.await.unwrap();
        self.origin.await.unwrap();
    }
}

#[tokio::test]
async fn ordinary_operations_outlive_setup_timeout_but_remain_cancelable() {
    let mut harness = Harness::with_config(Config {
        request_timeout: Duration::from_millis(100),
        header_read_timeout: Duration::from_millis(100),
        ..Config::default()
    })
    .await;
    let _source = harness.sources.recv().await.unwrap();
    harness.ready().await;
    let response = harness
        .client
        .request(harness.request("GET", "/delayed", full(Bytes::new())))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "completed"
    );
    let (tx, rx) = mpsc::channel(4);
    let stream = futures_util::stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|frame| (Ok::<_, Error>(frame), rx))
    });
    let request = harness.request(
        "POST",
        "/upload",
        StreamBody::new(Box::pin(stream)).boxed_unsync(),
    );
    let client = harness.client.clone();
    let upload = tokio::spawn(async move { client.request(request).await.unwrap() });
    tx.send(Frame::data(Bytes::from_static(b"first")))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    tx.send(Frame::data(Bytes::from_static(b"second")))
        .await
        .unwrap();
    drop(tx);
    let response = upload.await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "firstsecond"
    );

    let address = harness.base.strip_prefix("http://").unwrap();
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket.write_all(format!("POST /cancel-upload HTTP/1.1\r\nHost: {address}\r\nContent-Length: 999\r\n\r\npartial").as_bytes()).await.unwrap();
    assert_eq!(harness.probes.recv().await.unwrap(), "upload-started");
    drop(socket);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), harness.probes.recv())
            .await
            .unwrap()
            .unwrap(),
        "upload-canceled"
    );

    let request = harness.request(
        "POST",
        "/upload",
        StreamBody::new(futures_util::stream::pending::<Result<Frame<Bytes>, Error>>())
            .boxed_unsync(),
    );
    let client = harness.client.clone();
    let pending = tokio::spawn(async move { client.request(request).await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    harness.close().await;
    let _ = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn idle_headers_and_upgrade_tunnels_share_connection_budget() {
    let mut harness = Harness::with_config(Config {
        max_connections: 1,
        header_read_timeout: Duration::from_millis(200),
        ..Config::default()
    })
    .await;
    let _source = harness.sources.recv().await.unwrap();
    let address = harness.base.strip_prefix("http://").unwrap();
    let mut idle = TcpStream::connect(address).await.unwrap();
    idle.write_all(b"GET /delayed HTTP/1.1\r\n").await.unwrap();
    let mut next = TcpStream::connect(address).await.unwrap();
    next.write_all(
        format!("GET /delayed HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n").as_bytes(),
    )
    .await
    .unwrap();
    let mut buf = [0; 1024];
    assert!(
        tokio::time::timeout(Duration::from_millis(75), next.read(&mut buf))
            .await
            .is_err(),
        "cap must block the second socket while first headers are pending"
    );
    let mut idle_reply = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), idle.read_to_end(&mut idle_reply))
        .await
        .unwrap()
        .unwrap();
    let size = tokio::time::timeout(Duration::from_secs(2), next.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert!(buf[..size].starts_with(b"HTTP/1.1 200"));
    drop(next);
    drop(idle);

    let mut tunnel = TcpStream::connect(address).await.unwrap();
    tunnel.write_all(format!("GET /upgrade HTTP/1.1\r\nHost: {address}\r\nConnection: upgrade\r\nUpgrade: echo\r\n\r\nping").as_bytes()).await.unwrap();
    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !received.ends_with(b"ping") {
            let size = tunnel.read(&mut buf).await.unwrap();
            assert!(size > 0);
            received.extend_from_slice(&buf[..size]);
        }
    })
    .await
    .unwrap();
    assert!(received.starts_with(b"HTTP/1.1 101"));
    let mut next = TcpStream::connect(address).await.unwrap();
    next.write_all(
        format!("GET /delayed HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n").as_bytes(),
    )
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(275), next.read(&mut buf))
            .await
            .is_err(),
        "upgrade lease must remain held beyond the header deadline"
    );
    drop(tunnel);
    let size = tokio::time::timeout(Duration::from_secs(2), next.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert!(buf[..size].starts_with(b"HTTP/1.1 200"));
    drop(next);
    harness.close().await;
}
struct Feed {
    body: Incoming,
    decoder: SseDecoder,
    pending: std::collections::VecDeque<Vec<u8>>,
}
impl Feed {
    async fn aborted(&mut self) -> bool {
        assert!(self.pending.is_empty(), "unexpected queued events");
        matches!(
            tokio::time::timeout(Duration::from_secs(3), self.body.frame())
                .await
                .unwrap(),
            Some(Err(_))
        )
    }
    async fn next(&mut self) -> Option<serde_json::Value> {
        self.next_raw()
            .await
            .map(|data| serde_json::from_slice(&data).unwrap())
    }
    async fn next_raw(&mut self) -> Option<Vec<u8>> {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(data) = self.pending.pop_front() {
                    return Some(data);
                }
                let frame = self.body.frame().await?.unwrap();
                if let Ok(bytes) = frame.into_data() {
                    self.pending.extend(self.decoder.push(&bytes).unwrap());
                }
            }
        })
        .await
        .unwrap()
    }
}
fn event(kind: &str, session: Option<&str>, marker: &str) -> Bytes {
    let mut scope = serde_json::json!({});
    if let Some(session) = session {
        scope["sessionID"] = session.into();
    }
    Bytes::from(format!(
        "data: {}\n\n",
        serde_json::json!({"type":if kind == "broadcast" { "future.public.event" } else { "session.text.delta" },"scope":scope,"id":marker,"data":{"sessionID":"payload-is-not-authority"}})
    ))
}

#[tokio::test]
async fn selection_outage_reconnect_and_stale_ids() {
    let mut harness = Harness::new().await;
    let source = harness.sources.recv().await.unwrap();
    harness.ready().await;
    let mut a = harness.feed(true).await;
    let mut b = harness.feed(true).await;
    let mut legacy = harness.feed(false).await;
    let aid = a.next().await.unwrap()["data"]["subscriptionID"]
        .as_str()
        .unwrap()
        .to_owned();
    let bid = b.next().await.unwrap()["data"]["subscriptionID"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(legacy.next().await.unwrap()["type"], "server.connected");
    assert_eq!(harness.interest(&aid, "ses_a").await, 204);
    assert_eq!(harness.interest(&bid, "ses_b").await, 204);
    assert_eq!(a.next().await.unwrap()["type"], "server.connected");
    assert_eq!(b.next().await.unwrap()["type"], "server.connected");
    source
        .send(event("session", Some("ses_a"), "a"))
        .await
        .unwrap();
    source
        .send(event("broadcast", None, "broad"))
        .await
        .unwrap();
    assert_eq!(a.next().await.unwrap()["id"], "a");
    assert_eq!(a.next().await.unwrap()["id"], "broad");
    assert_eq!(b.next().await.unwrap()["id"], "broad");
    assert_eq!(legacy.next().await.unwrap()["id"], "a");
    assert_eq!(legacy.next().await.unwrap()["id"], "broad");
    drop(source);
    assert!(a.aborted().await);
    assert!(b.aborted().await);
    assert!(legacy.aborted().await);
    let source2 = harness.sources.recv().await.unwrap();
    harness.ready().await;
    assert_eq!(harness.interest(&aid, "ses_a").await, 404);
    let mut fresh = harness.feed(false).await;
    assert_eq!(fresh.next().await.unwrap()["type"], "server.connected");
    source2.send(event("broadcast", None, "new")).await.unwrap();
    assert_eq!(fresh.next().await.unwrap()["id"], "new");
    drop(fresh);
    harness.close().await;
}

#[tokio::test]
async fn streaming_forwarding_cookie_auth_cors_and_raw_upgrade() {
    let mut harness = Harness::new().await;
    let _source = harness.sources.recv().await.unwrap();
    harness.ready().await;
    let (tx, rx) = mpsc::channel(4);
    let stream = futures_util::stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|frame| (Ok::<_, Error>(frame), rx))
    });
    let mut request = harness.request(
        "POST",
        "/echo%2Fraw?x=%2f&x=second",
        StreamBody::new(Box::pin(stream)).boxed_unsync(),
    );
    request
        .headers_mut()
        .insert("connection", "x-remove".parse().unwrap());
    request
        .headers_mut()
        .insert("x-remove", "private".parse().unwrap());
    request
        .headers_mut()
        .insert("host", "external.test:9876".parse().unwrap());
    request
        .headers_mut()
        .insert("origin", "http://external.test:9876".parse().unwrap());
    request.headers_mut().remove("authorization");
    request
        .headers_mut()
        .insert("te", "trailers".parse().unwrap());
    request
        .headers_mut()
        .insert("trailer", "x-checksum".parse().unwrap());
    tx.send(Frame::data(Bytes::from_static(b"first")))
        .await
        .unwrap();
    let mut response = harness.client.request(request).await.unwrap();
    assert_eq!(response.status(), 202);
    assert_eq!(response.headers()["x-target"], "/echo%2Fraw?x=%2f&x=second");
    assert_eq!(response.headers()["x-host"], "external.test:9876");
    assert_eq!(response.headers()["x-origin"], "http://external.test:9876");
    assert_eq!(response.headers()["x-auth"], "absent");
    assert!(!response.headers().contains_key("x-remove-response"));
    let first = tokio::time::timeout(Duration::from_secs(2), response.body_mut().frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert_eq!(first, "first");
    tx.send(Frame::data(Bytes::from_static(b"second")))
        .await
        .unwrap();
    let mut trailers = http::HeaderMap::new();
    trailers.insert("x-checksum", "verified".parse().unwrap());
    tx.send(Frame::trailers(trailers)).await.unwrap();
    drop(tx);
    let collected = response.into_body().collect().await.unwrap();
    assert_eq!(collected.trailers().unwrap()["x-checksum"], "verified");
    assert_eq!(collected.to_bytes(), "second");
    let pair = harness
        .client
        .request(harness.request("GET", "/pair", full(Bytes::new())))
        .await
        .unwrap();
    assert_eq!(pair.status(), 302);
    assert_eq!(pair.headers()["location"], "/");
    let cookie = pair.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let mut local = harness.request("GET", "/_bus/health", full(Bytes::new()));
    local.headers_mut().remove("authorization");
    local
        .headers_mut()
        .insert("cookie", cookie.parse().unwrap());
    local
        .headers_mut()
        .insert("origin", harness.base.parse().unwrap());
    assert_eq!(harness.client.request(local).await.unwrap().status(), 200);
    let mut cross = harness.request("GET", "/_bus/health", full(Bytes::new()));
    cross.headers_mut().remove("authorization");
    cross
        .headers_mut()
        .insert("cookie", cookie.parse().unwrap());
    cross
        .headers_mut()
        .insert("origin", "http://localhost:1".parse().unwrap());
    cross
        .headers_mut()
        .insert("sec-fetch-mode", "cors".parse().unwrap());
    let response = harness.client.request(cross).await.unwrap();
    assert_eq!(response.status(), 401);
    assert!(!response.headers().contains_key("www-authenticate"));
    assert_eq!(
        response.headers()["access-control-allow-origin"],
        "http://localhost:1"
    );
    let query = harness
        .client
        .request(harness.request(
            "GET",
            &format!(
                    "/_bus/health?auth_token={}",
                    url::form_urlencoded::byte_serialize(
                        STANDARD.encode("opencode:secret").as_bytes()
                    )
                    .collect::<String>()
                ),
            full(Bytes::new()),
        ))
        .await
        .unwrap();
    assert_eq!(query.status(), 200);
    assert_eq!(
        harness
            .client
            .request(harness.request("GET", "/api/experimental/event/source", full(Bytes::new())))
            .await
            .unwrap()
            .status(),
        202
    );
    assert_eq!(
        harness
            .client
            .request(harness.request(
                "GET",
                "/api/experimental/event/%73ource",
                full(Bytes::new())
            ))
            .await
            .unwrap()
            .status(),
        202
    );
    let address = harness.base.strip_prefix("http://").unwrap();
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket.write_all(format!("GET /upgrade?ticket=opaque HTTP/1.1\r\nHost: {address}\r\nConnection: upgrade\r\nUpgrade: echo\r\n\r\nping").as_bytes()).await.unwrap();
    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let mut buf = [0; 512];
            let size = socket.read(&mut buf).await.unwrap();
            assert!(size > 0);
            received.extend_from_slice(&buf[..size]);
            if received.windows(4).any(|v| v == b"ping") {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(received.starts_with(b"HTTP/1.1 101"));
    assert!(received.ends_with(b"ping"));
    drop(socket);
    harness.close().await;
}

#[tokio::test]
async fn source_bounce_aborts_both_feeds_and_accepts_fresh_subscriptions() {
    let mut harness = Harness::new().await;
    let source = harness.sources.recv().await.unwrap();
    harness.ready().await;
    let mut legacy = harness.feed(false).await;
    legacy.next().await.unwrap();
    let mut controlled = harness.feed(true).await;
    let id = controlled.next().await.unwrap()["data"]["subscriptionID"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(harness.interest(&id, "ses_a").await, 204);
    controlled.next().await.unwrap();

    // Even a graceful origin EOF must become a transport error downstream,
    // not successful iterator completion that can bypass clients' retries.
    drop(source);
    for feed in [&mut legacy, &mut controlled] {
        let frame = tokio::time::timeout(Duration::from_secs(2), feed.body.frame())
            .await
            .unwrap();
        assert!(matches!(frame, Some(Err(_))), "feed ended cleanly");
    }
    assert_eq!(harness.interest(&id, "ses_a").await, 404);

    let source = tokio::time::timeout(Duration::from_secs(2), harness.sources.recv())
        .await
        .unwrap()
        .unwrap();
    harness.ready().await;
    let mut legacy = harness.feed(false).await;
    legacy.next().await.unwrap();
    let mut controlled = harness.feed(true).await;
    let new_id = controlled.next().await.unwrap()["data"]["subscriptionID"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(id, new_id);
    assert_eq!(harness.interest(&new_id, "ses_a").await, 204);
    controlled.next().await.unwrap();
    source
        .send(event("broadcast", None, "after-bounce"))
        .await
        .unwrap();
    assert_eq!(legacy.next().await.unwrap()["id"], "after-bounce");
    assert_eq!(controlled.next().await.unwrap()["id"], "after-bounce");
    harness.close().await;
}

#[tokio::test]
async fn invalid_source_and_idle_timeout_end_downstream() {
    let mut harness = Harness::new().await;
    let source = harness.sources.recv().await.unwrap();
    harness.ready().await;
    let mut feed = harness.feed(false).await;
    feed.next().await.unwrap();
    source
        .send(Bytes::from_static(
            b"data: {\"type\":\"server.connected\",\"id\":\"evt_repeated\",\"data\":{}}\n\n",
        ))
        .await
        .unwrap();
    assert!(feed.aborted().await);
    let source = harness.sources.recv().await.unwrap();
    harness.ready().await;
    let mut feed = harness.feed(false).await;
    feed.next().await.unwrap();
    source
        .send(Bytes::from_static(b"data: not json\n\n"))
        .await
        .unwrap();
    assert!(feed.aborted().await);
    let _source = harness.sources.recv().await.unwrap();
    harness.ready().await;
    let mut feed = harness.feed(false).await;
    feed.next().await.unwrap();
    _source
        .send(Bytes::from(format!("data: {}\n\n", "x".repeat(2048))))
        .await
        .unwrap();
    assert!(
        feed.aborted().await,
        "oversized source frame must retire the epoch"
    );
    let _source = harness.sources.recv().await.unwrap();
    harness.ready().await;
    let mut feed = harness.feed(false).await;
    feed.next().await.unwrap();
    assert!(
        feed.aborted().await,
        "silent source must not survive with local heartbeats"
    );
    harness.close().await;
}

#[tokio::test]
async fn negotiation_requires_supported_header_and_first_connected_marker() {
    for (header, first) in [
        (None, None),
        (Some("2"), None),
        (Some("1, 1"), None),
        (
            Some("1"),
            Some("data: {\"type\":\"future\",\"scope\":{}}\n\n"),
        ),
        (
            Some("1"),
            Some("data: {\"type\":\"server.connected\",\"id\":\"bad\"}\n\n"),
        ),
        (Some("1"), Some("data: {\"type\":\"event-feed.ready\"}\n\n")),
    ] {
        let mut harness = Harness::with_source(Config::default(), header, first).await;
        // A second request proves the first was rejected rather than becoming ready.
        let _first = harness.sources.recv().await.unwrap();
        let _second = tokio::time::timeout(Duration::from_secs(2), harness.sources.recv())
            .await
            .unwrap()
            .unwrap();
        let response = harness
            .client
            .request(harness.request("GET", "/api/event", full(Bytes::new())))
            .await
            .unwrap();
        assert_eq!(response.status(), 503, "header={header:?}, first={first:?}");
        harness.close().await;
    }
}

#[tokio::test]
async fn malformed_scope_and_truncated_data_retire_live_epoch() {
    for raw in [
        "data: {\"type\":\"future\"}\n\n",
        "data: {\"type\":\"future\",\"scope\":null}\n\n",
        "data: {\"type\":\"future\",\"scope\":{\"sessionID\":null}}\n\n",
        "data: {\"type\":\"future\",\"scope\":{\"locations\":[{\"directory\":\"x\",\"workspaceID\":null}]}}\n\n",
        "data: {\"type\":\"future\",\"scope\":{}}",
    ] {
        let mut harness = Harness::new().await;
        let source = harness.sources.recv().await.unwrap();
        harness.ready().await;
        let mut feed = harness.feed(false).await;
        feed.next().await.unwrap();
        source
            .send(Bytes::from_static(raw.as_bytes()))
            .await
            .unwrap();
        drop(source);
        assert!(feed.aborted().await);
        harness.close().await;
    }
}

#[tokio::test]
async fn whole_json_and_scope_survive_proxy_chain_with_authoritative_selection() {
    let mut harness = Harness::new().await;
    let source = harness.sources.recv().await.unwrap();
    harness.ready().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let stop = CancellationToken::new();
    let (router, router_task) = Router::spawn(Limits::default(), stop.clone()).unwrap();
    let gateway = tokio::spawn(serve(
        listener,
        Config {
            upstream: harness.base.clone(),
            password: Some("secret".into()),
            ..Config::default()
        },
        router.clone(),
        stop.clone(),
    ));
    for _ in 0..100 {
        if router.snapshot().await.unwrap().epoch.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(router.snapshot().await.unwrap().epoch.is_some());
    let response = harness
        .client
        .request(
            Request::builder()
                .uri(format!("{base}/api/experimental/event"))
                .header(
                    "authorization",
                    format!("Basic {}", STANDARD.encode("opencode:secret")),
                )
                .body(full(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.headers()["OpenCode-Event-Scope"], "1");
    let mut feed = Feed {
        body: response.into_body(),
        decoder: SseDecoder::new(4096),
        pending: Default::default(),
    };
    let id = feed.next().await.unwrap()["data"]["subscriptionID"]
        .as_str()
        .unwrap()
        .to_owned();
    router
        .replace(
            id,
            bus_proxy_protocol::Interest {
                sessions: vec!["ses_authority".into()],
            },
        )
        .await
        .unwrap();
    assert_eq!(feed.next().await.unwrap()["type"], "server.connected");
    source
        .send(event("session", Some("ses_other"), "not-selected"))
        .await
        .unwrap();
    source
        .send(event("session", None, "payload-not-authority"))
        .await
        .unwrap();
    let raw = b"  { \"type\":\"session.text.delta\", \"scope\":{\"sessionID\":\"ses_authority\",\"locations\":[{\"directory\":\"relative\",\"workspaceID\":\"wrk\"},{\"directory\":\"\"}]}, \"data\":{\"sessionID\":\"ses_spoof\"}, \"float\":1.2300e+03, \"escaped\":\"\\u0061\" }  ";
    source
        .send(bus_proxy_protocol::event_frame(raw))
        .await
        .unwrap();
    assert_eq!(feed.next_raw().await.unwrap(), raw);
    let broad = b"{\"type\":\"future.public\",\"scope\":{\"locations\":[]},\"data\":{\"sessionID\":\"ses_other\"}}";
    source
        .send(bus_proxy_protocol::event_frame(broad))
        .await
        .unwrap();
    assert_eq!(feed.next_raw().await.unwrap(), broad);
    drop(source);
    assert!(
        feed.aborted().await,
        "origin gap invalidates both proxy generations"
    );
    stop.cancel();
    gateway.await.unwrap().unwrap();
    router_task.await.unwrap();
    harness.close().await;
}

#[tokio::test]
async fn obvious_listener_self_targets_are_rejected() {
    for (bind, host) in [
        ("127.0.0.1:0", "127.0.0.1"),
        ("127.0.0.1:0", "localhost"),
        ("0.0.0.0:0", "127.0.0.1"),
    ] {
        let listener = TcpListener::bind(bind).await.unwrap();
        let upstream = format!("http://{host}:{}", listener.local_addr().unwrap().port());
        let stop = CancellationToken::new();
        let (router, task) = Router::spawn(Limits::default(), stop.clone()).unwrap();
        assert!(matches!(
            serve(
                listener,
                Config {
                    upstream,
                    ..Config::default()
                },
                router,
                stop.clone()
            )
            .await,
            Err(bus_proxy_gateway::GatewayError::Configuration(_))
        ));
        stop.cancel();
        task.await.unwrap();
    }
}
