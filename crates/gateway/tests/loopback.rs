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
    async fn with_config(mut config: Config) -> Self {
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
                                        "/api/experimental/event/source" => {
                                            assert_eq!(request.headers()["authorization"], format!("Basic {}", STANDARD.encode("opencode:secret")));
                                            let (tx, rx) = mpsc::channel(32);
                                            tx.send(Bytes::from(format!("data: {{\"type\":\"event-source.ready\",\"version\":1,\"epoch\":\"{}\"}}\n\n", rand::random::<u64>()))).await.unwrap();
                                            source_tx.send(tx).await.unwrap();
                                            let stream = futures_util::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|bytes| (Ok::<_, Error>(Frame::data(bytes)), rx)) });
                                            Response::builder().header("content-type", "text/event-stream").body(StreamBody::new(Box::pin(stream)).boxed_unsync()).unwrap()
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
    async fn next(&mut self) -> Option<serde_json::Value> {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(data) = self.pending.pop_front() {
                    return Some(serde_json::from_slice(&data).unwrap());
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
    let route = if kind == "broadcast" {
        serde_json::json!({"kind":"broadcast"})
    } else {
        serde_json::json!({"kind":"session", "sessionID":session})
    };
    Bytes::from(format!(
        "data: {}\n\n",
        serde_json::json!({"type":"event-source.event","route":route,"event":{"type":"session.text.delta","id":marker,"data":{"sessionID":"payload-is-not-authority"}}})
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
    assert!(a.next().await.is_none());
    assert!(b.next().await.is_none());
    assert!(legacy.next().await.is_none());
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
        404
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
        404
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
async fn invalid_source_and_idle_timeout_end_downstream() {
    let mut harness = Harness::new().await;
    let source = harness.sources.recv().await.unwrap();
    harness.ready().await;
    let mut feed = harness.feed(false).await;
    feed.next().await.unwrap();
    source
        .send(Bytes::from_static(
            b"data: {\"type\":\"event-source.ready\",\"version\":1,\"epoch\":\"repeated\"}\n\n",
        ))
        .await
        .unwrap();
    assert!(feed.next().await.is_none());
    let source = harness.sources.recv().await.unwrap();
    harness.ready().await;
    let mut feed = harness.feed(false).await;
    feed.next().await.unwrap();
    source
        .send(Bytes::from_static(b"data: not json\n\n"))
        .await
        .unwrap();
    assert!(feed.next().await.is_none());
    let _source = harness.sources.recv().await.unwrap();
    harness.ready().await;
    let mut feed = harness.feed(false).await;
    feed.next().await.unwrap();
    _source
        .send(Bytes::from(format!("data: {}\n\n", "x".repeat(2048))))
        .await
        .unwrap();
    assert!(
        feed.next().await.is_none(),
        "oversized source frame must retire the epoch"
    );
    let _source = harness.sources.recv().await.unwrap();
    harness.ready().await;
    let mut feed = harness.feed(false).await;
    feed.next().await.unwrap();
    assert!(
        feed.next().await.is_none(),
        "silent source must not survive with local heartbeats"
    );
    harness.close().await;
}
