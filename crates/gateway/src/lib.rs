//! Same-origin HTTP gateway and sole routed-event source owner.
mod auth;

use base64::{Engine, engine::general_purpose::STANDARD};
use bus_proxy_protocol::{SourceItem, SseDecoder};
use bus_proxy_router::{Mode, Router, RouterError};
use bytes::Bytes;
use http::{HeaderMap, Method, Request, Response, StatusCode, Uri, header};
use http_body_util::{BodyExt, Full, StreamBody, combinators::UnsyncBoxBody};
use hyper::{
    body::{Frame, Incoming},
    service::service_fn,
};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::{TokioExecutor, TokioIo},
};
use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Body = UnsyncBoxBody<Bytes, BoxError>;
type HttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Body>;
const SOURCE: &str = "/api/experimental/event/source";
const CONTROL_PREFIX: &str = "/api/experimental/event/subscriptions/";
const INTEREST_BYTES: usize = 262144;

#[derive(Clone)]
pub struct Config {
    pub upstream: String,
    pub username: String,
    pub password: Option<String>,
    pub cors: Vec<String>,
    pub max_source_frame_bytes: usize,
    pub source_idle_timeout: Duration,
    pub request_timeout: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            upstream: "http://127.0.0.1:4096".into(),
            username: "opencode".into(),
            password: None,
            cors: vec![],
            max_source_frame_bytes: 16 * 1024 * 1024,
            source_idle_timeout: Duration::from_secs(45),
            request_timeout: Duration::from_secs(10),
        }
    }
}
#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("invalid gateway configuration: {0}")]
    Configuration(&'static str),
    #[error("listener failed")]
    Listener(#[source] std::io::Error),
    #[error("source supervisor failed")]
    SourceTask(#[source] tokio::task::JoinError),
}
#[derive(Default)]
struct Metrics {
    frames: AtomicU64,
    bytes: AtomicU64,
    failures: AtomicU64,
}
struct State {
    config: Config,
    base: String,
    client: HttpClient,
    router: Router,
    shutdown: CancellationToken,
    tasks: TaskTracker,
    metrics: Metrics,
}

fn full(value: impl Into<Bytes>) -> Body {
    Full::new(value.into())
        .map_err(|never| match never {})
        .boxed_unsync()
}
fn json(status: StatusCode, value: serde_json::Value) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(full(value.to_string()))
        .unwrap()
}
fn failure(status: StatusCode, message: &str) -> Response<Body> {
    json(status, serde_json::json!({"message":message}))
}

pub async fn serve(
    listener: TcpListener,
    config: Config,
    router: Router,
    shutdown: CancellationToken,
) -> Result<(), GatewayError> {
    let url = url::Url::parse(&config.upstream)
        .map_err(|_| GatewayError::Configuration("upstream URL"))?;
    if !["http", "https"].contains(&url.scheme())
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err(GatewayError::Configuration(
            "upstream must be an HTTP(S) origin without credentials or path",
        ));
    }
    if config.max_source_frame_bytes == 0
        || config.source_idle_timeout.is_zero()
        || config.request_timeout.is_zero()
    {
        return Err(GatewayError::Configuration("limits must be positive"));
    }
    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    let client = Client::builder(TokioExecutor::new()).build(connector);
    let stop = shutdown.child_token();
    let state = Arc::new(State {
        base: config.upstream.trim_end_matches('/').into(),
        config,
        client,
        router,
        shutdown: stop.clone(),
        tasks: TaskTracker::new(),
        metrics: Metrics::default(),
    });
    let mut source = tokio::spawn(source_owner(state.clone()));
    let mut connections = JoinSet::new();
    let result = loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break Ok(()),
            result = &mut source => break result.map_err(GatewayError::SourceTask),
            Some(_) = connections.join_next(), if !connections.is_empty() => {},
            accepted = listener.accept() => {
                let (socket, _) = match accepted { Ok(v) => v, Err(e) => break Err(GatewayError::Listener(e)) };
                let state = state.clone();
                connections.spawn(async move {
                    let service_state = state.clone();
                    let service = service_fn(move |request| {
                        let state = service_state.clone();
                        async move { Ok::<_, Infallible>(handle(request, state).await) }
                    });
                    let connection = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(socket), service).with_upgrades();
                    tokio::pin!(connection);
                    tokio::select! {
                        _ = state.shutdown.cancelled() => { connection.as_mut().graceful_shutdown(); let _ = connection.await; },
                        _ = &mut connection => {},
                    }
                });
            }
        }
    };
    stop.cancel();
    state.tasks.close();
    let cleanup = async {
        if !source.is_finished() {
            let _ = (&mut source).await;
        }
        while connections.join_next().await.is_some() {}
        state.tasks.wait().await;
    };
    if tokio::time::timeout(Duration::from_secs(5), cleanup)
        .await
        .is_err()
    {
        source.abort();
        connections.abort_all();
    }
    result
}

async fn source_owner(state: Arc<State>) {
    let mut attempt = 0u32;
    loop {
        if state.shutdown.is_cancelled() {
            break;
        }
        let mut epoch = None;
        let result = tokio::select! {
            biased;
            _ = state.shutdown.cancelled() => Ok(()),
            result = source_connection(&state, &mut epoch) => result,
        };
        if let Some(epoch) = epoch {
            let _ = state.router.end_epoch(epoch).await;
            attempt = 0;
        }
        if state.shutdown.is_cancelled() {
            break;
        }
        state.metrics.failures.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            failed = result.is_err(),
            "event source disconnected; reconnecting"
        );
        let cap = (250u64.saturating_mul(1 << attempt.min(7))).min(30_000);
        attempt = attempt.saturating_add(1);
        let delay = Duration::from_millis(rand::random_range(cap / 2..=cap));
        tokio::select! { _ = state.shutdown.cancelled() => break, _ = tokio::time::sleep(delay) => {} }
    }
}
async fn source_connection(state: &State, epoch: &mut Option<String>) -> Result<(), BoxError> {
    let mut request = Request::builder()
        .uri(format!("{}{SOURCE}", state.base))
        .header(header::ACCEPT, "text/event-stream")
        .body(full(Bytes::new()))?;
    if let Some(password) = &state.config.password {
        request.headers_mut().insert(
            header::AUTHORIZATION,
            format!(
                "Basic {}",
                STANDARD.encode(format!("{}:{password}", state.config.username))
            )
            .parse()?,
        );
    }
    let response =
        tokio::time::timeout(state.config.request_timeout, state.client.request(request)).await??;
    if response.status() != StatusCode::OK
        || response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_none_or(|v| {
                !v.split(';')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .eq_ignore_ascii_case("text/event-stream")
            })
    {
        return Err("invalid source response".into());
    }
    let mut body = response.into_body();
    let mut decoder = SseDecoder::new(state.config.max_source_frame_bytes);
    let ready_deadline = tokio::time::Instant::now() + state.config.request_timeout;
    loop {
        let deadline = if epoch.is_none() {
            ready_deadline.min(tokio::time::Instant::now() + state.config.source_idle_timeout)
        } else {
            tokio::time::Instant::now() + state.config.source_idle_timeout
        };
        let Some(frame) = tokio::time::timeout_at(deadline, body.frame()).await? else {
            decoder.finish()?;
            return Err("source EOF".into());
        };
        let frame = frame?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        state
            .metrics
            .bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        // Chunk slicing prevents one transport chunk from allocating an arbitrarily large decoded batch.
        for chunk in data.chunks(8192) {
            for data in decoder.push(chunk)? {
                match bus_proxy_protocol::decode_source(&data)? {
                    SourceItem::Ready {
                        version: 1,
                        epoch: new,
                    } if epoch.is_none() => {
                        state.router.begin_epoch(new.clone()).await?;
                        *epoch = Some(new);
                        tracing::info!("event source ready");
                    }
                    SourceItem::Event { route, event } if epoch.is_some() => {
                        state
                            .router
                            .publish(
                                epoch.as_ref().unwrap().clone(),
                                route,
                                bus_proxy_protocol::event_frame(&event),
                            )
                            .await?;
                        state.metrics.frames.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => return Err("invalid source readiness sequence".into()),
                }
            }
        }
    }
}

fn control_id(path: &str) -> Option<&str> {
    path.strip_prefix(CONTROL_PREFIX)?
        .strip_suffix("/interests")
        .filter(|id| !id.is_empty() && !id.contains('/'))
}
async fn handle(mut request: Request<Incoming>, state: Arc<State>) -> Response<Body> {
    let path = request.uri().path();
    let owned = path == SOURCE
        || path == "/api/event"
        || path == "/api/experimental/event"
        || control_id(path).is_some()
        || path.starts_with("/_bus/");
    if !owned {
        return forward(request, state).await;
    }
    let origin = request.headers().get(header::ORIGIN).cloned();
    let response = if request.method() == Method::OPTIONS && path != SOURCE {
        Response::builder()
            .status(204)
            .header(header::ACCESS_CONTROL_ALLOW_METHODS, "GET, PUT, OPTIONS")
            .header(
                header::ACCESS_CONTROL_ALLOW_HEADERS,
                request
                    .headers()
                    .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
                    .cloned()
                    .unwrap_or_else(|| "authorization, content-type".parse().unwrap()),
            )
            .body(full(Bytes::new()))
            .unwrap()
    } else if !auth::authorized(&request, &state.config) {
        let mut response = json(
            StatusCode::UNAUTHORIZED,
            serde_json::json!({"_tag":"UnauthorizedError","message":"Authentication required"}),
        );
        if request
            .headers()
            .get("sec-fetch-mode")
            .is_none_or(|v| v == "navigate")
        {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                "Basic realm=\"Secure Area\"".parse().unwrap(),
            );
        }
        response
    } else if path == SOURCE {
        failure(StatusCode::NOT_FOUND, "Not found")
    } else if request.method() == Method::GET
        && ["/api/event", "/api/experimental/event"].contains(&path)
    {
        let mode = if path == "/api/event" {
            Mode::Legacy
        } else {
            Mode::Controlled
        };
        match state.router.subscribe(mode).await {
            Ok(subscription) => {
                let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
                heartbeat.tick().await;
                let stream = futures_util::stream::unfold(
                    (subscription, heartbeat, state.shutdown.clone()),
                    |(mut sub, mut beat, stop)| async move {
                        let bytes = tokio::select! { biased; _ = stop.cancelled() => return None, value = sub.recv() => value?, _ = beat.tick() => Bytes::from_static(b": heartbeat\n\n") };
                        Some((Ok::<_, BoxError>(Frame::data(bytes)), (sub, beat, stop)))
                    },
                );
                Response::builder()
                    .header(header::CONTENT_TYPE, "text/event-stream")
                    .header(header::CACHE_CONTROL, "no-cache, no-transform")
                    .header("x-accel-buffering", "no")
                    .header("x-content-type-options", "nosniff")
                    .body(StreamBody::new(Box::pin(stream)).boxed_unsync())
                    .unwrap()
            }
            Err(error) => router_error(error, ""),
        }
    } else if request.method() == Method::PUT && control_id(path).is_some() {
        let id = control_id(path).unwrap().to_owned();
        let read = async {
            let mut data = Vec::new();
            while let Some(frame) = request.body_mut().frame().await {
                if let Ok(bytes) = frame.map_err(|_| ())?.into_data() {
                    if data.len().saturating_add(bytes.len()) > INTEREST_BYTES {
                        return Err(());
                    }
                    data.extend_from_slice(&bytes);
                }
            }
            Ok(data)
        };
        match tokio::time::timeout(state.config.request_timeout, read).await {
            Ok(Ok(data)) => match bus_proxy_protocol::decode_interest(&data) {
                Ok(interest) => match state.router.replace(id.clone(), interest).await {
                    Ok(()) => Response::builder()
                        .status(204)
                        .body(full(Bytes::new()))
                        .unwrap(),
                    Err(error) => router_error(error, &id),
                },
                Err(error) => json(
                    StatusCode::BAD_REQUEST,
                    serde_json::json!({"_tag":"InvalidRequestError","message":error.message,"field":error.field}),
                ),
            },
            _ => json(
                StatusCode::BAD_REQUEST,
                serde_json::json!({"_tag":"InvalidRequestError","message":"Invalid event interest request body","field":"interest"}),
            ),
        }
    } else if request.method() == Method::GET && path.starts_with("/_bus/") {
        match state.router.snapshot().await {
            Ok(snapshot) => {
                let value = serde_json::to_value(snapshot).unwrap();
                match path {
                    "/_bus/health" => json(
                        if value["epoch"].is_string() {
                            StatusCode::OK
                        } else {
                            StatusCode::SERVICE_UNAVAILABLE
                        },
                        serde_json::json!({"ready":value["epoch"].is_string()}),
                    ),
                    "/_bus/subscriptions" => json(StatusCode::OK, value),
                    "/_bus/metrics" => {
                        let mut text = format!(
                            "bus_proxy_ready {}\nbus_proxy_source_frames_total {}\nbus_proxy_source_bytes_total {}\nbus_proxy_source_failures_total {}\n",
                            u8::from(value["epoch"].is_string()),
                            state.metrics.frames.load(Ordering::Relaxed),
                            state.metrics.bytes.load(Ordering::Relaxed),
                            state.metrics.failures.load(Ordering::Relaxed)
                        );
                        metric_numbers("bus_proxy_router", &value, &mut text);
                        Response::builder()
                            .header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
                            .body(full(text))
                            .unwrap()
                    }
                    _ => failure(StatusCode::NOT_FOUND, "Not found"),
                }
            }
            Err(error) => router_error(error, ""),
        }
    } else {
        failure(StatusCode::NOT_FOUND, "Not found")
    };
    let mut response = response;
    if let Some(origin) = origin.filter(|v| {
        v.to_str()
            .is_ok_and(|v| auth::cors_allowed(v, &state.config))
    }) {
        response
            .headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        response.headers_mut().insert(
            header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
            "true".parse().unwrap(),
        );
        response
            .headers_mut()
            .append(header::VARY, "Origin".parse().unwrap());
    }
    response
}
fn metric_numbers(prefix: &str, value: &serde_json::Value, output: &mut String) {
    if let Some(number) = value.as_u64() {
        output.push_str(&format!("{prefix} {number}\n"));
    }
    if let Some(object) = value.as_object() {
        for (key, value) in object {
            metric_numbers(&format!("{prefix}_{key}"), value, output);
        }
    }
}
fn router_error(error: RouterError, id: &str) -> Response<Body> {
    match error {
        RouterError::NotFound => json(
            StatusCode::NOT_FOUND,
            serde_json::json!({"_tag":"EventSubscriptionNotFoundError","subscriptionID":id,"message":format!("Controlled event subscription {id} was not found")}),
        ),
        _ => failure(StatusCode::SERVICE_UNAVAILABLE, "Event source unavailable"),
    }
}

fn strip_hop(headers: &mut HeaderMap, upgrade: bool) {
    let dynamic: Vec<String> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|v| v.trim().to_owned())
        .collect();
    let protocol = headers.get(header::UPGRADE).cloned();
    for name in dynamic {
        headers.remove(name);
    }
    for name in [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "proxy-connection",
    ] {
        headers.remove(name);
    }
    if upgrade && let Some(protocol) = protocol {
        headers.insert(header::UPGRADE, protocol);
        headers.insert(header::CONNECTION, "upgrade".parse().unwrap());
    }
}
async fn forward(mut request: Request<Incoming>, state: Arc<State>) -> Response<Body> {
    if request.method() == Method::CONNECT {
        return failure(StatusCode::METHOD_NOT_ALLOWED, "CONNECT is unsupported");
    }
    let upgrade = request.headers().contains_key(header::UPGRADE)
        && request
            .headers()
            .get_all(header::CONNECTION)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .any(|v| v.trim().eq_ignore_ascii_case("upgrade"));
    let downstream = upgrade.then(|| hyper::upgrade::on(&mut request));
    let uri: Uri = match format!(
        "{}{}",
        state.base,
        request
            .uri()
            .path_and_query()
            .map(|v| v.as_str())
            .unwrap_or("/")
    )
    .parse()
    {
        Ok(v) => v,
        Err(_) => return failure(StatusCode::BAD_REQUEST, "Invalid request target"),
    };
    *request.uri_mut() = uri;
    *request.version_mut() = http::Version::HTTP_11;
    strip_hop(request.headers_mut(), upgrade);
    let request = request.map(|body| body.map_err(|e| -> BoxError { Box::new(e) }).boxed_unsync());
    let response = tokio::select! {
        _ = state.shutdown.cancelled() => return failure(StatusCode::SERVICE_UNAVAILABLE, "Gateway stopping"),
        response = tokio::time::timeout(state.config.request_timeout, state.client.request(request)) => response,
    };
    let mut response = match response {
        Ok(Ok(value)) => value,
        Ok(Err(_)) => return failure(StatusCode::BAD_GATEWAY, "Upstream request failed"),
        Err(_) => return failure(StatusCode::GATEWAY_TIMEOUT, "Upstream response timeout"),
    };
    let switched = response.status() == StatusCode::SWITCHING_PROTOCOLS;
    if switched {
        let Some(downstream) = downstream else {
            return failure(StatusCode::BAD_GATEWAY, "Unexpected upstream upgrade");
        };
        if !response.headers().contains_key(header::UPGRADE) {
            return failure(StatusCode::BAD_GATEWAY, "Invalid upstream upgrade");
        }
        let upstream = hyper::upgrade::on(&mut response);
        let stop = state.shutdown.clone();
        let timeout = state.config.request_timeout;
        state.tasks.spawn(async move {
            let transfer = async {
                let (down, up) =
                    tokio::time::timeout(timeout, async { tokio::try_join!(downstream, upstream) })
                        .await
                        .map_err(|e| -> BoxError { Box::new(e) })??;
                let _ =
                    tokio::io::copy_bidirectional(&mut TokioIo::new(down), &mut TokioIo::new(up))
                        .await;
                Ok::<(), BoxError>(())
            };
            tokio::select! { _ = stop.cancelled() => {}, _ = transfer => {} }
        });
    }
    strip_hop(response.headers_mut(), switched);
    response.map(|body| body.map_err(|e| -> BoxError { Box::new(e) }).boxed_unsync())
}
