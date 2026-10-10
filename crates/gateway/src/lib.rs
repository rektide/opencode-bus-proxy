//! Same-origin HTTP gateway and sole routed-event source owner.
mod auth;

use base64::{Engine, engine::general_purpose::STANDARD};
use bus_proxy_protocol::{
    EVENT_PATH, EVENT_SCOPE_HEADER, EVENT_SCOPE_VERSION, SseDecoder, UpstreamEvent,
};
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
    rt::{TokioExecutor, TokioIo, TokioTimer},
};
use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::Instrument;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Body = UnsyncBoxBody<Bytes, BoxError>;
type HttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Body>;
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
    /// Upstream TCP connect and source/upgrade setup deadline, not an ordinary operation deadline.
    pub request_timeout: Duration,
    /// Maximum admitted downstream HTTP connections, including upgraded tunnels.
    pub max_connections: usize,
    /// Deadline for receiving each HTTP/1 request's headers; bodies are not timed out.
    pub header_read_timeout: Duration,
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
            max_connections: 1024,
            header_read_timeout: Duration::from_secs(10),
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
        || config.max_connections == 0
        || config.max_connections > Semaphore::MAX_PERMITS
        || config.header_read_timeout.is_zero()
    {
        return Err(GatewayError::Configuration("limits must be positive"));
    }
    let local = listener.local_addr().map_err(GatewayError::Listener)?;
    let host_ip = url.host_str().and_then(|host| {
        host.trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .ok()
    });
    let loopback =
        url.host_str() == Some("localhost") || host_ip.is_some_and(|ip| ip.is_loopback());
    if url.port_or_known_default() == Some(local.port())
        && (host_ip == Some(local.ip())
            || (loopback && (local.ip().is_loopback() || local.ip().is_unspecified()))
            || (local.ip().is_unspecified() && host_ip.is_some_and(|ip| ip.is_unspecified())))
    {
        return Err(GatewayError::Configuration(
            "upstream targets gateway listener",
        ));
    }
    let mut http_connector = HttpConnector::new();
    http_connector.enforce_http(false);
    http_connector.set_connect_timeout(Some(config.request_timeout));
    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .wrap_connector(http_connector);
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
    let permits = Arc::new(Semaphore::new(state.config.max_connections));
    let result = loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break Ok(()),
            result = &mut source => break result.map_err(GatewayError::SourceTask),
            Some(_) = connections.join_next(), if !connections.is_empty() => {},
            accepted = async {
                let permit = permits.clone().acquire_owned().await.expect("connection semaphore stays open");
                listener.accept().await.map(|(socket, _)| (socket, Arc::new(permit)))
            } => {
                let (socket, permit) = match accepted { Ok(v) => v, Err(e) => break Err(GatewayError::Listener(e)) };
                let state = state.clone();
                connections.spawn(async move {
                    let service_state = state.clone();
                    let service_permit = permit.clone();
                    let service = service_fn(move |mut request: Request<Incoming>| {
                        // The upgrade task takes this lease before Hyper releases its
                        // connection driver, so tunnels share the same admission cap.
                        request.extensions_mut().insert(service_permit.clone());
                        let state = service_state.clone();
                        let span = tracing::info_span!("http_request", method = method_category(request.method()), route = route_category(request.uri().path()));
                        async move {
                            tracing::debug!("HTTP request started");
                            let response = handle(request, state).await;
                            tracing::debug!(status = response.status().as_u16(), "HTTP response established");
                            Ok::<_, Infallible>(response)
                        }.instrument(span)
                    });
                    let connection = hyper::server::conn::http1::Builder::new()
                        .timer(TokioTimer::new()).header_read_timeout(state.config.header_read_timeout)
                        .serve_connection(TokioIo::new(socket), service).with_upgrades();
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
        let attempt_span = tracing::info_span!("source_attempt", attempt);
        let (result, was_ready) = async {
            // This lifetime starts as a candidate connection; readiness records its
            // epoch. Keep retirement inside the same span, even during shutdown.
            let epoch_span = tracing::info_span!("source_epoch", epoch = tracing::field::Empty);
            async {
                tracing::debug!("source attempt started");
                let mut epoch = None;
                let result = tokio::select! {
                    biased;
                    _ = state.shutdown.cancelled() => Ok(()),
                    result = source_connection(&state, &mut epoch) => result,
                };
                let was_ready = epoch.is_some();
                if let Some(epoch) = epoch {
                    let _ = state.router.end_epoch(epoch).await;
                    tracing::info!(
                        shutdown = state.shutdown.is_cancelled(),
                        "source epoch retired"
                    );
                }
                (result, was_ready)
            }
            .instrument(epoch_span)
            .await
        }
        .instrument(attempt_span)
        .await;
        if was_ready {
            attempt = 0;
        }
        if state.shutdown.is_cancelled() {
            break;
        }
        state.metrics.failures.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            reason = result.as_ref().err().map_or("closed", |error| error.0),
            "event source disconnected; reconnecting"
        );
        let cap = (250u64.saturating_mul(1 << attempt.min(7))).min(30_000);
        attempt = attempt.saturating_add(1);
        let delay = Duration::from_millis(rand::random_range(cap / 2..=cap));
        tokio::select! { _ = state.shutdown.cancelled() => break, _ = tokio::time::sleep(delay) => {} }
    }
}
// Only fixed categories leave the source boundary: decoder/HTTP errors can
// contain peer-supplied values, URLs, or event content and must not enter logs.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct SourceFailure(&'static str);

async fn source_connection(state: &State, epoch: &mut Option<String>) -> Result<(), SourceFailure> {
    let mut request = Request::builder()
        .uri(format!("{}{EVENT_PATH}", state.base))
        .header(header::ACCEPT, "text/event-stream")
        .body(full(Bytes::new()))
        .map_err(|_| SourceFailure("request_configuration"))?;
    if let Some(password) = &state.config.password {
        request.headers_mut().insert(
            header::AUTHORIZATION,
            format!(
                "Basic {}",
                STANDARD.encode(format!("{}:{password}", state.config.username))
            )
            .parse()
            .map_err(|_| SourceFailure("credential_configuration"))?,
        );
    }
    let response =
        tokio::time::timeout(state.config.request_timeout, state.client.request(request))
            .await
            .map_err(|_| SourceFailure("response_timeout"))?
            .map_err(|_| SourceFailure("connection"))?;
    if response.status() != StatusCode::OK {
        tracing::warn!(
            status = response.status().as_u16(),
            "source HTTP status rejected"
        );
        return Err(SourceFailure("http_status"));
    }
    if response
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
        return Err(SourceFailure("content_type"));
    }
    let mut versions = response.headers().get_all(EVENT_SCOPE_HEADER).iter();
    if versions
        .next()
        .is_none_or(|value| value != EVENT_SCOPE_VERSION)
        || versions.next().is_some()
    {
        return Err(SourceFailure("scope_version"));
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
        let Some(frame) = tokio::time::timeout_at(deadline, body.frame())
            .await
            .map_err(|_| {
                SourceFailure(if epoch.is_none() {
                    "ready_timeout"
                } else {
                    "idle_timeout"
                })
            })?
        else {
            decoder
                .finish()
                .map_err(|_| SourceFailure("truncated_frame"))?;
            return Err(SourceFailure("eof"));
        };
        let frame = frame.map_err(|_| SourceFailure("body_transport"))?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        state
            .metrics
            .bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        // Chunk slicing prevents one transport chunk from allocating an arbitrarily large decoded batch.
        for chunk in data.chunks(8192) {
            for data in decoder
                .push(chunk)
                .map_err(|_| SourceFailure("framing_or_size"))?
            {
                match bus_proxy_protocol::decode_event(Bytes::from(data))
                    .map_err(|_| SourceFailure("event_metadata"))?
                {
                    UpstreamEvent::Connected { epoch: new } if epoch.is_none() => {
                        state
                            .router
                            .begin_epoch(new.clone())
                            .await
                            .map_err(|_| SourceFailure("router_begin"))?;
                        tracing::Span::current().record("epoch", &new);
                        *epoch = Some(new);
                        tracing::info!("event source ready");
                    }
                    UpstreamEvent::Domain { route, event } if epoch.is_some() => {
                        state
                            .router
                            .publish(
                                epoch.as_ref().unwrap().clone(),
                                route,
                                bus_proxy_protocol::event_frame(&event),
                            )
                            .await
                            .map_err(|_| SourceFailure("router_admission"))?;
                        state.metrics.frames.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => return Err(SourceFailure("readiness_sequence")),
                }
            }
        }
    }
}

fn method_category(method: &Method) -> &'static str {
    match *method {
        Method::GET => "GET",
        Method::PUT => "PUT",
        Method::POST => "POST",
        Method::PATCH => "PATCH",
        Method::DELETE => "DELETE",
        Method::HEAD => "HEAD",
        Method::OPTIONS => "OPTIONS",
        Method::CONNECT => "CONNECT",
        Method::TRACE => "TRACE",
        _ => "OTHER",
    }
}
fn route_category(path: &str) -> &'static str {
    match path {
        "/api/event" => "legacy_feed",
        "/api/experimental/event" => "controlled_feed",
        "/_bus/health" => "health",
        "/_bus/subscriptions" => "subscriptions",
        "/_bus/metrics" => "metrics",
        _ if control_id(path).is_some() => "interests",
        _ if path.starts_with("/_bus/") => "diagnostics",
        _ => "forward",
    }
}

fn control_id(path: &str) -> Option<&str> {
    path.strip_prefix(CONTROL_PREFIX)?
        .strip_suffix("/interests")
        .filter(|id| !id.is_empty() && !id.contains('/'))
}
async fn handle(mut request: Request<Incoming>, state: Arc<State>) -> Response<Body> {
    let path = request.uri().path();
    let owned = path == "/api/event"
        || path == "/api/experimental/event"
        || control_id(path).is_some()
        || path.starts_with("/_bus/");
    if !owned {
        return forward(request, state).await;
    }
    let origin = request.headers().get(header::ORIGIN).cloned();
    let response = if request.method() == Method::OPTIONS {
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
                    .header(EVENT_SCOPE_HEADER, EVENT_SCOPE_VERSION)
                    .header(header::ACCESS_CONTROL_EXPOSE_HEADERS, EVENT_SCOPE_HEADER)
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
                if let Ok(bytes) = frame.map_err(|_| false)?.into_data() {
                    if data.len().saturating_add(bytes.len()) > INTEREST_BYTES {
                        return Err(true);
                    }
                    data.extend_from_slice(&bytes);
                }
            }
            Ok(data)
        };
        match tokio::time::timeout(state.config.request_timeout, read).await {
            Ok(Err(true)) => json(
                StatusCode::BAD_REQUEST,
                serde_json::json!({"_tag":"InvalidRequestError","message":format!("Interest exceeds {INTEREST_BYTES} request bytes"),"field":"interest"}),
            ),
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
    // TE is hop-by-hop, but recreate its sole proxy-safe value on the next hop:
    // Hyper's HTTP/1 server otherwise suppresses response trailer frames.
    let trailers = headers
        .get_all(header::TE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|v| v.trim().eq_ignore_ascii_case("trailers"));
    let dynamic: Vec<String> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|v| v.trim().to_owned())
        .collect();
    let protocol = headers.get(header::UPGRADE).cloned();
    // Re-declare end-to-end trailers for the newly framed outgoing HTTP/1 body.
    let declared: Vec<String> = headers
        .get_all(header::TRAILER)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| {
            http::HeaderName::from_bytes(v.as_bytes()).is_ok()
                && !dynamic.iter().any(|name| name.eq_ignore_ascii_case(v))
                && ![
                    "connection",
                    "keep-alive",
                    "proxy-authenticate",
                    "proxy-authorization",
                    "te",
                    "trailer",
                    "transfer-encoding",
                    "upgrade",
                    "proxy-connection",
                    "content-length",
                    "host",
                ]
                .contains(&v.as_str())
        })
        .collect();
    for name in &dynamic {
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
    if trailers {
        headers.insert(header::TE, "trailers".parse().unwrap());
    }
    if !declared.is_empty() {
        headers.insert(header::TRAILER, declared.join(", ").parse().unwrap());
    }
}
async fn forward(mut request: Request<Incoming>, state: Arc<State>) -> Response<Body> {
    let connection_permit = request
        .extensions_mut()
        .remove::<Arc<OwnedSemaphorePermit>>();
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
    let requested_protocol = request.headers().get(header::UPGRADE).cloned();
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
        response = async {
            if upgrade {
                tokio::time::timeout(state.config.request_timeout, state.client.request(request)).await
            } else {
                // Model execution and uploads can legitimately precede response headers
                // by minutes. Only the connector, not the operation, has a deadline.
                Ok(state.client.request(request).await)
            }
        } => response,
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
        let valid_protocol = response
            .headers()
            .get(header::UPGRADE)
            .and_then(|v| v.to_str().ok())
            .zip(requested_protocol.as_ref().and_then(|v| v.to_str().ok()))
            .is_some_and(|(actual, requested)| {
                requested
                    .split(',')
                    .any(|v| v.trim().eq_ignore_ascii_case(actual.trim()))
            });
        let valid_connection = response
            .headers()
            .get_all(header::CONNECTION)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .any(|v| v.trim().eq_ignore_ascii_case("upgrade"));
        if !valid_protocol || !valid_connection {
            return failure(StatusCode::BAD_GATEWAY, "Invalid upstream upgrade");
        }
        let upstream = hyper::upgrade::on(&mut response);
        let stop = state.shutdown.clone();
        let timeout = state.config.request_timeout;
        state.tasks.spawn(async move {
            let _connection_permit = connection_permit;
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
