//! Narrow, resumable HTTP/1 SSE transports. Hyper still owns general exchanges.
//! No syscall is separated from recording its byte count by an await point.
use crate::handover::{ClientImage, SourceImage, monotonic_ms};
use crate::{SourceFailure, State, auth, ordinary_connection};
use base64::{Engine, engine::general_purpose::STANDARD};
use bus_proxy_protocol::{EVENT_SCOPE_HEADER, EVENT_SCOPE_VERSION, SseDecoder, UpstreamEvent};
use bus_proxy_router::{Mode, Subscription};
use bytes::Bytes;
use http::{Method, Request, header};
use serde::{Deserialize, Serialize};
use std::{
    io,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::TcpStream,
    sync::OwnedSemaphorePermit,
};
use tracing::Instrument;

const HEADER_LIMIT: usize = 32768;

pub(crate) struct ClientOwner {
    pub socket: TcpStream,
    pub sub: Subscription,
    pub image: ClientImage,
    output: Output,
    pub _permit: Arc<OwnedSemaphorePermit>,
}

/// One dequeued shared frame, not a second per-client payload allocation. The
/// header and trailer are separate immutable segments; the offset is advanced
/// synchronously with each successful socket write. Flatten only at a cut.
#[derive(Default)]
struct Output {
    parts: [Bytes; 3],
    offset: usize,
}
impl Output {
    fn wire(bytes: Vec<u8>, offset: u64) -> Self {
        if offset == bytes.len() as u64 {
            return Self::default();
        }
        Self {
            parts: [Bytes::from(bytes), Bytes::new(), Bytes::new()],
            offset: offset as usize,
        }
    }
    fn chunk(frame: Bytes) -> Self {
        Self {
            parts: [
                Bytes::from(format!("{:x}\r\n", frame.len())),
                frame,
                Bytes::from_static(b"\r\n"),
            ],
            offset: 0,
        }
    }
    fn len(&self) -> usize {
        self.parts.iter().map(Bytes::len).sum()
    }
    fn pending(&self) -> bool {
        self.offset < self.len()
    }
    fn slices(&self) -> [io::IoSlice<'_>; 3] {
        let mut offset = self.offset;
        std::array::from_fn(|index| {
            let part = &self.parts[index];
            let skip = offset.min(part.len());
            offset -= skip;
            io::IoSlice::new(&part[skip..])
        })
    }
    fn advance(&mut self, n: usize) {
        self.offset += n;
        if !self.pending() {
            *self = Self::default();
        }
    }
    fn snapshot(&self) -> (Vec<u8>, u64) {
        let mut bytes = Vec::with_capacity(self.len());
        for part in &self.parts {
            bytes.extend_from_slice(part);
        }
        (bytes, self.offset as u64)
    }
}
impl ClientOwner {
    pub(crate) fn new(
        socket: TcpStream,
        sub: Subscription,
        mut image: ClientImage,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> Self {
        let output = Output::wire(
            std::mem::take(&mut image.output),
            std::mem::take(&mut image.offset),
        );
        Self {
            socket,
            sub,
            image,
            output,
            _permit: permit,
        }
    }
    pub(crate) fn output_len(&self) -> usize {
        self.output.len()
    }
    pub(crate) fn snapshot(&self) -> ClientImage {
        let mut image = self.image.clone();
        (image.output, image.offset) = self.output.snapshot();
        image
    }
}

pub(crate) struct SourceOwner {
    pub socket: TcpStream,
    pub decoder: SseDecoder,
    pub image: SourceImage,
}

/// Preserve pre-read bytes when an ordinary first request goes to Hyper.
pub(crate) struct Prefixed {
    socket: TcpStream,
    bytes: Bytes,
}
impl AsyncRead for Prefixed {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.bytes.is_empty() {
            let n = self.bytes.len().min(buf.remaining());
            buf.put_slice(&self.bytes.split_to(n));
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.socket).poll_read(cx, buf)
    }
}
impl AsyncWrite for Prefixed {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.socket).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

async fn request_headers(socket: &mut TcpStream) -> io::Result<(Request<()>, Vec<u8>, usize)> {
    let mut buffer = Vec::new();
    loop {
        let mut headers = [httparse::EMPTY_HEADER; 128];
        let mut parsed = httparse::Request::new(&mut headers);
        if let httparse::Status::Complete(end) = parsed.parse(&buffer).map_err(io::Error::other)? {
            let mut request = Request::builder()
                .method(parsed.method.unwrap())
                .uri(parsed.path.unwrap())
                .version(if parsed.version == Some(1) {
                    http::Version::HTTP_11
                } else {
                    http::Version::HTTP_10
                });
            for h in parsed.headers {
                request = request.header(h.name, h.value);
            }
            return Ok((request.body(()).map_err(io::Error::other)?, buffer, end));
        }
        if buffer.len() >= HEADER_LIMIT {
            return Err(io::Error::other("header limit"));
        }
        let mut bytes = [0; 4096];
        let n = socket.read(&mut bytes).await?;
        if n == 0 {
            return Err(io::Error::other("header EOF"));
        }
        buffer.extend_from_slice(&bytes[..n]);
    }
}

pub(crate) async fn accepted(
    mut socket: TcpStream,
    permit: Arc<OwnedSemaphorePermit>,
    state: Arc<State>,
    key: String,
) -> Option<ClientOwner> {
    let registration = Arc::new(Registration(state.registry.clone(), key.clone()));
    let read = tokio::select! {
        biased;
        _ = state.pause.cancelled() => return None,
        _ = state.shutdown.cancelled() => return None,
        result = tokio::time::timeout(state.config.header_read_timeout, request_headers(&mut socket)) => result,
    };
    let Ok(Ok((request, buffer, end))) = read else {
        return None;
    };
    let eligible = request.method() == Method::GET
        && ["/api/event", "/api/experimental/event"].contains(&request.uri().path())
        && request.version() == http::Version::HTTP_11
        && request.headers().get_all(header::HOST).iter().count() == 1
        && !request.headers().contains_key(header::TRANSFER_ENCODING)
        && !request.headers().contains_key(header::EXPECT)
        && !request.headers().contains_key(header::UPGRADE)
        && request
            .headers()
            .get_all(header::CONTENT_LENGTH)
            .iter()
            .all(|v| v == "0")
        && buffer.len() == end
        && auth::authorized(&request, &state.config);
    if !eligible {
        ordinary_connection(
            Prefixed {
                socket,
                bytes: Bytes::from(buffer),
            },
            permit,
            state,
            registration,
        )
        .await;
        return None;
    }
    let mode = if request.uri().path() == "/api/event" {
        Mode::Legacy
    } else {
        Mode::Controlled
    };
    let sub = match state.router.subscribe(mode).await {
        Ok(sub) => sub,
        Err(_) => {
            let _ = socket.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await;
            return None;
        }
    };
    let mut headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n{EVENT_SCOPE_HEADER}: {EVENT_SCOPE_VERSION}\r\nAccess-Control-Expose-Headers: {EVENT_SCOPE_HEADER}\r\nCache-Control: no-cache, no-transform\r\nX-Accel-Buffering: no\r\nX-Content-Type-Options: nosniff\r\n"
    );
    if let Some(origin) = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .filter(|v| auth::cors_allowed(v, &state.config))
    {
        headers.push_str(&format!("Access-Control-Allow-Origin: {origin}\r\nAccess-Control-Allow-Credentials: true\r\nVary: Origin\r\n"));
    }
    headers.push_str("\r\n");
    state.registry.register(&key, Some(sub.id()));
    let image = ClientImage {
        key,
        descriptor: String::new(),
        subscription: sub.id().into(),
        output: headers.into_bytes(),
        offset: 0,
        read_eof: false,
    };
    client(ClientOwner::new(socket, sub, image, permit), state).await
}

pub(crate) async fn client(mut owner: ClientOwner, state: Arc<State>) -> Option<ClientOwner> {
    state
        .registry
        .register(&owner.image.key, Some(&owner.image.subscription));
    let _registration = Registration(state.registry.clone(), owner.image.key.clone());
    let retired = owner.sub.cancellation_token();
    let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
    heartbeat.tick().await;
    loop {
        tokio::select! {
            biased;
            _ = state.pause.cancelled() => return Some(owner),
            _ = state.shutdown.cancelled() => return None,
            _ = retired.cancelled() => return None,
            ready = owner.socket.writable(), if owner.output.pending() => {
                if ready.is_err() { return None; }
                match owner.socket.try_write_vectored(&owner.output.slices()) {
                    Ok(0) => return None,
                    Ok(n) => {
                        owner.output.advance(n);
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {},
                    Err(_) => return None,
                }
            }
            ready = owner.socket.readable(), if !owner.image.read_eof => {
                if ready.is_err() { return None; }
                let mut byte = [0; 1];
                match owner.socket.try_read(&mut byte) {
                    Ok(0) => owner.image.read_eof = true, // A request-side half close need not stop its response.
                    Ok(_) => return None, // Pipelining on a dedicated live-only response is unsupported.
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {},
                    Err(_) => return None,
                }
            }
            frame = owner.sub.recv(), if !owner.output.pending() => {
                owner.output = Output::chunk(frame?);
            }
            _ = heartbeat.tick(), if !owner.output.pending() => {
                owner.output = Output::chunk(Bytes::from_static(b": heartbeat\n\n"));
            }
        }
    }
}

pub(crate) struct Registration(pub crate::handover::Registry, pub String);
impl Drop for Registration {
    fn drop(&mut self) {
        self.0.remove(&self.1);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Framing {
    Close,
    Length(u64),
    Chunked(ChunkPhase),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChunkPhase {
    Size,
    Data(u64),
    CrLf(u8),
    Trailers,
    Done,
}

impl Framing {
    /// Consume at most 8192 body bytes. Partial chunk headers/trailers remain in
    /// the read-ahead buffer; CRLF progress is explicitly represented.
    fn take(&mut self, buffer: &mut Vec<u8>) -> Result<Option<Vec<u8>>, SourceFailure> {
        loop {
            match self {
                Self::Close => {
                    return Ok(if buffer.is_empty() {
                        None
                    } else {
                        Some(buffer.drain(..buffer.len().min(8192)).collect())
                    });
                }
                Self::Length(left) => {
                    if *left == 0 {
                        return Err(SourceFailure("eof"));
                    }
                    let n = buffer
                        .len()
                        .min(8192)
                        .min((*left).min(usize::MAX as u64) as usize);
                    if n == 0 {
                        return Ok(None);
                    }
                    *left -= n as u64;
                    return Ok(Some(buffer.drain(..n).collect()));
                }
                Self::Chunked(phase) => match phase {
                    ChunkPhase::Size => {
                        let Some(end) = buffer.windows(2).position(|v| v == b"\r\n") else {
                            if buffer.len() > HEADER_LIMIT {
                                return Err(SourceFailure("chunk_header"));
                            }
                            return Ok(None);
                        };
                        let line = std::str::from_utf8(&buffer[..end])
                            .map_err(|_| SourceFailure("chunk_header"))?;
                        let size = line.split(';').next().unwrap();
                        if size.is_empty() || !size.bytes().all(|v| v.is_ascii_hexdigit()) {
                            return Err(SourceFailure("chunk_header"));
                        }
                        let n = u64::from_str_radix(size, 16)
                            .map_err(|_| SourceFailure("chunk_header"))?;
                        buffer.drain(..end + 2);
                        *phase = if n == 0 {
                            ChunkPhase::Trailers
                        } else {
                            ChunkPhase::Data(n)
                        };
                    }
                    ChunkPhase::Data(left) => {
                        let n = buffer
                            .len()
                            .min(8192)
                            .min((*left).min(usize::MAX as u64) as usize);
                        if n == 0 {
                            return Ok(None);
                        }
                        *left -= n as u64;
                        if *left == 0 {
                            *phase = ChunkPhase::CrLf(0);
                        }
                        return Ok(Some(buffer.drain(..n).collect()));
                    }
                    ChunkPhase::CrLf(progress) => {
                        if buffer.is_empty() {
                            return Ok(None);
                        }
                        if buffer.remove(0) != b"\r\n"[*progress as usize] {
                            return Err(SourceFailure("chunk_terminator"));
                        }
                        *progress += 1;
                        if *progress == 2 {
                            *phase = ChunkPhase::Size;
                        }
                    }
                    ChunkPhase::Trailers => {
                        if buffer.starts_with(b"\r\n") {
                            buffer.drain(..2);
                            *phase = ChunkPhase::Done;
                        } else if let Some(end) = buffer.windows(4).position(|v| v == b"\r\n\r\n") {
                            buffer.drain(..end + 4);
                            *phase = ChunkPhase::Done;
                        } else {
                            if buffer.len() > HEADER_LIMIT {
                                return Err(SourceFailure("chunk_trailers"));
                            }
                            return Ok(None);
                        }
                    }
                    ChunkPhase::Done => return Err(SourceFailure("eof")),
                },
            }
        }
    }

    pub(crate) fn valid(&self) -> bool {
        !matches!(self, Self::Chunked(ChunkPhase::CrLf(n)) if *n > 1)
            && !matches!(
                self,
                Self::Chunked(ChunkPhase::Data(0) | ChunkPhase::Done) | Self::Length(0)
            )
    }
}

async fn connect_source(state: &State) -> Result<SourceOwner, SourceFailure> {
    let url =
        url::Url::parse(&state.config.upstream).map_err(|_| SourceFailure("configuration"))?;
    let host = url.host_str().unwrap().trim_matches(['[', ']']);
    let port = url.port_or_known_default().unwrap();
    let mut socket = TcpStream::connect((host, port))
        .await
        .map_err(|_| SourceFailure("connection"))?;
    let mut request = format!(
        "GET /api/event HTTP/1.1\r\nHost: {}\r\nAccept: text/event-stream\r\nConnection: keep-alive\r\n",
        &url[url::Position::BeforeHost..url::Position::AfterPort]
    );
    if let Some(password) = &state.config.password {
        request.push_str(&format!(
            "Authorization: Basic {}\r\n",
            STANDARD.encode(format!("{}:{password}", state.config.username))
        ));
    }
    request.push_str("\r\n");
    socket
        .write_all(request.as_bytes())
        .await
        .map_err(|_| SourceFailure("request_transport"))?;
    let mut buffer = Vec::new();
    let framing = loop {
        let mut headers = [httparse::EMPTY_HEADER; 128];
        let mut response = httparse::Response::new(&mut headers);
        if let httparse::Status::Complete(end) = response
            .parse(&buffer)
            .map_err(|_| SourceFailure("response_headers"))?
        {
            if response.code != Some(200) || response.version != Some(1) {
                return Err(SourceFailure("http_status"));
            }
            let mut map = http::HeaderMap::new();
            for h in response.headers {
                map.append(
                    http::HeaderName::from_bytes(h.name.as_bytes())
                        .map_err(|_| SourceFailure("response_headers"))?,
                    http::HeaderValue::from_bytes(h.value)
                        .map_err(|_| SourceFailure("response_headers"))?,
                );
            }
            if map
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .is_none_or(|v| {
                    !v.split(';')
                        .next()
                        .unwrap()
                        .trim()
                        .eq_ignore_ascii_case("text/event-stream")
                })
            {
                return Err(SourceFailure("content_type"));
            }
            if map.get_all(EVENT_SCOPE_HEADER).iter().count() != 1
                || map
                    .get(EVENT_SCOPE_HEADER)
                    .is_none_or(|v| v != EVENT_SCOPE_VERSION)
            {
                return Err(SourceFailure("scope_version"));
            }
            if map.contains_key(header::CONTENT_ENCODING) {
                return Err(SourceFailure("content_encoding"));
            }
            let transfers: Vec<_> = map.get_all(header::TRANSFER_ENCODING).iter().collect();
            let lengths: Vec<_> = map.get_all(header::CONTENT_LENGTH).iter().collect();
            let framing = if !transfers.is_empty() {
                if transfers.len() != 1
                    || !transfers[0]
                        .to_str()
                        .is_ok_and(|v| v.eq_ignore_ascii_case("chunked"))
                    || !lengths.is_empty()
                {
                    return Err(SourceFailure("body_framing"));
                }
                Framing::Chunked(ChunkPhase::Size)
            } else if !lengths.is_empty() {
                if lengths.len() != 1 {
                    return Err(SourceFailure("body_framing"));
                }
                let length = lengths[0]
                    .to_str()
                    .ok()
                    .filter(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
                    .and_then(|v| v.parse().ok())
                    .ok_or(SourceFailure("body_framing"))?;
                Framing::Length(length)
            } else {
                Framing::Close
            };
            buffer.drain(..end);
            break framing;
        }
        if buffer.len() >= HEADER_LIMIT {
            return Err(SourceFailure("response_headers"));
        }
        let mut bytes = [0; 8192];
        let n = socket
            .read(&mut bytes)
            .await
            .map_err(|_| SourceFailure("response_transport"))?;
        if n == 0 {
            return Err(SourceFailure("eof"));
        }
        buffer.extend_from_slice(&bytes[..n]);
    };
    let decoder = SseDecoder::new(state.config.max_source_frame_bytes);
    let image = SourceImage {
        descriptor: String::new(),
        epoch: None,
        framing,
        buffer,
        decoder: decoder.export().unwrap(),
        last_activity_ms: monotonic_ms(),
        ready_deadline_ms: monotonic_ms() + state.config.request_timeout.as_millis() as u64,
    };
    Ok(SourceOwner {
        socket,
        decoder,
        image,
    })
}

async fn drive_source(owner: &mut SourceOwner, state: &State) -> Result<(), SourceFailure> {
    loop {
        if state.pause.is_cancelled() || state.shutdown.is_cancelled() {
            return Ok(());
        }
        if let Some(bytes) = owner.image.framing.take(&mut owner.image.buffer)? {
            state
                .metrics
                .bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            // Always settle the complete decode/publication batch before parking.
            for frame in owner
                .decoder
                .push(&bytes)
                .map_err(|_| SourceFailure("framing_or_size"))?
            {
                match bus_proxy_protocol::decode_event(Bytes::from(frame))
                    .map_err(|_| SourceFailure("event_metadata"))?
                {
                    UpstreamEvent::Connected { epoch } if owner.image.epoch.is_none() => {
                        state
                            .router
                            .begin_epoch(epoch.clone())
                            .await
                            .map_err(|_| SourceFailure("router_begin"))?;
                        owner.image.epoch = Some(epoch);
                        tracing::Span::current()
                            .record("epoch", owner.image.epoch.as_deref().unwrap());
                        tracing::info!("event source ready");
                        state.source_ready.store(true, Ordering::Release);
                    }
                    UpstreamEvent::Domain { route, event } if owner.image.epoch.is_some() => {
                        state
                            .router
                            .publish(
                                owner.image.epoch.clone().unwrap(),
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
            continue;
        }
        let idle = owner
            .image
            .last_activity_ms
            .saturating_add(state.config.source_idle_timeout.as_millis() as u64);
        let deadline = if owner.image.epoch.is_none() {
            idle.min(owner.image.ready_deadline_ms)
        } else {
            idle
        };
        tokio::select! {
            biased;
            _ = state.pause.cancelled() => return Ok(()),
            _ = state.shutdown.cancelled() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_millis(deadline.saturating_sub(monotonic_ms()))) => return Err(SourceFailure("idle_timeout")),
            ready = owner.socket.readable() => {
                ready.map_err(|_| SourceFailure("body_transport"))?;
                let mut bytes = [0; 8192];
                match owner.socket.try_read(&mut bytes) {
                    Ok(0) => return Err(SourceFailure("eof")),
                    Ok(n) => { owner.image.buffer.extend_from_slice(&bytes[..n]); owner.image.last_activity_ms = monotonic_ms(); }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {},
                    Err(_) => return Err(SourceFailure("body_transport")),
                }
            }
        }
    }
}

pub(crate) async fn source(
    state: Arc<State>,
    mut imported: Option<SourceOwner>,
) -> Option<SourceOwner> {
    let mut attempt = 0u32;
    loop {
        if state.shutdown.is_cancelled() || state.pause.is_cancelled() {
            return imported;
        }
        let connected = match imported.take() {
            Some(owner) => Ok(owner),
            None => tokio::select! {
                biased;
                _ = state.pause.cancelled() => return None,
                _ = state.shutdown.cancelled() => return None,
                result = tokio::time::timeout(state.config.request_timeout, connect_source(&state)) => result.unwrap_or(Err(SourceFailure("response_timeout"))),
            },
        };
        let attempt_span = tracing::info_span!("source_attempt", attempt);
        let (result, parked) = async {
            match connected {
                Ok(mut owner) => {
                    if let Some(epoch) = &owner.image.epoch {
                        tracing::Span::current().record("epoch", epoch);
                    }
                    state
                        .source_ready
                        .store(owner.image.epoch.is_some(), Ordering::Release);
                    let result = drive_source(&mut owner, &state).await;
                    if result.is_ok()
                        && state.pause.is_cancelled()
                        && !state.shutdown.is_cancelled()
                    {
                        owner.image.decoder = owner
                            .decoder
                            .export()
                            .expect("successful source decoder is open");
                        return (Ok(()), Some(owner));
                    }
                    state.source_ready.store(false, Ordering::Release);
                    if let Some(epoch) = owner.image.epoch.take() {
                        let _ = state.router.end_epoch(epoch).await;
                        attempt = 0;
                        tracing::info!(
                            shutdown = state.shutdown.is_cancelled(),
                            "source epoch retired"
                        );
                    }
                    (result, None)
                }
                Err(e) => (Err(e), None),
            }
        }
        .instrument(tracing::info_span!(
            "source_epoch",
            epoch = tracing::field::Empty
        ))
        .instrument(attempt_span)
        .await;
        if let Some(owner) = parked {
            return Some(owner);
        }
        if state.shutdown.is_cancelled() {
            return None;
        }
        state.metrics.failures.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            reason = result.as_ref().err().map_or("closed", |e| e.0),
            "event source disconnected; reconnecting"
        );
        let cap = (250u64.saturating_mul(1 << attempt.min(7))).min(30_000);
        attempt = attempt.saturating_add(1);
        tokio::select! {
            _ = state.pause.cancelled() => return None,
            _ = state.shutdown.cancelled() => return None,
            _ = tokio::time::sleep(Duration::from_millis(rand::random_range(cap / 2..=cap))) => {},
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_output_shares_payload_and_restores_every_segment_offset() {
        let frame = Bytes::from(vec![b'x'; 100]);
        let wire = Output::chunk(frame.clone()).snapshot().0;
        let owner = Output::chunk(frame.clone());
        assert_eq!(
            owner.parts[1].as_ptr(),
            frame.as_ptr(),
            "no per-client payload copy"
        );
        for cut in 0..wire.len() {
            let mut old = Output::chunk(frame.clone());
            old.advance(cut);
            let (snapshot, offset) = old.snapshot();
            let restored = Output::wire(snapshot, offset);
            let remaining: Vec<_> = restored
                .slices()
                .iter()
                .flat_map(|s| s.iter().copied())
                .collect();
            assert_eq!(remaining, wire[cut..], "cut {cut}");
        }
    }

    fn drain(framing: &mut Framing, buffer: &mut Vec<u8>, output: &mut Vec<u8>) {
        loop {
            match framing.take(buffer) {
                Ok(Some(bytes)) => output.extend_from_slice(&bytes),
                Ok(None) => break,
                Err(SourceFailure("eof")) => break,
                Err(error) => panic!("unexpected framing error: {error}"),
            }
        }
    }

    #[test]
    fn every_http_chunk_cut_restores_header_data_crlf_and_trailers_without_replay() {
        let mut wire =
            b"3;extension=ok\r\none\r\n3\r\ntwo\r\n0\r\nignored: trailer\r\n\r\n".to_vec();
        // Chunk syntax is HTTP material, independent of SSE decoder boundaries.
        for cut in 0..=wire.len() {
            let mut old = Framing::Chunked(ChunkPhase::Size);
            let mut buffer = wire[..cut].to_vec();
            let mut output = Vec::new();
            drain(&mut old, &mut buffer, &mut output);
            let mut successor: Framing =
                serde_json::from_slice(&serde_json::to_vec(&old).unwrap()).unwrap();
            buffer.extend_from_slice(&wire[cut..]);
            drain(&mut successor, &mut buffer, &mut output);
            assert_eq!(output, b"onetwo", "cut {cut}");
            assert!(buffer.is_empty());
            assert!(matches!(successor, Framing::Chunked(ChunkPhase::Done)));
        }
        wire.clear();
        assert!(!Framing::Chunked(ChunkPhase::CrLf(2)).valid());
        assert!(!Framing::Chunked(ChunkPhase::Data(0)).valid());
    }

    #[test]
    fn fixed_length_and_close_delimited_body_buffers_resume_exactly() {
        for framing in [Framing::Length(6), Framing::Close] {
            let mut old = framing;
            let mut buffer = b"one".to_vec();
            let mut output = Vec::new();
            drain(&mut old, &mut buffer, &mut output);
            let mut successor: Framing =
                serde_json::from_slice(&serde_json::to_vec(&old).unwrap()).unwrap();
            buffer.extend_from_slice(b"two");
            drain(&mut successor, &mut buffer, &mut output);
            assert_eq!(output, b"onetwo");
            assert!(buffer.is_empty());
        }
    }
}
