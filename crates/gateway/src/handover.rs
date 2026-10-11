//! A planned cut, not crash recovery. Parked owners retain their actual sockets
//! and guards until commit or resume; the private image never substitutes a
//! sampled inspection for exact transport state.
use crate::{
    Config, GatewayError, Metrics, State, source_owner,
    transport::{self, ClientOwner, Framing, SourceOwner},
};
use bus_proxy_protocol::{DecoderImage, SseDecoder};
use bus_proxy_router::{Limits, Router, RouterImage, Subscription};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    io,
    net::SocketAddr,
    os::fd::{AsFd, AsRawFd, OwnedFd},
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

pub const FORMAT_VERSION: u32 = 1;
pub const IMAGE_LIMIT: usize = 256 * 1024 * 1024;

#[derive(Clone, Default)]
pub(crate) struct Registry(Arc<Mutex<HashMap<String, serde_json::Value>>>);
impl Registry {
    pub(crate) fn register(&self, key: &str, sub: Option<&str>) {
        self.0.lock().unwrap().insert(key.into(), serde_json::json!({"connection": key, "subscription": sub,
            "capability": if sub.is_some() { "transferable" } else { "unsupported" },
            "role": if sub.is_some() { "http1_sse" } else { "http_or_tunnel" },
            "reason": if sub.is_some() { "established_serializable_transport" } else { "opaque_hyper_or_pending_headers" }}));
    }
    pub(crate) fn remove(&self, key: &str) {
        self.0.lock().unwrap().remove(key);
    }
    pub(crate) fn inspect(&self, state: &State) -> serde_json::Value {
        let mut connections: Vec<_> = self.0.lock().unwrap().values().cloned().collect();
        connections.sort_by(|a, b| a["connection"].as_str().cmp(&b["connection"].as_str()));
        let ready = state.source_ready.load(Ordering::Acquire);
        serde_json::json!({"format_version": FORMAT_VERSION, "source_transferable": ready && state.config.upstream.starts_with("http://"),
            "source_reason": if !state.config.upstream.starts_with("http://") { "tls_state_unsupported" } else if !ready { "source_not_ready" } else { "established_http1" },
            "connections": connections, "consistency": "observational", "image_limit_bytes": IMAGE_LIMIT})
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Image {
    pub version: u32,
    pub generation: String,
    pub boot_id: String,
    pub cut_ms: u64,
    pub compatibility: String,
    pub listener_address: SocketAddr,
    pub sockets: Vec<SocketImage>,
    pub source: SourceImage,
    pub clients: Vec<ClientImage>,
    pub router: RouterImage,
    pub metrics: [u64; 3],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SocketImage {
    pub name: String,
    pub local: SocketAddr,
    pub peer: SocketAddr,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientImage {
    pub key: String,
    pub descriptor: String,
    pub subscription: String,
    #[serde(with = "bus_proxy_protocol::buffer")]
    pub output: Vec<u8>,
    pub offset: u64,
    pub read_eof: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceImage {
    pub descriptor: String,
    pub epoch: Option<String>,
    pub framing: Framing,
    #[serde(with = "bus_proxy_protocol::buffer")]
    pub buffer: Vec<u8>,
    pub decoder: DecoderImage,
    pub last_activity_ms: u64,
    pub ready_deadline_ms: u64,
}

pub fn monotonic_ms() -> u64 {
    let mut clock: libc::timespec = unsafe { std::mem::zeroed() };
    // Linux's same-boot monotonic clock survives process replacement.
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut clock) },
        0
    );
    clock.tv_sec as u64 * 1000 + clock.tv_nsec as u64 / 1_000_000
}

fn boot_id() -> io::Result<String> {
    Ok(std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?
        .trim()
        .to_owned())
}
fn compatibility(config: &Config) -> String {
    // Credentials do not enter the image; their digest prevents an established
    // authorization being silently carried across a changed security policy.
    let values = serde_json::json!([
        config.upstream,
        config.username,
        config.password,
        config.cors,
        config.max_source_frame_bytes,
        config.source_idle_timeout.as_millis() as u64,
        config.request_timeout.as_millis() as u64,
        config.max_connections,
        config.header_read_timeout.as_millis() as u64
    ]);
    format!("{:x}", Sha256::digest(values.to_string().as_bytes()))
}

/// Versioned, bounded JSON + base64 envelope. Hash protects accidental corruption,
/// not trust: only the service manager may supply this sealed private memfd.
pub fn encode(image: &Image) -> io::Result<Vec<u8>> {
    let body = serde_json::to_vec(image).map_err(io::Error::other)?;
    if body.len() > IMAGE_LIMIT {
        return Err(io::Error::other("handover image exceeds 256 MiB"));
    }
    let mut output = b"OBPHO001".to_vec();
    output.extend_from_slice(&(body.len() as u64).to_le_bytes());
    output.extend_from_slice(&Sha256::digest(&body));
    output.extend_from_slice(&body);
    Ok(output)
}

pub fn decode(bytes: &[u8]) -> io::Result<Image> {
    if bytes.len() < 48 || &bytes[..8] != b"OBPHO001" {
        return Err(io::Error::other("unsupported handover envelope"));
    }
    let length = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    if length > IMAGE_LIMIT as u64
        || length != (bytes.len() - 48) as u64
        || Sha256::digest(&bytes[48..])[..] != bytes[16..48]
    {
        return Err(io::Error::other("invalid handover length or integrity"));
    }
    serde_json::from_slice(&bytes[48..]).map_err(io::Error::other)
}

pub struct Imported {
    source: SourceOwner,
    clients: Vec<(TcpStream, ClientImage, Subscription)>,
    metrics: [u64; 3],
}

impl Image {
    /// Validate complete named membership and state before any socket I/O.
    pub fn import(
        self,
        config: &Config,
        limits: Limits,
        mut sockets: HashMap<String, OwnedFd>,
        shutdown: CancellationToken,
    ) -> io::Result<(Router, tokio::task::JoinHandle<()>, Imported)> {
        let invalid = || io::Error::other("incompatible or incomplete handover image");
        if self.version != FORMAT_VERSION
            || self.boot_id != boot_id()?
            || self.compatibility != compatibility(config)
            || !config.upstream.starts_with("http://")
            || self.cut_ms > monotonic_ms()
            || self.source.last_activity_ms > self.cut_ms
            || self.source.epoch.as_deref() != Some(self.router.epoch.as_str())
            || !self.source.framing.valid()
            || self.source.buffer.len() > 40960
            || self.clients.len() > config.max_connections
            || self.clients.len() != self.router.subscriptions.len()
            || self.sockets.len() != self.clients.len() + 1
            || sockets.len() != self.sockets.len()
            || self.generation.is_empty()
            || self.generation.len() > 64
        {
            return Err(invalid());
        }
        let mut names = HashSet::new();
        let mut connections = HashSet::new();
        let mut subscriptions = HashSet::new();
        let expected: HashSet<_> = self
            .router
            .subscriptions
            .iter()
            .map(|s| s.id.as_str())
            .collect();
        for client in &self.clients {
            if client.key.is_empty()
                || client.key.len() > 128
                || !connections.insert(&client.key)
                || !subscriptions.insert(client.subscription.as_str())
                || !expected.contains(client.subscription.as_str())
                || client.offset > client.output.len() as u64
                || client.output.len()
                    > config
                        .max_source_frame_bytes
                        .saturating_mul(2)
                        .saturating_add(65536)
                || !names.insert(client.descriptor.as_str())
            {
                return Err(invalid());
            }
        }
        if !names.insert(self.source.descriptor.as_str()) {
            return Err(invalid());
        }
        let decoder =
            SseDecoder::import(self.source.decoder.clone(), config.max_source_frame_bytes)
                .map_err(io::Error::other)?;
        let mut adopted = HashMap::new();
        for socket in &self.sockets {
            if !names.remove(socket.name.as_str())
                || !socket.name.starts_with(&format!("bp-{}-", self.generation))
            {
                return Err(invalid());
            }
            let fd = sockets.remove(&socket.name).ok_or_else(invalid)?;
            let mut kind: libc::c_int = 0;
            let mut length = std::mem::size_of_val(&kind) as libc::socklen_t;
            if unsafe {
                libc::getsockopt(
                    fd.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_TYPE,
                    (&mut kind as *mut libc::c_int).cast(),
                    &mut length,
                )
            } < 0
                || kind != libc::SOCK_STREAM
            {
                return Err(invalid());
            }
            let stream = std::net::TcpStream::from(fd);
            if stream.local_addr()? != socket.local || stream.peer_addr()? != socket.peer {
                return Err(invalid());
            }
            stream.set_nonblocking(true)?;
            adopted.insert(socket.name.clone(), TcpStream::from_std(stream)?);
        }
        if !names.is_empty() || !sockets.is_empty() {
            return Err(invalid());
        }
        let source = SourceOwner {
            socket: adopted
                .remove(&self.source.descriptor)
                .ok_or_else(invalid)?,
            decoder,
            image: self.source,
        };
        let (router, task, guards) =
            Router::import(self.router, limits, shutdown).map_err(io::Error::other)?;
        let mut guards: HashMap<_, _> =
            guards.into_iter().map(|s| (s.id().to_owned(), s)).collect();
        let clients = self
            .clients
            .into_iter()
            .map(|c| {
                let socket = adopted
                    .remove(&c.descriptor)
                    .expect("validated socket membership");
                let sub = guards
                    .remove(&c.subscription)
                    .expect("validated subscription membership");
                (socket, c, sub)
            })
            .collect();
        Ok((
            router,
            task,
            Imported {
                source,
                clients,
                metrics: self.metrics,
            },
        ))
    }
}

struct Parked {
    listener: TcpListener,
    source: SourceOwner,
    clients: Vec<ClientOwner>,
    image: Arc<Image>,
}

/// A preparation lease. The gateway retains custody of every parked I/O owner;
/// abandoning this lease (including a canceled oneshot receive) resumes them.
/// Closing duplicated descriptors never calls shutdown on a shared socket.
pub struct Frozen {
    image: Arc<Image>,
    descriptors: Vec<(String, OwnedFd)>,
    abandoned: CancellationToken,
}

impl Drop for Frozen {
    fn drop(&mut self) {
        self.abandoned.cancel();
    }
}
impl Frozen {
    pub fn image(&self) -> &Image {
        &self.image
    }
    pub fn descriptors(&self) -> io::Result<Vec<(String, OwnedFd)>> {
        self.descriptors
            .iter()
            .map(|(name, fd)| Ok((name.clone(), fd.as_fd().try_clone_to_owned()?)))
            .collect()
    }
}

impl Parked {
    fn lease(&self, abandoned: CancellationToken) -> io::Result<Frozen> {
        let mut fds = vec![
            (
                "listener".into(),
                self.listener.as_fd().try_clone_to_owned()?,
            ),
            (
                self.image.source.descriptor.clone(),
                self.source.socket.as_fd().try_clone_to_owned()?,
            ),
        ];
        for client in &self.clients {
            fds.push((
                client.image.descriptor.clone(),
                client.socket.as_fd().try_clone_to_owned()?,
            ));
        }
        Ok(Frozen {
            image: self.image.clone(),
            descriptors: fds,
            abandoned,
        })
    }
}

enum Command {
    Freeze(oneshot::Sender<Result<Frozen, String>>),
    Resume(String, oneshot::Sender<()>),
}
#[derive(Clone)]
pub struct Controller(mpsc::Sender<Command>);
pub struct Receiver(mpsc::Receiver<Command>);
pub fn channel() -> (Controller, Receiver) {
    let (send, recv) = mpsc::channel(2);
    (Controller(send), Receiver(recv))
}
impl Controller {
    pub async fn freeze(&self) -> Result<Frozen, String> {
        let (send, recv) = oneshot::channel();
        self.0
            .send(Command::Freeze(send))
            .await
            .map_err(|_| "gateway closed")?;
        recv.await.map_err(|_| "gateway closed")?
    }
    pub async fn resume(&self, frozen: Frozen) -> Result<(), String> {
        let (send, recv) = oneshot::channel();
        self.0
            .send(Command::Resume(frozen.image.generation.clone(), send))
            .await
            .map_err(|_| "gateway closed")?;
        let result = recv.await.map_err(|_| "gateway closed".into());
        drop(frozen); // Keep the lease alive until the owners have actually resumed.
        result
    }
}

fn spawn_source(
    state: Arc<State>,
    source: Option<SourceOwner>,
) -> tokio::task::JoinHandle<Option<SourceOwner>> {
    tokio::spawn(async move {
        if state.config.upstream.starts_with("http://") {
            transport::source(state, source).await
        } else {
            source_owner(state).await;
            None
        }
    })
}

pub(crate) async fn run(
    mut listener: TcpListener,
    mut state: Arc<State>,
    shutdown: CancellationToken,
    mut control: Receiver,
    imported: Option<Imported>,
) -> Result<(), GatewayError> {
    let permits = Arc::new(Semaphore::new(state.config.max_connections));
    let mut connections = JoinSet::new();
    let mut source = if let Some(imported) = imported {
        state
            .metrics
            .frames
            .store(imported.metrics[0], Ordering::Relaxed);
        state
            .metrics
            .bytes
            .store(imported.metrics[1], Ordering::Relaxed);
        state
            .metrics
            .failures
            .store(imported.metrics[2], Ordering::Relaxed);
        for (socket, image, sub) in imported.clients {
            state
                .registry
                .register(&image.key, Some(&image.subscription));
            let permit = Arc::new(
                permits
                    .clone()
                    .try_acquire_owned()
                    .expect("validated inherited connection cap"),
            );
            connections.spawn(transport::client(
                ClientOwner {
                    socket,
                    image,
                    sub,
                    _permit: permit,
                },
                state.clone(),
            ));
        }
        spawn_source(state.clone(), Some(imported.source))
    } else {
        spawn_source(state.clone(), None)
    };
    let result = loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break Ok(()),
            result = &mut source => break result.map(|_| ()).map_err(GatewayError::SourceTask),
            Some(_) = connections.join_next(), if !connections.is_empty() => {},
            command = control.0.recv(), if !control.0.is_closed() => {
                let Some(Command::Freeze(reply)) = command else { continue; };
                if !state.config.upstream.starts_with("http://") || !state.source_ready.load(Ordering::Acquire) {
                    let _ = reply.send(Err("handover requires a ready plain-HTTP source; HTTPS/TLS and offline sources are unsupported".into()));
                    continue;
                }
                state.pause.cancel();
                let mut parked = Vec::new();
                while let Some(result) = connections.join_next().await {
                    if let Ok(Some(client)) = result { parked.push(client); }
                }
                state.tasks.close();
                state.tasks.wait().await;
                let parked_source = (&mut source).await.map_err(GatewayError::SourceTask)?;
                let router_image = state.router.freeze().await;
                let frozen = match (parked_source, router_image) {
                    (Some(source_owner), Ok(router_image)) => {
                        let generation = format!("{:016x}", rand::random::<u64>());
                        let source_name = format!("bp-{generation}-source");
                        let mut source_owner = source_owner;
                        source_owner.image.descriptor = source_name.clone();
                        let mut sockets = vec![SocketImage { name: source_name, local: source_owner.socket.local_addr().map_err(GatewayError::Listener)?, peer: source_owner.socket.peer_addr().map_err(GatewayError::Listener)? }];
                        for client in &mut parked {
                            client.image.descriptor = format!("bp-{generation}-{}", client.image.key);
                            sockets.push(SocketImage { name: client.image.descriptor.clone(), local: client.socket.local_addr().map_err(GatewayError::Listener)?, peer: client.socket.peer_addr().map_err(GatewayError::Listener)? });
                        }
                        let image = Image { version: FORMAT_VERSION, generation, boot_id: boot_id().map_err(GatewayError::Listener)?, cut_ms: monotonic_ms(),
                            compatibility: compatibility(&state.config), listener_address: listener.local_addr().map_err(GatewayError::Listener)?, sockets,
                            source: source_owner.image.clone(), clients: parked.iter().map(|c| c.image.clone()).collect(), router: router_image,
                            metrics: [state.metrics.frames.load(Ordering::Relaxed), state.metrics.bytes.load(Ordering::Relaxed), state.metrics.failures.load(Ordering::Relaxed)] };
                         Parked { listener, source: source_owner, clients: parked, image: Arc::new(image) }
                    }
                    (source_owner, _) => {
                        let _ = state.router.thaw().await;
                        state = renewed(&state);
                        for client in parked { connections.spawn(transport::client(client, state.clone())); }
                        source = spawn_source(state.clone(), source_owner);
                        let _ = reply.send(Err("source failed while establishing the handover cut; old epoch cannot be preserved".into()));
                        continue;
                    }
                };
                let abandoned = CancellationToken::new();
                let mut ack = None;
                match frozen.lease(abandoned.clone()) {
                    Ok(lease) => {
                        // Even send(Ok) can race with cancellation of its receiver.
                        // Custody stays here; dropping the lease wakes this owner.
                        let _ = reply.send(Ok(lease));
                        loop {
                            tokio::select! {
                                _ = shutdown.cancelled() => return Ok(()),
                                _ = abandoned.cancelled() => break,
                                command = control.0.recv() => match command {
                                    Some(Command::Resume(generation, sender)) if generation == frozen.image.generation => { ack = Some(sender); break; }
                                    Some(Command::Resume(_, _)) => {},
                                    Some(Command::Freeze(reply)) => { let _ = reply.send(Err("handover already prepared".into())); }
                                    None => break,
                                }
                            }
                        }
                    }
                    Err(_) => { let _ = reply.send(Err("unable to duplicate parked descriptors; old owners resumed".into())); }
                }
                listener = frozen.listener;
                state.router.thaw().await.map_err(|_| GatewayError::Configuration("router thaw failed"))?;
                state = renewed(&state);
                for client in frozen.clients { connections.spawn(transport::client(client, state.clone())); }
                source = spawn_source(state.clone(), Some(frozen.source));
                if let Some(ack) = ack { let _ = ack.send(()); }
            }
            accepted = async {
                let permit = permits.clone().acquire_owned().await.expect("connection semaphore stays open");
                listener.accept().await.map(|(socket, _)| (socket, Arc::new(permit)))
            } => {
                let (socket, permit) = match accepted { Ok(v) => v, Err(e) => break Err(GatewayError::Listener(e)) };
                let key = format!("c-{:032x}", rand::random::<u128>());
                state.registry.register(&key, None);
                connections.spawn(transport::accepted(socket, permit, state.clone(), key));
            }
        }
    };
    state.shutdown.cancel();
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

fn renewed(old: &State) -> Arc<State> {
    Arc::new(State {
        config: old.config.clone(),
        base: old.base.clone(),
        client: old.client.clone(),
        router: old.router.clone(),
        shutdown: old.shutdown.clone(),
        tasks: TaskTracker::new(),
        pause: CancellationToken::new(),
        source_ready: std::sync::atomic::AtomicBool::new(false),
        metrics: Metrics {
            frames: old.metrics.frames.load(Ordering::Relaxed).into(),
            bytes: old.metrics.bytes.load(Ordering::Relaxed).into(),
            failures: old.metrics.failures.load(Ordering::Relaxed).into(),
        },
        registry: old.registry.clone(),
    })
}
