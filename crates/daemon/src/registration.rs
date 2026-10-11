//! Optional contribution to OpenCode's single operator-owned discovery slot.
//! /api/info remains origin-owned: PID/version/password MUST stay unchanged.
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    net::{IpAddr, SocketAddr},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    time::Duration,
};

use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use http::{Request, header};
use http_body_util::{BodyExt, Empty, Limited};
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

pub mod watch;

use crate::Result;

#[derive(Clone, Copy, Debug, clap::ValueEnum, PartialEq, Eq)]
pub enum Mode {
    /// Supplement endpoints, leaving the publisher's contact URL unchanged.
    Cooperate,
    /// Reapply this gateway's contact URL whenever the origin republishes.
    Stomp,
}

const MAX_RECORD_BYTES: u64 = 64 * 1024;
const POLL_INTERVAL: Duration = Duration::from_secs(1);
type HttpClient = Client<hyper_util::client::legacy::connect::HttpConnector, Empty<Bytes>>;

#[derive(Clone, Serialize, Deserialize)]
struct Record {
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    url: String,
    pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    password: Option<String>,
    #[serde(rename = "_busProxy", skip_serializing_if = "Option::is_none")]
    contribution: Option<Contribution>,
    #[serde(flatten)]
    extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Contribution {
    format: u32,
    pid: u32,
    token: String,
    #[serde(rename = "originUrl")]
    origin_url: String,
    #[serde(
        rename = "previousUrl",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    previous_url: Option<String>,
    #[serde(rename = "addedDiscovery", default)]
    added_discovery: bool,
    #[serde(rename = "addedProxy", default)]
    added_proxy: bool,
    #[serde(
        rename = "previousProxyPid",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    previous_proxy_pid: Option<u32>,
    #[serde(rename = "proxyEndpoint", default = "proxy_endpoint")]
    proxy_endpoint: String,
}

struct Owned {
    bytes: Vec<u8>,
    origin: Record,
}

pub struct Registration {
    file: PathBuf,
    upstream: Url,
    advertised: Url,
    password: Option<String>,
    token: String,
    pid: u32,
    timeout: Duration,
    client: HttpClient,
    owned: Option<Owned>,
    // Retained for the process lifetime, including shutdown restoration. The
    // file itself stays: unlinking a locked file breaks cross-process locking.
    _lock: File,
    mode: Mode,
    metadata: bool,
}

impl Registration {
    pub fn new(
        file: PathBuf,
        config: &bus_proxy_gateway::Config,
        advertised: Option<&str>,
        address: SocketAddr,
        mode: Mode,
        metadata: bool,
    ) -> Result<Self> {
        if !file.is_absolute() {
            return Err("--service-file must be an absolute path".into());
        }
        if config.username != "opencode" {
            return Err("native discovery requires --username opencode".into());
        }
        let upstream = origin_url(&config.upstream)?;
        // This is a local discovery contribution, not a remote service registry.
        // Limiting upstream to HTTP also avoids adding another TLS/auth policy.
        if upstream.scheme() != "http" || !local_host(&upstream) {
            return Err("--service-file requires a local HTTP upstream".into());
        }
        let advertised = origin_url(
            advertised
                .map(str::to_owned)
                .unwrap_or_else(|| listener_url(address))
                .as_str(),
        )?;
        if advertised.scheme() != "http" || !local_host(&advertised) {
            return Err("--service-url must be a local HTTP origin".into());
        }
        if same_origin(&upstream, &advertised) {
            return Err("discovery URL must not target the upstream".into());
        }
        let parent = file.parent().ok_or("service file has no parent")?;
        std::fs::create_dir_all(parent)?;
        let mut lock_name = file.as_os_str().to_owned();
        lock_name.push(".bus-proxy.lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(PathBuf::from(lock_name))?;
        lock.try_lock().map_err(
            |_| "another proxy holds this discovery contribution lock (or locking is unsupported)",
        )?;
        Ok(Self {
            file,
            upstream,
            advertised,
            password: config.password.clone(),
            token: Uuid::new_v4().to_string(),
            pid: std::process::id(),
            timeout: config.request_timeout,
            client: Client::builder(TokioExecutor::new()).build_http(),
            owned: None,
            _lock: lock,
            mode,
            metadata,
        })
    }

    pub async fn run(mut self, shutdown: CancellationToken, watcher: Option<watch::Options>) {
        let (events, mut changes) = tokio::sync::mpsc::channel(8);
        let watching = watcher.map(|options| {
            tokio::spawn(watch::supervise(
                options,
                self.file.clone(),
                events,
                shutdown.clone(),
            ))
        });
        let mut ticks = tokio::time::interval(POLL_INTERVAL);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut failed = false;
        let mut watch_ready = false;
        let mut watch_open = watching.is_some();
        let mut safety_ticks = 0u32;
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                event = changes.recv(), if watch_open => {
                    match event {
                        Some(watch::Event::Ready) => {
                            watch_ready = true;
                            tracing::info!("Effect discovery watcher ready");
                        },
                        Some(watch::Event::Change) => {},
                        Some(watch::Event::Failed) => watch_ready = false,
                        Some(watch::Event::Disabled) => {
                            watch_ready = false;
                            tracing::info!("Effect discovery watcher disabled; using polling");
                        },
                        None => { watch_ready = false; watch_open = false; },
                    }
                },
                _ = ticks.tick() => {
                    safety_ticks += 1;
                    if watch_ready && self.owned.is_some() && safety_ticks < 30 { continue; }
                    safety_ticks = 0;
                },
            }
            let result = tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                result = self.reconcile() => result,
            };
            match result {
                Err(error) if !failed => {
                    // Neither record content nor HTTP bodies enter diagnostics.
                    tracing::warn!(%error, "native discovery contribution failed; will retry");
                    failed = true;
                }
                Ok(()) => failed = false,
                Err(_) => {}
            }
        }
        if let Err(error) = self.restore() {
            tracing::warn!(%error, "native discovery origin restoration failed");
        }
        if let Some(task) = watching {
            let _ = task.await;
        }
    }

    async fn reconcile(&mut self) -> Result<()> {
        let bytes = match read_record(&self.file) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.owned = None;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        if self
            .owned
            .as_ref()
            .is_some_and(|owned| owned.bytes == bytes)
        {
            return Ok(());
        }
        // A changed slot ceases to be ours, even if publication below is delayed
        // by an origin restart. Shutdown must not put an old origin over it.
        self.owned = None;
        let Ok(mut record) = serde_json::from_slice::<Record>(&bytes) else {
            return Err("invalid discovery record; left untouched".into());
        };
        if record.mode.as_deref() != Some("server") || record.pid == 0 {
            return Ok(());
        }
        if let Some(marker) = &record.contribution {
            if marker.format != 1
                || marker.pid == 0
                || marker.token.is_empty()
                || !matches!(marker.proxy_endpoint.as_str(), "proxy" | "busProxy")
                || process_alive(marker.pid)
                || origin_url(&record.url).ok().as_ref() != Some(&self.advertised)
            {
                return Ok(());
            }
            // Recovery from SIGKILL/crash: only a dead contributor for this
            // listener and configured origin can be reclaimed.
            record.url = marker
                .previous_url
                .as_ref()
                .unwrap_or(&marker.origin_url)
                .clone();
            recover_metadata(&mut record.extra, marker);
            record.contribution = None;
        }
        let Some(origin_url_value) = candidate_origin(&record, &self.advertised, &self.upstream)
        else {
            return Ok(());
        };
        let Ok(origin) = origin_url(&origin_url_value) else {
            return Ok(());
        };
        if !same_origin(&origin, &self.upstream) || record.password != self.password {
            return Ok(());
        }
        if !self.ready(&record).await {
            return Ok(());
        }
        let original = record.clone();
        let mut marker = Contribution {
            format: 1,
            pid: self.pid,
            token: self.token.clone(),
            origin_url: origin_url_value,
            previous_url: Some(original.url.clone()),
            added_discovery: false,
            added_proxy: false,
            previous_proxy_pid: None,
            proxy_endpoint: proxy_endpoint(),
        };
        if self.metadata && !enrich(&mut record, &self.advertised, &mut marker) {
            return Ok(());
        }
        if self.mode == Mode::Stomp {
            record.url = self.advertised.as_str().trim_end_matches('/').to_owned();
        }
        // Metadata disabled in cooperative mode means no contribution at all.
        if self.mode == Mode::Cooperate && !self.metadata {
            return Ok(());
        }
        record.contribution = Some(marker);
        let replacement = serde_json::to_vec(&record)?;
        if replace_if_unchanged(&self.file, &bytes, &replacement)? {
            tracing::info!(
                origin_pid = record.pid,
                proxy_pid = self.pid,
                mode = ?self.mode,
                "native discovery contribution published"
            );
            self.owned = Some(Owned {
                bytes: replacement,
                origin: original,
            });
        }
        Ok(())
    }

    async fn ready(&self, record: &Record) -> bool {
        // Probe through the actual serving gateway, not only the origin. A
        // successful /api/info alone does not establish event-source readiness.
        let Some(info) = self.get_json("/api/info").await else {
            return false;
        };
        if info.get("pid").and_then(serde_json::Value::as_u64) != Some(record.pid.into()) {
            return false;
        }
        let Some(version) = info.get("version").and_then(serde_json::Value::as_str) else {
            return false;
        };
        if record
            .version
            .as_deref()
            .is_some_and(|expected| expected != version)
        {
            return false;
        }
        self.get_json("/_bus/health").await.is_some()
    }

    async fn get_json(&self, path: &str) -> Option<serde_json::Value> {
        let url = self.advertised.join(path).ok()?;
        let mut request = Request::get(url.as_str());
        if let Some(password) = &self.password {
            request = request.header(
                header::AUTHORIZATION,
                format!("Basic {}", STANDARD.encode(format!("opencode:{password}"))),
            );
        }
        let request = request.body(Empty::<Bytes>::new()).ok()?;
        tokio::time::timeout(self.timeout, async {
            let response = self.client.request(request).await.ok()?;
            if !response.status().is_success() {
                return None;
            }
            let bytes = Limited::new(response.into_body(), MAX_RECORD_BYTES as usize)
                .collect()
                .await
                .ok()?
                .to_bytes();
            serde_json::from_slice(&bytes).ok()
        })
        .await
        .ok()
        .flatten()
    }

    fn restore(&mut self) -> Result<()> {
        if let Some(owned) = self.owned.take() {
            let replacement = serde_json::to_vec(&owned.origin)?;
            if replace_if_unchanged(&self.file, &owned.bytes, &replacement)? {
                tracing::info!("native discovery restored to origin");
            }
        }
        Ok(())
    }
}

/// Markup is descriptive. Only the service endpoint identifies the authorized
/// upstream; unknown versions/shapes are never overwritten by this daemon.
fn candidate_origin(record: &Record, advertised: &Url, upstream: &Url) -> Option<String> {
    if let Some(discovery) = record.extra.get("discovery") {
        if discovery.get("version")?.as_u64()? != 1 {
            return None;
        }
        let endpoints = discovery.get("endpoints")?.as_object()?;
        let service = endpoints.get("service")?;
        if service
            .get("pid")
            .is_some_and(|pid| pid.as_u64() != Some(record.pid.into()))
        {
            return None;
        }
        return service.get("url")?.as_str().map(str::to_owned);
    }
    let contact = origin_url(&record.url).ok()?;
    if same_origin(&contact, upstream) {
        return Some(record.url.clone());
    }
    // A native publisher can disable markup while publishing this listener.
    // Binding and the subsequent forwarded identity probe establish the route.
    if contact == *advertised {
        return Some(upstream.as_str().trim_end_matches('/').to_owned());
    }
    None
}

fn enrich(record: &mut Record, advertised: &Url, marker: &mut Contribution) -> bool {
    marker.added_discovery = !record.extra.contains_key("discovery");
    let discovery = record.extra.entry("discovery".into()).or_insert_with(|| {
        serde_json::json!({
            "version":1, "source":"daemon", "endpoints":{
                "service":{"url":marker.origin_url,"pid":record.pid}
            }
        })
    });
    let Some(endpoints) = discovery
        .get_mut("endpoints")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return false;
    };
    if endpoints
        .get("proxy")
        .and_then(|proxy| proxy.get("url"))
        .and_then(serde_json::Value::as_str)
        .and_then(|url| origin_url(url).ok())
        .as_ref()
        .is_some_and(|url| url != advertised)
    {
        // Keep another publisher-described proxy URL explicitly known; this
        // gateway is an additional endpoint, not a deletion of that knowledge.
        marker.proxy_endpoint = "busProxy".into();
    }
    marker.added_proxy = !endpoints.contains_key(&marker.proxy_endpoint);
    let proxy = endpoints
        .entry(&marker.proxy_endpoint)
        .or_insert_with(|| serde_json::json!({"url":advertised.as_str().trim_end_matches('/')}));
    let Some(proxy) = proxy.as_object_mut() else {
        return false;
    };
    if proxy
        .get("url")
        .and_then(serde_json::Value::as_str)
        .and_then(|url| origin_url(url).ok())
        .as_ref()
        != Some(advertised)
    {
        return false; // Do not steal another explicitly described proxy endpoint.
    }
    marker.previous_proxy_pid = proxy
        .get("pid")
        .and_then(serde_json::Value::as_u64)
        .and_then(|pid| u32::try_from(pid).ok());
    if proxy.contains_key("pid") && marker.previous_proxy_pid.is_none() {
        return false;
    }
    proxy.insert("pid".into(), marker.pid.into());
    true
}

fn recover_metadata(extra: &mut BTreeMap<String, serde_json::Value>, marker: &Contribution) {
    if marker.added_discovery {
        extra.remove("discovery");
        return;
    }
    let Some(endpoints) = extra
        .get_mut("discovery")
        .and_then(|value| value.get_mut("endpoints"))
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };
    if marker.added_proxy {
        endpoints.remove(&marker.proxy_endpoint);
        return;
    }
    if let Some(proxy) = endpoints
        .get_mut(&marker.proxy_endpoint)
        .and_then(serde_json::Value::as_object_mut)
    {
        match marker.previous_proxy_pid {
            Some(pid) => {
                proxy.insert("pid".into(), pid.into());
            }
            None => {
                proxy.remove("pid");
            }
        }
    }
}

fn proxy_endpoint() -> String {
    "proxy".into()
}

fn origin_url(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|_| "invalid discovery/upstream origin URL")?;
    if url.scheme() != "http"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("discovery requires an HTTP origin without credentials or path".into());
    }
    Ok(url)
}

fn host_ip(url: &Url) -> Option<IpAddr> {
    url.host_str()?.trim_matches(['[', ']']).parse().ok()
}

fn local_host(url: &Url) -> bool {
    url.host_str() == Some("localhost")
        || host_ip(url).is_some_and(|ip| ip.is_loopback() || ip.is_unspecified())
}

fn same_origin(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left.port_or_known_default() == right.port_or_known_default()
        && (left.host_str() == right.host_str()
            || (local_host(left)
                && local_host(right)
                && match (host_ip(left), host_ip(right)) {
                    (Some(left), Some(right)) => left.is_ipv4() == right.is_ipv4(),
                    _ => true, // localhost is the configured local listener.
                }))
}

fn listener_url(mut address: SocketAddr) -> String {
    if address.ip().is_unspecified() {
        address.set_ip(if address.is_ipv4() {
            std::net::Ipv4Addr::LOCALHOST.into()
        } else {
            std::net::Ipv6Addr::LOCALHOST.into()
        });
    }
    format!("http://{address}")
}

fn process_alive(pid: u32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return true;
    };
    // SAFETY: signal zero checks existence/permission without sending a signal.
    unsafe {
        libc::kill(pid, 0) == 0 || io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }
}

fn read_record(path: &Path) -> io::Result<Vec<u8>> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::other("discovery record must be a regular file"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_RECORD_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(io::Error::other("discovery record exceeds size limit"));
    }
    Ok(bytes)
}

fn replace_if_unchanged(path: &Path, expected: &[u8], replacement: &[u8]) -> io::Result<bool> {
    let mut name = path.as_os_str().to_owned();
    name.push(format!(".bus-proxy.{}.tmp", Uuid::new_v4()));
    let temporary = PathBuf::from(name);
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temporary)?;
        file.write_all(replacement)?;
        file.sync_all()?;
        match read_record(path) {
            Ok(current) if current == expected => {}
            Ok(_) => return Ok(false),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        }
        // Native OpenCode does not take our lock. This check+rename minimizes,
        // but CANNOT eliminate, the non-cooperating publisher race.
        std::fs::rename(&temporary, path)?;
        Ok(true)
    })();
    let _ = std::fs::remove_file(&temporary);
    result
}

#[cfg(test)]
mod tests;
