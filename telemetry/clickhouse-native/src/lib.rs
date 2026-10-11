//! Bounded native ClickHouse insertion using published `clickhouse-arrow`.
//! No environment reads, global TLS/runtime/subscriber setup, or table creation.
//! Native I/O is isolated from the caller's executor on an owned thread.

pub use arrow_array::RecordBatch;
use clickhouse_arrow::ArrowClient;
use futures_util::StreamExt;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};

/// Audited upstream 0.2.1 has a non-preemptible 30-second TCP connect call.
/// An operation/cancellation deadline can overrun by at most this allowance.
pub const CONNECT_BLOCKING_ALLOWANCE: Duration = Duration::from_secs(30);

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid native ClickHouse configuration: {0}")]
    Config(&'static str),
    #[error("native ClickHouse exporter is shut down")]
    Closed,
    #[error("native ClickHouse exporter already has pending work")]
    Busy,
    #[error(
        "native ClickHouse operation timed out; insert acknowledgment is uncertain; no retry was attempted"
    )]
    Timeout,
    #[error("native ClickHouse connection failed (details suppressed to protect credentials)")]
    Connect,
    #[error(
        "native ClickHouse insert failed or acknowledgment was interrupted (details suppressed); no retry was attempted"
    )]
    Insert,
    #[error("native ClickHouse server rejected operation (code {0}; details suppressed); no retry was attempted")]
    Server(i32),
    #[error("native ClickHouse export worker failed")]
    Worker,
    #[error("cannot start native ClickHouse export thread: {0}")]
    Thread(#[from] std::io::Error),
}

/// Simple identifiers only, intentionally excluding arbitrary SQL expressions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table {
    database: String,
    name: String,
}
impl Table {
    pub fn new(database: impl Into<String>, name: impl Into<String>) -> Result<Self, Error> {
        let table = Self {
            database: database.into(),
            name: name.into(),
        };
        for identifier in [&table.database, &table.name] {
            if identifier.is_empty()
                || identifier.len() > 128
                || !identifier.bytes().enumerate().all(|(i, c)| {
                    c == b'_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit())
                })
            {
                return Err(Error::Config(
                    "database/table must be ASCII identifiers (letters, digits, underscore; no leading digit), at most 128 bytes",
                ));
            }
        }
        Ok(table)
    }
    pub fn database(&self) -> &str {
        &self.database
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    fn insert_query(&self, batch: &RecordBatch) -> Result<String, Error> {
        // Explicit columns allow server-owned DEFAULT columns and omit materialized
        // columns. Validate names rather than interpolating unconstrained SQL.
        let mut columns = Vec::new();
        for field in batch.schema().fields() {
            let name = field.name();
            if name.is_empty()
                || !name
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'.'))
            {
                return Err(Error::Config(
                    "Arrow field names must contain only ASCII letters/digits/underscore/dot",
                ));
            }
            if columns.contains(name) {
                return Err(Error::Config("duplicate Arrow field name"));
            }
            columns.push(name.clone());
        }
        if columns.is_empty() {
            return Err(Error::Config("batch has no columns"));
        }
        Ok(format!(
            "INSERT INTO `{}`.`{}` ({}) VALUES",
            self.database,
            self.name,
            columns
                .iter()
                .map(|c| format!("`{c}`"))
                .collect::<Vec<_>>()
                .join(",")
        ))
    }
}

/// Host-resolved secrets are redacted by Debug. TLS uses WebPKI roots only;
/// custom CA/mTLS are not supported by the audited upstream native path.
#[derive(Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub tls_server_name: Option<String>,
    pub username: String,
    pub password: String,
    pub table: Table,
    /// DNS, handshake, insertion and full response drain deadline. The fixed
    /// upstream TCP blocking allowance is additional (see constant above).
    pub operation_timeout: Duration,
    pub max_batch_rows: usize,
    pub max_batch_bytes: usize,
}
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("tls", &self.tls)
            .field("tls_server_name", &self.tls_server_name)
            .field("username", &"[redacted]")
            .field("password", &"[redacted]")
            .field("table", &self.table)
            .field("operation_timeout", &self.operation_timeout)
            .field("max_batch_rows", &self.max_batch_rows)
            .field("max_batch_bytes", &self.max_batch_bytes)
            .finish()
    }
}
impl Config {
    pub fn validate(&self) -> Result<(), Error> {
        if self.host.is_empty()
            || self.host.contains("://")
            || self.host.contains(['/', '@'])
            || self.host.chars().any(char::is_whitespace)
            || self.port == 0
        {
            return Err(Error::Config(
                "use native host and nonzero port, not a URL or credentials",
            ));
        }
        if !self.tls && self.tls_server_name.is_some() {
            return Err(Error::Config("TLS server name requires TLS"));
        }
        if self.tls && rustls::crypto::CryptoProvider::get_default().is_none() {
            return Err(Error::Config(
                "host must install a rustls CryptoProvider before native TLS construction",
            ));
        }
        if self.operation_timeout.is_zero() || self.operation_timeout > Duration::from_secs(300) {
            return Err(Error::Config(
                "operation timeout must be positive and at most 300 seconds",
            ));
        }
        if self.max_batch_rows == 0 || self.max_batch_bytes == 0 {
            return Err(Error::Config("batch limits must be positive"));
        }
        Ok(())
    }
}

struct Request {
    batch: RecordBatch,
    query: String,
    cancelled: oneshot::Receiver<()>,
    response: oneshot::Sender<Result<(), Error>>,
}
struct Inner {
    config: Config,
    sender: mpsc::Sender<Request>,
    stop: watch::Sender<bool>,
    closed: AtomicBool,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
}
impl Inner {
    fn shutdown(&self) -> Result<(), Error> {
        self.closed.store(true, Ordering::Release);
        let _ = self.stop.send(true);
        if let Some(worker) = self.worker.lock().map_err(|_| Error::Worker)?.take() {
            worker.join().map_err(|_| Error::Worker)?;
        }
        Ok(())
    }
}
impl Drop for Inner {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

/// Cloneable bounded batch destination. One active and at most one queued batch;
/// excess submissions fail with Busy instead of building an unbounded backlog.
/// Construction does not connect; collector outages never gate host readiness.
/// Shutdown/last drop join the owned thread and belong off async application workers.
#[derive(Clone)]
pub struct BatchClient {
    inner: Arc<Inner>,
}
impl std::fmt::Debug for BatchClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchClient")
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}
impl BatchClient {
    pub fn new(config: Config) -> Result<Self, Error> {
        config.validate()?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let (sender, receiver) = mpsc::channel(1);
        let (stop, stopped) = watch::channel(false);
        let worker_config = config.clone();
        let worker = thread::Builder::new()
            .name("clickhouse-native-export".into())
            .spawn(move || {
                runtime.block_on(run(worker_config, receiver, stopped));
                // All async I/O is canceled. OS getaddrinfo (upstream lookup_host)
                // cannot be preempted; allow at most 1s to finish its blocking task.
                // That residual DNS lookup cannot continue a canceled insert.
                runtime.shutdown_timeout(Duration::from_secs(1));
            })?;
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                sender,
                stop,
                closed: AtomicBool::new(false),
                worker: Mutex::new(Some(worker)),
            }),
        })
    }

    /// Resolves only after server EndOfStream (or error), not just local send.
    /// Dropping this future cancels its operation and closes that connection.
    /// An interrupted acknowledgment may follow a committed insert; no retry.
    pub async fn insert(&self, batch: RecordBatch) -> Result<(), Error> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if batch.num_rows() > self.inner.config.max_batch_rows
            || batch.get_array_memory_size() > self.inner.config.max_batch_bytes
        {
            return Err(Error::Config("batch exceeds configured row/memory limit"));
        }
        let query = self.inner.config.table.insert_query(&batch)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let (cancel, cancelled) = oneshot::channel();
        let (response, receive) = oneshot::channel();
        self.inner
            .sender
            .try_send(Request {
                batch,
                query,
                cancelled,
                response,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => Error::Busy,
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })?;
        // Sender lifetime is the cancellation guard; no spawned caller-side task.
        let result = receive.await.map_err(|_| Error::Worker)?;
        drop(cancel);
        result
    }

    /// Stops active/pending work, then joins. Usually immediate; may wait up to
    /// the upstream 30s non-preemptible connect allowance. No detached cleanup.
    pub fn shutdown(&self) -> Result<(), Error> {
        self.inner.shutdown()
    }
}

async fn run(
    config: Config,
    mut receiver: mpsc::Receiver<Request>,
    mut stopped: watch::Receiver<bool>,
) {
    let mut client = None;
    loop {
        let request = tokio::select! { biased;
            _ = stopped.changed() => break,
            request = receiver.recv() => match request { Some(request) => request, None => break },
        };
        let Request {
            batch,
            query,
            mut cancelled,
            response,
        } = request;
        let result = tokio::select! { biased;
            _ = stopped.changed() => Err(Error::Closed),
            _ = &mut cancelled => Err(Error::Closed),
            result = tokio::time::timeout(config.operation_timeout, insert(&config, &mut client, batch, query)) => result.unwrap_or(Err(Error::Timeout)),
        };
        if result.is_err() {
            drop(client.take());
        }
        // Dropping the operation drops its sole client, aborting upstream's
        // JoinSet. Drive that cancellation before acknowledging or the next job.
        tokio::task::yield_now().await;
        let _ = response.send(result);
        if *stopped.borrow() {
            break;
        }
    }
    drop(client);
    tokio::task::yield_now().await;
    receiver.close();
    while let Ok(request) = receiver.try_recv() {
        let _ = request.response.send(Err(Error::Closed));
    }
}

async fn insert(
    config: &Config,
    client: &mut Option<ArrowClient>,
    batch: RecordBatch,
    query: String,
) -> Result<(), Error> {
    if client.is_none() {
        let mut builder = ArrowClient::builder()
            .with_host_port(&config.host, config.port)
            .with_tls(config.tls)
            .with_compression(clickhouse_arrow::CompressionMethod::None)
            .with_username(&config.username)
            .with_password(config.password.clone())
            .with_database(config.table.database());
        if let Some(domain) = &config.tls_server_name {
            builder = builder.with_domain(domain);
        }
        // Server-side async_insert must not weaken acknowledgment semantics.
        builder = builder.with_settings(vec![("async_insert", "0")]);
        *client = Some(builder.build_arrow().await.map_err(|_| Error::Connect)?);
    }
    let client = client.as_ref().ok_or(Error::Worker)?;
    {
        let stream = client
            .insert(query, batch, None)
            .await
            .map_err(insert_error)?;
        tokio::pin!(stream);
        while let Some(result) = stream.next().await {
            result.map_err(insert_error)?;
        }
    }
    // Upstream 0.2.1 closes the response channel on both EndOfStream AND
    // fatal I/O failure. A drained stream alone is therefore insufficient.
    // Ping the same connection after drain: lost acknowledgments must fail.
    client.health_check(true).await.map_err(|_| Error::Insert)
}

fn insert_error(error: clickhouse_arrow::Error) -> Error {
    match error {
        clickhouse_arrow::Error::ServerException(error) => Error::Server(error.code),
        _ => Error::Insert,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn identifiers_reject_sql_and_debug_redacts() {
        assert!(Table::new("default", "traces_v1").is_ok());
        for value in ["", "a; DROP TABLE x", "db.table", "1table", "a`b"] {
            assert!(Table::new("default", value).is_err());
        }
        let config = Config {
            host: "127.0.0.1".into(),
            port: 9000,
            tls: false,
            tls_server_name: None,
            username: "secret-user".into(),
            password: "secret-password".into(),
            table: Table::new("default", "test").unwrap(),
            operation_timeout: Duration::from_millis(100),
            max_batch_rows: 10,
            max_batch_bytes: 1024,
        };
        assert!(!format!("{config:?}").contains("secret"));
        assert!(config.validate().is_ok());
        let client = BatchClient::new(config).unwrap();
        client.shutdown().unwrap();
        client.shutdown().unwrap();
    }
}
