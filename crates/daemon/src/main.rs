use std::{error::Error, net::SocketAddr, os::fd::AsFd, path::PathBuf, time::Duration};

use bus_proxy_gateway::Config;
use bus_proxy_router::{Limits, Router};
use clap::Parser;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

mod systemd;
mod telemetry;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

/// A subscription-aware OpenCode V2 event gateway.
#[derive(Parser)]
#[command(version, about)]
struct Arguments {
    /// Bus-smart origin with the routed event-source extension installed.
    #[arg(
        long,
        env = "BUS_PROXY_UPSTREAM",
        default_value = "http://127.0.0.1:4096"
    )]
    upstream: String,

    /// Bind address; defaults to 127.0.0.1:4097. Under systemd socket
    /// activation the ListenStream= socket unit owns the address instead.
    #[arg(long, env = "BUS_PROXY_LISTEN")]
    listen: Option<SocketAddr>,

    #[arg(long, env = "OPENCODE_SERVER_USERNAME", default_value = "opencode")]
    username: String,

    /// Shared origin password file; otherwise read OPENCODE_SERVER_PASSWORD.
    #[arg(long, env = "BUS_PROXY_PASSWORD_FILE")]
    password_file: Option<PathBuf>,

    /// Shared origin password systemd credential name, read from $CREDENTIALS_DIRECTORY.
    #[arg(long, env = "BUS_PROXY_CREDENTIAL")]
    credential: Option<String>,

    /// Explicitly allow an origin and gateway without authentication.
    #[arg(long)]
    allow_unauthenticated: bool,

    /// Additional allowed browser origins (repeatable).
    #[arg(long)]
    cors: Vec<String>,

    #[arg(long, default_value_t = Limits::default().queue_frames, value_parser = positive)]
    queue_frames: usize,

    /// Logical queued bytes per subscriber, excluding HTTP/socket buffers.
    #[arg(long, default_value_t = Limits::default().queue_bytes, value_parser = positive)]
    queue_bytes: usize,

    /// Includes pending controlled subscriptions.
    #[arg(long, default_value_t = Limits::default().max_subscribers, value_parser = positive)]
    max_subscribers: usize,

    /// Concurrent HTTP connections, including upgraded tunnels.
    #[arg(long, default_value_t = 1024, value_parser = positive)]
    max_connections: usize,

    /// Deadline for reading inbound HTTP headers.
    #[arg(long, default_value_t = 10, value_parser = positive)]
    header_read_seconds: usize,

    #[arg(long, default_value_t = Limits::default().command_capacity, value_parser = positive)]
    command_capacity: usize,

    #[arg(long, default_value_t = 16 * 1024 * 1024, value_parser = positive)]
    max_source_frame_bytes: usize,

    /// Upstream inactivity deadline; source comments count as activity.
    #[arg(long, default_value_t = 45, value_parser = positive)]
    source_idle_seconds: usize,

    /// Upstream connect/source/upgrade setup timeout, not an API execution limit.
    #[arg(long, default_value_t = 10, value_parser = positive)]
    request_timeout_seconds: usize,

    /// Emit structured JSON tracing. RUST_LOG controls filtering in either mode.
    #[arg(long)]
    log_json: bool,
}

fn positive(value: &str) -> std::result::Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| "expected a positive integer".to_owned())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Arguments::parse();
    let activation = systemd::requested(
        std::env::var("LISTEN_PID").ok().as_deref(),
        std::env::var("LISTEN_FDS").ok().as_deref(),
        std::process::id(),
    )?;
    if activation {
        remove_activation_environment();
    }
    let telemetry = telemetry::init(args.log_json)?;
    let result = run(args, activation).await;
    // The batch worker uses blocking HTTP; flush outside Tokio's worker threads.
    tokio::task::spawn_blocking(move || telemetry.shutdown()).await?;
    result
}

async fn run(args: Arguments, activation: bool) -> Result<()> {
    if args.password_file.is_some() && args.credential.is_some() {
        return Err("choose one password source: --password-file or --credential".into());
    }
    let password = read_password(
        args.password_file.as_ref(),
        args.credential.as_deref(),
        std::env::var_os("CREDENTIALS_DIRECTORY"),
    )
    .await?;
    if password.is_none() && !args.allow_unauthenticated {
        return Err("set OPENCODE_SERVER_PASSWORD, --password-file, or --credential; use --allow-unauthenticated only for an unauthenticated origin".into());
    }
    let limits = Limits {
        queue_frames: args.queue_frames,
        queue_bytes: args.queue_bytes,
        max_subscribers: args.max_subscribers,
        command_capacity: args.command_capacity,
    };
    let config = Config {
        upstream: args.upstream,
        username: args.username,
        password,
        cors: args.cors,
        max_source_frame_bytes: args.max_source_frame_bytes,
        source_idle_timeout: Duration::from_secs(args.source_idle_seconds as u64),
        request_timeout: Duration::from_secs(args.request_timeout_seconds as u64),
        max_connections: args.max_connections,
        header_read_timeout: Duration::from_secs(args.header_read_seconds as u64),
    };
    let notify_socket = std::env::var_os("NOTIFY_SOCKET");
    let listener = if activation {
        if let Some(address) = args.listen {
            return Err(format!(
                "--listen {address} conflicts with systemd socket activation; \
                 the socket unit's ListenStream= owns the bind address"
            )
            .into());
        }
        let (listener, address) = systemd::adopt(systemd::activated_fd()?)?;
        tracing::info!(%address, "listener adopted from systemd socket activation");
        TcpListener::from_std(listener)?
    } else {
        let address = args
            .listen
            .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 4097)));
        let listener = TcpListener::bind(address).await?;
        // sd_notify fdstore: offer a self-bound listener to the manager so
        // Restart= keeps the listening socket. Only this branch stores — a
        // LISTEN_FDS handover (socket unit or replayed fdstore) is already
        // retained by the manager, and re-storing would accumulate duplicate
        // entries. Best-effort: without a manager this is a no-op, and a
        // failed store only means restarts rebind.
        if let Err(error) = systemd::fdstore(notify_socket.as_deref(), "listener", listener.as_fd())
        {
            tracing::debug!(%error, "listener fdstore handoff to the service manager failed");
        }
        listener
    };
    let address = listener.local_addr()?;
    let shutdown = CancellationToken::new();
    let (router, mut routing) = Router::spawn(limits, shutdown.clone())?;
    let mut gateway = tokio::spawn(bus_proxy_gateway::serve(
        listener,
        config,
        router,
        shutdown.clone(),
    ));
    tracing::info!(%address, "event gateway listening");
    let _ = systemd::notify(
        notify_socket.as_deref(),
        &format!("READY=1\nSTATUS=event gateway on {address}"),
    );
    if let Some(interval) =
        systemd::watchdog_interval(std::env::var("WATCHDOG_USEC").ok().as_deref())
    {
        let socket = notify_socket.clone();
        let stopped = shutdown.clone();
        tokio::spawn(async move {
            let mut heartbeat = tokio::time::interval(interval);
            heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    biased;
                    _ = stopped.cancelled() => break,
                    _ = heartbeat.tick() => {
                        if systemd::notify(socket.as_deref(), "WATCHDOG=1").is_err() {
                            break;
                        }
                    }
                }
            }
        });
    }

    enum Stopped {
        Signal(std::io::Result<()>),
        Gateway(
            std::result::Result<
                std::result::Result<(), bus_proxy_gateway::GatewayError>,
                tokio::task::JoinError,
            >,
        ),
        Router(std::result::Result<(), tokio::task::JoinError>),
    }
    let stopped = tokio::select! {
        result = stop_signal() => Stopped::Signal(result),
        result = &mut gateway => Stopped::Gateway(result),
        result = &mut routing => Stopped::Router(result),
    };
    let _ = systemd::notify(notify_socket.as_deref(), "STOPPING=1");
    shutdown.cancel();
    tracing::info!("event gateway stopping");
    match stopped {
        Stopped::Signal(result) => {
            result?;
            let (gateway, router) =
                tokio::join!(join_shutdown(&mut gateway), join_shutdown(&mut routing));
            gateway??;
            router?;
        }
        Stopped::Gateway(result) => {
            join_shutdown(&mut routing).await?;
            result??;
        }
        Stopped::Router(result) => {
            join_shutdown(&mut gateway).await??;
            result?;
            return Err("routing actor exited unexpectedly".into());
        }
    }
    Ok(())
}

async fn read_password(
    file: Option<&PathBuf>,
    credential: Option<&str>,
    credential_directory: Option<std::ffi::OsString>,
) -> Result<Option<String>> {
    let value = if let Some(file) = file {
        Some(tokio::fs::read_to_string(file).await?)
    } else if let Some(name) = credential {
        let path = credential_path(name, credential_directory.as_deref())?;
        Some(tokio::fs::read_to_string(&path).await.map_err(
            |error| -> Box<dyn Error + Send + Sync> {
                format!("read credential {}: {error}", path.display()).into()
            },
        )?)
    } else {
        match std::env::var("OPENCODE_SERVER_PASSWORD") {
            Ok(value) => Some(value),
            Err(std::env::VarError::NotPresent) => None,
            Err(error) => return Err(error.into()),
        }
    };
    // Files and systemd credentials conventionally end in a newline; env values
    // and other password whitespace are meaningful.
    let value = value.map(|value| {
        if file.is_some() || credential.is_some() {
            value.trim_end_matches(['\r', '\n']).to_owned()
        } else {
            value
        }
    });
    Ok(value.filter(|value| !value.is_empty()))
}

/// systemd credential names are filename-safe identifiers. The leading byte
/// must be alphanumeric so a joined name cannot traverse out of the directory.
fn valid_credential_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

fn credential_path(name: &str, directory: Option<&std::ffi::OsStr>) -> Result<PathBuf> {
    if !valid_credential_name(name) {
        return Err(format!(
            "credential name {name:?} must start with a letter or digit and contain only letters, digits, '.', '-', or '_'"
        )
        .into());
    }
    let directory = directory.filter(|value| !value.is_empty()).ok_or_else(|| {
        Box::<dyn Error + Send + Sync>::from(
            "--credential requires $CREDENTIALS_DIRECTORY, provided by a systemd unit with LoadCredential=, LoadCredentialEncrypted=, SetCredential=, or ImportCredential=",
        )
    })?;
    Ok(PathBuf::from(directory).join(name))
}

/// sd_listen_fds(3) hands the activation environment to this process only;
/// consuming it keeps any future child from re-adopting the descriptors.
fn remove_activation_environment() {
    // SAFETY: startup runs on the main thread before tasks are spawned, so no
    // concurrent reader of the environment exists.
    unsafe {
        std::env::remove_var("LISTEN_FDS");
        std::env::remove_var("LISTEN_PID");
        std::env::remove_var("LISTEN_FDNAMES");
    }
}

async fn join_shutdown<T>(task: &mut JoinHandle<T>) -> Result<T> {
    match tokio::time::timeout(Duration::from_secs(10), &mut *task).await {
        Ok(result) => Ok(result?),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err("task exceeded shutdown grace".into())
        }
    }
}

async fn stop_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_names_must_be_filename_safe() {
        for name in [
            "opencode-password",
            "busproxy.password",
            "cred_1",
            "a",
            "A9._-",
        ] {
            assert!(valid_credential_name(name), "{name}");
        }
        for name in [
            "",
            ".",
            "..",
            "../escape",
            "/absolute",
            "a/b",
            "a\\b",
            "a b",
            ".hidden",
            "_reserved",
            "café",
        ] {
            assert!(!valid_credential_name(name), "{name}");
        }
    }

    #[test]
    fn credential_paths_require_directory_and_validated_name() {
        let path = credential_path(
            "opencode-password",
            Some(std::ffi::OsStr::new("/run/creds")),
        )
        .expect("valid name and directory");
        assert_eq!(path, PathBuf::from("/run/creds/opencode-password"));
        assert!(credential_path("..", Some(std::ffi::OsStr::new("/run"))).is_err());
        assert!(credential_path("ok", None).is_err());
        assert!(credential_path("ok", Some(std::ffi::OsStr::new(""))).is_err());
    }

    #[tokio::test]
    async fn credential_files_trim_trailing_newlines_like_password_files() {
        let directory = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            directory.path().join("opencode-password"),
            "shared-secret\r\n",
        )
        .expect("write credential");
        let value = read_password(
            None,
            Some("opencode-password"),
            Some(directory.path().as_os_str().to_owned()),
        )
        .await
        .expect("read password");
        assert_eq!(value.as_deref(), Some("shared-secret"));
    }

    #[tokio::test]
    async fn empty_credential_yields_no_password() {
        let directory = tempfile::tempdir().expect("tempdir");
        std::fs::write(directory.path().join("empty"), "\n").expect("write credential");
        let value = read_password(
            None,
            Some("empty"),
            Some(directory.path().as_os_str().to_owned()),
        )
        .await
        .expect("read password");
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn missing_credential_directory_is_an_actionable_error() {
        let error = read_password(None, Some("opencode-password"), None)
            .await
            .expect_err("directory required");
        assert!(
            error.to_string().contains("$CREDENTIALS_DIRECTORY"),
            "{}",
            error
        );
    }

    #[tokio::test]
    async fn missing_credential_file_names_the_path() {
        let directory = tempfile::tempdir().expect("tempdir");
        let error = read_password(
            None,
            Some("absent"),
            Some(directory.path().as_os_str().to_owned()),
        )
        .await
        .expect_err("file required");
        assert!(error.to_string().contains("absent"), "{error}");
    }
}
