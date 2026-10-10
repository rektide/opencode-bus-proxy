use std::{error::Error, net::SocketAddr, path::PathBuf, time::Duration};

use bus_proxy_gateway::Config;
use bus_proxy_router::{Limits, Router};
use clap::Parser;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

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

    #[arg(long, env = "BUS_PROXY_LISTEN", default_value = "127.0.0.1:4097")]
    listen: SocketAddr,

    #[arg(long, env = "OPENCODE_SERVER_USERNAME", default_value = "opencode")]
    username: String,

    /// Shared origin password file; otherwise read OPENCODE_SERVER_PASSWORD.
    #[arg(long, env = "BUS_PROXY_PASSWORD_FILE")]
    password_file: Option<PathBuf>,

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

    #[arg(long, default_value_t = Limits::default().command_capacity, value_parser = positive)]
    command_capacity: usize,

    #[arg(long, default_value_t = 16 * 1024 * 1024, value_parser = positive)]
    max_source_frame_bytes: usize,

    /// Upstream inactivity deadline; source comments count as activity.
    #[arg(long, default_value_t = 45, value_parser = positive)]
    source_idle_seconds: usize,

    /// Request/header establishment timeout, not a stream lifetime limit.
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
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,hyper=warn,h2=warn"));
    let subscriber = tracing_subscriber::fmt().with_env_filter(filter);
    if args.log_json {
        subscriber.json().try_init()?;
    } else {
        subscriber.try_init()?;
    }

    let password = read_password(args.password_file.as_ref()).await?;
    if password.is_none() && !args.allow_unauthenticated {
        return Err("set OPENCODE_SERVER_PASSWORD or --password-file; use --allow-unauthenticated only for an unauthenticated origin".into());
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
        ..Config::default()
    };
    let listener = TcpListener::bind(args.listen).await?;
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

async fn read_password(file: Option<&PathBuf>) -> Result<Option<String>> {
    let value = match file {
        Some(file) => Some(tokio::fs::read_to_string(file).await?),
        None => match std::env::var("OPENCODE_SERVER_PASSWORD") {
            Ok(value) => Some(value),
            Err(std::env::VarError::NotPresent) => None,
            Err(error) => return Err(error.into()),
        },
    };
    // Files conventionally end in a newline; other password whitespace is meaningful.
    let value = value.map(|value| {
        if file.is_some() {
            value.trim_end_matches(['\r', '\n']).to_owned()
        } else {
            value
        }
    });
    Ok(value.filter(|value| !value.is_empty()))
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
