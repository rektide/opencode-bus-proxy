use std::{error::Error, net::SocketAddr, path::PathBuf, time::Duration};

use bus_proxy_discovery as registration;
use bus_proxy_gateway::Config;
use bus_proxy_router::Limits;
use clap::Parser;
use tracing_subscriber::EnvFilter;

use crate::Result;

/// A subscription-aware OpenCode V2 event gateway.
#[derive(Parser)]
#[command(version, about)]
pub struct Arguments {
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

    /// Contribute the gateway URL to this existing operator-owned OpenCode record.
    #[arg(long, env = "BUS_PROXY_SERVICE_FILE")]
    service_file: Option<PathBuf>,

    /// Advertised local URL; defaults to the actual listener with wildcard mapped to loopback.
    #[arg(long, env = "BUS_PROXY_SERVICE_URL", requires = "service_file")]
    service_url: Option<String>,

    /// Cooperate supplements endpoints; stomp also reapplies this gateway's contact URL.
    #[arg(
        long,
        env = "BUS_PROXY_SERVICE_MODE",
        value_enum,
        default_value = "cooperate"
    )]
    service_mode: registration::Mode,

    /// Include public service/proxy endpoint descriptions (no additional secrets).
    #[arg(long, env = "BUS_PROXY_SERVICE_METADATA", default_value_t = true, action = clap::ArgAction::Set)]
    service_metadata: bool,

    /// Bun invalidation helper reusing the existing Effect watcher (optional).
    #[arg(long, env = "BUS_PROXY_SERVICE_WATCH_HELPER", requires_all = ["service_file", "service_watch_source"])]
    service_watch_helper: Option<PathBuf>,

    /// OpenCode source root containing effect-watch's util modules.
    #[arg(
        long,
        env = "BUS_PROXY_SERVICE_WATCH_SOURCE",
        requires = "service_watch_helper"
    )]
    service_watch_source: Option<PathBuf>,

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

pub fn initialize_tracing(args: &Arguments) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,hyper=warn,h2=warn"));
    let subscriber = tracing_subscriber::fmt().with_env_filter(filter);
    if args.log_json {
        subscriber.json().try_init()?;
    } else {
        subscriber.try_init()?;
    }

    Ok(())
}

pub struct Configuration {
    pub gateway: Config,
    pub limits: Limits,
    pub listen: Option<SocketAddr>,
    pub discovery: Discovery,
}

pub struct Discovery {
    file: Option<PathBuf>,
    url: Option<String>,
    mode: registration::Mode,
    metadata: bool,
    pub watcher: Option<registration::watch::Options>,
}

impl Discovery {
    pub fn prepare(
        &self,
        config: &Config,
        address: SocketAddr,
    ) -> Result<Option<registration::Registration>> {
        self.file
            .as_ref()
            .map(|file| {
                registration::Registration::new(
                    file.clone(),
                    &registration::Origin {
                        upstream: &config.upstream,
                        username: &config.username,
                        password: config.password.as_deref(),
                        request_timeout: config.request_timeout,
                    },
                    self.url.as_deref(),
                    address,
                    self.mode,
                    self.metadata,
                )
            })
            .transpose()
    }
}

impl Arguments {
    pub async fn resolve(self) -> Result<Configuration> {
        let args = self;
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
        Ok(Configuration {
            gateway: config,
            limits,
            listen: args.listen,
            discovery: Discovery {
                file: args.service_file,
                url: args.service_url,
                mode: args.service_mode,
                metadata: args.service_metadata,
                watcher: args
                    .service_watch_helper
                    .zip(args.service_watch_source)
                    .map(|(helper, source)| registration::watch::Options { helper, source }),
            },
        })
    }
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
