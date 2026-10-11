//! Concrete service-manager policy, independent of routing and HTTP serving.
//!
//! Capture activation before starting the async runtime: activation environment
//! belongs to this process and must not be inherited by discovery helpers.

use std::{ffi::OsString, net::SocketAddr, os::fd::AsFd, time::Duration};

use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

mod systemd;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Captured manager state and the single owned activation descriptor, if any.
pub struct Manager {
    notify_socket: Option<OsString>,
    watchdog_interval: Option<Duration>,
    inherited: Option<std::os::fd::OwnedFd>,
}

impl Manager {
    /// Must be called before any threads are started.
    ///
    /// # Safety
    /// No other thread may read or write process environment while this consumes
    /// LISTEN_PID/LISTEN_FDS/LISTEN_FDNAMES. Descriptor 3 must be owned exclusively
    /// by this activation handover when the environment requests activation.
    pub unsafe fn capture() -> Result<Self> {
        let activation = systemd::requested(
            std::env::var("LISTEN_PID").ok().as_deref(),
            std::env::var("LISTEN_FDS").ok().as_deref(),
            std::process::id(),
        )?;
        let inherited = if activation {
            let descriptor = systemd::activated_fd()?;
            // SAFETY: guaranteed by this function's caller.
            unsafe {
                std::env::remove_var("LISTEN_FDS");
                std::env::remove_var("LISTEN_PID");
                std::env::remove_var("LISTEN_FDNAMES");
            }
            Some(descriptor)
        } else {
            None
        };
        Ok(Self {
            inherited,
            notify_socket: std::env::var_os("NOTIFY_SOCKET"),
            watchdog_interval: systemd::watchdog_interval(
                std::env::var("WATCHDOG_USEC").ok().as_deref(),
            ),
        })
    }

    /// Adopt exactly one handed-over socket, or bind and offer it to fdstore.
    /// A handed-over socket is already retained: never store it a second time.
    pub async fn acquire_listener(&mut self, listen: Option<SocketAddr>) -> Result<TcpListener> {
        if self.inherited.is_some()
            && let Some(address) = listen
        {
            return Err(format!(
                "--listen {address} conflicts with systemd socket activation; \
                 the socket unit's ListenStream= owns the bind address"
            )
            .into());
        }
        if let Some(descriptor) = self.inherited.take() {
            let (listener, address) = systemd::adopt(descriptor)?;
            tracing::info!(%address, "listener adopted from systemd socket activation");
            Ok(TcpListener::from_std(listener)?)
        } else {
            let address = listen.unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 4097)));
            let listener = TcpListener::bind(address).await?;
            // Best effort: failed handoff only means restarts rebind.
            if let Err(error) =
                systemd::fdstore(self.notify_socket.as_deref(), "listener", listener.as_fd())
            {
                tracing::debug!(%error, "listener fdstore handoff to the service manager failed");
            }
            Ok(listener)
        }
    }

    /// Report readiness at the host's chosen boundary and own its heartbeat.
    pub fn ready(&self, address: SocketAddr, shutdown: CancellationToken) -> Heartbeat {
        let _ = systemd::notify(
            self.notify_socket.as_deref(),
            &format!("READY=1\nSTATUS=event gateway on {address}"),
        );
        let task = self.watchdog_interval.map(|interval| {
            let socket = self.notify_socket.clone();
            tokio::spawn(async move {
                let mut heartbeat = tokio::time::interval(interval);
                heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => break,
                        _ = heartbeat.tick() => {
                            if systemd::notify(socket.as_deref(), "WATCHDOG=1").is_err() {
                                break;
                            }
                        }
                    }
                }
            })
        });
        Heartbeat(task)
    }

    pub fn stopping(&self) {
        let _ = systemd::notify(self.notify_socket.as_deref(), "STOPPING=1");
    }
}

/// Dropping the host's guard stops even a heartbeat whose shutdown is interrupted.
pub struct Heartbeat(Option<JoinHandle<()>>);

impl Drop for Heartbeat {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixDatagram;

    fn manager(socket: Option<OsString>, inherited: Option<std::os::fd::OwnedFd>) -> Manager {
        Manager {
            notify_socket: socket,
            watchdog_interval: None,
            inherited,
        }
    }

    #[tokio::test]
    async fn self_bound_listener_is_stored_but_adopted_listener_is_not() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("notify");
        let receiver = UnixDatagram::bind(&path).unwrap();
        receiver.set_nonblocking(true).unwrap();
        let mut bound = manager(Some(path.as_os_str().to_owned()), None);
        let listener = bound
            .acquire_listener(Some("127.0.0.1:0".parse().unwrap()))
            .await
            .unwrap();
        let mut buffer = [0; 128];
        let read = receiver.recv(&mut buffer).unwrap();
        assert_eq!(&buffer[..read], b"FDSTORE=1\nFDNAME=listener");
        let address = listener.local_addr().unwrap();
        let descriptor = listener.into_std().unwrap().into();
        let mut inherited = manager(Some(path.as_os_str().to_owned()), Some(descriptor));
        assert_eq!(
            inherited
                .acquire_listener(None)
                .await
                .unwrap()
                .local_addr()
                .unwrap(),
            address
        );
        assert_eq!(
            receiver.recv(&mut buffer).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    #[tokio::test]
    async fn explicit_address_conflicts_even_if_it_matches_inherited_socket() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let mut manager = manager(None, Some(listener.into()));
        let error = manager.acquire_listener(Some(address)).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("conflicts with systemd socket activation")
        );
        assert!(manager.inherited.is_some());
    }
}
