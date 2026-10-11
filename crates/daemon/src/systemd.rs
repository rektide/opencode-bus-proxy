//! systemd service-manager integration: socket-activation adoption.

use std::{
    io,
    net::{SocketAddr, TcpListener},
    os::fd::{FromRawFd, OwnedFd},
};

/// sd_listen_fds(3): decide whether the service manager handed this process
/// listening descriptors. An absent or foreign PID means no handover; a
/// descriptor count other than one is a unit configuration error.
pub(crate) fn requested(
    pid_value: Option<&str>,
    fds_value: Option<&str>,
    pid: u32,
) -> Result<bool, String> {
    let Some(fds) = fds_value else {
        return Ok(false);
    };
    if fds.trim() == "0" {
        return Ok(false);
    }
    match pid_value.map(str::trim).map(str::parse::<u32>) {
        Some(Ok(listen_pid)) if listen_pid == pid => {}
        Some(Ok(_)) => return Ok(false),
        Some(Err(_)) => return Err("invalid LISTEN_PID from service manager".into()),
        None => return Err("LISTEN_FDS set without LISTEN_PID".into()),
    }
    match fds.trim().parse::<u32>() {
        Ok(1) => Ok(true),
        Ok(count) => Err(format!(
            "systemd socket activation passed {count} listeners; this gateway takes exactly one ListenStream socket"
        )),
        Err(_) => Err("invalid LISTEN_FDS from service manager".into()),
    }
}

/// Adopt an activated listening descriptor. The address check rejects
/// non-socket or unbound handovers (for example a ListenFIFO= unit) with an
/// actionable error.
pub(crate) fn adopt(fd: OwnedFd) -> io::Result<(TcpListener, SocketAddr)> {
    let listener = TcpListener::from(fd);
    listener.set_nonblocking(true)?;
    let address = listener.local_addr().map_err(|_| {
        io::Error::other(
            "activated descriptor is not a bound internet socket; configure ListenStream= in the socket unit",
        )
    })?;
    Ok((listener, address))
}

/// sd_listen_fds(3) reserves descriptor 3 for the first activated socket.
pub(crate) fn activated_fd() -> OwnedFd {
    // SAFETY: the service manager duplicated the listening socket into this
    // process at descriptor 3; ownership transfers here and nowhere else.
    unsafe { OwnedFd::from_raw_fd(3) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activation_requires_local_pid_and_single_listener() {
        let pid = 42u32.to_string();
        assert!(!requested(None, None, 42).unwrap());
        assert!(!requested(None, Some("0"), 42).unwrap());
        assert!(!requested(Some(&pid), Some("0"), 42).unwrap());
        assert!(requested(Some(&pid), Some("1"), 42).unwrap());
        assert!(
            !requested(Some("7"), Some("1"), 42).unwrap(),
            "a foreign pid is not this process's activation"
        );
        assert!(requested(Some(&pid), Some("2"), 42).is_err());
        assert!(requested(Some(&pid), Some("x"), 42).is_err());
        assert!(requested(None, Some("1"), 42).is_err());
        assert!(requested(Some("x"), Some("1"), 42).is_err());
    }

    #[test]
    fn adopted_listeners_report_their_bound_address() {
        let bound = TcpListener::bind("127.0.0.1:0").expect("bind");
        let expected = bound.local_addr().unwrap();
        // try_clone leaves the original owner of its own descriptor.
        let (listener, address) = adopt(bound.try_clone().expect("clone").into()).expect("adopt");
        assert_eq!(address, expected);
        assert_eq!(listener.local_addr().unwrap(), expected);
    }

    #[test]
    fn non_socket_descriptors_are_rejected_with_guidance() {
        let directory = tempfile::tempdir().expect("tempdir");
        let file = std::fs::File::create(directory.path().join("fifo")).expect("create");
        let error = adopt(file.into()).expect_err("not a socket");
        assert!(error.to_string().contains("ListenStream"), "{error}");
    }

    #[tokio::test]
    async fn adopted_listeners_are_nonblocking_for_tokio() {
        let bound = TcpListener::bind("127.0.0.1:0").expect("bind");
        let (listener, _) = adopt(bound.try_clone().expect("clone").into()).expect("adopt");
        tokio::net::TcpListener::from_std(listener).expect("nonblocking listener");
    }
}
