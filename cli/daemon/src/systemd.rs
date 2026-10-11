//! systemd service-manager integration: socket-activation adoption and
//! sd_notify readiness/watchdog/fdstore messages.

use std::{
    collections::HashMap,
    ffi::OsStr,
    io,
    net::{SocketAddr, TcpListener},
    os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd},
    os::unix::ffi::OsStrExt,
    time::Duration,
};

/// sd_listen_fds(3): decide whether the service manager handed this process
/// descriptors. An absent or foreign PID means no handover. Counts are bounded
/// before ownership is taken; individual names, not numbers, identify material.
pub(crate) fn requested(
    pid_value: Option<&str>,
    fds_value: Option<&str>,
    pid: u32,
) -> Result<u32, String> {
    let Some(fds) = fds_value else {
        return Ok(0);
    };
    if fds.trim() == "0" {
        return Ok(0);
    }
    match pid_value.map(str::trim).map(str::parse::<u32>) {
        Some(Ok(listen_pid)) if listen_pid == pid => {}
        Some(Ok(_)) => return Ok(0),
        Some(Err(_)) => return Err("invalid LISTEN_PID from service manager".into()),
        None => return Err("LISTEN_FDS set without LISTEN_PID".into()),
    }
    match fds.trim().parse::<u32>() {
        Ok(count @ 1..=4096) => Ok(count),
        Ok(_) => Err("LISTEN_FDS exceeds the supported descriptor limit".into()),
        Err(_) => Err("invalid LISTEN_FDS from service manager".into()),
    }
}

/// Adopt an activated listening descriptor. The address check rejects
/// non-socket or unbound handovers (for example a ListenFIFO= unit) with an
/// actionable error.
pub(crate) fn adopt(fd: OwnedFd) -> io::Result<(TcpListener, SocketAddr)> {
    let mut listening: libc::c_int = 0;
    let mut length = std::mem::size_of_val(&listening) as libc::socklen_t;
    if unsafe {
        libc::getsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_ACCEPTCONN,
            (&mut listening as *mut libc::c_int).cast(),
            &mut length,
        )
    } < 0
        || listening != 1
    {
        return Err(io::Error::other(
            "activated descriptor is not a listening TCP socket; configure ListenStream=",
        ));
    }
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
pub(crate) fn activated_fds(
    count: u32,
    names: Option<&str>,
) -> io::Result<HashMap<String, OwnedFd>> {
    let names = descriptor_names(count, names)?;
    let mut output = HashMap::new();
    for (index, name) in names.into_iter().enumerate() {
        let raw = 3 + index as libc::c_int;
        if unsafe { libc::fcntl(raw, libc::F_GETFD) } < 0 {
            return Err(io::Error::other("activated descriptor is closed"));
        }
        // Take each inherited descriptor once, and prevent children from reusing it.
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        if unsafe { libc::fcntl(raw, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error());
        }
        output.insert(name, fd);
    }
    Ok(output)
}

fn descriptor_names(count: u32, names: Option<&str>) -> io::Result<Vec<String>> {
    let names: Vec<String> = match names {
        Some(names) => names.split(':').map(str::to_owned).collect(),
        None if count == 1 => vec!["listener".into()],
        None => {
            return Err(io::Error::other(
                "multi-FD activation requires LISTEN_FDNAMES",
            ));
        }
    };
    if names.len() != count as usize
        || names.iter().any(|n| !valid_name(n))
        || names.iter().collect::<std::collections::HashSet<_>>().len() != names.len()
    {
        return Err(io::Error::other(
            "invalid, duplicate or incomplete LISTEN_FDNAMES",
        ));
    }
    Ok(names)
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// sd_notify(3): best-effort datagram to the service manager. An absent or
/// empty NOTIFY_SOCKET means no manager and reports success.
pub(crate) fn notify(socket: Option<&OsStr>, message: &str) -> io::Result<()> {
    send_to_manager(socket, message.as_bytes(), None)
}

/// sd_notify(3) fdstore: hand a descriptor to the service manager so it is
/// held across a restart and replayed to the next service instance through
/// the ordinary LISTEN_FDS activation handover. Requires the unit's
/// FileDescriptorStoreMax= to be non-zero. An absent or empty NOTIFY_SOCKET
/// means no manager and reports success, the same contract as notify().
pub(crate) fn fdstore(socket: Option<&OsStr>, name: &str, fd: BorrowedFd<'_>) -> io::Result<()> {
    if !valid_name(name) {
        return Err(io::Error::other("invalid fdstore name"));
    }
    send_to_manager(
        socket,
        format!("FDSTORE=1\nFDNAME={name}").as_bytes(),
        Some(fd),
    )
}

pub(crate) fn store_transient(
    socket: Option<&OsStr>,
    name: &str,
    fd: BorrowedFd<'_>,
) -> io::Result<()> {
    if socket.is_none_or(|s| s.is_empty()) || !valid_name(name) {
        return Err(io::Error::other(
            "handover requires a manager and valid descriptor names",
        ));
    }
    send_to_manager(
        socket,
        format!("FDSTORE=1\nFDPOLL=0\nFDNAME={name}").as_bytes(),
        Some(fd),
    )
}

pub(crate) fn remove(socket: Option<&OsStr>, name: &str) -> io::Result<()> {
    if !valid_name(name) {
        return Err(io::Error::other("invalid fdstore name"));
    }
    notify(socket, &format!("FDSTOREREMOVE=1\nFDNAME={name}"))
}

/// Confirm processing, NOT acceptance. Callers must separately inspect the store.
pub(crate) fn barrier(socket: Option<&OsStr>) -> io::Result<()> {
    if socket.is_none_or(|s| s.is_empty()) {
        return Err(io::Error::other("handover requires NOTIFY_SOCKET"));
    }
    let mut raw = [0; 2];
    if unsafe { libc::pipe2(raw.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let read = unsafe { OwnedFd::from_raw_fd(raw[0]) };
    let write = unsafe { OwnedFd::from_raw_fd(raw[1]) };
    use std::os::fd::AsFd;
    send_to_manager(socket, b"BARRIER=1", Some(write.as_fd()))?;
    drop(write);
    let mut poll = libc::pollfd {
        fd: read.as_raw_fd(),
        events: libc::POLLHUP,
        revents: 0,
    };
    let result = unsafe { libc::poll(&mut poll, 1, 3000) };
    if result <= 0 || poll.revents & libc::POLLHUP == 0 {
        return Err(io::Error::other("notify barrier timed out"));
    }
    Ok(())
}

/// Control-message buffer bytes for one SCM_RIGHTS descriptor: a cmsghdr
/// plus one file descriptor, padded like the CMSG_SPACE() macro.
const fn control_space() -> usize {
    // SAFETY: CMSG_SPACE is pure size arithmetic on its argument.
    unsafe { libc::CMSG_SPACE(std::mem::size_of::<libc::c_int>() as libc::c_uint) as usize }
}

/// Send one datagram to the manager's notify socket, attaching the
/// descriptor through an SCM_RIGHTS control message when given.
fn send_to_manager(
    socket: Option<&OsStr>,
    payload: &[u8],
    fd: Option<BorrowedFd<'_>>,
) -> io::Result<()> {
    let Some(socket) = socket.filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    let (address, length) = manager_address(socket.as_bytes())?;
    let descriptor =
        unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut vectors = [libc::iovec {
        iov_base: payload.as_ptr() as *mut libc::c_void,
        iov_len: payload.len(),
    }];
    let mut control = [0usize; control_space().div_ceil(std::mem::size_of::<usize>())];
    let mut message = libc::msghdr {
        msg_name: &address as *const _ as *mut libc::c_void,
        msg_namelen: length,
        msg_iov: vectors.as_mut_ptr(),
        msg_iovlen: 1,
        msg_control: control.as_mut_ptr().cast::<libc::c_void>(),
        msg_controllen: control_space(),
        msg_flags: 0,
    };
    if let Some(fd) = fd {
        // SAFETY: CMSG_FIRSTHDR on the freshly built msghdr above returns a
        // pointer into `control`, whose CONTROL_SPACE bytes fit exactly one
        // cmsghdr with a one-descriptor SCM_RIGHTS payload.
        let header = unsafe { libc::CMSG_FIRSTHDR(&message) };
        let raw = fd.as_raw_fd();
        unsafe {
            (*header).cmsg_level = libc::SOL_SOCKET;
            (*header).cmsg_type = libc::SCM_RIGHTS;
            (*header).cmsg_len =
                libc::CMSG_LEN(std::mem::size_of::<libc::c_int>() as libc::c_uint) as usize;
            std::ptr::copy_nonoverlapping(
                &raw as *const libc::c_int as *const u8,
                libc::CMSG_DATA(header),
                std::mem::size_of::<libc::c_int>(),
            );
        }
    } else {
        message.msg_control = std::ptr::null_mut();
        message.msg_controllen = 0;
    }
    let sent = unsafe { libc::sendmsg(descriptor, &message, 0) };
    let result = if sent < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    };
    unsafe { libc::close(descriptor) };
    result
}

/// Build the datagram address for a notify socket. A leading '@' names an
/// abstract-namespace socket: the byte becomes the address's leading NUL,
/// which never touches the filesystem. Any other value is a filesystem path.
fn manager_address(socket: &[u8]) -> io::Result<(libc::sockaddr_un, libc::socklen_t)> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidInput, "invalid notify socket name");
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    let (name, offset) = if socket.first() == Some(&b'@') {
        (&socket[1..], 1)
    } else if socket.is_empty() {
        return Err(invalid());
    } else if socket.contains(&0) {
        // A filesystem path must be NUL-terminated inside sun_path.
        return Err(invalid());
    } else {
        (socket, 0)
    };
    if name.is_empty() || offset + name.len() >= address.sun_path.len() {
        return Err(invalid());
    }
    for (slot, byte) in address.sun_path[offset..].iter_mut().zip(name) {
        *slot = *byte as libc::c_char;
    }
    let length = std::mem::size_of::<libc::sa_family_t>() + offset + name.len();
    Ok((address, length as libc::socklen_t))
}

/// sd_notify(3) watchdog contract: heartbeat at most half the configured
/// timeout. Zero, unparseable, or absent values mean no watchdog.
pub(crate) fn watchdog_interval(value: Option<&str>) -> Option<Duration> {
    let microseconds: u64 = value?.trim().parse().ok()?;
    let interval = Duration::from_micros(microseconds / 2);
    (interval.as_nanos() > 0).then_some(interval)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{os::fd::AsFd, os::unix::net::UnixDatagram};

    #[test]
    fn activation_requires_local_pid_and_single_listener() {
        let pid = 42u32.to_string();
        assert_eq!(requested(None, None, 42).unwrap(), 0);
        assert_eq!(requested(None, Some("0"), 42).unwrap(), 0);
        assert_eq!(requested(Some(&pid), Some("0"), 42).unwrap(), 0);
        assert_eq!(requested(Some(&pid), Some("1"), 42).unwrap(), 1);
        assert!(
            requested(Some("7"), Some("1"), 42).unwrap() == 0,
            "a foreign pid is not this process's activation"
        );
        assert_eq!(requested(Some(&pid), Some("2"), 42).unwrap(), 2);
        assert!(requested(Some(&pid), Some("4097"), 42).is_err());
        assert!(requested(Some(&pid), Some("x"), 42).is_err());
        assert!(requested(None, Some("1"), 42).is_err());
        assert!(requested(Some("x"), Some("1"), 42).is_err());
    }

    #[test]
    fn descriptor_identity_follows_names_not_numbers_and_rejects_ambiguity() {
        assert_eq!(
            descriptor_names(3, Some("bp-g-source:listener:bp-g-manifest")).unwrap(),
            ["bp-g-source", "listener", "bp-g-manifest"]
        );
        assert_eq!(descriptor_names(1, None).unwrap(), ["listener"]);
        for names in [
            None,
            Some("listener:listener"),
            Some("listener"),
            Some("listener:bad\nname"),
            Some("listener:"),
        ] {
            assert!(descriptor_names(2, names).is_err());
        }
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

    #[test]
    fn notify_without_a_manager_is_success() {
        assert!(notify(None, "READY=1").is_ok());
        assert!(notify(Some(OsStr::new("")), "READY=1").is_ok());
    }

    #[test]
    fn notify_datagrams_reach_filesystem_managers() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("notify");
        let receiver = UnixDatagram::bind(&path).expect("bind receiver");
        notify(Some(path.as_os_str()), "READY=1\nSTATUS=listening").expect("notify");
        let mut buffer = [0u8; 128];
        let (read, _) = receiver.recv_from(&mut buffer).expect("receive");
        assert_eq!(&buffer[..read], b"READY=1\nSTATUS=listening");
    }

    #[test]
    fn notify_datagrams_reach_abstract_managers() {
        let name = format!("bus-proxy-notify-test-{}", std::process::id());
        let descriptor = bind_abstract(name.as_bytes()).expect("bind abstract receiver");
        let address = std::ffi::OsString::from(format!("@{name}"));
        notify(Some(address.as_os_str()), "WATCHDOG=1").expect("notify");
        let mut buffer = [0u8; 64];
        let read = unsafe {
            libc::recvfrom(
                descriptor,
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        let result = if read < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(read as usize)
        };
        unsafe { libc::close(descriptor) };
        assert_eq!(&buffer[..result.expect("receive")], b"WATCHDOG=1");
    }

    #[test]
    fn fdstore_without_a_manager_is_success() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        assert!(fdstore(None, "listener", listener.as_fd()).is_ok());
        assert!(fdstore(Some(OsStr::new("")), "listener", listener.as_fd()).is_ok());
    }

    #[test]
    fn fdstore_hands_the_listener_to_filesystem_managers() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("notify");
        let receiver = UnixDatagram::bind(&path).expect("bind receiver");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        fdstore(Some(path.as_os_str()), "listener", listener.as_fd()).expect("fdstore");
        let (payload, received) = receive_with_fd(receiver.as_raw_fd()).expect("receive");
        assert_eq!(payload.as_slice(), b"FDSTORE=1\nFDNAME=listener");
        let received = received.expect("one descriptor in SCM_RIGHTS");
        assert_eq!(
            TcpListener::from(received).local_addr().unwrap(),
            listener.local_addr().unwrap(),
            "the stored descriptor names the same listening socket",
        );
    }

    #[test]
    fn fdstore_hands_the_listener_to_abstract_managers() {
        let name = format!("bus-proxy-fdstore-test-{}", std::process::id());
        let descriptor = bind_abstract(name.as_bytes()).expect("bind abstract receiver");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let address = std::ffi::OsString::from(format!("@{name}"));
        fdstore(Some(address.as_os_str()), "listener", listener.as_fd()).expect("fdstore");
        let (payload, received) = receive_with_fd(descriptor).expect("receive");
        unsafe { libc::close(descriptor) };
        assert_eq!(payload.as_slice(), b"FDSTORE=1\nFDNAME=listener");
        let received = received.expect("one descriptor in SCM_RIGHTS");
        assert_eq!(
            TcpListener::from(received).local_addr().unwrap(),
            listener.local_addr().unwrap(),
            "the stored descriptor names the same listening socket",
        );
    }

    /// Receive one datagram plus any SCM_RIGHTS-attached descriptor from a
    /// unix datagram socket, checking the control message's level and type.
    fn receive_with_fd(socket: libc::c_int) -> io::Result<(Vec<u8>, Option<OwnedFd>)> {
        let mut buffer = [0u8; 128];
        let mut control = [0u8; control_space()];
        let mut vector = libc::iovec {
            iov_base: buffer.as_mut_ptr().cast::<libc::c_void>(),
            iov_len: buffer.len(),
        };
        let mut message = libc::msghdr {
            msg_name: std::ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: &mut vector,
            msg_iovlen: 1,
            msg_control: control.as_mut_ptr().cast::<libc::c_void>(),
            msg_controllen: control.len(),
            msg_flags: 0,
        };
        // SAFETY: MSG_CMSG_CLOEXEC makes any received descriptor
        // close-on-exec, so owning it below cannot leak into children.
        let read = unsafe { libc::recvmsg(socket, &mut message, libc::MSG_CMSG_CLOEXEC) };
        if read < 0 {
            return Err(io::Error::last_os_error());
        }
        let mut received: Option<OwnedFd> = None;
        // SAFETY: CMSG_FIRSTHDR/CMSG_NXTHDR walk the control buffer the
        // kernel just validated into `message`; accepted headers lie inside
        // it, and a descriptor payload is exactly one c_int wide.
        let mut header = unsafe { libc::CMSG_FIRSTHDR(&message) };
        while !header.is_null() {
            unsafe {
                if (*header).cmsg_level == libc::SOL_SOCKET
                    && (*header).cmsg_type == libc::SCM_RIGHTS
                {
                    let payload = (*header).cmsg_len - libc::CMSG_LEN(0) as usize;
                    assert_eq!(payload, std::mem::size_of::<libc::c_int>());
                    let mut raw: libc::c_int = 0;
                    std::ptr::copy_nonoverlapping(
                        libc::CMSG_DATA(header).cast::<libc::c_int>(),
                        &mut raw,
                        1,
                    );
                    // SAFETY: the kernel duplicated the sender's descriptor
                    // into this process for us to own.
                    assert!(
                        received.replace(OwnedFd::from_raw_fd(raw)).is_none(),
                        "one SCM_RIGHTS message"
                    );
                }
            }
            header = unsafe { libc::CMSG_NXTHDR(&message, header) };
        }
        Ok((buffer[..read as usize].to_vec(), received))
    }

    fn bind_abstract(name: &[u8]) -> io::Result<libc::c_int> {
        let descriptor =
            unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        address.sun_family = libc::AF_UNIX as libc::sa_family_t;
        for (slot, byte) in address.sun_path[1..].iter_mut().zip(name) {
            *slot = *byte as libc::c_char;
        }
        let length = std::mem::size_of::<libc::sa_family_t>() + 1 + name.len();
        let bound = unsafe {
            libc::bind(
                descriptor,
                &address as *const _ as *const libc::sockaddr,
                length as libc::socklen_t,
            )
        };
        if bound < 0 {
            let error = io::Error::last_os_error();
            unsafe { libc::close(descriptor) };
            return Err(error);
        }
        Ok(descriptor)
    }

    #[test]
    fn watchdog_intervals_half_the_configured_timeout() {
        assert_eq!(watchdog_interval(None), None);
        assert_eq!(watchdog_interval(Some("bogus")), None);
        assert_eq!(watchdog_interval(Some("0")), None);
        assert_eq!(
            watchdog_interval(Some("1")),
            None,
            "half a microsecond rounds away"
        );
        assert_eq!(
            watchdog_interval(Some("30000000")),
            Some(Duration::from_millis(15000))
        );
    }
}
