//! Exercise descriptor/environment handover through the real host startup.
#![cfg(unix)]

use std::{
    io,
    net::TcpListener,
    os::{
        fd::AsRawFd,
        unix::{net::UnixDatagram, process::CommandExt},
    },
    process::{Child, Command, Stdio},
    time::Duration,
};

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn activated(listener: &TcpListener, socket: &std::path::Path) -> Command {
    let mut command = Command::new("/bin/sh");
    // The shell sets the child's own PID, then exec preserves that PID. No
    // test thread mutates process-global environment to simulate activation.
    command
        .arg("-c")
        .arg("LISTEN_PID=$$ exec \"$@\"")
        .arg("activation-test")
        .arg(env!("CARGO_BIN_EXE_opencode-bus-proxy"))
        .arg("--allow-unauthenticated")
        .env("LISTEN_FDS", "1")
        .env("NOTIFY_SOCKET", socket)
        .env("BUS_PROXY_UPSTREAM", "http://127.0.0.1:1")
        .env_remove("OPENCODE_SERVER_PASSWORD")
        .env_remove("BUS_PROXY_LISTEN")
        .env_remove("BUS_PROXY_PASSWORD_FILE")
        .env_remove("BUS_PROXY_CREDENTIAL")
        .env_remove("BUS_PROXY_SERVICE_FILE")
        .env_remove("BUS_PROXY_SERVICE_URL")
        .env_remove("BUS_PROXY_SERVICE_WATCH_HELPER")
        .env_remove("BUS_PROXY_SERVICE_WATCH_SOURCE")
        .env_remove("WATCHDOG_USEC")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let descriptor = listener.as_raw_fd();
    // SAFETY: child-only setup uses async-signal-safe descriptor operations;
    // the parent listener remains alive through spawn and owns its original fd.
    unsafe {
        command.pre_exec(move || {
            if libc::dup2(descriptor, 3) < 0 || libc::fcntl(3, libc::F_SETFD, 0) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    command
}

#[test]
fn activated_host_reports_actual_address_without_reoffering_fdstore() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("notify");
    let receiver = UnixDatagram::bind(&socket).unwrap();
    receiver
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let mut process = Process(activated(&listener, &socket).spawn().unwrap());
    let mut buffer = [0; 256];
    let read = receiver.recv(&mut buffer).unwrap();
    assert_eq!(
        &buffer[..read],
        format!("READY=1\nSTATUS=event gateway on {address}").as_bytes(),
        "the first manager message must be readiness, not another FDSTORE"
    );
    // Readiness follows task launch, not signal-handler installation. Give the
    // host its next scheduling turn before exercising normal shutdown.
    std::thread::sleep(Duration::from_millis(50));
    // SAFETY: the child is still owned, and SIGTERM exercises normal shutdown.
    assert_eq!(
        unsafe { libc::kill(process.0.id() as libc::pid_t, libc::SIGTERM) },
        0
    );
    let read = receiver.recv(&mut buffer).unwrap();
    assert_eq!(&buffer[..read], b"STOPPING=1");
    assert!(process.0.wait().unwrap().success());
}

#[test]
fn activated_host_rejects_explicit_listen_even_when_address_matches() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("notify");
    let receiver = UnixDatagram::bind(&socket).unwrap();
    receiver.set_nonblocking(true).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let output = activated(&listener, &socket)
        .arg("--listen")
        .arg(listener.local_addr().unwrap().to_string())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("conflicts with systemd socket activation")
    );
    assert_eq!(
        receiver.recv(&mut [0; 256]).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

#[test]
fn listen_conflict_precedes_invalid_handover_descriptor() {
    let directory = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut command = activated(&listener, &directory.path().join("notify"));
    command.arg("--listen").arg("127.0.0.1:0");
    // SAFETY: child-only async-signal-safe close deliberately invalidates the
    // handover after the preceding pre_exec hook installed it.
    unsafe {
        command.pre_exec(|| {
            libc::close(3);
            Ok(())
        });
    }
    let output = command.output().unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("conflicts with systemd socket activation")
    );
}
