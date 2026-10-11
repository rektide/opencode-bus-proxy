//! Actual process replacement, using only uniquely named disposable user units.
//! Run explicitly: cargo test -p opencode-bus-proxy --test handover_systemd -- --ignored
use bus_proxy_protocol::{SseDecoder, event_frame};
use bytes::Bytes;
use http::Request;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use std::{collections::VecDeque, net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::Command,
};

struct Unit(String);
impl Drop for Unit {
    fn drop(&mut self) {
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "stop", &self.0])
            .output();
    }
}
async fn command(args: &[&str]) -> String {
    let output = Command::new("systemctl")
        .arg("--user")
        .args(args)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}
async fn property(unit: &Unit, name: &str) -> String {
    command(&["show", "--value", "--property", name, &unit.0]).await
}
async fn changed_pid(unit: &Unit, old: &str) -> String {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let pid = property(unit, "MainPID").await;
            if pid != "0" && pid != old {
                return pid;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}

struct Feed {
    body: Incoming,
    decoder: SseDecoder,
    pending: VecDeque<serde_json::Value>,
}
impl Feed {
    async fn open(address: SocketAddr) -> Self {
        let socket = TcpStream::connect(address).await.unwrap();
        let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
            .await
            .unwrap();
        tokio::spawn(connection);
        let response = client
            .send_request(
                Request::builder()
                    .uri("/api/experimental/event")
                    .header("host", address.to_string())
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        Self {
            body: response.into_body(),
            decoder: SseDecoder::new(1024 * 1024),
            pending: VecDeque::new(),
        }
    }
    async fn next(&mut self) -> serde_json::Value {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(frame) = self.pending.pop_front() {
                    return frame;
                }
                let frame = self
                    .body
                    .frame()
                    .await
                    .expect("same response did not end")
                    .expect("same response framing survived");
                if let Ok(data) = frame.into_data() {
                    for data in self.decoder.push(&data).unwrap() {
                        self.pending
                            .push_back(serde_json::from_slice(&data).unwrap());
                    }
                }
            }
        })
        .await
        .unwrap()
    }
}
async fn interest(address: SocketAddr, id: &str) -> u16 {
    let socket = TcpStream::connect(address).await.unwrap();
    let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
        .await
        .unwrap();
    tokio::spawn(connection);
    client.send_request(Request::builder().method("PUT").uri(format!("/api/experimental/event/subscriptions/{id}/interests"))
        .header("host", address.to_string()).body(Full::new(Bytes::from_static(b"{\"locations\":[],\"sessions\":[\"ses_exact\"],\"profile\":\"session-streaming\"}"))).unwrap()).await.unwrap().status().as_u16()
}
fn chunk(bytes: &[u8]) -> Vec<u8> {
    let mut wire = format!("{:x}\r\n", bytes.len()).into_bytes();
    wire.extend_from_slice(bytes);
    wire.extend_from_slice(b"\r\n");
    wire
}
fn event(id: &str) -> Bytes {
    event_frame(
        serde_json::json!({"type":"session.text.delta","id":id,"scope":{"sessionID":"ses_exact"}})
            .to_string()
            .as_bytes(),
    )
}
async fn source(origin: &TcpListener) -> TcpStream {
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(5), origin.accept())
        .await
        .unwrap()
        .unwrap();
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        bytes.push(socket.read_u8().await.unwrap());
    }
    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nOpenCode-Event-Scope: 1\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
    socket
        .write_all(&chunk(
            b"data: {\"type\":\"server.connected\",\"id\":\"evt_isolated\"}\n\n",
        ))
        .await
        .unwrap();
    socket
}

async fn start(label: &str, capacity: usize) -> (Unit, TcpListener, SocketAddr) {
    let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let unit = Unit(format!(
        "obp-handover-test-{label}-{}.service",
        std::process::id()
    ));
    let output = Command::new("systemd-run")
        .args([
            "--user",
            "--collect",
            "--unit",
            &unit.0,
            "--property=Type=notify",
            "--property=Restart=on-failure",
            "--property=RestartSec=100ms",
            "--property=NotifyAccess=main",
            &format!("--property=FileDescriptorStoreMax={capacity}"),
            env!("CARGO_BIN_EXE_opencode-bus-proxy"),
            "--allow-unauthenticated",
            "--upstream",
            &format!("http://{}", origin.local_addr().unwrap()),
            "--listen",
            &address.to_string(),
            "--handover-unit",
            &unit.0,
            "--handover-user",
            "--handover-successor",
            env!("CARGO_BIN_EXE_opencode-bus-proxy"),
        ])
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    (unit, origin, address)
}

#[tokio::test]
#[ignore = "requires systemd user manager and busctl; creates only obp-handover-test-* unit"]
async fn real_systemd_replacement_preserves_source_clients_and_invalidates_image_before_crash() {
    let (unit, origin, address) = start("survival", 32).await;
    let mut upstream = source(&origin).await;
    tokio::time::sleep(Duration::from_millis(40)).await;
    let mut feed = Feed::open(address).await;
    let id = feed.next().await["data"]["subscriptionID"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(interest(address, &id).await, 204);
    assert_eq!(feed.next().await["type"], "server.connected");
    upstream.write_all(&chunk(&event("before"))).await.unwrap();
    assert_eq!(feed.next().await["id"], "before");
    for round in 0..2 {
        let old = property(&unit, "MainPID").await;
        assert_eq!(property(&unit, "NFileDescriptorStore").await, "1");
        let wire = chunk(&event(&format!("after-{round}")));
        upstream.write_all(&wire[..wire.len() / 2]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        command(&["kill", "--signal=SIGUSR2", "--kill-whom=main", &unit.0]).await;
        let new = changed_pid(&unit, &old).await;
        assert_ne!(old, new);
        upstream.write_all(&wire[wire.len() / 2..]).await.unwrap();
        assert_eq!(feed.next().await["id"], format!("after-{round}"));
        assert_eq!(
            interest(address, &id).await,
            204,
            "same controlled ID and interest owner survived"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), origin.accept())
                .await
                .is_err(),
            "no source reconnection or live-only gap"
        );
        assert_eq!(
            property(&unit, "NFileDescriptorStore").await,
            "1",
            "manifest and client/source FDs consumed before resumed I/O"
        );
    }
    // A later unplanned crash must NOT resurrect the consumed image or its IDs.
    let old = property(&unit, "MainPID").await;
    command(&["kill", "--signal=SIGKILL", "--kill-whom=main", &unit.0]).await;
    changed_pid(&unit, &old).await;
    let _fresh_source = source(&origin).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(interest(address, &id).await, 404);
    let frame = tokio::time::timeout(Duration::from_secs(3), feed.body.frame())
        .await
        .unwrap();
    assert!(
        frame.is_none() || frame.unwrap().is_err(),
        "old response did not replay stale output"
    );
}

#[tokio::test]
#[ignore = "requires systemd user manager and busctl; creates only obp-handover-test-* unit"]
async fn insufficient_fdstore_capacity_rejects_reset_without_client_bounce() {
    let (unit, origin, address) = start("reject", 1).await;
    let mut upstream = source(&origin).await;
    tokio::time::sleep(Duration::from_millis(40)).await;
    let mut feed = Feed::open(address).await;
    let id = feed.next().await["data"]["subscriptionID"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(interest(address, &id).await, 204);
    feed.next().await;
    let old = property(&unit, "MainPID").await;
    command(&["kill", "--signal=SIGUSR2", "--kill-whom=main", &unit.0]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(property(&unit, "MainPID").await, old);
    upstream
        .write_all(&chunk(&event("still-owned")))
        .await
        .unwrap();
    assert_eq!(feed.next().await["id"], "still-owned");
}
