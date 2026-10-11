use bus_proxy_gateway::{Config, handover, serve_with_handover};
use bus_proxy_protocol::{SseDecoder, event_frame};
use bus_proxy_router::{Limits, Router};
use bytes::Bytes;
use http::Request;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::CancellationToken;

struct Feed {
    body: Incoming,
    decoder: SseDecoder,
    frames: VecDeque<serde_json::Value>,
}
impl Feed {
    async fn open(address: std::net::SocketAddr, controlled: bool) -> Self {
        let socket = TcpStream::connect(address).await.unwrap();
        let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
            .await
            .unwrap();
        tokio::spawn(connection);
        let response = client
            .send_request(
                Request::builder()
                    .uri(if controlled {
                        "/api/experimental/event"
                    } else {
                        "/api/event"
                    })
                    .header("host", address.to_string())
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        Self {
            body: response.into_body(),
            decoder: SseDecoder::new(16 * 1024 * 1024),
            frames: VecDeque::new(),
        }
    }
    async fn next(&mut self) -> serde_json::Value {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(frame) = self.frames.pop_front() {
                    return frame;
                }
                let frame = self
                    .body
                    .frame()
                    .await
                    .expect("same established response remains open")
                    .expect("HTTP framing survives");
                if let Ok(data) = frame.into_data() {
                    for frame in self.decoder.push(&data).unwrap() {
                        self.frames
                            .push_back(serde_json::from_slice(&frame).unwrap());
                    }
                }
            }
        })
        .await
        .unwrap()
    }
}

async fn put(address: std::net::SocketAddr, id: &str, sessions: &[&str]) -> u16 {
    let socket = TcpStream::connect(address).await.unwrap();
    let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
        .await
        .unwrap();
    tokio::spawn(connection);
    let data =
        serde_json::json!({"profile":"session-streaming", "locations":[], "sessions":sessions})
            .to_string();
    let response = client
        .send_request(
            Request::builder()
                .method("PUT")
                .uri(format!(
                    "/api/experimental/event/subscriptions/{id}/interests"
                ))
                .header("host", address.to_string())
                .body(Full::new(Bytes::from(data)))
                .unwrap(),
        )
        .await
        .unwrap();
    response.status().as_u16()
}

fn chunk(bytes: &[u8]) -> Vec<u8> {
    let mut data = format!("{:x};ext=yes\r\n", bytes.len()).into_bytes();
    data.extend_from_slice(bytes);
    data.extend_from_slice(b"\r\n");
    data
}
fn event(session: &str, id: &str) -> Bytes {
    event_frame(
        serde_json::json!({"type":"session.text.delta", "id":id, "scope":{"sessionID":session}})
            .to_string()
            .as_bytes(),
    )
}

#[tokio::test]
async fn established_responses_and_live_only_source_survive_two_successors_and_abort() {
    let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let config = Config {
        upstream: format!("http://{}", origin.local_addr().unwrap()),
        ..Config::default()
    };
    let mut stop = CancellationToken::new();
    let (mut router, mut routing) = Router::spawn(Limits::default(), stop.clone()).unwrap();
    let (mut control, receiver) = handover::channel();
    let mut gateway = tokio::spawn(serve_with_handover(
        listener,
        config.clone(),
        router.clone(),
        stop.clone(),
        receiver,
        None,
    ));
    let (mut source, _) = origin.accept().await.unwrap();
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        request.push(source.read_u8().await.unwrap());
    }
    source.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nOpenCode-Event-Scope: 1\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
    source
        .write_all(&chunk(
            b"data: {\"type\":\"server.connected\",\"id\":\"evt_original\"}\r\n\r\n",
        ))
        .await
        .unwrap();
    while router.snapshot().await.unwrap().epoch.is_none() {
        tokio::task::yield_now().await;
    }
    let mut active = Feed::open(address, true).await;
    let id = active.next().await["data"]["subscriptionID"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(put(address, &id, &["ses_a"]).await, 204);
    assert_eq!(active.next().await["type"], "server.connected");
    let mut pending = Feed::open(address, true).await;
    let pending_id = pending.next().await["data"]["subscriptionID"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut legacy = Feed::open(address, false).await;
    assert_eq!(legacy.next().await["type"], "server.connected");
    source
        .write_all(&chunk(&event("ses_a", "before")))
        .await
        .unwrap();
    assert_eq!(active.next().await["id"], "before");
    assert_eq!(legacy.next().await["id"], "before");

    // Failed preparation resumes the same owners and never retires the epoch.
    let frozen = control.freeze().await.unwrap();
    control.resume(frozen).await.unwrap();
    source
        .write_all(&chunk(&event("ses_a", "after-abort")))
        .await
        .unwrap();
    assert_eq!(active.next().await["id"], "after-abort");
    assert_eq!(legacy.next().await["id"], "after-abort");

    // Explicit abandonment and a canceled receive retain owner custody in the
    // gateway, then automatically resume without dropping parked transports.
    drop(control.freeze().await.unwrap());
    source
        .write_all(&chunk(&event("ses_a", "abandoned-lease")))
        .await
        .unwrap();
    assert_eq!(active.next().await["id"], "abandoned-lease");
    assert_eq!(legacy.next().await["id"], "abandoned-lease");
    let mut canceled = Box::pin(control.freeze());
    assert!(futures_util::poll!(canceled.as_mut()).is_pending());
    drop(canceled);
    source
        .write_all(&chunk(&event("ses_a", "canceled-request")))
        .await
        .unwrap();
    assert_eq!(active.next().await["id"], "canceled-request");
    assert_eq!(legacy.next().await["id"], "canceled-request");

    for round in 0..2 {
        if round == 0 {
            let before = router.snapshot().await.unwrap().counters.publications;
            let large = event_frame(serde_json::json!({"type":"session.text.delta", "id":"partial-write", "scope":{"sessionID":"ses_a"}, "payload":"x".repeat(6 * 1024 * 1024)}).to_string().as_bytes());
            source.write_all(&chunk(&large)).await.unwrap();
            while router.snapshot().await.unwrap().counters.publications == before {
                tokio::task::yield_now().await;
            }
        }
        // First cut: in-body SSE line and remaining HTTP chunk count. Second:
        // halfway through the chunk-size header. The origin socket never reopens.
        let wire = chunk(&event("ses_a", &format!("across-{round}")));
        let split = if round == 0 { wire.len() / 2 } else { 1 };
        source.write_all(&wire[..split]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        let frozen = control.freeze().await.unwrap();
        let encoded = handover::encode(frozen.image()).unwrap();
        let image = handover::decode(&encoded).unwrap();
        let mut corrupted = encoded.clone();
        *corrupted.last_mut().unwrap() ^= 1;
        assert!(handover::decode(&corrupted).is_err());
        assert!(handover::decode(&encoded[..encoded.len() - 1]).is_err());
        for case in 0..4 {
            let mut rejected = image.clone();
            let mut fds: HashMap<_, _> = frozen.descriptors().unwrap().into_iter().collect();
            fds.remove("listener");
            match case {
                0 => rejected.version += 1,
                1 => {
                    fds.remove(&rejected.source.descriptor);
                }
                2 => rejected.clients[0].offset = u64::MAX,
                _ => rejected.clients[0].subscription = "evsub_not_owned".into(),
            }
            assert!(
                rejected
                    .import(&config, Limits::default(), fds, CancellationToken::new())
                    .is_err()
            );
        }
        assert_eq!(image.router.epoch, "evt_original");
        assert_eq!(image.clients.len(), 3);
        if round == 0 {
            assert!(
                image
                    .clients
                    .iter()
                    .any(|c| c.offset > 0 && c.offset < c.output.len() as u64),
                "stalled client has a dequeued, partly written HTTP chunk in userspace"
            );
        }
        if round == 0 {
            assert!(
                !serde_json::to_value(&image.source.decoder).unwrap()["line"]
                    .as_str()
                    .unwrap()
                    .is_empty()
            );
        } else {
            assert_eq!(image.source.buffer, wire[..1]);
        }
        let mut fds: HashMap<_, _> = frozen.descriptors().unwrap().into_iter().rev().collect();
        // Reordered FD numbers/iteration never define transport identity.
        let inherited_listener = std::net::TcpListener::from(fds.remove("listener").unwrap());
        inherited_listener.set_nonblocking(true).unwrap();
        stop.cancel();
        gateway.await.unwrap().unwrap();
        routing.await.unwrap();
        drop(frozen);
        stop = CancellationToken::new();
        let (new_router, new_routing, imported) = image
            .import(&config, Limits::default(), fds, stop.clone())
            .unwrap();
        router = new_router;
        routing = new_routing;
        let (new_control, receiver) = handover::channel();
        control = new_control;
        gateway = tokio::spawn(serve_with_handover(
            TcpListener::from_std(inherited_listener).unwrap(),
            config.clone(),
            router.clone(),
            stop.clone(),
            receiver,
            Some(imported),
        ));
        source.write_all(&wire[split..]).await.unwrap();
        if round == 0 {
            let frame = active.next().await;
            assert_eq!(frame["id"], "partial-write");
            assert_eq!(
                frame["payload"].as_str().unwrap(),
                "x".repeat(6 * 1024 * 1024)
            );
            assert_eq!(legacy.next().await["id"], "partial-write");
        }
        assert_eq!(active.next().await["id"], format!("across-{round}"));
        assert_eq!(legacy.next().await["id"], format!("across-{round}"));
        assert!(
            tokio::time::timeout(Duration::from_millis(40), origin.accept())
                .await
                .is_err(),
            "no new source connection, hence no live-only observation gap"
        );
    }
    let snapshot = router.snapshot().await.unwrap();
    assert_eq!(snapshot.counters.epochs_started, 1);
    assert_eq!(snapshot.counters.epochs_ended, 0);
    assert_eq!(
        snapshot
            .subscriptions
            .iter()
            .find(|s| s.id == id)
            .unwrap()
            .sessions,
        ["ses_a"]
    );
    assert!(
        !snapshot
            .subscriptions
            .iter()
            .find(|s| s.id == pending_id)
            .unwrap()
            .active
    );
    assert_eq!(put(address, &pending_id, &["ses_b"]).await, 204);
    assert_eq!(pending.next().await["type"], "server.connected");
    source
        .write_all(&chunk(&event("ses_b", "other")))
        .await
        .unwrap();
    source
        .write_all(&chunk(&event("ses_a", "selected")))
        .await
        .unwrap();
    assert_eq!(pending.next().await["id"], "other");
    assert_eq!(active.next().await["id"], "selected");
    assert_eq!(legacy.next().await["id"], "other");
    assert_eq!(legacy.next().await["id"], "selected");
    stop.cancel();
    gateway.await.unwrap().unwrap();
    routing.await.unwrap();
}

#[tokio::test]
async fn https_and_offline_preparation_are_explicitly_rejected() {
    for upstream in ["https://127.0.0.1:9", "http://127.0.0.1:9"] {
        let stop = CancellationToken::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (router, routing) = Router::spawn(Limits::default(), stop.clone()).unwrap();
        let (control, receiver) = handover::channel();
        let gateway = tokio::spawn(serve_with_handover(
            listener,
            Config {
                upstream: upstream.into(),
                ..Config::default()
            },
            router,
            stop.clone(),
            receiver,
            None,
        ));
        assert!(
            control
                .freeze()
                .await
                .err()
                .unwrap()
                .contains("unsupported")
        );
        stop.cancel();
        gateway.await.unwrap().unwrap();
        routing.await.unwrap();
    }
}
