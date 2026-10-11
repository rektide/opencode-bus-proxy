//! Release-mode actor + queue + concurrent-drainer benchmark, not a socket test.
//! cargo run --release -p bus-proxy-router --example routing_bench -- 128 20000 512

use std::time::Instant;

use bus_proxy_protocol::{Interest, Route, event_frame};
use bus_proxy_router::{Limits, Mode, Router};
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug)]
enum Workload {
    Selective,
    Broad,
    NoFollowers,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let argument = |index, default| {
        args.get(index)
            .map(|value: &String| value.parse::<usize>().expect("positive integer argument"))
            .unwrap_or(default)
    };
    let clients = argument(0, 128);
    let events = argument(1, 20_000);
    let payload_bytes = argument(2, 512);
    assert!((1..=4096).contains(&clients));
    assert!((1..=10_000_000).contains(&events));
    assert!((128..=1024 * 1024).contains(&payload_bytes));

    let raw = serde_json::value::RawValue::from_string(
        serde_json::json!({
            "id": "evt_benchmark", "type": "session.text.delta",
            "data": { "delta": "x".repeat(payload_bytes) }
        })
        .to_string(),
    )
    .unwrap();
    let frame = event_frame(raw.get().as_bytes());
    println!(
        "clients={clients} events={events} frame_bytes={} tokio_workers=4",
        frame.len()
    );
    for workload in [Workload::Selective, Workload::Broad, Workload::NoFollowers] {
        run(workload, clients, events, frame.clone()).await;
    }
}

async fn run(workload: Workload, clients: usize, events: usize, frame: Bytes) {
    let shutdown = CancellationToken::new();
    let (router, actor) = Router::spawn(
        Limits {
            queue_frames: 4096,
            queue_bytes: (frame.len() * 4096).min(64 * 1024 * 1024),
            max_subscribers: clients,
            ..Limits::default()
        },
        shutdown.clone(),
    )
    .unwrap();
    router.begin_epoch("bench".into()).await.unwrap();
    let marker = Bytes::from_static(b": bench-finish\n\n");
    let mut readers = Vec::new();
    for client in 0..clients {
        let mut subscription = router.subscribe(Mode::Controlled).await.unwrap();
        subscription.recv().await.expect("ready");
        router
            .replace(
                subscription.id().into(),
                Interest {
                    sessions: vec![format!("session-{client}")],
                },
            )
            .await
            .unwrap();
        subscription.recv().await.expect("connected");
        let expected = match workload {
            Workload::Selective => events / clients + usize::from(client < events % clients),
            Workload::Broad => events,
            Workload::NoFollowers => 0,
        };
        let payload = frame.clone();
        let marker = marker.clone();
        readers.push(tokio::spawn(async move {
            let mut received = 0usize;
            loop {
                let incoming = subscription
                    .recv()
                    .await
                    .expect("unexpected overflow/cancellation");
                if incoming == marker {
                    break;
                }
                assert_eq!(incoming, payload, "payload changed");
                received += 1;
            }
            assert_eq!(
                received, expected,
                "missing or duplicate deliveries for client {client}"
            );
            received
        }));
    }
    let started = Instant::now();
    for event in 0..events {
        let route = match workload {
            Workload::Selective => Route::Session {
                session_id: Some(format!("session-{}", event % clients)),
            },
            Workload::Broad => Route::Broadcast,
            Workload::NoFollowers => Route::Session {
                session_id: Some("unobserved".into()),
            },
        };
        router
            .publish("bench".into(), route, frame.clone())
            .await
            .unwrap();
    }
    // FIFO sentinel drains every client's backlog without end_epoch discarding
    // its tail. Keep no-follower subscribers alive until this point too.
    router
        .publish("bench".into(), Route::Broadcast, marker.clone())
        .await
        .unwrap();
    let mut received = 0usize;
    for reader in readers {
        received += reader.await.unwrap();
    }
    let elapsed = started.elapsed();
    let snapshot = router.snapshot().await.unwrap();
    let expected = match workload {
        Workload::Selective => events,
        Workload::Broad => events * clients,
        Workload::NoFollowers => 0,
    };
    assert_eq!(received, expected);
    assert_eq!(snapshot.counters.publications, events as u64 + 1);
    assert_eq!(snapshot.counters.admissions, (expected + clients) as u64);
    assert_eq!(
        snapshot.counters.admitted_bytes,
        expected as u64 * frame.len() as u64 + clients as u64 * marker.len() as u64
    );
    assert_eq!(
        snapshot.counters.no_recipient_publications,
        if matches!(workload, Workload::NoFollowers) {
            events as u64
        } else {
            0
        }
    );
    assert_eq!(snapshot.counters.queue_overflows, 0);
    assert_eq!(snapshot.gauges.subscriptions, 0);
    println!(
        "{workload:?}: {:.3}s events/s={:.0} deliveries/s={:.0} validated_deliveries={received} overflows=0",
        elapsed.as_secs_f64(),
        events as f64 / elapsed.as_secs_f64(),
        received as f64 / elapsed.as_secs_f64()
    );
    shutdown.cancel();
    actor.await.unwrap();
}
