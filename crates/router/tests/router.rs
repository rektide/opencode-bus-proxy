use bus_proxy_protocol::{Interest, Route, ready_frame};
use bus_proxy_router::{Limits, Mode, Router, RouterError, Subscription};
use bytes::Bytes;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

struct Fixture {
    router: Router,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new(limits: Limits) -> Self {
        let shutdown = CancellationToken::new();
        let (router, task) = Router::spawn(limits, shutdown.clone()).unwrap();
        router.begin_epoch("one".into()).await.unwrap();
        Self {
            router,
            shutdown,
            task,
        }
    }

    async fn done(self) {
        self.shutdown.cancel();
        timeout(Duration::from_secs(2), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}

async fn recv(subscription: &mut Subscription) -> Option<Bytes> {
    timeout(Duration::from_secs(2), subscription.recv())
        .await
        .unwrap()
}

fn interest(sessions: &[&str]) -> Interest {
    Interest {
        sessions: sessions.iter().map(|value| (*value).into()).collect(),
    }
}

fn session(value: Option<&str>) -> Route {
    Route::Session {
        session_id: value.map(str::to_owned),
    }
}

fn assert_connected(frame: Bytes) {
    let data = frame
        .strip_prefix(b"data: ")
        .unwrap()
        .strip_suffix(b"\n\n")
        .unwrap();
    let event: serde_json::Value = serde_json::from_slice(data).unwrap();
    assert_eq!(event["type"], "server.connected");
    assert!(event["id"].as_str().unwrap().starts_with("evt_"));
    assert_eq!(event["data"], serde_json::json!({}));
}

async fn activate(router: &Router, sessions: &[&str]) -> Subscription {
    let mut subscription = router.subscribe(Mode::Controlled).await.unwrap();
    assert_eq!(
        recv(&mut subscription).await.unwrap(),
        ready_frame(subscription.id())
    );
    router
        .replace(subscription.id().into(), interest(sessions))
        .await
        .unwrap();
    assert_connected(recv(&mut subscription).await.unwrap());
    subscription
}

#[tokio::test]
async fn readiness_activation_and_complete_replacement_cut() {
    let fixture = Fixture::new(Limits::default()).await;
    let router = &fixture.router;
    let mut subscription = router.subscribe(Mode::Controlled).await.unwrap();
    assert_eq!(
        recv(&mut subscription).await.unwrap(),
        ready_frame(subscription.id())
    );
    router
        .publish(
            "one".into(),
            Route::Broadcast,
            Bytes::from_static(b"pending-skip"),
        )
        .await
        .unwrap();
    assert_eq!(router.snapshot().await.unwrap().counters.admissions, 0);
    router
        .replace(subscription.id().into(), interest(&["a", "a"]))
        .await
        .unwrap();
    router
        .publish(
            "one".into(),
            session(Some("a")),
            Bytes::from_static(b"old-buffered"),
        )
        .await
        .unwrap();
    router
        .replace(subscription.id().into(), interest(&["b"]))
        .await
        .unwrap();
    router
        .publish(
            "one".into(),
            session(Some("a")),
            Bytes::from_static(b"old-skip"),
        )
        .await
        .unwrap();
    router
        .publish("one".into(), session(Some("b")), Bytes::from_static(b"new"))
        .await
        .unwrap();
    assert_connected(recv(&mut subscription).await.unwrap());
    assert_eq!(recv(&mut subscription).await.unwrap(), "old-buffered");
    assert_eq!(recv(&mut subscription).await.unwrap(), "new");
    let snapshot = router.snapshot().await.unwrap();
    assert_eq!(snapshot.counters.handshake_frames, 2);
    assert_eq!(snapshot.counters.admissions, 2);
    assert_eq!(snapshot.subscriptions[0].sessions, ["b"]);
    assert_eq!(snapshot.gauges.memberships, 1);
    assert_eq!(snapshot.gauges.backlog_frames, 0);
    fixture.done().await;
}

#[tokio::test]
async fn legacy_gets_broad_and_every_session_controlled_uses_index() {
    let fixture = Fixture::new(Limits::default()).await;
    let router = &fixture.router;
    let mut legacy = router.subscribe(Mode::Legacy).await.unwrap();
    assert_connected(recv(&mut legacy).await.unwrap());
    let mut focused = activate(router, &["a"]).await;
    let mut empty = activate(router, &[]).await;
    for (route, data) in [
        (session(Some("a")), "a"),
        (session(Some("b")), "b"),
        (session(None), "unattributed"),
        (Route::Broadcast, "broad"),
    ] {
        router
            .publish("one".into(), route, Bytes::from(data))
            .await
            .unwrap();
    }
    for expected in ["a", "b", "unattributed", "broad"] {
        assert_eq!(recv(&mut legacy).await.unwrap(), expected);
    }
    assert_eq!(recv(&mut focused).await.unwrap(), "a");
    assert_eq!(recv(&mut focused).await.unwrap(), "broad");
    assert_eq!(recv(&mut empty).await.unwrap(), "broad");
    let snapshot = router.snapshot().await.unwrap();
    assert_eq!(snapshot.counters.admissions, 7);
    assert_eq!(snapshot.gauges.legacy, 1);
    assert_eq!(snapshot.gauges.controlled, 2);
    assert_eq!(snapshot.gauges.backlog_bytes, 0);
    assert_eq!(
        router.replace(legacy.id().into(), interest(&[])).await,
        Err(RouterError::NotFound)
    );
    fixture.done().await;
}

#[tokio::test]
async fn count_overflow_discards_backlog_without_harming_fast_neighbor() {
    let fixture = Fixture::new(Limits {
        queue_frames: 2,
        ..Limits::default()
    })
    .await;
    let router = &fixture.router;
    let mut slow = activate(router, &["a"]).await;
    let mut fast = activate(router, &["a"]).await;
    let slow_id = slow.id().to_owned();
    for expected in ["1", "2", "3"] {
        router
            .publish("one".into(), session(Some("a")), Bytes::from(expected))
            .await
            .unwrap();
        assert_eq!(recv(&mut fast).await.unwrap(), expected);
    }
    assert!(recv(&mut slow).await.is_none());
    assert_eq!(
        router.replace(slow_id, interest(&["a"])).await,
        Err(RouterError::NotFound)
    );
    let snapshot = router.snapshot().await.unwrap();
    assert_eq!(snapshot.counters.queue_overflows, 1);
    assert_eq!(snapshot.counters.admissions, 5);
    assert_eq!(snapshot.gauges.subscriptions, 1);
    assert_eq!(snapshot.gauges.memberships, 1);
    fixture.done().await;
}

#[tokio::test]
async fn byte_overflow_and_dequeue_release_are_independent_of_count() {
    let fixture = Fixture::new(Limits {
        queue_frames: 20,
        queue_bytes: 1024,
        ..Limits::default()
    })
    .await;
    let router = &fixture.router;
    let mut slow = activate(router, &["a"]).await;
    let mut fast = activate(router, &["a"]).await;
    let payload = Bytes::from(vec![b'x'; 600]);
    for _ in 0..2 {
        router
            .publish("one".into(), session(Some("a")), payload.clone())
            .await
            .unwrap();
        assert_eq!(recv(&mut fast).await.unwrap(), payload);
    }
    assert!(recv(&mut slow).await.is_none());
    let snapshot = router.snapshot().await.unwrap();
    assert_eq!(snapshot.counters.queue_overflows, 1);
    assert_eq!(snapshot.gauges.backlog_bytes, 0);
    assert_eq!(snapshot.gauges.subscriptions, 1);
    fixture.done().await;
}

#[tokio::test]
async fn activation_cannot_bypass_handshake_queue_bounds() {
    let fixture = Fixture::new(Limits {
        queue_frames: 1,
        ..Limits::default()
    })
    .await;
    let router = &fixture.router;
    let mut pending = router.subscribe(Mode::Controlled).await.unwrap();
    assert_eq!(
        router.replace(pending.id().into(), interest(&["a"])).await,
        Err(RouterError::Overloaded)
    );
    assert!(recv(&mut pending).await.is_none());
    let snapshot = router.snapshot().await.unwrap();
    assert_eq!(snapshot.gauges.subscriptions, 0);
    assert_eq!(snapshot.gauges.memberships, 0);
    assert_eq!(snapshot.counters.queue_overflows, 1);
    fixture.done().await;
}

#[tokio::test]
async fn oversized_single_frame_fails_selected_subscriber_not_source() {
    let fixture = Fixture::new(Limits {
        queue_bytes: 1024,
        ..Limits::default()
    })
    .await;
    let router = &fixture.router;
    let mut selected = activate(router, &["a"]).await;
    let untouched = activate(router, &["b"]).await;
    router
        .publish("one".into(), session(Some("a")), Bytes::from(vec![0; 1025]))
        .await
        .unwrap();
    assert!(recv(&mut selected).await.is_none());
    let snapshot = router.snapshot().await.unwrap();
    assert_eq!(snapshot.epoch.as_deref(), Some("one"));
    assert_eq!(snapshot.subscriptions[0].id, untouched.id());
    fixture.done().await;
}

#[tokio::test]
async fn source_loss_reset_and_stale_epoch_isolation() {
    let fixture = Fixture::new(Limits::default()).await;
    let router = &fixture.router;
    let mut old = activate(router, &["a"]).await;
    let old_id = old.id().to_owned();
    router
        .publish(
            "one".into(),
            Route::Broadcast,
            Bytes::from_static(b"discard"),
        )
        .await
        .unwrap();
    router.end_epoch("one".into()).await.unwrap();
    assert!(recv(&mut old).await.is_none());
    assert!(router.snapshot().await.unwrap().epoch.is_none());
    assert!(matches!(
        router.subscribe(Mode::Legacy).await,
        Err(RouterError::Unavailable)
    ));
    router.begin_epoch("two".into()).await.unwrap();
    let mut current = activate(router, &["a"]).await;
    assert_eq!(
        router
            .publish("one".into(), Route::Broadcast, Bytes::from_static(b"stale"))
            .await,
        Err(RouterError::Unavailable)
    );
    assert_eq!(
        router.end_epoch("one".into()).await,
        Err(RouterError::Unavailable)
    );
    assert_eq!(
        router.replace(old_id, interest(&["a"])).await,
        Err(RouterError::NotFound)
    );
    router
        .publish("two".into(), Route::Broadcast, Bytes::from_static(b"new"))
        .await
        .unwrap();
    assert_eq!(recv(&mut current).await.unwrap(), "new");
    // A begin without an explicit end is itself a reset barrier.
    router.begin_epoch("three".into()).await.unwrap();
    assert!(recv(&mut current).await.is_none());
    assert_eq!(
        router.begin_epoch("three".into()).await,
        Err(RouterError::Unavailable)
    );
    fixture.done().await;
}

#[tokio::test]
async fn pending_subscriptions_count_toward_cap_and_drop_removes_membership() {
    let fixture = Fixture::new(Limits {
        max_subscribers: 2,
        ..Limits::default()
    })
    .await;
    let router = &fixture.router;
    let pending = router.subscribe(Mode::Controlled).await.unwrap();
    let active = activate(router, &["a", "b"]).await;
    assert!(matches!(
        router.subscribe(Mode::Legacy).await,
        Err(RouterError::Overloaded)
    ));
    drop(active);
    let replacement = activate(router, &["c"]).await;
    let snapshot = router.snapshot().await.unwrap();
    assert_eq!(snapshot.gauges.subscriptions, 2);
    assert_eq!(snapshot.gauges.pending, 1);
    assert_eq!(snapshot.gauges.memberships, 1);
    assert_eq!(snapshot.counters.subscriptions_closed, 1);
    drop(pending);
    drop(replacement);
    assert_eq!(router.snapshot().await.unwrap().gauges.subscriptions, 0);
    fixture.done().await;
}

#[tokio::test]
async fn concurrent_publish_and_replace_have_one_admission_order() {
    let fixture = Fixture::new(Limits::default()).await;
    let router = &fixture.router;
    let mut subscriber = activate(router, &["a"]).await;
    let (published, replaced) = tokio::join!(
        router.publish(
            "one".into(),
            session(Some("a")),
            Bytes::from_static(b"racing-old")
        ),
        router.replace(subscriber.id().into(), interest(&["b"]))
    );
    published.unwrap();
    replaced.unwrap();
    let racing_admissions = router.snapshot().await.unwrap().counters.admissions;
    assert!(racing_admissions <= 1);
    router
        .publish(
            "one".into(),
            session(Some("a")),
            Bytes::from_static(b"must-skip"),
        )
        .await
        .unwrap();
    router
        .publish(
            "one".into(),
            session(Some("b")),
            Bytes::from_static(b"must-deliver"),
        )
        .await
        .unwrap();
    if racing_admissions == 1 {
        assert_eq!(recv(&mut subscriber).await.unwrap(), "racing-old");
    }
    assert_eq!(recv(&mut subscriber).await.unwrap(), "must-deliver");
    assert_eq!(
        router.snapshot().await.unwrap().counters.admissions,
        racing_admissions + 1
    );
    fixture.done().await;
}

#[tokio::test]
async fn shutdown_and_task_abort_end_even_idle_or_buffered_receivers() {
    let fixture = Fixture::new(Limits::default()).await;
    let mut buffered = fixture.router.subscribe(Mode::Legacy).await.unwrap();
    let mut idle = activate(&fixture.router, &[]).await;
    fixture.task.abort();
    assert!(fixture.task.await.unwrap_err().is_cancelled());
    assert!(recv(&mut buffered).await.is_none());
    assert!(recv(&mut idle).await.is_none());
    assert!(matches!(
        fixture.router.snapshot().await,
        Err(RouterError::Closed)
    ));

    let fixture = Fixture::new(Limits::default()).await;
    let mut subscription = fixture.router.subscribe(Mode::Legacy).await.unwrap();
    fixture.done().await;
    assert!(recv(&mut subscription).await.is_none());
}

#[tokio::test]
async fn snapshot_is_serializable_and_reports_admissions_not_receipts() {
    let fixture = Fixture::new(Limits::default()).await;
    let router = &fixture.router;
    let _pending = router.subscribe(Mode::Controlled).await.unwrap();
    let _active = activate(router, &["z", "a", "a"]).await;
    router
        .publish(
            "one".into(),
            Route::Broadcast,
            Bytes::from_static(b"queued"),
        )
        .await
        .unwrap();
    let snapshot = router.snapshot().await.unwrap();
    let json = serde_json::to_value(&snapshot).unwrap();
    assert_eq!(json["epoch"], "one");
    assert_eq!(json["counters"]["admissions"], 1);
    assert_eq!(json["gauges"]["memberships"], 2);
    assert_eq!(snapshot.gauges.backlog_frames, 2); // pending ready + queued domain frame
    assert_eq!(
        snapshot
            .subscriptions
            .iter()
            .find(|info| info.active)
            .unwrap()
            .sessions,
        ["a", "z"]
    );
    fixture.done().await;
}

#[tokio::test]
async fn invalid_limits_and_unready_subscribe_do_not_panic() {
    for limits in [
        Limits {
            queue_frames: 0,
            ..Limits::default()
        },
        Limits {
            queue_bytes: 0,
            ..Limits::default()
        },
        Limits {
            max_subscribers: 0,
            ..Limits::default()
        },
        Limits {
            command_capacity: 0,
            ..Limits::default()
        },
        Limits {
            command_capacity: usize::MAX,
            ..Limits::default()
        },
        Limits {
            max_subscribers: usize::MAX,
            ..Limits::default()
        },
        Limits {
            queue_bytes: usize::MAX,
            ..Limits::default()
        },
    ] {
        assert!(matches!(
            Router::spawn(limits, CancellationToken::new()),
            Err(RouterError::InvalidLimits)
        ));
    }
    let token = CancellationToken::new();
    let (router, task) = Router::spawn(Limits::default(), token.clone()).unwrap();
    assert!(matches!(
        router.subscribe(Mode::Legacy).await,
        Err(RouterError::Unavailable)
    ));
    assert_eq!(
        router.begin_epoch(String::new()).await,
        Err(RouterError::Unavailable)
    );
    token.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn canceling_idle_receive_does_not_lose_later_frame_or_close_subscription() {
    let fixture = Fixture::new(Limits::default()).await;
    let mut subscription = activate(&fixture.router, &["a"]).await;
    assert!(
        timeout(Duration::from_millis(1), subscription.recv())
            .await
            .is_err()
    );
    fixture
        .router
        .publish(
            "one".into(),
            session(Some("a")),
            Bytes::from_static(b"after-canceled-recv"),
        )
        .await
        .unwrap();
    assert_eq!(
        recv(&mut subscription).await.unwrap(),
        "after-canceled-recv"
    );
    assert_eq!(
        fixture
            .router
            .snapshot()
            .await
            .unwrap()
            .gauges
            .subscriptions,
        1
    );
    fixture.done().await;
}
