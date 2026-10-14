use std::sync::{Arc, Mutex};

use bus_proxy_protocol::{Interest, Route};
use bus_proxy_router::{Limits, Mode, Router};
use bytes::Bytes;
use tokio_util::sync::CancellationToken;
use tracing::{
    Subscriber,
    span::{Attributes, Id},
};
use tracing_subscriber::{Layer, layer::Context, prelude::*, registry::LookupSpan};

#[derive(Default)]
struct Observations {
    created: Vec<(&'static str, Option<&'static str>)>,
    closed: Vec<&'static str>,
}

struct Observe(Arc<Mutex<Observations>>);

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Observe {
    fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
        let span = context.span(id).unwrap();
        let parent = span.parent().map(|span| span.metadata().name());
        self.0
            .lock()
            .unwrap()
            .created
            .push((attributes.metadata().name(), parent));
    }

    fn on_close(&self, id: Id, context: Context<'_, S>) {
        self.0
            .lock()
            .unwrap()
            .closed
            .push(context.span(&id).unwrap().metadata().name());
    }
}

#[tokio::test]
async fn subscription_span_is_retained_controls_are_children_and_publish_has_no_spans() {
    let observations = Arc::new(Mutex::new(Observations::default()));
    let subscriber = tracing_subscriber::registry().with(Observe(observations.clone()));
    let _default = tracing::subscriber::set_default(subscriber);
    let shutdown = CancellationToken::new();
    let (router, actor) = Router::spawn(Limits::default(), shutdown.clone()).unwrap();
    router.begin_epoch("one".into()).await.unwrap();
    let mut subscription = router.subscribe(Mode::Controlled).await.unwrap();
    subscription.recv().await.unwrap();
    router
        .replace(subscription.id().into(), Interest { sessions: vec![] })
        .await
        .unwrap();
    subscription.recv().await.unwrap();
    assert!(
        !observations
            .lock()
            .unwrap()
            .closed
            .contains(&"event.subscription")
    );
    let before = observations.lock().unwrap().created.len();
    for _ in 0..10 {
        router
            .publish(
                "one".into(),
                Route::Broadcast,
                Bytes::from_static(b"payload"),
            )
            .await
            .unwrap();
        subscription.recv().await.unwrap();
    }
    assert_eq!(observations.lock().unwrap().created.len(), before);
    router
        .replace(
            subscription.id().into(),
            Interest {
                sessions: vec!["a".into()],
            },
        )
        .await
        .unwrap();
    {
        let observed = observations.lock().unwrap();
        assert_eq!(
            observed
                .created
                .iter()
                .filter(|(name, _)| *name == "event.subscription")
                .count(),
            1
        );
        let controls: Vec<_> = observed
            .created
            .iter()
            .filter(|(name, _)| *name == "event.subscription.replace")
            .collect();
        assert_eq!(controls.len(), 2);
        assert!(
            controls
                .iter()
                .all(|(_, parent)| *parent == Some("event.subscription"))
        );
    }
    drop(subscription);
    assert_eq!(router.snapshot().await.unwrap().gauges.subscriptions, 0);
    assert!(
        observations
            .lock()
            .unwrap()
            .closed
            .contains(&"event.subscription")
    );
    shutdown.cancel();
    actor.await.unwrap();
}
