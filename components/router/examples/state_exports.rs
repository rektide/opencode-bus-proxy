//! Observational export prototype; no router freeze, import, or production hooks.
use std::collections::BTreeMap;

use bus_proxy_protocol::Interest;
use bus_proxy_router::{Limits, Mode, Router, Snapshot};
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use tracing::field::Empty;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SubscriptionRow {
    id: String,
    mode: &'static str,
    active: bool,
    interest_count: u64,
    backlog_frames: u64,
    backlog_bytes: u64,
}

impl SubscriptionRow {
    fn record(&self, span: &tracing::Span, revision: u64) {
        span.record("revision", revision);
        span.record("active", self.active);
        span.record("interest_count", self.interest_count);
        span.record("backlog_frames", self.backlog_frames);
        span.record("backlog_bytes", self.backlog_bytes);
    }
}

#[derive(Serialize)]
struct MembershipRow {
    id: String,
    session: String,
}

#[derive(Serialize)]
struct Inspection {
    schema_version: u32,
    revision: u64,
    consistency: &'static str,
    subscriptions: Vec<SubscriptionRow>,
    memberships: Vec<MembershipRow>,
    // Keep the native counters/gauges/epoch and their existing meaning.
    router: Snapshot,
}

// A small typed materialized view, NOT the proposed full material registry.
// Reconciles bounded current state; removal drops spans, no history retained.
#[derive(Default)]
struct View {
    revision: u64,
    subscriptions: BTreeMap<String, (SubscriptionRow, tracing::Span)>,
}

impl View {
    fn observe(&mut self, snapshot: Snapshot) -> Inspection {
        self.revision = self.revision.checked_add(1).expect("revision exhausted");
        let mut rows = Vec::new();
        let mut memberships = Vec::new();
        for sub in &snapshot.subscriptions {
            let row = SubscriptionRow {
                id: sub.id.clone(),
                mode: match sub.mode {
                    Mode::Legacy => "legacy",
                    Mode::Controlled => "controlled",
                },
                active: sub.active,
                interest_count: sub.sessions.len() as u64,
                backlog_frames: sub.backlog_frames as u64,
                backlog_bytes: sub.backlog_bytes as u64,
            };
            match self.subscriptions.get_mut(&row.id) {
                Some((previous, span)) if previous != &row => {
                    row.record(span, self.revision);
                    tracing::info!(parent: &*span, revision = self.revision,
                        active = row.active, interest_count = row.interest_count,
                        backlog_frames = row.backlog_frames, backlog_bytes = row.backlog_bytes,
                        "observed subscription changed");
                    *previous = row.clone();
                }
                Some(_) => {}
                None => {
                    let span = tracing::info_span!("export.subscription", subscription_id = %row.id,
                        mode = row.mode, revision = Empty, active = Empty,
                        interest_count = Empty, backlog_frames = Empty, backlog_bytes = Empty);
                    row.record(&span, self.revision);
                    self.subscriptions
                        .insert(row.id.clone(), (row.clone(), span));
                }
            }
            memberships.extend(sub.sessions.iter().map(|session| MembershipRow {
                id: row.id.clone(),
                session: session.clone(),
            }));
            rows.push(row);
        }
        let live: std::collections::HashSet<_> = rows.iter().map(|row| &row.id).collect();
        self.subscriptions.retain(|id, (_, span)| {
            if live.contains(id) {
                true
            } else {
                tracing::info!(parent: &*span, revision = self.revision, "observed subscription removed");
                false
            }
        });
        Inspection {
            schema_version: 1,
            revision: self.revision,
            consistency: "observational",
            subscriptions: rows,
            memberships,
            router: snapshot,
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .json()
        .with_writer(std::io::stderr)
        .init();
    let shutdown = CancellationToken::new();
    let (router, task) = Router::spawn(Limits::default(), shutdown.clone())?;
    router.begin_epoch("export-demo".into()).await?;
    let mut subscription = router.subscribe(Mode::Controlled).await?;
    subscription.recv().await;
    let mut view = View::default();
    view.observe(router.snapshot().await?);
    router
        .replace(
            subscription.id().into(),
            Interest {
                sessions: vec!["a".into(), "b".into()],
            },
        )
        .await?;
    subscription.recv().await;
    let image = view.observe(router.snapshot().await?);
    assert_eq!(image.subscriptions[0].interest_count, 2);
    assert!(image.subscriptions[0].active);
    println!("{}", serde_json::to_string(&image)?);
    drop(subscription);
    assert!(
        view.observe(router.snapshot().await?)
            .subscriptions
            .is_empty()
    );
    assert!(view.subscriptions.is_empty());
    shutdown.cancel();
    task.await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tracing::{
        Subscriber,
        field::{Field, Visit},
        span::{Id, Record},
    };
    use tracing_subscriber::{Layer, layer::Context, prelude::*};

    #[derive(Clone, Default)]
    struct Records(Arc<Mutex<Vec<(&'static str, String)>>>);
    impl Visit for Records {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0
                .lock()
                .unwrap()
                .push((field.name(), format!("{value:?}")));
        }
    }
    impl<S: Subscriber> Layer<S> for Records {
        fn on_record(&self, _: &Id, values: &Record<'_>, _: Context<'_, S>) {
            values.record(&mut self.clone());
        }
    }

    #[test]
    fn declared_fields_react_undeclared_fields_do_not() {
        let records = Records::default();
        let subscriber = tracing_subscriber::registry().with(records.clone());
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(
                "test.material",
                revision = Empty,
                active = Empty,
                interest_count = Empty,
                backlog_frames = Empty,
                backlog_bytes = Empty
            );
            let mut row = SubscriptionRow {
                id: "demo".into(),
                mode: "controlled",
                active: false,
                interest_count: 0,
                backlog_frames: 0,
                backlog_bytes: 0,
            };
            row.record(&span, 1);
            row.active = true;
            row.interest_count = 2;
            row.record(&span, 2);
            span.record("not_declared", 42_u64);
        });
        let observed = records.0.lock().unwrap();
        assert!(observed.contains(&("active", "false".into())));
        assert!(observed.contains(&("active", "true".into())));
        assert!(observed.contains(&("interest_count", "2".into())));
        assert!(!observed.iter().any(|(field, _)| *field == "not_declared"));
    }
}
