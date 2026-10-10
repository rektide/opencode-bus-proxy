//! Single-owner indexed admission. Acknowledgments are admission cuts, not socket
//! receipts: already queued events survive replacement, but never source failure.
//!
//! Commands use prompt `try_send` overload rejection. Callers must validate
//! interests and bound source frames using `bus-proxy-protocol`; one source
//! publisher should await each acknowledgment. Epoch strings must never be reused.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use bus_proxy_protocol::{Interest, Route, connected_frame, ready_frame};
use bytes::Bytes;
use serde::Serialize;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// Backlog limits include ready/connected frames. Queue buffers allocate lazily.
#[derive(Clone, Debug)]
pub struct Limits {
    pub queue_frames: usize,
    pub queue_bytes: usize,
    pub max_subscribers: usize,
    pub command_capacity: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            queue_frames: 4096,
            queue_bytes: 16 * 1024 * 1024,
            max_subscribers: 1024,
            command_capacity: 256,
        }
    }
}

impl Limits {
    fn validate(&self) -> Result<(), RouterError> {
        // Tokio channels use a semaphore internally. Keep byte/count values
        // representable by u32 as well, so alternate queue implementations cannot
        // silently truncate their budgets. No preallocation uses these capacities.
        if self.queue_frames == 0
            || self.queue_bytes == 0
            || self.max_subscribers == 0
            || self.command_capacity == 0
            || self.queue_frames > u32::MAX as usize
            || self.queue_bytes > u32::MAX as usize
            || self.command_capacity > tokio::sync::Semaphore::MAX_PERMITS
            || self.max_subscribers.checked_mul(self.queue_bytes).is_none()
            || self
                .max_subscribers
                .checked_mul(self.queue_frames)
                .is_none()
        {
            return Err(RouterError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Legacy,
    Controlled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RouterError {
    #[error("event source unavailable or epoch does not match")]
    Unavailable,
    #[error("subscription not found or no longer live")]
    NotFound,
    #[error("router or subscriber capacity exceeded")]
    Overloaded,
    #[error("router closed")]
    Closed,
    #[error("invalid router limits")]
    InvalidLimits,
}

/// Monotonic, saturating lifetime counters; no unbounded per-ID labels or history.
/// `admissions`/`admitted_bytes` count domain frame offers accepted by queues,
/// not network receipts. Handshake offers are counted separately. Publishing
/// successfully does not mean every selected subscriber remained healthy.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Counters {
    pub publications: u64,
    pub published_bytes: u64,
    pub admissions: u64,
    pub admitted_bytes: u64,
    pub handshake_frames: u64,
    pub no_recipient_publications: u64,
    pub queue_overflows: u64,
    pub subscriptions_created: u64,
    pub subscriptions_closed: u64,
    pub epochs_started: u64,
    pub epochs_ended: u64,
}

/// Current queue backlog excludes frames returned by `Subscription::recv` and
/// any Hyper/TLS/kernel buffering. Bytes are logical per-recipient retention;
/// shared backing allocations and queue overhead make this different from RSS.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Gauges {
    pub subscriptions: usize,
    pub active: usize,
    pub pending: usize,
    pub legacy: usize,
    pub controlled: usize,
    pub memberships: usize,
    pub backlog_frames: usize,
    pub backlog_bytes: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct SubscriptionInfo {
    pub id: String,
    pub mode: Mode,
    pub active: bool,
    pub sessions: Vec<String>,
    pub backlog_frames: usize,
    pub backlog_bytes: usize,
}

/// Index/epoch snapshot at one actor turn. Socket readers can concurrently reduce
/// individual backlogs; aggregate gauges sum the per-subscription samples here.
/// Arrays are sorted for stable inspection; IDs/session names are never labels.
#[derive(Clone, Debug, Serialize)]
pub struct Snapshot {
    pub epoch: Option<String>,
    pub subscriptions: Vec<SubscriptionInfo>,
    pub counters: Counters,
    pub gauges: Gauges,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Queue locks protect only short, non-awaiting bookkeeping. Even poisoning
    // must not prevent the actor's unwind guard from canceling every subscriber.
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

struct Backlog {
    frames: VecDeque<Bytes>,
    bytes: usize,
    closed: bool,
}

struct Queue {
    backlog: Mutex<Backlog>,
    notify: Notify,
    canceled: CancellationToken,
    epoch_canceled: CancellationToken,
    max_frames: usize,
    max_bytes: usize,
}

impl Queue {
    fn new(limits: &Limits, epoch_canceled: CancellationToken) -> Self {
        Self {
            backlog: Mutex::new(Backlog {
                frames: VecDeque::new(),
                bytes: 0,
                closed: false,
            }),
            notify: Notify::new(),
            canceled: CancellationToken::new(),
            epoch_canceled,
            max_frames: limits.queue_frames,
            max_bytes: limits.queue_bytes,
        }
    }

    fn is_closed(&self) -> bool {
        self.canceled.is_cancelled() || self.epoch_canceled.is_cancelled()
    }

    fn close(&self) {
        {
            let mut backlog = lock(&self.backlog);
            backlog.closed = true;
            backlog.frames.clear();
            backlog.bytes = 0;
        }
        self.canceled.cancel();
    }

    fn offer(&self, frame: Bytes) -> Result<(), RouterError> {
        if self.is_closed() {
            return Err(RouterError::NotFound);
        }
        let mut backlog = lock(&self.backlog);
        if backlog.closed || self.is_closed() {
            return Err(RouterError::NotFound);
        }
        if backlog.frames.len() >= self.max_frames || frame.len() > self.max_bytes - backlog.bytes {
            return Err(RouterError::Overloaded);
        }
        backlog.bytes += frame.len();
        backlog.frames.push_back(frame);
        drop(backlog);
        self.notify.notify_one();
        Ok(())
    }

    async fn recv(&self) -> Option<Bytes> {
        loop {
            // One receiver (&mut Subscription) plus notify_one's saved permit
            // avoids a lost wake between an empty check and awaiting notification.
            let notified = self.notify.notified();
            {
                let mut backlog = lock(&self.backlog);
                if backlog.closed || self.is_closed() {
                    backlog.frames.clear();
                    backlog.bytes = 0;
                    return None;
                }
                if let Some(frame) = backlog.frames.pop_front() {
                    backlog.bytes -= frame.len();
                    return Some(frame);
                }
            }
            tokio::select! {
                biased;
                _ = self.canceled.cancelled() => return None,
                _ = self.epoch_canceled.cancelled() => {
                    self.close();
                    return None;
                }
                _ = notified => {}
            }
        }
    }

    fn sample(&self) -> (usize, usize) {
        let backlog = lock(&self.backlog);
        (backlog.frames.len(), backlog.bytes)
    }
}

pub struct Subscription {
    id: String,
    queue: Arc<Queue>,
    commands: mpsc::Sender<Command>,
}

impl Subscription {
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Cancel-safe dequeue. No frame is yielded after observing cancellation;
    /// frames already handed to a caller cannot be retracted by failure/reset.
    pub async fn recv(&mut self) -> Option<Bytes> {
        self.queue.recv().await
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.queue.close();
        // Cleanup cannot be lost: the queue's canceled flag persists if full,
        // and the actor reaps it periodically and before every command.
        let _ = self.commands.try_send(Command::Remove(self.id.clone()));
    }
}

type Reply<T> = oneshot::Sender<Result<T, RouterError>>;

enum Command {
    Begin(String, Reply<()>),
    End(String, Reply<()>),
    Subscribe(Mode, Reply<Subscription>),
    Replace(String, Interest, Reply<()>),
    Publish(String, Route, Bytes, Reply<()>),
    Snapshot(Reply<Snapshot>),
    Remove(String),
}

// Only the actor installs/removes epochs. External end_epoch can cancel the
// current token out-of-band even when the bounded command mailbox is saturated.
type EpochSignal = Arc<Mutex<Option<(String, CancellationToken)>>>;

#[derive(Clone)]
pub struct Router {
    commands: mpsc::Sender<Command>,
    epoch_signal: EpochSignal,
    shutdown: CancellationToken,
}

impl Router {
    pub fn spawn(
        limits: Limits,
        shutdown: CancellationToken,
    ) -> Result<(Self, tokio::task::JoinHandle<()>), RouterError> {
        limits.validate()?;
        let (commands, receiver) = mpsc::channel(limits.command_capacity);
        let epoch_signal = Arc::new(Mutex::new(None));
        let router = Self {
            commands: commands.clone(),
            epoch_signal: epoch_signal.clone(),
            shutdown: shutdown.clone(),
        };
        let actor = Actor {
            limits,
            commands: commands.downgrade(),
            epoch_signal,
            epoch: None,
            subscribers: HashMap::new(),
            active: HashSet::new(),
            legacy: HashSet::new(),
            sessions: HashMap::new(),
            counters: Counters::default(),
        };
        let join = tokio::spawn(actor.run(receiver, shutdown));
        Ok((router, join))
    }

    async fn request<T>(
        &self,
        command: impl FnOnce(Reply<T>) -> Command,
    ) -> Result<T, RouterError> {
        if self.shutdown.is_cancelled() {
            return Err(RouterError::Closed);
        }
        let (reply, result) = oneshot::channel();
        self.commands
            .try_send(command(reply))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => RouterError::Overloaded,
                mpsc::error::TrySendError::Closed(_) => RouterError::Closed,
            })?;
        result.await.map_err(|_| RouterError::Closed)?
    }

    /// Starts/replaces a source generation, invalidating all old attachments.
    /// Repeating the current epoch or supplying an empty name is rejected.
    pub async fn begin_epoch(&self, epoch: String) -> Result<(), RouterError> {
        self.request(|reply| Command::Begin(epoch, reply)).await
    }

    /// Cancellation bypasses command capacity. Even an Overloaded result has
    /// invalidated the matching source token. A subsequent acknowledged Begin
    /// also acts as the retirement barrier. A stale name never cancels the current
    /// generation; repeating End after retirement returns Unavailable.
    pub async fn end_epoch(&self, epoch: String) -> Result<(), RouterError> {
        {
            let signal = lock(&self.epoch_signal);
            match signal.as_ref() {
                Some((current, token)) if current == &epoch => token.cancel(),
                _ => return Err(RouterError::Unavailable),
            }
        }
        self.request(|reply| Command::End(epoch, reply)).await
    }

    pub async fn subscribe(&self, mode: Mode) -> Result<Subscription, RouterError> {
        self.request(|reply| Command::Subscribe(mode, reply)).await
    }

    pub async fn replace(&self, id: String, interest: Interest) -> Result<(), RouterError> {
        self.request(|reply| Command::Replace(id, interest, reply))
            .await
    }

    pub async fn publish(
        &self,
        epoch: String,
        route: Route,
        frame: Bytes,
    ) -> Result<(), RouterError> {
        self.request(|reply| Command::Publish(epoch, route, frame, reply))
            .await
    }

    pub async fn snapshot(&self) -> Result<Snapshot, RouterError> {
        self.request(Command::Snapshot).await
    }
}

struct Subscriber {
    mode: Mode,
    active: bool,
    sessions: HashSet<String>,
    queue: Arc<Queue>,
}

struct Actor {
    limits: Limits,
    commands: mpsc::WeakSender<Command>,
    epoch_signal: EpochSignal,
    epoch: Option<(String, CancellationToken)>,
    subscribers: HashMap<String, Subscriber>,
    active: HashSet<String>,
    legacy: HashSet<String>,
    sessions: HashMap<String, HashSet<String>>,
    counters: Counters,
}

impl Drop for Actor {
    fn drop(&mut self) {
        // Runs on normal exit, task abort, or panic, not just graceful shutdown.
        self.retire_epoch();
    }
}

impl Actor {
    async fn run(mut self, mut receiver: mpsc::Receiver<Command>, shutdown: CancellationToken) {
        let mut reap = tokio::time::interval(Duration::from_millis(100));
        reap.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let epoch_token = self.epoch.as_ref().map(|(_, token)| token.clone());
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                _ = async {
                    match epoch_token {
                        Some(token) => token.cancelled().await,
                        None => std::future::pending().await,
                    }
                } => self.retire_epoch(),
                _ = reap.tick() => self.reap(),
                command = receiver.recv() => {
                    let Some(command) = command else { break };
                    if self.epoch.as_ref().is_some_and(|(_, token)| token.is_cancelled()) {
                        self.retire_epoch();
                    }
                    self.handle(command);
                }
            }
        }
    }

    fn retire_epoch(&mut self) {
        if let Some((_, token)) = self.epoch.take() {
            tracing::debug!("retiring event source epoch");
            token.cancel();
            self.counters.epochs_ended = self.counters.epochs_ended.saturating_add(1);
        }
        *lock(&self.epoch_signal) = None;
        for (_, subscriber) in self.subscribers.drain() {
            subscriber.queue.close();
            self.counters.subscriptions_closed =
                self.counters.subscriptions_closed.saturating_add(1);
        }
        self.active.clear();
        self.legacy.clear();
        self.sessions.clear();
    }

    fn remove(&mut self, id: &str) {
        let Some(subscriber) = self.subscribers.remove(id) else {
            return;
        };
        tracing::debug!(subscription_id = id, "removing event subscription");
        subscriber.queue.close();
        self.active.remove(id);
        self.legacy.remove(id);
        for session in subscriber.sessions {
            if let Some(members) = self.sessions.get_mut(&session) {
                members.remove(id);
                if members.is_empty() {
                    self.sessions.remove(&session);
                }
            }
        }
        self.counters.subscriptions_closed = self.counters.subscriptions_closed.saturating_add(1);
    }

    fn reap(&mut self) {
        let dead: Vec<_> = self
            .subscribers
            .iter()
            .filter(|(_, subscriber)| subscriber.queue.is_closed())
            .map(|(id, _)| id.clone())
            .collect();
        for id in dead {
            self.remove(&id);
        }
    }

    fn handle(&mut self, command: Command) {
        match command {
            Command::Begin(epoch, reply) => {
                let result = if epoch.is_empty()
                    || self.epoch.as_ref().is_some_and(|(name, _)| name == &epoch)
                {
                    Err(RouterError::Unavailable)
                } else {
                    self.retire_epoch();
                    let token = CancellationToken::new();
                    self.epoch = Some((epoch.clone(), token.clone()));
                    *lock(&self.epoch_signal) = Some((epoch, token));
                    self.counters.epochs_started = self.counters.epochs_started.saturating_add(1);
                    tracing::debug!("event source epoch ready");
                    Ok(())
                };
                let _ = reply.send(result);
            }
            Command::End(epoch, reply) => {
                // The API already authenticated/canceled this epoch's token.
                // A queued old End must not retire a newer Begin.
                if self.epoch.as_ref().is_some_and(|(name, _)| name == &epoch) {
                    self.retire_epoch();
                }
                let _ = reply.send(Ok(()));
            }
            Command::Subscribe(mode, reply) => {
                self.reap();
                let result = self.subscribe(mode);
                // Failed/canceled acknowledgment drops its Subscription guard.
                let _ = reply.send(result);
            }
            Command::Replace(id, interest, reply) => {
                let result = self.replace(&id, interest);
                let _ = reply.send(result);
            }
            Command::Publish(epoch, route, frame, reply) => {
                let result = self.publish(&epoch, route, frame);
                let _ = reply.send(result);
            }
            Command::Snapshot(reply) => {
                self.reap();
                let _ = reply.send(Ok(self.snapshot()));
            }
            Command::Remove(id) => self.remove(&id),
        }
    }

    fn subscribe(&mut self, mode: Mode) -> Result<Subscription, RouterError> {
        let (_, epoch_token) = self.epoch.as_ref().ok_or(RouterError::Unavailable)?;
        if epoch_token.is_cancelled() {
            return Err(RouterError::Unavailable);
        }
        if self.subscribers.len() >= self.limits.max_subscribers {
            return Err(RouterError::Overloaded);
        }
        let id = uuid::Uuid::new_v4().to_string();
        let queue = Arc::new(Queue::new(&self.limits, epoch_token.clone()));
        queue.offer(match mode {
            Mode::Legacy => connected_frame(),
            Mode::Controlled => ready_frame(&id),
        })?;
        let commands = self.commands.upgrade().ok_or(RouterError::Closed)?;
        self.counters.handshake_frames = self.counters.handshake_frames.saturating_add(1);
        self.counters.subscriptions_created = self.counters.subscriptions_created.saturating_add(1);
        let active = mode == Mode::Legacy;
        if active {
            self.active.insert(id.clone());
            self.legacy.insert(id.clone());
        }
        self.subscribers.insert(
            id.clone(),
            Subscriber {
                mode,
                active,
                sessions: HashSet::new(),
                queue: queue.clone(),
            },
        );
        tracing::debug!(subscription_id = %id, ?mode, "event subscription registered");
        Ok(Subscription {
            id,
            queue,
            commands,
        })
    }

    fn replace(&mut self, id: &str, interest: Interest) -> Result<(), RouterError> {
        let subscriber = self.subscribers.get(id).ok_or(RouterError::NotFound)?;
        if subscriber.queue.is_closed() || subscriber.mode != Mode::Controlled {
            return Err(RouterError::NotFound);
        }
        if !subscriber.active {
            if let Err(error) = subscriber.queue.offer(connected_frame()) {
                if error == RouterError::Overloaded {
                    self.counters.queue_overflows = self.counters.queue_overflows.saturating_add(1);
                }
                self.remove(id);
                return Err(error);
            }
            self.counters.handshake_frames = self.counters.handshake_frames.saturating_add(1);
        }
        let subscriber = self
            .subscribers
            .get_mut(id)
            .expect("subscriber still owned by actor");
        for session in subscriber.sessions.drain() {
            if let Some(members) = self.sessions.get_mut(&session) {
                members.remove(id);
                if members.is_empty() {
                    self.sessions.remove(&session);
                }
            }
        }
        subscriber.sessions = interest.sessions.into_iter().collect();
        for session in &subscriber.sessions {
            self.sessions
                .entry(session.clone())
                .or_default()
                .insert(id.to_owned());
        }
        subscriber.active = true;
        self.active.insert(id.to_owned());
        tracing::debug!(subscription_id = id, "event interest replaced");
        Ok(())
    }

    fn publish(&mut self, epoch: &str, route: Route, frame: Bytes) -> Result<(), RouterError> {
        if !self
            .epoch
            .as_ref()
            .is_some_and(|(name, token)| name == epoch && !token.is_cancelled())
        {
            return Err(RouterError::Unavailable);
        }
        self.counters.publications = self.counters.publications.saturating_add(1);
        self.counters.published_bytes = self
            .counters
            .published_bytes
            .saturating_add(frame.len() as u64);
        let mut failed = Vec::new();
        let mut selected = 0usize;
        let (first, second) = match &route {
            Route::Broadcast => (&self.active, None),
            Route::Session { session_id } => (
                &self.legacy,
                session_id.as_ref().and_then(|id| self.sessions.get(id)),
            ),
        };
        for id in first.iter().chain(second.into_iter().flatten()) {
            let subscriber = &self.subscribers[id];
            if subscriber.queue.is_closed() {
                failed.push(id.clone());
                continue;
            }
            selected += 1;
            match subscriber.queue.offer(frame.clone()) {
                Ok(()) => {
                    self.counters.admissions = self.counters.admissions.saturating_add(1);
                    self.counters.admitted_bytes = self
                        .counters
                        .admitted_bytes
                        .saturating_add(frame.len() as u64);
                }
                Err(error) => {
                    if error == RouterError::Overloaded {
                        self.counters.queue_overflows =
                            self.counters.queue_overflows.saturating_add(1);
                    }
                    failed.push(id.clone());
                }
            }
        }
        if selected == 0 {
            self.counters.no_recipient_publications =
                self.counters.no_recipient_publications.saturating_add(1);
        }
        for id in failed {
            self.remove(&id);
        }
        Ok(())
    }

    fn snapshot(&self) -> Snapshot {
        let mut subscriptions = Vec::with_capacity(self.subscribers.len());
        let mut gauges = Gauges::default();
        for (id, subscriber) in &self.subscribers {
            let (backlog_frames, backlog_bytes) = subscriber.queue.sample();
            let mut sessions: Vec<_> = subscriber.sessions.iter().cloned().collect();
            sessions.sort_unstable();
            subscriptions.push(SubscriptionInfo {
                id: id.clone(),
                mode: subscriber.mode,
                active: subscriber.active,
                sessions,
                backlog_frames,
                backlog_bytes,
            });
            gauges.subscriptions += 1;
            if subscriber.active {
                gauges.active += 1;
            } else {
                gauges.pending += 1;
            }
            match subscriber.mode {
                Mode::Legacy => gauges.legacy += 1,
                Mode::Controlled => gauges.controlled += 1,
            }
            gauges.memberships = gauges.memberships.saturating_add(subscriber.sessions.len());
            gauges.backlog_frames = gauges.backlog_frames.saturating_add(backlog_frames);
            gauges.backlog_bytes = gauges.backlog_bytes.saturating_add(backlog_bytes);
        }
        subscriptions.sort_unstable_by(|a, b| a.id.cmp(&b.id));
        Snapshot {
            epoch: self.epoch.as_ref().map(|(epoch, _)| epoch.clone()),
            subscriptions,
            counters: self.counters.clone(),
            gauges,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn router(capacity: usize) -> (Router, CancellationToken, tokio::task::JoinHandle<()>) {
        let shutdown = CancellationToken::new();
        let (router, task) = Router::spawn(
            Limits {
                command_capacity: capacity,
                ..Limits::default()
            },
            shutdown.clone(),
        )
        .unwrap();
        router.begin_epoch("one".into()).await.unwrap();
        (router, shutdown, task)
    }

    #[tokio::test]
    async fn saturated_controls_reject_promptly_but_source_cancel_bypasses_mailbox() {
        let (router, shutdown, task) = router(1).await;
        let mut subscription = router.subscribe(Mode::Legacy).await.unwrap();
        let (reply, ack) = oneshot::channel();
        router
            .commands
            .try_send(Command::Publish(
                "one".into(),
                Route::Broadcast,
                Bytes::from_static(b"queued-command"),
                reply,
            ))
            .unwrap_or_else(|_| panic!("empty mailbox"));
        // No yields: actor cannot empty the channel before these try_send calls.
        assert_eq!(
            router.snapshot().await.unwrap_err(),
            RouterError::Overloaded
        );
        assert_eq!(
            router.end_epoch("one".into()).await,
            Err(RouterError::Overloaded)
        );
        assert!(subscription.recv().await.is_none()); // even queued connected discarded
        assert_eq!(subscription.queue.sample(), (0, 0));
        assert_eq!(ack.await.unwrap(), Err(RouterError::Unavailable));
        router.begin_epoch("two".into()).await.unwrap();
        assert_eq!(
            router.snapshot().await.unwrap().epoch.as_deref(),
            Some("two")
        );
        shutdown.cancel();
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn dropped_subscription_is_reaped_when_remove_command_cannot_fit() {
        let (router, shutdown, task) = router(1).await;
        let mut subscription = router.subscribe(Mode::Legacy).await.unwrap();
        subscription.recv().await.unwrap();
        let queue = Arc::downgrade(&subscription.queue);
        let (reply, ack) = oneshot::channel();
        router
            .commands
            .try_send(Command::Begin("one".into(), reply))
            .unwrap_or_else(|_| panic!("empty mailbox"));
        drop(subscription); // Remove cannot fit; token is durable cleanup notice.
        assert_eq!(ack.await.unwrap(), Err(RouterError::Unavailable));
        assert!(queue.upgrade().is_some()); // Begin does not do opportunistic reap.
        tokio::time::advance(Duration::from_millis(101)).await;
        tokio::task::yield_now().await;
        assert!(queue.upgrade().is_none()); // periodic fallback, not snapshot side effect
        let snapshot = router.snapshot().await.unwrap();
        assert_eq!(snapshot.gauges.subscriptions, 0);
        assert_eq!(snapshot.counters.subscriptions_closed, 1);
        shutdown.cancel();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn canceled_subscribe_ack_drops_provisional_subscription() {
        let (router, shutdown, task) = router(2).await;
        let (reply, result) = oneshot::channel();
        router
            .commands
            .try_send(Command::Subscribe(Mode::Controlled, reply))
            .unwrap_or_else(|_| panic!("empty mailbox"));
        drop(result);
        let snapshot = router.snapshot().await.unwrap();
        assert_eq!(snapshot.gauges.subscriptions, 0);
        assert_eq!(snapshot.counters.subscriptions_created, 1);
        assert_eq!(snapshot.counters.subscriptions_closed, 1);
        shutdown.cancel();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn canceled_recipient_is_not_offered_or_replaced_before_cleanup_turn() {
        let (router, shutdown, task) = router(2).await;
        let mut subscription = router.subscribe(Mode::Controlled).await.unwrap();
        subscription.recv().await.unwrap();
        router
            .replace(
                subscription.id().into(),
                Interest {
                    sessions: vec!["a".into()],
                },
            )
            .await
            .unwrap();
        subscription.recv().await.unwrap();
        subscription.queue.close(); // cancellation without a Remove notification
        assert_eq!(
            router
                .replace(
                    subscription.id().into(),
                    Interest {
                        sessions: vec!["b".into()]
                    }
                )
                .await,
            Err(RouterError::NotFound)
        );
        router
            .publish(
                "one".into(),
                Route::Session {
                    session_id: Some("a".into()),
                },
                Bytes::from_static(b"not-offered"),
            )
            .await
            .unwrap();
        let snapshot = router.snapshot().await.unwrap();
        assert_eq!(snapshot.counters.admissions, 0);
        assert_eq!(snapshot.gauges.memberships, 0);
        shutdown.cancel();
        task.await.unwrap();
    }

    #[test]
    fn queue_poison_does_not_prevent_close_on_unwind() {
        let queue = Arc::new(Queue::new(&Limits::default(), CancellationToken::new()));
        queue.offer(Bytes::from_static(b"queued")).unwrap();
        let cloned = queue.clone();
        let _ = std::panic::catch_unwind(move || {
            let _guard = cloned.backlog.lock().unwrap();
            panic!("test poisoning");
        });
        queue.close();
        assert!(queue.is_closed());
        assert_eq!(queue.sample(), (0, 0));
    }
}
