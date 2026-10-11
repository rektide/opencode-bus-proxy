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
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

/// Backlog limits include ready/connected frames. Queue buffers allocate lazily.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
        if self.queue_bytes == 0
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    #[error("router is frozen for handover")]
    Frozen,
    #[error("invalid router handover image")]
    InvalidImage,
    #[error("router handover image exceeds its serialization budget")]
    ImageLimit,
}

/// Monotonic, saturating lifetime counters; no unbounded per-ID labels or history.
/// `admissions`/`admitted_bytes` count domain frame offers accepted by queues,
/// not network receipts. Handshake offers are counted separately. Publishing
/// successfully does not mean every selected subscriber remained healthy.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
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
    pub direct_frames: usize,
    pub direct_bytes: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct SubscriptionInfo {
    pub id: String,
    pub mode: Mode,
    pub active: bool,
    pub sessions: Vec<String>,
    pub backlog_frames: usize,
    pub backlog_bytes: usize,
    pub queue_frame_target: usize,
    pub direct_frames: usize,
    pub direct_bytes: usize,
    pub receive_credit: bool,
}

/// Index/epoch snapshot at one actor turn. Socket readers can concurrently reduce
/// individual backlogs; aggregate gauges sum the per-subscription samples here.
/// Arrays are sorted for stable inspection; IDs/session names are never labels.
#[derive(Clone, Debug, Serialize)]
pub struct Snapshot {
    pub epoch: Option<String>,
    pub queue_frame_target: usize,
    pub subscriptions: Vec<SubscriptionInfo>,
    pub counters: Counters,
    pub gauges: Gauges,
}

/// Only export after all subscription receivers have parked. Freeze prevents
/// actor mutations; it does not secretly stop independent queue receivers.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouterImage {
    pub limits: Limits,
    pub queue_frame_target: usize,
    pub epoch: String,
    pub subscriptions: Vec<SubscriptionImage>,
    pub counters: Counters,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionImage {
    pub id: String,
    pub mode: Mode,
    pub active: bool,
    pub sessions: Vec<String>,
    pub frames: Vec<FrameImage>,
    pub direct: Option<FrameImage>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FrameImage(#[serde(with = "bus_proxy_protocol::buffer")] pub Vec<u8>);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Queue locks protect only short, non-awaiting bookkeeping. Even poisoning
    // must not prevent the actor's unwind guard from canceling every subscriber.
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

struct Backlog {
    frames: VecDeque<Bytes>,
    bytes: usize,
    closed: bool,
    target_frames: usize,
    waiting: bool,
    direct: Option<Bytes>,
}

struct Queue {
    backlog: Mutex<Backlog>,
    notify: Notify,
    canceled: CancellationToken,
    epoch_canceled: CancellationToken,
    max_bytes: usize,
}

impl Queue {
    fn new(limits: &Limits, epoch_canceled: CancellationToken) -> Self {
        Self {
            backlog: Mutex::new(Backlog {
                frames: VecDeque::new(),
                bytes: 0,
                closed: false,
                target_frames: limits.queue_frames,
                waiting: false,
                direct: None,
            }),
            notify: Notify::new(),
            canceled: epoch_canceled.child_token(),
            epoch_canceled,
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
            backlog.direct = None;
            backlog.waiting = false;
        }
        self.canceled.cancel();
    }

    fn offer(&self, frame: Bytes) -> Result<(), RouterError> {
        self.admit(frame, false)
    }

    fn opening(&self, frame: Bytes) -> Result<(), RouterError> {
        self.admit(frame, true)
    }

    fn set_target(&self, frames: usize) {
        lock(&self.backlog).target_frames = frames;
    }

    fn arm_owned_receive(&self) {
        let mut backlog = lock(&self.backlog);
        if !self.is_closed()
            && backlog.target_frames == 0
            && backlog.frames.is_empty()
            && backlog.direct.is_none()
        {
            backlog.waiting = true;
        }
    }

    fn admit(&self, frame: Bytes, opening: bool) -> Result<(), RouterError> {
        if self.is_closed() {
            return Err(RouterError::NotFound);
        }
        let mut backlog = lock(&self.backlog);
        if backlog.closed || self.is_closed() {
            return Err(RouterError::NotFound);
        }
        let direct_bytes = backlog.direct.as_ref().map_or(0, Bytes::len);
        if frame.len() > self.max_bytes - backlog.bytes - direct_bytes {
            return Err(RouterError::Overloaded);
        }
        if backlog.target_frames == 0 {
            if !backlog.frames.is_empty()
                || backlog.direct.is_some()
                || (!backlog.waiting && !opening)
            {
                return Err(RouterError::Overloaded);
            }
            backlog.direct = Some(frame);
            backlog.waiting = false; // Consume exactly one granted delivery credit.
        } else {
            if backlog.frames.len() >= backlog.target_frames {
                return Err(RouterError::Overloaded);
            }
            backlog.bytes += frame.len();
            backlog.frames.push_back(frame);
        }
        drop(backlog);
        self.notify.notify_one();
        Ok(())
    }

    async fn recv(&self) -> Option<Bytes> {
        struct Credit<'a>(&'a Queue);
        impl Drop for Credit<'_> {
            fn drop(&mut self) {
                lock(&self.0.backlog).waiting = false;
            }
        }
        loop {
            // One receiver (&mut Subscription) plus notify_one's saved permit
            // avoids a lost wake between an empty check and awaiting notification.
            let notified = self.notify.notified();
            {
                let mut backlog = lock(&self.backlog);
                if backlog.closed || self.is_closed() {
                    backlog.frames.clear();
                    backlog.bytes = 0;
                    backlog.direct = None;
                    return None;
                }
                if let Some(frame) = backlog.direct.take() {
                    backlog.waiting = false;
                    return Some(frame);
                }
                if let Some(frame) = backlog.frames.pop_front() {
                    backlog.waiting = false;
                    backlog.bytes -= frame.len();
                    return Some(frame);
                }
                backlog.waiting = true;
            }
            let _credit = Credit(self);
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

    fn try_recv(&self) -> Option<Bytes> {
        let mut backlog = lock(&self.backlog);
        backlog.waiting = false;
        if backlog.closed || self.is_closed() {
            return None;
        }
        if let Some(frame) = backlog.direct.take() {
            return Some(frame);
        }
        let frame = backlog.frames.pop_front()?;
        backlog.bytes -= frame.len();
        Some(frame)
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

    pub fn cancellation_token(&self) -> CancellationToken {
        self.queue.canceled.clone()
    }

    /// Cancel-safe dequeue. No frame is yielded after observing cancellation;
    /// frames already handed to a caller cannot be retracted by failure/reset.
    pub async fn recv(&mut self) -> Option<Bytes> {
        self.queue.recv().await
    }

    /// Drain only already admitted material; does not arm a new receive credit.
    pub fn try_recv(&mut self) -> Option<Bytes> {
        self.queue.try_recv()
    }

    /// Arm the first zero-queue receive synchronously before restarting a source.
    /// The transport owner must retain this guard and have no output in flight.
    /// Normal receives revoke idle credit on cancellation; dropping the guard
    /// closes its queue. This avoids an import task-scheduling admission race.
    pub fn arm_owned_receive(&mut self) {
        self.queue.arm_owned_receive();
    }

    /// Relinquish the old guard without closing its queue. Only the handover
    /// coordinator may use this, after exporting and irrevocably giving up I/O.
    pub fn relinquish(mut self) {
        self.commands = mpsc::channel(1).0;
        self.id.clear();
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if self.id.is_empty() {
            return;
        }
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
    QueueFrames(usize, Reply<()>),
    Publish(String, Route, Bytes, Reply<()>),
    Snapshot(Reply<Snapshot>),
    Freeze(usize, Reply<RouterImage>),
    Thaw(Reply<()>),
    Remove(String),
    #[cfg(test)]
    Panic,
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
            queue_frame_target: limits.queue_frames,
            limits,
            commands: commands.downgrade(),
            epoch_signal,
            epoch: None,
            subscribers: HashMap::new(),
            active: HashSet::new(),
            legacy: HashSet::new(),
            sessions: HashMap::new(),
            counters: Counters::default(),
            frozen: false,
        };
        let join = tokio::spawn(
            actor
                .run(receiver, shutdown)
                .instrument(tracing::info_span!("event.router")),
        );
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

    /// Atomic actor-owned target update across existing and future subscriptions.
    /// Existing admitted material is retained, even when occupancy exceeds target.
    pub async fn set_queue_frames(&self, frames: usize) -> Result<(), RouterError> {
        if frames > u32::MAX as usize {
            return Err(RouterError::InvalidLimits);
        }
        self.request(|reply| Command::QueueFrames(frames, reply))
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

    pub async fn freeze(&self) -> Result<RouterImage, RouterError> {
        self.freeze_bounded(usize::MAX).await
    }

    /// Conservative JSON/base64 budget, checked before copying queued frames.
    /// Call only after independent receivers have parked, as for `freeze`.
    pub async fn freeze_bounded(&self, json_budget: usize) -> Result<RouterImage, RouterError> {
        self.request(|reply| Command::Freeze(json_budget, reply))
            .await
    }

    pub async fn thaw(&self) -> Result<(), RouterError> {
        self.request(Command::Thaw).await
    }

    /// Rebuild indexes and guards without admitting new handshake frames or IDs.
    pub fn import(
        image: RouterImage,
        limits: Limits,
        shutdown: CancellationToken,
    ) -> Result<(Self, tokio::task::JoinHandle<()>, Vec<Subscription>), RouterError> {
        limits.validate()?;
        if image.limits != limits
            || image.queue_frame_target > u32::MAX as usize
            || image.epoch.is_empty()
            || image.subscriptions.len() > limits.max_subscribers
        {
            return Err(RouterError::InvalidImage);
        }
        let (commands, receiver) = mpsc::channel(limits.command_capacity);
        let token = CancellationToken::new();
        let epoch_signal = Arc::new(Mutex::new(Some((image.epoch.clone(), token.clone()))));
        let router = Self {
            commands: commands.clone(),
            epoch_signal: epoch_signal.clone(),
            shutdown: shutdown.clone(),
        };
        let mut actor = Actor {
            queue_frame_target: image.queue_frame_target,
            limits,
            commands: commands.downgrade(),
            epoch_signal,
            epoch: Some((image.epoch, token.clone())),
            subscribers: HashMap::new(),
            active: HashSet::new(),
            legacy: HashSet::new(),
            sessions: HashMap::new(),
            counters: image.counters,
            frozen: false,
        };
        let mut guards = Vec::new();
        for sub in image.subscriptions {
            if !sub.id.starts_with("evsub_")
                || sub.id.len() > 128
                || actor.subscribers.contains_key(&sub.id)
                || sub.sessions.len() > bus_proxy_protocol::SESSION_INTEREST_CAPACITY
                || sub
                    .sessions
                    .iter()
                    .any(|s| s.is_empty() || s.len() > bus_proxy_protocol::INTEREST_BYTE_CAPACITY)
                || sub.sessions.iter().collect::<HashSet<_>>().len() != sub.sessions.len()
                || (!sub.active && !sub.sessions.is_empty())
                || (sub.mode == Mode::Legacy && (!sub.active || !sub.sessions.is_empty()))
            {
                return Err(RouterError::InvalidImage);
            }
            let queue = Arc::new(Queue::new(&actor.limits, token.clone()));
            {
                let mut backlog = lock(&queue.backlog);
                backlog.target_frames = actor.queue_frame_target;
                // A lowered target is not a license to discard older admissions.
                for frame in sub.frames {
                    if frame.0.is_empty() || frame.0.len() > queue.max_bytes - backlog.bytes {
                        return Err(RouterError::InvalidImage);
                    }
                    backlog.bytes += frame.0.len();
                    backlog.frames.push_back(Bytes::from(frame.0));
                }
                if let Some(frame) = sub.direct {
                    if frame.0.is_empty() || frame.0.len() > queue.max_bytes - backlog.bytes {
                        return Err(RouterError::InvalidImage);
                    }
                    backlog.direct = Some(Bytes::from(frame.0));
                }
            }
            let span = tracing::info_span!("event.subscription", subscription_id = %sub.id, mode = ?sub.mode);
            if sub.active {
                actor.active.insert(sub.id.clone());
            }
            if sub.mode == Mode::Legacy {
                actor.legacy.insert(sub.id.clone());
            }
            for session in &sub.sessions {
                actor
                    .sessions
                    .entry(session.clone())
                    .or_default()
                    .insert(sub.id.clone());
            }
            guards.push(Subscription {
                id: sub.id.clone(),
                queue: queue.clone(),
                commands: commands.clone(),
            });
            actor.subscribers.insert(
                sub.id,
                Subscriber {
                    mode: sub.mode,
                    active: sub.active,
                    sessions: sub.sessions.into_iter().collect(),
                    queue,
                    span,
                },
            );
        }
        let join = tokio::spawn(
            actor
                .run(receiver, shutdown)
                .instrument(tracing::info_span!("event.router")),
        );
        Ok((router, join, guards))
    }
}

struct Subscriber {
    mode: Mode,
    active: bool,
    sessions: HashSet<String>,
    queue: Arc<Queue>,
    // Retained until removal/reset, not recreated during event admission.
    span: tracing::Span,
}

struct Actor {
    limits: Limits,
    queue_frame_target: usize,
    commands: mpsc::WeakSender<Command>,
    epoch_signal: EpochSignal,
    epoch: Option<(String, CancellationToken)>,
    subscribers: HashMap<String, Subscriber>,
    active: HashSet<String>,
    legacy: HashSet<String>,
    sessions: HashMap<String, HashSet<String>>,
    counters: Counters,
    frozen: bool,
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
                }, if !self.frozen => self.retire_epoch(),
                _ = reap.tick(), if !self.frozen => self.reap(),
                command = receiver.recv() => {
                    let Some(command) = command else { break };
                    if !self.frozen && self.epoch.as_ref().is_some_and(|(_, token)| token.is_cancelled()) {
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
            subscriber
                .span
                .in_scope(|| tracing::debug!("subscription invalidated by source retirement"));
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
        let _entered = subscriber.span.enter();
        tracing::debug!("removing event subscription");
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
        if self.frozen {
            match command {
                Command::Thaw(reply) => {
                    self.frozen = false;
                    let _ = reply.send(Ok(()));
                }
                Command::Snapshot(reply) => {
                    let _ = reply.send(Ok(self.snapshot()));
                }
                Command::Freeze(_, reply) => {
                    let _ = reply.send(Err(RouterError::Frozen));
                }
                Command::Begin(_, reply)
                | Command::End(_, reply)
                | Command::QueueFrames(_, reply)
                | Command::Replace(_, _, reply)
                | Command::Publish(_, _, _, reply) => {
                    let _ = reply.send(Err(RouterError::Frozen));
                }
                Command::Subscribe(_, reply) => {
                    let _ = reply.send(Err(RouterError::Frozen));
                }
                Command::Remove(_) => {}
                #[cfg(test)]
                Command::Panic => panic!("router fault-injection test"),
            }
            return;
        }
        match command {
            Command::Freeze(json_budget, reply) => {
                self.reap();
                let result = self.export(json_budget);
                if result.is_ok() {
                    self.frozen = true;
                }
                if reply.send(result).is_err() {
                    self.frozen = false;
                }
            }
            Command::Thaw(reply) => {
                let _ = reply.send(Ok(()));
            }
            Command::QueueFrames(frames, reply) => {
                self.queue_frame_target = frames;
                for sub in self.subscribers.values() {
                    sub.queue.set_target(frames);
                }
                let _ = reply.send(Ok(()));
            }
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
            #[cfg(test)]
            Command::Panic => panic!("router fault-injection test"),
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
        let id = format!("evsub_{}", uuid::Uuid::new_v4().simple());
        let span = tracing::info_span!("event.subscription", subscription_id = %id, ?mode);
        let queue = Arc::new(Queue::new(&self.limits, epoch_token.clone()));
        queue.set_target(self.queue_frame_target);
        queue.opening(match mode {
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
                span: span.clone(),
            },
        );
        span.in_scope(|| tracing::debug!("event subscription registered"));
        Ok(Subscription {
            id,
            queue,
            commands,
        })
    }

    fn replace(&mut self, id: &str, interest: Interest) -> Result<(), RouterError> {
        let subscriber = self.subscribers.get(id).ok_or(RouterError::NotFound)?;
        let span = tracing::info_span!(parent: &subscriber.span,
            "event.subscription.replace", interest_count = interest.sessions.len(),
            activating = !subscriber.active);
        let _entered = span.enter();
        if subscriber.queue.is_closed() || subscriber.mode != Mode::Controlled {
            return Err(RouterError::NotFound);
        }
        if !subscriber.active {
            if let Err(error) = subscriber.queue.opening(connected_frame()) {
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
        tracing::debug!("event interest replaced");
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
            let direct_bytes = lock(&subscriber.queue.backlog)
                .direct
                .as_ref()
                .map_or(0, Bytes::len);
            let receive_credit = lock(&subscriber.queue.backlog).waiting;
            let direct_frames = usize::from(direct_bytes > 0);
            let mut sessions: Vec<_> = subscriber.sessions.iter().cloned().collect();
            sessions.sort_unstable();
            subscriptions.push(SubscriptionInfo {
                id: id.clone(),
                mode: subscriber.mode,
                active: subscriber.active,
                sessions,
                backlog_frames,
                backlog_bytes,
                queue_frame_target: self.queue_frame_target,
                direct_frames,
                direct_bytes,
                receive_credit,
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
            gauges.direct_frames = gauges.direct_frames.saturating_add(direct_frames);
            gauges.direct_bytes = gauges.direct_bytes.saturating_add(direct_bytes);
        }
        subscriptions.sort_unstable_by(|a, b| a.id.cmp(&b.id));
        Snapshot {
            epoch: self.epoch.as_ref().map(|(epoch, _)| epoch.clone()),
            queue_frame_target: self.queue_frame_target,
            subscriptions,
            counters: self.counters.clone(),
            gauges,
        }
    }

    fn export(&self, json_budget: usize) -> Result<RouterImage, RouterError> {
        let epoch = self
            .epoch
            .as_ref()
            .filter(|(_, t)| !t.is_cancelled())
            .ok_or(RouterError::Unavailable)?
            .0
            .as_str();
        // Queue allowance can total many GiB even when frames physically share
        // storage. Refuse an oversized private image before fan-out duplication.
        // JSON strings take at most six bytes per UTF-8 input byte; base64 takes
        // four per three input bytes. Fixed margins include keys/array separators.
        let mut weight = 4096usize.saturating_add(epoch.len().saturating_mul(6));
        for (id, sub) in &self.subscribers {
            weight = weight
                .saturating_add(2048)
                .saturating_add(id.len().saturating_mul(6));
            for session in &sub.sessions {
                weight = weight
                    .saturating_add(4)
                    .saturating_add(session.len().saturating_mul(6));
            }
            let backlog = lock(&sub.queue.backlog);
            for frame in &backlog.frames {
                weight = weight
                    .saturating_add(4)
                    .saturating_add(frame.len().div_ceil(3).saturating_mul(4));
            }
            if let Some(frame) = &backlog.direct {
                weight = weight
                    .saturating_add(4)
                    .saturating_add(frame.len().div_ceil(3).saturating_mul(4));
            }
            if weight > json_budget {
                return Err(RouterError::ImageLimit);
            }
        }
        if weight > json_budget {
            return Err(RouterError::ImageLimit);
        }
        let mut subscriptions = Vec::new();
        for (id, sub) in &self.subscribers {
            let mut sessions: Vec<_> = sub.sessions.iter().cloned().collect();
            sessions.sort_unstable();
            let backlog = lock(&sub.queue.backlog);
            subscriptions.push(SubscriptionImage {
                id: id.clone(),
                mode: sub.mode,
                active: sub.active,
                sessions,
                frames: backlog
                    .frames
                    .iter()
                    .map(|b| FrameImage(b.to_vec()))
                    .collect(),
                direct: backlog
                    .direct
                    .as_ref()
                    .map(|frame| FrameImage(frame.to_vec())),
            });
        }
        subscriptions.sort_unstable_by(|a, b| a.id.cmp(&b.id));
        Ok(RouterImage {
            limits: self.limits.clone(),
            queue_frame_target: self.queue_frame_target,
            epoch: epoch.to_owned(),
            subscriptions,
            counters: self.counters.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::Future, task::Poll};

    async fn arm<F: Future>(future: &mut std::pin::Pin<Box<F>>) {
        std::future::poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }

    #[tokio::test]
    async fn zero_queue_credit_is_cancel_safe_bounded_and_openings_still_work() {
        let stop = CancellationToken::new();
        let (router, task) = Router::spawn(
            Limits {
                queue_frames: 0,
                ..Limits::default()
            },
            stop.clone(),
        )
        .unwrap();
        router.begin_epoch("one".into()).await.unwrap();
        let mut legacy = router.subscribe(Mode::Legacy).await.unwrap();
        let mut controlled = router.subscribe(Mode::Controlled).await.unwrap();
        assert!(legacy.recv().await.is_some());
        assert!(controlled.recv().await.is_some());
        router
            .replace(controlled.id().into(), Interest { sessions: vec![] })
            .await
            .unwrap();
        assert!(controlled.recv().await.is_some());
        let mut receiving = Box::pin(legacy.recv());
        arm(&mut receiving).await;
        router
            .publish(
                "one".into(),
                Route::Broadcast,
                Bytes::from_static(b"direct"),
            )
            .await
            .unwrap();
        // Another controlled zero-queue subscriber with no credit fails locally.
        assert!(controlled.recv().await.is_none());
        let snapshot = router.snapshot().await.unwrap();
        assert_eq!(snapshot.gauges.backlog_frames, 0);
        assert_eq!(snapshot.gauges.direct_frames, 1);
        drop(receiving); // The granted frame must not disappear with its receive future.
        assert_eq!(legacy.recv().await.unwrap(), b"direct"[..]);
        let mut canceled_credit = Box::pin(legacy.recv());
        arm(&mut canceled_credit).await;
        drop(canceled_credit);
        router
            .publish(
                "one".into(),
                Route::Broadcast,
                Bytes::from_static(b"no-credit"),
            )
            .await
            .unwrap();
        assert!(
            legacy.recv().await.is_none(),
            "canceling an empty receive revokes credit"
        );
        assert_eq!(router.snapshot().await.unwrap().counters.queue_overflows, 2);
        stop.cancel();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn live_shrink_drains_prior_admissions_and_policy_and_credit_survive_import() {
        let limits = Limits {
            queue_frames: 3,
            ..Limits::default()
        };
        let stop = CancellationToken::new();
        let (router, task) = Router::spawn(limits.clone(), stop.clone()).unwrap();
        router.begin_epoch("one".into()).await.unwrap();
        let mut sub = router.subscribe(Mode::Legacy).await.unwrap();
        sub.recv().await.unwrap();
        for bytes in ["first", "second", "third"] {
            router
                .publish("one".into(), Route::Broadcast, Bytes::from(bytes))
                .await
                .unwrap();
        }
        router.set_queue_frames(0).await.unwrap();
        let snapshot = router.snapshot().await.unwrap();
        assert_eq!(snapshot.queue_frame_target, 0);
        assert_eq!(
            snapshot.gauges.backlog_frames, 3,
            "shrink never discards queued bytes"
        );
        let image = router.freeze().await.unwrap();
        assert_eq!(router.set_queue_frames(2).await, Err(RouterError::Frozen));
        let id = sub.id().to_owned();
        sub.relinquish();
        stop.cancel();
        task.await.unwrap();
        let stop = CancellationToken::new();
        let (router, task, mut guards) =
            Router::import(image, limits.clone(), stop.clone()).unwrap();
        let mut sub = guards.pop().unwrap();
        assert_eq!(sub.id(), id);
        for expected in ["first", "second", "third"] {
            assert_eq!(sub.recv().await.unwrap(), expected);
        }
        let mut receiving = Box::pin(sub.recv());
        arm(&mut receiving).await;
        router
            .publish(
                "one".into(),
                Route::Broadcast,
                Bytes::from_static(b"credit-at-cut"),
            )
            .await
            .unwrap();
        drop(receiving);
        let image = router.freeze().await.unwrap();
        assert!(image.subscriptions[0].direct.is_some());
        sub.relinquish();
        stop.cancel();
        task.await.unwrap();
        let stop = CancellationToken::new();
        let (router, task, mut guards) = Router::import(image, limits, stop.clone()).unwrap();
        let mut sub = guards.pop().unwrap();
        assert_eq!(sub.recv().await.unwrap(), b"credit-at-cut"[..]);
        router.set_queue_frames(2).await.unwrap();
        router
            .publish(
                "one".into(),
                Route::Broadcast,
                Bytes::from_static(b"grown-one"),
            )
            .await
            .unwrap();
        router
            .publish(
                "one".into(),
                Route::Broadcast,
                Bytes::from_static(b"grown-two"),
            )
            .await
            .unwrap();
        assert_eq!(sub.recv().await.unwrap(), b"grown-one"[..]);
        assert_eq!(sub.recv().await.unwrap(), b"grown-two"[..]);
        assert_eq!(router.snapshot().await.unwrap().queue_frame_target, 2);
        stop.cancel();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn handover_preserves_ids_pending_interests_order_and_limits() {
        let (old, stop, task) = router(16).await;
        let active = old.subscribe(Mode::Controlled).await.unwrap();
        let pending = old.subscribe(Mode::Controlled).await.unwrap();
        let legacy = old.subscribe(Mode::Legacy).await.unwrap();
        let id = active.id().to_owned();
        old.replace(
            id.clone(),
            Interest {
                sessions: vec!["ses_a".into()],
            },
        )
        .await
        .unwrap();
        old.publish(
            "one".into(),
            Route::Session {
                session_id: Some("ses_a".into()),
            },
            Bytes::from_static(b"queued-one"),
        )
        .await
        .unwrap();
        let image = old.freeze().await.unwrap();
        assert_eq!(
            old.publish("one".into(), Route::Broadcast, Bytes::new())
                .await,
            Err(RouterError::Frozen)
        );
        let image: RouterImage =
            serde_json::from_slice(&serde_json::to_vec(&image).unwrap()).unwrap();
        let limits = image.limits.clone();
        assert!(
            !image
                .subscriptions
                .iter()
                .find(|s| s.id == pending.id())
                .unwrap()
                .active
        );
        // Real transport owners park their guards; no normal drop before export.
        active.relinquish();
        pending.relinquish();
        legacy.relinquish();
        stop.cancel();
        task.await.unwrap();
        let new_stop = CancellationToken::new();
        let (new, new_task, mut guards) = Router::import(image, limits, new_stop.clone()).unwrap();
        let sub = guards.iter_mut().find(|s| s.id() == id).unwrap();
        assert!(
            std::str::from_utf8(&sub.recv().await.unwrap())
                .unwrap()
                .contains(&id)
        );
        assert!(
            std::str::from_utf8(&sub.recv().await.unwrap())
                .unwrap()
                .contains("server.connected")
        );
        assert_eq!(sub.recv().await.unwrap(), b"queued-one"[..]);
        new.publish(
            "one".into(),
            Route::Session {
                session_id: Some("ses_b".into()),
            },
            Bytes::from_static(b"not-selected"),
        )
        .await
        .unwrap();
        new.publish(
            "one".into(),
            Route::Session {
                session_id: Some("ses_a".into()),
            },
            Bytes::from_static(b"queued-two"),
        )
        .await
        .unwrap();
        assert_eq!(sub.recv().await.unwrap(), b"queued-two"[..]);
        let snapshot = new.snapshot().await.unwrap();
        assert_eq!(snapshot.gauges.pending, 1);
        assert_eq!(snapshot.counters.epochs_started, 1);
        new_stop.cancel();
        new_task.await.unwrap();
    }

    #[tokio::test]
    async fn aborted_freeze_thaws_without_retirement() {
        let (router, stop, task) = router(8).await;
        let mut sub = router.subscribe(Mode::Legacy).await.unwrap();
        router.freeze().await.unwrap();
        router.thaw().await.unwrap();
        assert!(
            std::str::from_utf8(&sub.recv().await.unwrap())
                .unwrap()
                .contains("server.connected")
        );
        assert_eq!(router.snapshot().await.unwrap().counters.epochs_ended, 0);
        stop.cancel();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn bounded_freeze_rejects_before_copying_and_does_not_retire_or_freeze() {
        let (router, stop, task) = router(8).await;
        let mut sub = router.subscribe(Mode::Legacy).await.unwrap();
        let ready = sub.recv().await.unwrap();
        router
            .publish(
                "one".into(),
                Route::Broadcast,
                Bytes::from_static(b"exact-backlog"),
            )
            .await
            .unwrap();
        assert!(matches!(
            router.freeze_bounded(1).await,
            Err(RouterError::ImageLimit)
        ));
        assert_eq!(sub.recv().await.unwrap(), b"exact-backlog"[..]);
        router
            .publish("one".into(), Route::Broadcast, ready)
            .await
            .unwrap();
        let image = router.freeze_bounded(65536).await.unwrap();
        assert!(serde_json::to_vec(&image).unwrap().len() <= 65536);
        assert_eq!(image.counters.epochs_ended, 0);
        router.thaw().await.unwrap();
        stop.cancel();
        task.await.unwrap();
    }

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

    #[tokio::test]
    async fn actor_panic_unwind_guard_discards_backlog_and_wakes_receivers() {
        let (router, shutdown, task) = router(2).await;
        let mut subscription = router.subscribe(Mode::Legacy).await.unwrap();
        router
            .commands
            .try_send(Command::Panic)
            .unwrap_or_else(|_| panic!("empty mailbox"));
        assert!(task.await.unwrap_err().is_panic());
        assert!(subscription.recv().await.is_none());
        assert_eq!(subscription.queue.sample(), (0, 0));
        assert_eq!(router.snapshot().await.unwrap_err(), RouterError::Closed);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn old_end_command_cannot_retire_new_epoch() {
        let (router, shutdown, task) = router(2).await;
        let (begin_reply, begin_ack) = oneshot::channel();
        let (end_reply, end_ack) = oneshot::channel();
        router
            .commands
            .try_send(Command::Begin("two".into(), begin_reply))
            .unwrap_or_else(|_| panic!("empty mailbox"));
        router
            .commands
            .try_send(Command::End("one".into(), end_reply))
            .unwrap_or_else(|_| panic!("one available slot"));
        begin_ack.await.unwrap().unwrap();
        end_ack.await.unwrap().unwrap();
        assert_eq!(
            router.snapshot().await.unwrap().epoch.as_deref(),
            Some("two")
        );
        shutdown.cancel();
        task.await.unwrap();
    }
}
