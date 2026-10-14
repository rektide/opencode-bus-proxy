//! Discovery process bridge, not another filesystem watcher implementation. Reuses
//! OpenCode's Effect FileSystemWatchClient in a supervised Bun child.
use std::{path::PathBuf, process::Stdio, time::Duration};

use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct Options {
    pub helper: PathBuf,
    pub source: PathBuf,
}

pub enum Event {
    Ready,
    Change,
    Failed,
    Disabled,
}

pub async fn supervise(
    options: Options,
    file: PathBuf,
    events: mpsc::Sender<Event>,
    shutdown: CancellationToken,
) {
    let mut attempt = 0u32;
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        let child = Command::new("bun")
            .arg(&options.helper)
            .arg(&file)
            .arg(&options.source)
            .env_remove("OPENCODE_PASSWORD")
            .env_remove("OPENCODE_SERVER_PASSWORD")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn();
        if let Ok(mut child) = child {
            let mut lines = BufReader::new(child.stdout.take().expect("piped stdout")).lines();
            let mut disabled = false;
            loop {
                let line = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    line = lines.next_line() => line,
                };
                let Ok(Some(line)) = line else {
                    break;
                };
                match line.as_str() {
                    "ready" => {
                        let _ = events.try_send(Event::Ready);
                    }
                    "change" => {
                        let _ = events.try_send(Event::Change);
                    }
                    "disabled" => {
                        disabled = true;
                        let _ = events.try_send(Event::Disabled);
                        break;
                    }
                    _ => break,
                }
            }
            if let Some(pid) = child.id() {
                // SAFETY: the child is still owned here; SIGTERM lets its Effect
                // runtime dispose watcher scopes before forced cleanup.
                unsafe {
                    libc::kill(pid as libc::pid_t, libc::SIGTERM);
                }
            }
            if tokio::time::timeout(Duration::from_secs(2), child.wait())
                .await
                .is_err()
            {
                let _ = child.kill().await;
            }
            if disabled || shutdown.is_cancelled() {
                return;
            }
        }
        let _ = events.try_send(Event::Failed);
        if attempt == 0 {
            tracing::warn!("Effect discovery watcher unavailable; polling and retrying helper");
        }
        let delay = Duration::from_secs((1u64 << attempt.min(4)).min(15));
        attempt = attempt.saturating_add(1);
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(delay) => {},
        }
    }
}
