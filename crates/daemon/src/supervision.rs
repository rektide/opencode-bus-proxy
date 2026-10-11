//! Application-specific exit policy and bounded cleanup, not a task framework.

use std::time::Duration;

use bus_proxy_service_manager::Manager;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::Result;

pub struct Tasks {
    pub gateway: JoinHandle<std::result::Result<(), bus_proxy_gateway::GatewayError>>,
    pub routing: JoinHandle<()>,
    pub registration: Option<JoinHandle<()>>,
}

impl Tasks {
    pub async fn supervise(mut self, manager: &Manager, shutdown: CancellationToken) -> Result<()> {
        enum Stopped {
            Signal(std::io::Result<()>),
            Gateway(
                std::result::Result<
                    std::result::Result<(), bus_proxy_gateway::GatewayError>,
                    tokio::task::JoinError,
                >,
            ),
            Router(std::result::Result<(), tokio::task::JoinError>),
            Registration(std::result::Result<(), tokio::task::JoinError>),
        }
        let stopped = tokio::select! {
            result = stop_signal() => Stopped::Signal(result),
            result = &mut self.gateway => Stopped::Gateway(result),
            result = &mut self.routing => Stopped::Router(result),
            result = async {
                match &mut self.registration {
                    Some(task) => task.await,
                    None => std::future::pending().await,
                }
            } => Stopped::Registration(result),
        };
        manager.stopping();
        shutdown.cancel();
        tracing::info!("event gateway stopping");
        if !matches!(stopped, Stopped::Registration(_))
            && let Some(task) = &mut self.registration
        {
            join_shutdown(task).await?;
        }
        match stopped {
            Stopped::Signal(result) => {
                result?;
                let (gateway, router) = tokio::join!(
                    join_shutdown(&mut self.gateway),
                    join_shutdown(&mut self.routing),
                );
                gateway??;
                router?;
            }
            Stopped::Gateway(result) => {
                join_shutdown(&mut self.routing).await?;
                result??;
            }
            Stopped::Router(result) => {
                join_shutdown(&mut self.gateway).await??;
                result?;
                return Err("routing actor exited unexpectedly".into());
            }
            Stopped::Registration(result) => {
                let (gateway, router) = tokio::join!(
                    join_shutdown(&mut self.gateway),
                    join_shutdown(&mut self.routing),
                );
                gateway??;
                router?;
                result?;
                return Err("discovery contribution task exited unexpectedly".into());
            }
        }
        Ok(())
    }
}

async fn join_shutdown<T>(task: &mut JoinHandle<T>) -> Result<T> {
    match tokio::time::timeout(Duration::from_secs(10), &mut *task).await {
        Ok(result) => Ok(result?),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err("task exceeded shutdown grace".into())
        }
    }
}

async fn stop_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    struct Dropped(Arc<AtomicBool>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_grace_aborts_and_reaps_a_stuck_task() {
        let dropped = Arc::new(AtomicBool::new(false));
        let marker = Dropped(dropped.clone());
        let mut task = tokio::spawn(async move {
            let _marker = marker;
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;
        let error = join_shutdown(&mut task).await.unwrap_err();
        assert_eq!(error.to_string(), "task exceeded shutdown grace");
        assert!(dropped.load(Ordering::SeqCst));
        assert!(task.is_finished());
    }

    #[tokio::test]
    async fn shutdown_keeps_task_results_and_panics_visible() {
        let mut task = tokio::spawn(async { 42 });
        assert_eq!(join_shutdown(&mut task).await.unwrap(), 42);
        let mut task = tokio::spawn(async { panic!("task failure") });
        let error = join_shutdown(&mut task).await.unwrap_err();
        assert!(
            error
                .downcast_ref::<tokio::task::JoinError>()
                .unwrap()
                .is_panic()
        );
    }
}
