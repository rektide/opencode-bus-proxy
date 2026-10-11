use std::error::Error;

use bus_proxy_router::Router;
use bus_proxy_service_manager::Manager;
use clap::Parser;
use tokio_util::sync::CancellationToken;

mod configuration;
mod supervision;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

fn main() -> Result<()> {
    let args = configuration::Arguments::parse();
    configuration::initialize_tracing(&args)?;
    // Resolve credentials before activation errors, as before. The temporary
    // runtime is fully dropped before consuming process-wide activation state.
    let config = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(args.resolve())?;
    // SAFETY: no runtime or application threads are alive; descriptor 3 is the
    // service manager's exclusive handover when LISTEN_PID matches this process.
    let manager = unsafe { Manager::capture()? };
    tokio::runtime::Runtime::new()?.block_on(run(config, manager))
}

async fn run(config: configuration::Configuration, mut manager: Manager) -> Result<()> {
    let listener = manager.acquire_listener(config.listen).await?;
    let address = listener.local_addr()?;
    // Validate and acquire discovery's ownership lock before starting serving.
    let registration = config.discovery.prepare(&config.gateway, address)?;

    let shutdown = CancellationToken::new();
    let (router, routing) = Router::spawn(config.limits, shutdown.clone())?;
    let gateway = tokio::spawn(bus_proxy_gateway::serve(
        listener,
        config.gateway,
        router,
        shutdown.clone(),
    ));
    tracing::info!(%address, "event gateway listening");

    let registration = registration.map(|registration| {
        tokio::spawn(registration.run(shutdown.clone(), config.discovery.watcher))
    });
    let _heartbeat = manager.ready(address, shutdown.clone());

    supervision::Tasks {
        gateway,
        routing,
        registration,
    }
    .supervise(&manager, shutdown)
    .await
}
