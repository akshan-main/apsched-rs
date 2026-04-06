//! Standalone APScheduler daemon binary.
//!
//! Reads job definitions from a YAML/JSON config file, runs the Rust scheduler
//! engine directly (no Python, no GIL), executes jobs as shell commands or HTTP
//! webhook calls, and exposes an HTTP admin API for job management.

mod api;
mod config;
mod executor;
mod history;
mod metrics;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use apsched_core::model::{CallableRef, CoalescePolicy, ScheduleSpec, SchedulerConfig, TaskSpec};
use apsched_core::{SchedulerEngine, WallClock};
use apsched_store::MemoryJobStore;

use crate::api::{config_to_trigger_state, AppState};
use crate::config::{ActionConfig, DaemonConfig};
use crate::executor::execute_action;
use crate::history::ExecutionHistory;
use crate::metrics::Metrics;

#[derive(Parser)]
#[command(name = "apscheduler-daemon")]
#[command(about = "Standalone APScheduler daemon - a high-performance job scheduler")]
#[command(version)]
struct Cli {
    /// Path to config file (YAML or JSON)
    #[arg(short, long, default_value = "config.yaml")]
    config: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,

    /// Validate config and exit without starting the daemon
    #[arg(long)]
    validate: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialize tracing
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cli.log_level));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(false)
        .init();

    // Load config
    let config_path = std::path::Path::new(&cli.config);
    let daemon_config = DaemonConfig::load(config_path)?;

    // Validate config
    daemon_config.validate()?;
    info!("configuration validated successfully");

    if cli.validate {
        info!(
            "config file '{}' is valid ({} jobs defined)",
            cli.config,
            daemon_config.jobs.len()
        );
        return Ok(());
    }

    // Build scheduler config
    let coalesce = if daemon_config.scheduler.coalesce {
        CoalescePolicy::On
    } else {
        CoalescePolicy::Off
    };

    let scheduler_config = SchedulerConfig::builder()
        .timezone(&daemon_config.scheduler.timezone)
        .misfire_grace_time(Duration::from_secs(
            daemon_config.scheduler.misfire_grace_time,
        ))
        .coalesce(coalesce)
        .build()?;

    // Create scheduler engine
    let clock = Arc::new(WallClock);
    let engine = Arc::new(SchedulerEngine::new(scheduler_config, clock));

    // Create store
    // For this initial implementation, we use MemoryJobStore. SQLite/Postgres/Redis
    // stores from apsched-store can be wired in based on config.store.store_type.
    let store: Arc<dyn apsched_core::traits::JobStore> =
        match daemon_config.store.store_type.as_str() {
            "memory" => Arc::new(MemoryJobStore::new()),
            "sqlite" | "postgres" | "redis" => {
                warn!(
                    "store type '{}' requested but falling back to memory store for now. \
                 SQLite/Postgres/Redis stores are available in the apsched-store crate.",
                    daemon_config.store.store_type
                );
                Arc::new(MemoryJobStore::new())
            }
            other => {
                anyhow::bail!("unsupported store type: '{}'", other);
            }
        };

    // Register store and a default executor
    engine.add_jobstore(store.clone(), "default")?;

    let executor = Arc::new(apsched_executors::ThreadPoolExecutor::new(32));
    engine.add_executor(executor, "default")?;

    // Create shared state
    let history = Arc::new(ExecutionHistory::new(100));
    let metrics = Arc::new(Metrics::new());
    let actions: Arc<RwLock<HashMap<String, ActionConfig>>> = Arc::new(RwLock::new(HashMap::new()));

    // Load jobs from config
    let mut job_count = 0;
    for job_config in &daemon_config.jobs {
        match load_job_from_config(job_config, &daemon_config) {
            Ok((spec, action)) => {
                actions.write().await.insert(job_config.id.clone(), action);
                match engine.add_job(spec).await {
                    Ok(()) => {
                        info!(job_id = %job_config.id, "loaded job from config");
                        job_count += 1;
                    }
                    Err(e) => {
                        error!(job_id = %job_config.id, "failed to add job: {}", e);
                    }
                }
            }
            Err(e) => {
                error!(job_id = %job_config.id, "failed to parse job config: {}", e);
            }
        }
    }

    metrics.jobs_total.store(job_count, Ordering::Relaxed);
    info!("loaded {} jobs from config", job_count);

    // Set up the daemon action executor.
    // We register an event listener on the scheduler that runs daemon actions
    // when jobs are submitted (the scheduler's built-in executor calls
    // submit_job which triggers results; here we hook into the event bus to
    // also fire our shell/http actions).
    {
        let history_clone = history.clone();
        let metrics_clone = metrics.clone();
        let actions_clone = actions.clone();
        let event_bus = engine.event_bus().clone();

        event_bus.add_listener(
            Arc::new(move |event| {
                if let apsched_core::SchedulerEvent::JobSubmitted { schedule_id, .. } = event {
                    let schedule_id = schedule_id.clone();
                    let actions = actions_clone.clone();
                    let history = history_clone.clone();
                    let metrics = metrics_clone.clone();

                    tokio::spawn(async move {
                        let action = {
                            let map = actions.read().await;
                            map.get(&schedule_id).cloned()
                        };
                        if let Some(action) = action {
                            execute_action(&schedule_id, &action, &history, &metrics).await;
                        }
                    });
                }
            }),
            apsched_core::event::EVENT_JOB_SUBMITTED,
        );
    }

    // Start the scheduler engine
    engine.start().await?;
    info!("scheduler engine started");

    // Start HTTP API server
    if daemon_config.api.enabled {
        let app_state = AppState {
            engine: engine.clone(),
            store: store.clone(),
            history: history.clone(),
            metrics: metrics.clone(),
            actions: actions.clone(),
        };

        let router = api::create_router(app_state);

        let bind_addr = &daemon_config.api.bind;
        let listener = tokio::net::TcpListener::bind(bind_addr).await?;
        info!("HTTP API listening on {}", bind_addr);

        // Run API server in background
        let api_handle = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, router).await {
                error!("API server error: {}", e);
            }
        });

        // Wait for shutdown signal
        let shutdown = tokio::signal::ctrl_c();
        tokio::select! {
            _ = shutdown => {
                info!("received shutdown signal");
            }
            _ = api_handle => {
                warn!("API server exited unexpectedly");
            }
        }
    } else {
        info!("HTTP API disabled, running scheduler only");
        // Wait for shutdown signal
        tokio::signal::ctrl_c().await?;
        info!("received shutdown signal");
    }

    // Graceful shutdown
    info!("shutting down scheduler...");
    engine.shutdown(true).await?;
    info!("daemon stopped");

    Ok(())
}

/// Parse a job config entry into a ScheduleSpec and ActionConfig.
fn load_job_from_config(
    job_config: &config::JobConfig,
    daemon_config: &DaemonConfig,
) -> anyhow::Result<(ScheduleSpec, ActionConfig)> {
    let trigger_state = config_to_trigger_state(&job_config.trigger)
        .map_err(|e| anyhow::anyhow!("trigger error for job '{}': {}", job_config.id, e))?;

    let now = chrono::Utc::now();
    let next_fire_time = trigger_state.compute_next_fire_time(now, now);

    let task = TaskSpec::new(CallableRef::ImportPath(format!("daemon:{}", job_config.id)));

    let grace_time = job_config
        .misfire_grace_time
        .unwrap_or(daemon_config.scheduler.misfire_grace_time);

    let coalesce = if daemon_config.scheduler.coalesce {
        CoalescePolicy::On
    } else {
        CoalescePolicy::Off
    };

    let mut spec = ScheduleSpec::new(job_config.id.clone(), task, trigger_state);
    spec.name = job_config.name.clone();
    spec.next_run_time = next_fire_time;
    spec.misfire_grace_time = Some(Duration::from_secs(grace_time));
    spec.coalesce = coalesce;
    spec.replace_existing = true;
    if let Some(max) = job_config.max_instances {
        spec.max_instances = max;
    }

    Ok((spec, job_config.action.clone()))
}
