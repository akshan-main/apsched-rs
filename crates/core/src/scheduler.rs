use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::clock::{Clock, WallClock};
use crate::error::SchedulerError;
use crate::event::{EventBus, ListenerId, SchedulerEvent};
use crate::model::{
    CompletionStatus, DeadLetterEntry, JobChanges, JobCompletion, JobResultEnvelope, JobSpec,
    ScheduleSpec, SchedulerConfig, SchedulerState, TaskSpec,
};
use crate::traits::{Executor, JobStore};

/// The core scheduler engine. Owns the main scheduling loop, job stores,
/// executors, and the event bus. All public methods are synchronous and
/// safe to call from any thread.
pub struct SchedulerEngine {
    config: SchedulerConfig,
    scheduler_id: String,
    state: Arc<RwLock<SchedulerState>>,
    stores: Arc<RwLock<HashMap<String, Arc<dyn JobStore>>>>,
    executors: Arc<RwLock<HashMap<String, Arc<dyn Executor>>>>,
    event_bus: Arc<EventBus>,
    clock: Arc<dyn Clock>,
    wakeup_notify: Arc<tokio::sync::Notify>,
    shutdown_notify: Arc<tokio::sync::Notify>,
    running_instances: Arc<DashMap<String, u32>>,
    runtime_handle: Arc<Mutex<Option<tokio::runtime::Handle>>>,
    /// Channel for receiving job results from executors.
    result_tx: mpsc::Sender<JobResultEnvelope>,
    result_rx: Arc<Mutex<Option<mpsc::Receiver<JobResultEnvelope>>>>,
    /// Dead letter queue for jobs that failed after exhausting retries.
    dead_letter: Arc<RwLock<VecDeque<DeadLetterEntry>>>,
    dead_letter_max: usize,
    /// Sliding window rate limiter: schedule_id -> timestamps of recent executions.
    rate_windows: Arc<DashMap<String, VecDeque<DateTime<Utc>>>>,
    /// Running instance counts per concurrency group.
    group_running: Arc<DashMap<String, AtomicU32>>,
    /// Most recent completion record per schedule_id, used to evaluate
    /// DAG-style job dependencies (`depends_on`).
    job_completions: Arc<DashMap<String, JobCompletion>>,
}

impl std::fmt::Debug for SchedulerEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchedulerEngine")
            .field("scheduler_id", &self.scheduler_id)
            .field("state", &*self.state.read())
            .field("config", &self.config)
            .finish()
    }
}

impl SchedulerEngine {
    /// Create a new scheduler engine with the given configuration and clock.
    pub fn new(config: SchedulerConfig, clock: Arc<dyn Clock>) -> Self {
        let (result_tx, result_rx) = mpsc::channel(4096);
        Self {
            config,
            scheduler_id: Uuid::new_v4().to_string(),
            state: Arc::new(RwLock::new(SchedulerState::Stopped)),
            stores: Arc::new(RwLock::new(HashMap::new())),
            executors: Arc::new(RwLock::new(HashMap::new())),
            event_bus: Arc::new(EventBus::new()),
            clock,
            wakeup_notify: Arc::new(tokio::sync::Notify::new()),
            shutdown_notify: Arc::new(tokio::sync::Notify::new()),
            running_instances: Arc::new(DashMap::new()),
            runtime_handle: Arc::new(Mutex::new(None)),
            result_tx,
            result_rx: Arc::new(Mutex::new(Some(result_rx))),
            dead_letter: Arc::new(RwLock::new(VecDeque::new())),
            dead_letter_max: 1000,
            rate_windows: Arc::new(DashMap::new()),
            group_running: Arc::new(DashMap::new()),
            job_completions: Arc::new(DashMap::new()),
        }
    }

    /// Return the most recent completion record for a schedule, if any.
    pub fn job_completion(&self, schedule_id: &str) -> Option<JobCompletion> {
        self.job_completions.get(schedule_id).map(|v| v.clone())
    }

    /// Return a reference to the completion map (used by the loop context).
    #[allow(dead_code)]
    pub(crate) fn job_completions_arc(&self) -> &Arc<DashMap<String, JobCompletion>> {
        &self.job_completions
    }

    /// Create a new scheduler with default configuration and wall clock.
    pub fn with_defaults() -> Self {
        Self::new(SchedulerConfig::default(), Arc::new(WallClock))
    }

    /// Returns the unique ID of this scheduler instance.
    pub fn scheduler_id(&self) -> &str {
        &self.scheduler_id
    }

    /// Returns the current state of the scheduler.
    pub fn state(&self) -> SchedulerState {
        *self.state.read()
    }

    /// Returns a reference to the event bus.
    pub fn event_bus(&self) -> &Arc<EventBus> {
        &self.event_bus
    }

    /// Returns a reference to the clock.
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    /// Returns a reference to the config.
    pub fn config(&self) -> &SchedulerConfig {
        &self.config
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /// Start the scheduler. Spawns the main scheduling loop on the tokio runtime.
    ///
    /// The caller must ensure a tokio runtime is available (either by calling this
    /// inside a `#[tokio::main]` or by providing a runtime handle).
    pub async fn start(&self) -> Result<(), SchedulerError> {
        {
            let mut state = self.state.write();
            match *state {
                SchedulerState::Running | SchedulerState::Starting => {
                    return Err(SchedulerError::AlreadyRunning);
                }
                _ => {
                    *state = SchedulerState::Starting;
                }
            }
        }

        // Store the runtime handle for later use
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let mut rt = self.runtime_handle.lock().unwrap();
            *rt = Some(handle.clone());
        }

        // Bug fix: clean up stale leases from crashed schedulers before starting.
        {
            let now = self.clock.now();
            let store_snapshot: Vec<(String, Arc<dyn JobStore>)> = {
                let stores = self.stores.read();
                stores
                    .iter()
                    .map(|(k, v)| (k.clone(), Arc::clone(v)))
                    .collect()
            };
            for (alias, store) in &store_snapshot {
                match store.cleanup_stale_leases(now).await {
                    Ok(count) if count > 0 => {
                        tracing::info!(
                            store = alias.as_str(),
                            "cleaned up {} stale lease(s) on startup",
                            count
                        );
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(
                            store = alias.as_str(),
                            "failed to clean up stale leases: {}",
                            e
                        );
                    }
                }
            }
        }

        // Start all executors (clone out of lock to avoid holding it across await)
        {
            let executor_snapshot: Vec<(String, Arc<dyn Executor>)> = {
                let executors = self.executors.read();
                executors
                    .iter()
                    .map(|(k, v)| (k.clone(), Arc::clone(v)))
                    .collect()
            };
            for (alias, executor) in &executor_snapshot {
                executor.start().await.map_err(|e| {
                    tracing::error!(executor = alias.as_str(), "failed to start executor: {}", e);
                    SchedulerError::ExecutorError(e)
                })?;
            }
        }

        // Transition to Running
        {
            let mut state = self.state.write();
            *state = SchedulerState::Running;
        }

        self.event_bus.emit(&SchedulerEvent::SchedulerStarted);

        // Spawn the main scheduling loop
        let engine = SchedulerLoopContext {
            state: Arc::clone(&self.state),
            stores: Arc::clone(&self.stores),
            executors: Arc::clone(&self.executors),
            event_bus: Arc::clone(&self.event_bus),
            dead_letter: Arc::clone(&self.dead_letter),
            dead_letter_max: self.dead_letter_max,
            rate_windows: Arc::clone(&self.rate_windows),
            group_running: Arc::clone(&self.group_running),
            job_completions: Arc::clone(&self.job_completions),
            clock: Arc::clone(&self.clock),
            wakeup_notify: Arc::clone(&self.wakeup_notify),
            shutdown_notify: Arc::clone(&self.shutdown_notify),
            running_instances: Arc::clone(&self.running_instances),
            result_tx: self.result_tx.clone(),
            scheduler_id: self.scheduler_id.clone(),
            config: self.config.clone(),
        };

        // Take the result_rx out so the loop can own it
        let result_rx = self.result_rx.lock().unwrap().take();

        tokio::spawn(async move {
            engine.run_loop(result_rx).await;
        });

        Ok(())
    }

    /// Signal the scheduler to shut down.
    ///
    /// If `wait` is true, this will block until all executors have finished their
    /// current jobs. If false, it signals shutdown and returns immediately.
    pub async fn shutdown(&self, wait: bool) -> Result<(), SchedulerError> {
        {
            let mut state = self.state.write();
            match *state {
                SchedulerState::Stopped => return Err(SchedulerError::NotRunning),
                SchedulerState::ShuttingDown => return Ok(()),
                _ => {
                    *state = SchedulerState::ShuttingDown;
                }
            }
        }

        // Signal the run loop to stop
        self.shutdown_notify.notify_one();
        self.wakeup_notify.notify_one();

        // Shut down executors (clone out of lock to avoid holding across await)
        {
            let executor_snapshot: Vec<Arc<dyn Executor>> = {
                let executors = self.executors.read();
                executors.values().cloned().collect()
            };
            for executor in &executor_snapshot {
                if let Err(e) = executor.shutdown(wait).await {
                    tracing::error!("error shutting down executor: {}", e);
                }
            }
        }

        {
            let mut state = self.state.write();
            *state = SchedulerState::Stopped;
        }

        self.event_bus.emit(&SchedulerEvent::SchedulerShutdown);

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Job management
    // -----------------------------------------------------------------------

    /// Add a job schedule. If `replace_existing` is true on the spec and a job with the
    /// same ID already exists, it will be replaced.
    pub async fn add_job(&self, schedule: ScheduleSpec) -> Result<(), SchedulerError> {
        // Bug fix: reject new jobs during shutdown to prevent silent job loss.
        {
            let state = self.state.read();
            if *state == SchedulerState::ShuttingDown {
                return Err(SchedulerError::SchedulerShuttingDown);
            }
        }

        let store_alias = schedule.jobstore.clone();
        let job_id = schedule.id.clone();

        let store = self.get_store(&store_alias)?;

        // Check for duplicates
        match store.get_job(&job_id).await {
            Ok(_existing) => {
                if schedule.replace_existing {
                    store
                        .update_job(schedule)
                        .await
                        .map_err(SchedulerError::StoreError)?;
                    self.event_bus.emit(&SchedulerEvent::JobModified {
                        job_id: job_id.clone(),
                        jobstore: store_alias.clone(),
                    });
                } else {
                    return Err(SchedulerError::DuplicateJobId { job_id });
                }
            }
            Err(_) => {
                store
                    .add_job(schedule)
                    .await
                    .map_err(SchedulerError::StoreError)?;
                self.event_bus.emit(&SchedulerEvent::JobAdded {
                    job_id: job_id.clone(),
                    jobstore: store_alias.clone(),
                });
            }
        }

        self.wakeup();
        Ok(())
    }

    /// Remove a job by ID. If `jobstore` is None, search all stores.
    pub async fn remove_job(
        &self,
        job_id: &str,
        jobstore: Option<&str>,
    ) -> Result<(), SchedulerError> {
        if let Some(alias) = jobstore {
            let store = self.get_store(alias)?;
            store
                .remove_job(job_id)
                .await
                .map_err(SchedulerError::StoreError)?;
            self.event_bus.emit(&SchedulerEvent::JobRemoved {
                job_id: job_id.to_string(),
                jobstore: alias.to_string(),
            });
        } else {
            // Clone stores out of the lock so we don't hold it across await.
            let store_snapshot: Vec<(String, Arc<dyn JobStore>)> = {
                let stores = self.stores.read();
                stores
                    .iter()
                    .map(|(k, v)| (k.clone(), Arc::clone(v)))
                    .collect()
            };
            let mut found = false;
            for (alias, store) in &store_snapshot {
                if store.remove_job(job_id).await.is_ok() {
                    self.event_bus.emit(&SchedulerEvent::JobRemoved {
                        job_id: job_id.to_string(),
                        jobstore: alias.clone(),
                    });
                    found = true;
                    break;
                }
            }
            if !found {
                return Err(SchedulerError::JobNotFound {
                    job_id: job_id.to_string(),
                });
            }
        }
        Ok(())
    }

    /// Get a job by ID. If `jobstore` is None, search all stores.
    pub async fn get_job(
        &self,
        job_id: &str,
        jobstore: Option<&str>,
    ) -> Result<ScheduleSpec, SchedulerError> {
        if let Some(alias) = jobstore {
            let store = self.get_store(alias)?;
            store
                .get_job(job_id)
                .await
                .map_err(SchedulerError::StoreError)
        } else {
            // Clone stores out of the lock so we don't hold it across await.
            let store_snapshot: Vec<Arc<dyn JobStore>> = {
                let stores = self.stores.read();
                stores.values().cloned().collect()
            };
            for store in &store_snapshot {
                if let Ok(spec) = store.get_job(job_id).await {
                    return Ok(spec);
                }
            }
            Err(SchedulerError::JobNotFound {
                job_id: job_id.to_string(),
            })
        }
    }

    /// Remove all jobs, optionally filtered to a specific store.
    pub async fn remove_all_jobs(&self, jobstore: Option<&str>) -> Result<(), SchedulerError> {
        if let Some(alias) = jobstore {
            let store = self.get_store(alias)?;
            store
                .remove_all_jobs()
                .await
                .map_err(SchedulerError::StoreError)
        } else {
            let store_snapshot: Vec<Arc<dyn JobStore>> = {
                let stores = self.stores.read();
                stores.values().cloned().collect()
            };
            for store in &store_snapshot {
                if let Err(e) = store.remove_all_jobs().await {
                    tracing::warn!("error removing all jobs from store: {}", e);
                }
            }
            Ok(())
        }
    }

    /// Get all jobs, optionally filtered to a specific store.
    pub async fn get_jobs(
        &self,
        jobstore: Option<&str>,
    ) -> Result<Vec<ScheduleSpec>, SchedulerError> {
        if let Some(alias) = jobstore {
            let store = self.get_store(alias)?;
            store
                .get_all_jobs()
                .await
                .map_err(SchedulerError::StoreError)
        } else {
            let store_snapshot: Vec<Arc<dyn JobStore>> = {
                let stores = self.stores.read();
                stores.values().cloned().collect()
            };
            let mut all = Vec::new();
            for store in &store_snapshot {
                match store.get_all_jobs().await {
                    Ok(jobs) => all.extend(jobs),
                    Err(e) => {
                        tracing::warn!("error getting jobs from store: {}", e);
                    }
                }
            }
            Ok(all)
        }
    }

    /// Modify a job's properties.
    pub async fn modify_job(
        &self,
        job_id: &str,
        jobstore: Option<&str>,
        changes: JobChanges,
    ) -> Result<ScheduleSpec, SchedulerError> {
        let (store, alias) = self.find_job_store(job_id, jobstore).await?;

        let mut spec = store
            .get_job(job_id)
            .await
            .map_err(SchedulerError::StoreError)?;

        changes.apply(&mut spec);

        store
            .update_job(spec.clone())
            .await
            .map_err(SchedulerError::StoreError)?;

        self.event_bus.emit(&SchedulerEvent::JobModified {
            job_id: job_id.to_string(),
            jobstore: alias,
        });

        self.wakeup();
        Ok(spec)
    }

    /// Pause a job (sets paused=true, clears next_run_time).
    pub async fn pause_job(
        &self,
        job_id: &str,
        jobstore: Option<&str>,
    ) -> Result<ScheduleSpec, SchedulerError> {
        self.modify_job(
            job_id,
            jobstore,
            JobChanges {
                paused: Some(true),
                next_run_time: Some(None),
                ..Default::default()
            },
        )
        .await
    }

    /// Resume a paused job. Recalculates next_run_time from the trigger state.
    pub async fn resume_job(
        &self,
        job_id: &str,
        jobstore: Option<&str>,
    ) -> Result<ScheduleSpec, SchedulerError> {
        // First get the current spec to access trigger_state
        let current = self.get_job(job_id, jobstore).await?;
        let now = self.clock.now();
        let next = current.trigger_state.compute_next_fire_time(now, now);

        let spec = self
            .modify_job(
                job_id,
                jobstore,
                JobChanges {
                    paused: Some(false),
                    next_run_time: Some(next),
                    ..Default::default()
                },
            )
            .await?;

        // Wake the scheduler loop so it can pick up the resumed job
        self.wakeup_notify.notify_one();
        Ok(spec)
    }

    // -----------------------------------------------------------------------
    // Scheduler pause/resume
    // -----------------------------------------------------------------------

    /// Pause the entire scheduler. Jobs will not be processed until resumed.
    pub fn pause(&self) -> Result<(), SchedulerError> {
        let mut state = self.state.write();
        match *state {
            SchedulerState::Running => {
                *state = SchedulerState::Paused;
                self.event_bus.emit(&SchedulerEvent::SchedulerPaused);
                Ok(())
            }
            SchedulerState::Paused => Ok(()),
            _ => Err(SchedulerError::NotRunning),
        }
    }

    /// Resume a paused scheduler.
    pub fn resume(&self) -> Result<(), SchedulerError> {
        let mut state = self.state.write();
        match *state {
            SchedulerState::Paused => {
                *state = SchedulerState::Running;
                self.event_bus.emit(&SchedulerEvent::SchedulerResumed);
                self.wakeup_notify.notify_one();
                Ok(())
            }
            SchedulerState::Running => Ok(()),
            _ => Err(SchedulerError::NotRunning),
        }
    }

    /// Force the scheduler to re-evaluate pending jobs immediately.
    pub fn wakeup(&self) {
        self.wakeup_notify.notify_one();
    }

    // -----------------------------------------------------------------------
    // Store / executor management
    // -----------------------------------------------------------------------

    /// Register a job store under the given alias.
    pub fn add_jobstore(
        &self,
        store: Arc<dyn JobStore>,
        alias: &str,
    ) -> Result<(), SchedulerError> {
        let mut stores = self.stores.write();
        if stores.contains_key(alias) {
            return Err(SchedulerError::DuplicateJobStore {
                alias: alias.to_string(),
            });
        }
        stores.insert(alias.to_string(), store);
        self.event_bus.emit(&SchedulerEvent::JobStoreAdded {
            alias: alias.to_string(),
        });
        Ok(())
    }

    /// Remove a job store by alias.
    pub fn remove_jobstore(&self, alias: &str) -> Result<(), SchedulerError> {
        let mut stores = self.stores.write();
        if stores.remove(alias).is_none() {
            return Err(SchedulerError::JobStoreNotFound {
                alias: alias.to_string(),
            });
        }
        self.event_bus.emit(&SchedulerEvent::JobStoreRemoved {
            alias: alias.to_string(),
        });
        Ok(())
    }

    /// Register an executor under the given alias.
    pub fn add_executor(
        &self,
        executor: Arc<dyn Executor>,
        alias: &str,
    ) -> Result<(), SchedulerError> {
        let mut executors = self.executors.write();
        if executors.contains_key(alias) {
            return Err(SchedulerError::DuplicateExecutor {
                alias: alias.to_string(),
            });
        }
        executors.insert(alias.to_string(), executor);
        self.event_bus.emit(&SchedulerEvent::ExecutorAdded {
            alias: alias.to_string(),
        });
        Ok(())
    }

    /// Remove an executor by alias.
    pub fn remove_executor(&self, alias: &str) -> Result<(), SchedulerError> {
        let mut executors = self.executors.write();
        if executors.remove(alias).is_none() {
            return Err(SchedulerError::ExecutorNotFound {
                alias: alias.to_string(),
            });
        }
        self.event_bus.emit(&SchedulerEvent::ExecutorRemoved {
            alias: alias.to_string(),
        });
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Event listener management
    // -----------------------------------------------------------------------

    /// Register an event listener that fires for events matching the mask.
    pub fn add_listener(
        &self,
        callback: Arc<dyn Fn(&SchedulerEvent) + Send + Sync>,
        mask: u32,
    ) -> ListenerId {
        self.event_bus.add_listener(callback, mask)
    }

    /// Remove a previously registered event listener.
    pub fn remove_listener(&self, id: ListenerId) -> bool {
        self.event_bus.remove_listener(id)
    }

    // -----------------------------------------------------------------------
    // Dead letter queue
    // -----------------------------------------------------------------------

    /// Return all dead letter entries.
    pub fn get_dead_letters(&self) -> Vec<DeadLetterEntry> {
        self.dead_letter.read().iter().cloned().collect()
    }

    /// Return a single dead letter entry by job_id.
    pub fn get_dead_letter(&self, job_id: &str) -> Option<DeadLetterEntry> {
        self.dead_letter
            .read()
            .iter()
            .find(|e| e.job_id == job_id)
            .cloned()
    }

    /// Remove a dead letter entry by job_id. Returns true if found and removed.
    pub fn remove_dead_letter(&self, job_id: &str) -> bool {
        let mut dl = self.dead_letter.write();
        let before = dl.len();
        dl.retain(|e| e.job_id != job_id);
        dl.len() < before
    }

    /// Clear all dead letter entries.
    pub fn clear_dead_letters(&self) {
        self.dead_letter.write().clear();
    }

    /// Replay a dead letter entry by re-adding it to the scheduler as a
    /// one-shot date trigger that fires immediately.
    pub async fn replay_dead_letter(&self, job_id: &str) -> Result<(), SchedulerError> {
        let entry = {
            let dl = self.dead_letter.read();
            dl.iter()
                .find(|e| e.job_id == job_id)
                .cloned()
                .ok_or_else(|| SchedulerError::JobNotFound {
                    job_id: job_id.to_string(),
                })?
        };

        // Build a one-shot schedule that fires immediately
        let now = self.clock.now();
        let trigger_state = crate::model::TriggerState::Date {
            run_date: now,
            timezone: "UTC".to_string(),
        };
        let mut spec = ScheduleSpec::new(
            format!("{}-replay", entry.job_id),
            entry.task.clone(),
            trigger_state,
        );
        spec.next_run_time = Some(now);
        spec.replace_existing = true;

        self.add_job(spec).await?;

        // Remove from DLQ
        self.remove_dead_letter(job_id);
        Ok(())
    }

    /// Internal: push a dead letter entry, evicting oldest if over capacity.
    #[allow(dead_code)]
    fn push_dead_letter(&self, entry: DeadLetterEntry) {
        let mut dl = self.dead_letter.write();
        if dl.len() >= self.dead_letter_max {
            dl.pop_front();
        }
        dl.push_back(entry);
    }

    /// Return a reference to the dead letter queue Arc (for sharing with loop context).
    #[allow(dead_code)]
    pub(crate) fn dead_letter_arc(&self) -> &Arc<RwLock<VecDeque<DeadLetterEntry>>> {
        &self.dead_letter
    }

    /// Return a reference to rate windows.
    #[allow(dead_code)]
    pub(crate) fn rate_windows_arc(&self) -> &Arc<DashMap<String, VecDeque<DateTime<Utc>>>> {
        &self.rate_windows
    }

    /// Return a reference to group running counts.
    #[allow(dead_code)]
    pub(crate) fn group_running_arc(&self) -> &Arc<DashMap<String, AtomicU32>> {
        &self.group_running
    }

    // -----------------------------------------------------------------------
    // Introspection helpers
    // -----------------------------------------------------------------------

    /// Returns the aliases of all registered job stores.
    pub fn store_aliases(&self) -> Vec<String> {
        self.stores.read().keys().cloned().collect()
    }

    /// Returns the aliases of all registered executors.
    pub fn executor_aliases(&self) -> Vec<String> {
        self.executors.read().keys().cloned().collect()
    }

    /// Returns the running instance count for a specific job, or 0 if not tracked.
    pub fn running_instance_count(&self, job_id: &str) -> u32 {
        self.running_instances.get(job_id).map(|v| *v).unwrap_or(0)
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    fn get_store(&self, alias: &str) -> Result<Arc<dyn JobStore>, SchedulerError> {
        let stores = self.stores.read();
        stores
            .get(alias)
            .cloned()
            .ok_or_else(|| SchedulerError::JobStoreNotFound {
                alias: alias.to_string(),
            })
    }

    /// Look up an executor by alias.
    #[allow(dead_code)]
    fn get_executor(&self, alias: &str) -> Result<Arc<dyn Executor>, SchedulerError> {
        let executors = self.executors.read();
        executors
            .get(alias)
            .cloned()
            .ok_or_else(|| SchedulerError::ExecutorNotFound {
                alias: alias.to_string(),
            })
    }

    /// Find the store that contains a given job, returning both the store and its alias.
    async fn find_job_store(
        &self,
        job_id: &str,
        jobstore: Option<&str>,
    ) -> Result<(Arc<dyn JobStore>, String), SchedulerError> {
        if let Some(alias) = jobstore {
            let store = self.get_store(alias)?;
            return Ok((store, alias.to_string()));
        }

        let stores = self.stores.read().clone();
        for (alias, store) in stores.iter() {
            if store.get_job(job_id).await.is_ok() {
                return Ok((Arc::clone(store), alias.clone()));
            }
        }

        Err(SchedulerError::JobNotFound {
            job_id: job_id.to_string(),
        })
    }
}

// ---------------------------------------------------------------------------
// Scheduler loop context (moved out so it can be 'static + Send)
// ---------------------------------------------------------------------------

struct SchedulerLoopContext {
    state: Arc<RwLock<SchedulerState>>,
    stores: Arc<RwLock<HashMap<String, Arc<dyn JobStore>>>>,
    executors: Arc<RwLock<HashMap<String, Arc<dyn Executor>>>>,
    event_bus: Arc<EventBus>,
    clock: Arc<dyn Clock>,
    wakeup_notify: Arc<tokio::sync::Notify>,
    shutdown_notify: Arc<tokio::sync::Notify>,
    running_instances: Arc<DashMap<String, u32>>,
    result_tx: mpsc::Sender<JobResultEnvelope>,
    #[allow(dead_code)]
    scheduler_id: String,
    #[allow(dead_code)]
    config: SchedulerConfig,
    dead_letter: Arc<RwLock<VecDeque<DeadLetterEntry>>>,
    dead_letter_max: usize,
    rate_windows: Arc<DashMap<String, VecDeque<DateTime<Utc>>>>,
    group_running: Arc<DashMap<String, AtomicU32>>,
    job_completions: Arc<DashMap<String, JobCompletion>>,
}

impl SchedulerLoopContext {
    async fn run_loop(self, result_rx: Option<mpsc::Receiver<JobResultEnvelope>>) {
        // Spawn a task to process results from executors
        let event_bus = Arc::clone(&self.event_bus);
        let running_instances = Arc::clone(&self.running_instances);
        let stores_for_results = Arc::clone(&self.stores);

        if let Some(mut rx) = result_rx {
            let eb = Arc::clone(&event_bus);
            let ri = Arc::clone(&running_instances);
            let st = Arc::clone(&stores_for_results);
            let dl = Arc::clone(&self.dead_letter);
            let dl_max = self.dead_letter_max;
            let gr = Arc::clone(&self.group_running);
            let jc = Arc::clone(&self.job_completions);
            let wakeup = Arc::clone(&self.wakeup_notify);
            tokio::spawn(async move {
                while let Some(envelope) = rx.recv().await {
                    tracing::debug!(
                        job_id = %envelope.job_id,
                        schedule_id = %envelope.schedule_id,
                        "processing job result"
                    );

                    // Decrement running count
                    if let Some(mut count) = ri.get_mut(&envelope.schedule_id) {
                        if *count > 0 {
                            *count -= 1;
                        }
                    }

                    // Clone stores out of the lock so we don't hold it across await
                    let store_snapshot: Vec<(String, Arc<dyn JobStore>)> = {
                        let stores = st.read();
                        stores
                            .iter()
                            .map(|(k, v)| (k.clone(), Arc::clone(v)))
                            .collect()
                    };

                    for (_alias, store) in &store_snapshot {
                        let _ = store.decrement_running_count(&envelope.schedule_id).await;
                    }

                    // Decrement concurrency group running count if applicable
                    {
                        // Look up the schedule to find its concurrency group
                        for (_alias, store) in &store_snapshot {
                            if let Ok(spec) = store.get_job(&envelope.schedule_id).await {
                                if let Some(ref group) = spec.concurrency_group {
                                    if let Some(counter) = gr.get(group) {
                                        let prev = counter.value().load(AtomicOrdering::Relaxed);
                                        if prev > 0 {
                                            counter.value().fetch_sub(1, AtomicOrdering::Relaxed);
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }

                    // Find which store has this job
                    let mut found_store = "default".to_string();
                    let mut found_task: Option<TaskSpec> = None;
                    for (alias, store) in &store_snapshot {
                        if let Ok(spec) = store.get_job(&envelope.schedule_id).await {
                            found_store = alias.clone();
                            found_task = Some(spec.task.clone());
                            break;
                        }
                    }

                    // If the job failed, add to dead letter queue
                    if let crate::model::JobOutcome::Error(ref err_msg) = envelope.outcome {
                        let entry = DeadLetterEntry {
                            job_id: envelope.job_id.to_string(),
                            schedule_id: envelope.schedule_id.clone(),
                            failed_at: envelope.completed_at,
                            scheduled_fire_time: envelope.completed_at,
                            error_type: "JobError".to_string(),
                            error_message: err_msg.clone(),
                            traceback: None,
                            attempt: 1,
                            task: found_task.unwrap_or_else(|| {
                                TaskSpec::new(crate::model::CallableRef::ImportPath(
                                    "unknown".to_string(),
                                ))
                            }),
                        };
                        let mut dlq = dl.write();
                        if dlq.len() >= dl_max {
                            dlq.pop_front();
                        }
                        dlq.push_back(entry);
                        tracing::warn!(
                            schedule_id = %envelope.schedule_id,
                            "job failed, added to dead letter queue: {}",
                            err_msg
                        );
                    }

                    let event = SchedulerEvent::from_outcome(
                        envelope.job_id,
                        envelope.schedule_id.clone(),
                        found_store,
                        envelope.completed_at,
                        &envelope.outcome,
                    );
                    eb.emit(&event);

                    // -------------------------------------------------
                    // DAG completion tracking + downstream triggering
                    // -------------------------------------------------
                    let status = match envelope.outcome {
                        crate::model::JobOutcome::Success => Some(CompletionStatus::Success),
                        crate::model::JobOutcome::Error(_) => Some(CompletionStatus::Failure),
                        _ => None,
                    };
                    if let Some(status) = status {
                        jc.insert(
                            envelope.schedule_id.clone(),
                            JobCompletion {
                                schedule_id: envelope.schedule_id.clone(),
                                last_run: envelope.completed_at,
                                status,
                            },
                        );

                        // Find any downstream jobs that declare this
                        // schedule in their `depends_on` list, and if
                        // *all* of their dependencies are satisfied,
                        // trigger them by setting next_run_time = now.
                        let mut triggered_any = false;
                        for (_alias, store) in &store_snapshot {
                            let all_jobs = match store.get_all_jobs().await {
                                Ok(j) => j,
                                Err(_) => continue,
                            };
                            for spec in all_jobs {
                                if spec.depends_on.is_empty() {
                                    continue;
                                }
                                if !spec.depends_on.contains(&envelope.schedule_id) {
                                    continue;
                                }
                                // Check whether all dependencies are
                                // satisfied.
                                let mut satisfied = true;
                                for dep in &spec.depends_on {
                                    match jc.get(dep) {
                                        Some(rec) => {
                                            if rec.status == CompletionStatus::Failure
                                                && !spec.run_on_failure
                                            {
                                                satisfied = false;
                                                break;
                                            }
                                        }
                                        None => {
                                            satisfied = false;
                                            break;
                                        }
                                    }
                                }
                                if !satisfied {
                                    continue;
                                }
                                // Avoid re-triggering a job that is
                                // already queued (next_run_time set).
                                if spec.next_run_time.is_some() {
                                    continue;
                                }
                                // Trigger: set next_run_time = now.
                                let now = envelope.completed_at;
                                if let Err(e) =
                                    store.update_next_run_time(&spec.id, Some(now)).await
                                {
                                    tracing::warn!(
                                        job_id = %spec.id,
                                        "failed to trigger dependency: {}",
                                        e
                                    );
                                } else {
                                    tracing::info!(
                                        job_id = %spec.id,
                                        upstream = %envelope.schedule_id,
                                        "dependency satisfied, triggering downstream job"
                                    );
                                    triggered_any = true;
                                }
                            }
                        }
                        if triggered_any {
                            wakeup.notify_one();
                        }
                    }
                }
            });
        }

        loop {
            // 1. Check if shutting down
            {
                let state = self.state.read();
                if *state == SchedulerState::ShuttingDown {
                    tracing::info!("scheduler loop exiting: state = {:?}", *state);
                    return;
                }
            }

            // 2. If paused, wait on wakeup or shutdown
            let is_paused = { *self.state.read() == SchedulerState::Paused };
            if is_paused {
                tokio::select! {
                    _ = self.wakeup_notify.notified() => continue,
                    _ = self.shutdown_notify.notified() => return,
                }
            }

            let now = self.clock.now();

            // 3. Get all due jobs across all stores
            // Clone stores out of the lock to avoid holding it across await points.
            tracing::trace!(
                scheduler_id = %self.scheduler_id,
                "scheduler_loop_iteration"
            );
            let store_snapshot: Vec<(String, Arc<dyn JobStore>)> = {
                let stores = self.stores.read();
                stores
                    .iter()
                    .map(|(k, v)| (k.clone(), Arc::clone(v)))
                    .collect()
            };

            let mut due_jobs: Vec<(String, ScheduleSpec)> = Vec::new();
            for (alias, store) in &store_snapshot {
                match store.get_due_jobs(now).await {
                    Ok(jobs) => {
                        for job in jobs {
                            due_jobs.push((alias.clone(), job));
                        }
                    }
                    Err(e) => {
                        tracing::error!(store = alias.as_str(), "error getting due jobs: {}", e);
                    }
                }
            }

            // 4. Process each due job
            for (store_alias, schedule) in &due_jobs {
                self.process_due_job(store_alias, schedule, now).await;
            }

            // 5. Calculate next wakeup time
            let next_wakeup = self.get_earliest_next_run_time().await;

            let sleep_duration = match next_wakeup {
                Some(next) => {
                    let delta = next - now;
                    if delta.num_milliseconds() <= 0 {
                        // Already due, loop immediately
                        tokio::time::Duration::from_millis(0)
                    } else {
                        tokio::time::Duration::from_millis(delta.num_milliseconds() as u64)
                    }
                }
                None => {
                    // No jobs scheduled, sleep for a long time (but wake on notify)
                    tokio::time::Duration::from_secs(300)
                }
            };

            // 6. Sleep until next due time, wakeup signal, or shutdown signal
            tokio::select! {
                _ = tokio::time::sleep(sleep_duration) => {},
                _ = self.wakeup_notify.notified() => {},
                _ = self.shutdown_notify.notified() => {
                    tracing::info!("scheduler loop received shutdown signal");
                    return;
                },
            }
        }
    }

    async fn process_due_job(
        &self,
        store_alias: &str,
        schedule: &ScheduleSpec,
        now: DateTime<Utc>,
    ) {
        let job_id = &schedule.id;

        let trigger_type = match &schedule.trigger_state {
            crate::model::TriggerState::Date { .. } => "date",
            crate::model::TriggerState::Interval { .. } => "interval",
            crate::model::TriggerState::Cron { .. } => "cron",
            crate::model::TriggerState::CalendarInterval { .. } => "calendarinterval",
            crate::model::TriggerState::Plugin { .. } => "plugin",
        };

        tracing::info!(
            job_id = %job_id,
            trigger_type = %trigger_type,
            scheduled_fire_time = %schedule.next_run_time.map(|t| t.to_string()).unwrap_or_default(),
            "job_execution"
        );

        // Skip paused jobs
        if schedule.paused {
            return;
        }

        let fire_time = match schedule.next_run_time {
            Some(t) => t,
            None => return,
        };

        // Determine whether the fire time is in the past (missed fires exist).
        let coalesced = schedule.coalesce == crate::model::CoalescePolicy::On && fire_time < now;

        // Apply misfire policy: if the fire time is too far in the past
        // (beyond misfire_grace_time), skip it entirely.
        if let Some(grace) = schedule.misfire_grace_time {
            let deadline =
                fire_time + chrono::Duration::from_std(grace).unwrap_or(chrono::Duration::zero());
            if now > deadline {
                // Misfired
                tracing::warn!(
                    job_id = job_id,
                    "job misfired (scheduled: {}, now: {}, grace: {:?})",
                    fire_time,
                    now,
                    grace
                );
                self.event_bus.emit(&SchedulerEvent::JobMissed {
                    job_id: job_id.clone(),
                    jobstore: store_alias.to_string(),
                    scheduled_run_time: fire_time,
                });

                // Advance to the next future fire time (skip all missed)
                let next = schedule
                    .trigger_state
                    .compute_next_future_fire_time(fire_time, now);
                let store = {
                    let stores = self.stores.read();
                    stores.get(store_alias).cloned()
                };
                if let Some(store) = store {
                    let _ = store.update_next_run_time(job_id, next).await;
                }
                if next.is_some() {
                    self.wakeup_notify.notify_one();
                }
                return;
            }
        }

        // Check max_instances
        let current_count = self.running_instances.get(job_id).map(|v| *v).unwrap_or(0);

        if current_count >= schedule.max_instances {
            tracing::debug!(
                job_id = job_id,
                "max instances reached ({}/{})",
                current_count,
                schedule.max_instances
            );
            self.event_bus.emit(&SchedulerEvent::JobMaxInstances {
                job_id: job_id.clone(),
                jobstore: store_alias.to_string(),
            });
            return;
        }

        // Check rate limit
        if let Some(ref rl) = schedule.rate_limit {
            let window = chrono::Duration::seconds(rl.window_seconds as i64);
            let cutoff = now - window;
            let mut entry = self.rate_windows.entry(job_id.clone()).or_default();
            // Evict old entries
            while entry.front().map(|t| *t < cutoff).unwrap_or(false) {
                entry.pop_front();
            }
            if entry.len() >= rl.max_executions as usize {
                tracing::debug!(
                    job_id = job_id,
                    "rate limit exceeded ({}/{} in {}s)",
                    entry.len(),
                    rl.max_executions,
                    rl.window_seconds,
                );
                return;
            }
            entry.push_back(now);
        }

        // Check concurrency group limit
        if let Some(ref group) = schedule.concurrency_group {
            let counter = self
                .group_running
                .entry(group.clone())
                .or_insert_with(|| AtomicU32::new(0));
            let current = counter.value().load(AtomicOrdering::Relaxed);
            if current >= schedule.max_group_instances {
                tracing::debug!(
                    job_id = job_id,
                    group = group,
                    "concurrency group limit reached ({}/{})",
                    current,
                    schedule.max_group_instances,
                );
                return;
            }
            counter.value().fetch_add(1, AtomicOrdering::Relaxed);
        }

        // Apply coalesce: we only submit once per due evaluation
        // (coalescing means if multiple fire times have passed, we only run once)

        // Create a concrete JobSpec
        let mut job = JobSpec::from_schedule(schedule, fire_time);
        job.actual_fire_time = Some(now);

        // Increment running count
        self.running_instances
            .entry(job_id.clone())
            .and_modify(|c| *c += 1)
            .or_insert(1);

        // Also increment in store
        {
            let store = {
                let stores = self.stores.read();
                stores.get(store_alias).cloned()
            };
            if let Some(store) = store {
                let _ = store.increment_running_count(job_id).await;
            }
        }

        // Submit to executor
        let executor_alias = &schedule.executor;
        let executor = {
            let executors = self.executors.read();
            executors.get(executor_alias).cloned()
        };

        match executor {
            Some(exec) => {
                self.event_bus.emit(&SchedulerEvent::JobSubmitted {
                    job_id: job.id,
                    schedule_id: job_id.clone(),
                    jobstore: store_alias.to_string(),
                    scheduled_run_time: fire_time,
                });

                if let Err(e) = exec.submit_job(job, self.result_tx.clone()).await {
                    tracing::error!(
                        job_id = job_id,
                        executor = executor_alias,
                        "failed to submit job: {}",
                        e
                    );
                    // Decrement running count on submission failure
                    if let Some(mut count) = self.running_instances.get_mut(job_id) {
                        if *count > 0 {
                            *count -= 1;
                        }
                    }
                }
            }
            None => {
                tracing::error!(
                    job_id = job_id,
                    executor = executor_alias,
                    "executor not found"
                );
                // Decrement running count
                if let Some(mut count) = self.running_instances.get_mut(job_id) {
                    if *count > 0 {
                        *count -= 1;
                    }
                }
            }
        }

        // Compute next fire time from the trigger state and update the store.
        // When coalescing, skip ahead to the next future fire time so that
        // intermediate missed fire times are not re-processed one by one.
        {
            let next = if coalesced {
                schedule
                    .trigger_state
                    .compute_next_future_fire_time(fire_time, now)
            } else {
                schedule
                    .trigger_state
                    .compute_next_fire_time(fire_time, now)
            };
            let store = {
                let stores = self.stores.read();
                stores.get(store_alias).cloned()
            };
            if let Some(store) = store {
                let _ = store.update_next_run_time(job_id, next).await;
            }
            // Wake the scheduler loop so it picks up the new next_run_time
            if next.is_some() {
                self.wakeup_notify.notify_one();
            }
        }
    }

    async fn get_earliest_next_run_time(&self) -> Option<DateTime<Utc>> {
        let store_snapshot: Vec<Arc<dyn JobStore>> = {
            let stores = self.stores.read();
            stores.values().cloned().collect()
        };
        let mut earliest: Option<DateTime<Utc>> = None;
        for store in &store_snapshot {
            match store.get_next_run_time().await {
                Ok(Some(t)) => {
                    earliest = Some(match earliest {
                        Some(e) if e < t => e,
                        _ => t,
                    });
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::error!("error getting next run time: {}", e);
                }
            }
        }
        earliest
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::TestClock;
    use crate::error::StoreError;
    use crate::event::EVENT_ALL;
    use crate::model::{CallableRef, JobOutcome, ScheduleSpec, TaskSpec, TriggerState};
    use async_trait::async_trait;
    use chrono::TimeZone;
    use parking_lot::Mutex as PLMutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    // -----------------------------------------------------------------------
    // Mock JobStore
    // -----------------------------------------------------------------------
    #[derive(Debug)]
    struct MockJobStore {
        jobs: Arc<PLMutex<HashMap<String, ScheduleSpec>>>,
        running_counts: Arc<PLMutex<HashMap<String, u32>>>,
    }

    impl MockJobStore {
        fn new() -> Self {
            Self {
                jobs: Arc::new(PLMutex::new(HashMap::new())),
                running_counts: Arc::new(PLMutex::new(HashMap::new())),
            }
        }
    }

    #[async_trait]
    impl JobStore for MockJobStore {
        async fn add_job(&self, schedule: ScheduleSpec) -> Result<(), StoreError> {
            let mut jobs = self.jobs.lock();
            if jobs.contains_key(&schedule.id) {
                return Err(StoreError::DuplicateJob {
                    job_id: schedule.id.clone(),
                });
            }
            jobs.insert(schedule.id.clone(), schedule);
            Ok(())
        }

        async fn update_job(&self, schedule: ScheduleSpec) -> Result<(), StoreError> {
            let mut jobs = self.jobs.lock();
            if !jobs.contains_key(&schedule.id) {
                return Err(StoreError::JobNotFound {
                    job_id: schedule.id.clone(),
                });
            }
            jobs.insert(schedule.id.clone(), schedule);
            Ok(())
        }

        async fn remove_job(&self, job_id: &str) -> Result<(), StoreError> {
            let mut jobs = self.jobs.lock();
            if jobs.remove(job_id).is_none() {
                return Err(StoreError::JobNotFound {
                    job_id: job_id.to_string(),
                });
            }
            Ok(())
        }

        async fn remove_all_jobs(&self) -> Result<(), StoreError> {
            self.jobs.lock().clear();
            Ok(())
        }

        async fn get_job(&self, job_id: &str) -> Result<ScheduleSpec, StoreError> {
            let jobs = self.jobs.lock();
            jobs.get(job_id)
                .cloned()
                .ok_or_else(|| StoreError::JobNotFound {
                    job_id: job_id.to_string(),
                })
        }

        async fn get_all_jobs(&self) -> Result<Vec<ScheduleSpec>, StoreError> {
            let jobs = self.jobs.lock();
            Ok(jobs.values().cloned().collect())
        }

        async fn get_due_jobs(&self, now: DateTime<Utc>) -> Result<Vec<ScheduleSpec>, StoreError> {
            let jobs = self.jobs.lock();
            Ok(jobs.values().filter(|j| j.is_due(now)).cloned().collect())
        }

        async fn get_next_run_time(&self) -> Result<Option<DateTime<Utc>>, StoreError> {
            let jobs = self.jobs.lock();
            let min = jobs
                .values()
                .filter(|j| !j.paused)
                .filter_map(|j| j.next_run_time)
                .min();
            Ok(min)
        }

        async fn acquire_jobs(
            &self,
            _scheduler_id: &str,
            max_jobs: usize,
            now: DateTime<Utc>,
        ) -> Result<Vec<crate::model::JobLease>, StoreError> {
            let jobs = self.jobs.lock();
            let leases: Vec<_> = jobs
                .values()
                .filter(|j| j.is_due(now))
                .take(max_jobs)
                .map(|j| crate::model::JobLease {
                    job_id: j.id.clone(),
                    scheduler_id: "test".to_string(),
                    acquired_at: now,
                    expires_at: now + chrono::Duration::seconds(30),
                    version: j.version,
                })
                .collect();
            Ok(leases)
        }

        async fn release_job(&self, _job_id: &str, _scheduler_id: &str) -> Result<(), StoreError> {
            Ok(())
        }

        async fn update_next_run_time(
            &self,
            job_id: &str,
            next: Option<DateTime<Utc>>,
        ) -> Result<(), StoreError> {
            let mut jobs = self.jobs.lock();
            if let Some(job) = jobs.get_mut(job_id) {
                job.next_run_time = next;
                Ok(())
            } else {
                Err(StoreError::JobNotFound {
                    job_id: job_id.to_string(),
                })
            }
        }

        async fn increment_running_count(&self, job_id: &str) -> Result<u32, StoreError> {
            let mut counts = self.running_counts.lock();
            let count = counts.entry(job_id.to_string()).or_insert(0);
            *count += 1;
            Ok(*count)
        }

        async fn decrement_running_count(&self, job_id: &str) -> Result<u32, StoreError> {
            let mut counts = self.running_counts.lock();
            let count = counts.entry(job_id.to_string()).or_insert(0);
            if *count > 0 {
                *count -= 1;
            }
            Ok(*count)
        }

        async fn get_running_count(&self, job_id: &str) -> Result<u32, StoreError> {
            let counts = self.running_counts.lock();
            Ok(*counts.get(job_id).unwrap_or(&0))
        }

        async fn cleanup_stale_leases(&self, _now: DateTime<Utc>) -> Result<u32, StoreError> {
            Ok(0)
        }
    }

    // -----------------------------------------------------------------------
    // Mock Executor
    // -----------------------------------------------------------------------
    #[derive(Debug)]
    struct MockExecutor {
        started: Arc<PLMutex<bool>>,
        submitted: Arc<PLMutex<Vec<JobSpec>>>,
    }

    impl MockExecutor {
        fn new() -> Self {
            Self {
                started: Arc::new(PLMutex::new(false)),
                submitted: Arc::new(PLMutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl Executor for MockExecutor {
        async fn start(&self) -> Result<(), crate::error::ExecutorError> {
            *self.started.lock() = true;
            Ok(())
        }

        async fn shutdown(&self, _wait: bool) -> Result<(), crate::error::ExecutorError> {
            *self.started.lock() = false;
            Ok(())
        }

        async fn submit_job(
            &self,
            job: JobSpec,
            result_tx: mpsc::Sender<JobResultEnvelope>,
        ) -> Result<(), crate::error::ExecutorError> {
            let schedule_id = job.schedule_id.clone();
            let job_id = job.id;
            self.submitted.lock().push(job);

            // Simulate immediate success
            let _ = result_tx
                .send(JobResultEnvelope {
                    job_id,
                    schedule_id,
                    outcome: JobOutcome::Success,
                    completed_at: Utc::now(),
                })
                .await;

            Ok(())
        }

        async fn running_job_count(&self) -> usize {
            0
        }

        fn executor_type(&self) -> &'static str {
            "mock"
        }
    }

    fn sample_task() -> TaskSpec {
        TaskSpec::new(CallableRef::ImportPath("test:func".to_string()))
    }

    fn sample_trigger() -> TriggerState {
        TriggerState::Date {
            run_date: Utc::now(),
            timezone: "UTC".to_string(),
        }
    }

    #[test]
    fn test_scheduler_engine_creation() {
        let t0 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let clock = Arc::new(TestClock::new(t0));
        let engine = SchedulerEngine::new(SchedulerConfig::default(), clock);
        assert_eq!(engine.state(), SchedulerState::Stopped);
        assert!(!engine.scheduler_id().is_empty());
    }

    #[test]
    fn test_scheduler_with_defaults() {
        let engine = SchedulerEngine::with_defaults();
        assert_eq!(engine.state(), SchedulerState::Stopped);
    }

    #[tokio::test]
    async fn test_add_and_remove_jobstore() {
        let engine = SchedulerEngine::with_defaults();
        let store = Arc::new(MockJobStore::new());

        engine.add_jobstore(store.clone(), "default").unwrap();
        assert!(engine.add_jobstore(store, "default").is_err()); // duplicate

        engine.remove_jobstore("default").unwrap();
        assert!(engine.remove_jobstore("default").is_err()); // not found
    }

    #[tokio::test]
    async fn test_add_and_remove_executor() {
        let engine = SchedulerEngine::with_defaults();
        let exec = Arc::new(MockExecutor::new());

        engine.add_executor(exec.clone(), "default").unwrap();
        assert!(engine.add_executor(exec, "default").is_err());

        engine.remove_executor("default").unwrap();
        assert!(engine.remove_executor("default").is_err());
    }

    #[tokio::test]
    async fn test_add_job() {
        let engine = SchedulerEngine::with_defaults();
        let store = Arc::new(MockJobStore::new());
        engine.add_jobstore(store.clone(), "default").unwrap();

        let spec = ScheduleSpec::new("job1", sample_task(), sample_trigger());
        engine.add_job(spec).await.unwrap();

        let retrieved = engine.get_job("job1", Some("default")).await.unwrap();
        assert_eq!(retrieved.id, "job1");
    }

    #[tokio::test]
    async fn test_add_duplicate_job() {
        let engine = SchedulerEngine::with_defaults();
        let store = Arc::new(MockJobStore::new());
        engine.add_jobstore(store, "default").unwrap();

        let spec = ScheduleSpec::new("job1", sample_task(), sample_trigger());
        engine.add_job(spec).await.unwrap();

        let spec2 = ScheduleSpec::new("job1", sample_task(), sample_trigger());
        assert!(engine.add_job(spec2).await.is_err());
    }

    #[tokio::test]
    async fn test_add_job_replace_existing() {
        let engine = SchedulerEngine::with_defaults();
        let store = Arc::new(MockJobStore::new());
        engine.add_jobstore(store, "default").unwrap();

        let spec = ScheduleSpec::new("job1", sample_task(), sample_trigger());
        engine.add_job(spec).await.unwrap();

        let mut spec2 = ScheduleSpec::new("job1", sample_task(), sample_trigger());
        spec2.replace_existing = true;
        spec2.max_instances = 5;
        engine.add_job(spec2).await.unwrap();

        let retrieved = engine.get_job("job1", None).await.unwrap();
        assert_eq!(retrieved.max_instances, 5);
    }

    #[tokio::test]
    async fn test_remove_job() {
        let engine = SchedulerEngine::with_defaults();
        let store = Arc::new(MockJobStore::new());
        engine.add_jobstore(store, "default").unwrap();

        let spec = ScheduleSpec::new("job1", sample_task(), sample_trigger());
        engine.add_job(spec).await.unwrap();

        engine.remove_job("job1", Some("default")).await.unwrap();
        assert!(engine.get_job("job1", None).await.is_err());
    }

    #[tokio::test]
    async fn test_remove_job_not_found() {
        let engine = SchedulerEngine::with_defaults();
        let store = Arc::new(MockJobStore::new());
        engine.add_jobstore(store, "default").unwrap();

        assert!(engine.remove_job("nonexistent", None).await.is_err());
    }

    #[tokio::test]
    async fn test_get_jobs() {
        let engine = SchedulerEngine::with_defaults();
        let store = Arc::new(MockJobStore::new());
        engine.add_jobstore(store, "default").unwrap();

        engine
            .add_job(ScheduleSpec::new("j1", sample_task(), sample_trigger()))
            .await
            .unwrap();
        engine
            .add_job(ScheduleSpec::new("j2", sample_task(), sample_trigger()))
            .await
            .unwrap();

        let jobs = engine.get_jobs(None).await.unwrap();
        assert_eq!(jobs.len(), 2);
    }

    #[tokio::test]
    async fn test_modify_job() {
        let engine = SchedulerEngine::with_defaults();
        let store = Arc::new(MockJobStore::new());
        engine.add_jobstore(store, "default").unwrap();

        let spec = ScheduleSpec::new("job1", sample_task(), sample_trigger());
        engine.add_job(spec).await.unwrap();

        let changes = JobChanges {
            max_instances: Some(10),
            name: Some("updated".to_string()),
            ..Default::default()
        };
        let updated = engine.modify_job("job1", None, changes).await.unwrap();
        assert_eq!(updated.max_instances, 10);
        assert_eq!(updated.name, Some("updated".to_string()));
        assert_eq!(updated.version, 2);
    }

    #[tokio::test]
    async fn test_pause_resume_job() {
        let engine = SchedulerEngine::with_defaults();
        let store = Arc::new(MockJobStore::new());
        engine.add_jobstore(store, "default").unwrap();

        let spec = ScheduleSpec::new("job1", sample_task(), sample_trigger());
        engine.add_job(spec).await.unwrap();

        let paused = engine.pause_job("job1", None).await.unwrap();
        assert!(paused.paused);
        assert!(paused.next_run_time.is_none());

        let resumed = engine.resume_job("job1", None).await.unwrap();
        assert!(!resumed.paused);
    }

    #[tokio::test]
    async fn test_pause_resume_scheduler() {
        let engine = SchedulerEngine::with_defaults();
        let store = Arc::new(MockJobStore::new());
        let exec = Arc::new(MockExecutor::new());
        engine.add_jobstore(store, "default").unwrap();
        engine.add_executor(exec, "default").unwrap();

        engine.start().await.unwrap();
        assert_eq!(engine.state(), SchedulerState::Running);

        engine.pause().unwrap();
        assert_eq!(engine.state(), SchedulerState::Paused);

        engine.resume().unwrap();
        assert_eq!(engine.state(), SchedulerState::Running);

        engine.shutdown(false).await.unwrap();
        assert_eq!(engine.state(), SchedulerState::Stopped);
    }

    #[tokio::test]
    async fn test_start_already_running() {
        let engine = SchedulerEngine::with_defaults();
        let exec = Arc::new(MockExecutor::new());
        engine.add_executor(exec, "default").unwrap();

        engine.start().await.unwrap();
        assert!(engine.start().await.is_err());
        engine.shutdown(false).await.unwrap();
    }

    #[tokio::test]
    async fn test_shutdown_not_running() {
        let engine = SchedulerEngine::with_defaults();
        assert!(engine.shutdown(false).await.is_err());
    }

    #[tokio::test]
    async fn test_event_listener_integration() {
        let engine = SchedulerEngine::with_defaults();
        let store = Arc::new(MockJobStore::new());
        engine.add_jobstore(store, "default").unwrap();

        let counter = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&counter);
        engine.add_listener(
            Arc::new(move |_| {
                c.fetch_add(1, Ordering::Relaxed);
            }),
            EVENT_ALL,
        );

        let spec = ScheduleSpec::new("job1", sample_task(), sample_trigger());
        engine.add_job(spec).await.unwrap();

        // Should have received at least the JobAdded event
        assert!(counter.load(Ordering::Relaxed) >= 1);
    }

    #[tokio::test]
    async fn test_get_job_store_not_found() {
        let engine = SchedulerEngine::with_defaults();
        let spec = ScheduleSpec::new("job1", sample_task(), sample_trigger());
        let result = engine.add_job(spec).await;
        assert!(matches!(
            result,
            Err(SchedulerError::JobStoreNotFound { .. })
        ));
    }

    #[test]
    fn test_pause_not_running() {
        let engine = SchedulerEngine::with_defaults();
        assert!(engine.pause().is_err());
    }

    #[test]
    fn test_resume_not_running() {
        let engine = SchedulerEngine::with_defaults();
        assert!(engine.resume().is_err());
    }

    #[tokio::test]
    async fn test_add_job_during_shutdown_rejected() {
        // Bug 4 fix test: jobs added during shutdown should be rejected, not silently lost.
        // We directly set the state to ShuttingDown to simulate the window between
        // the shutdown signal and the actual completion of shutdown.
        let engine = SchedulerEngine::with_defaults();
        let store = Arc::new(MockJobStore::new());
        engine.add_jobstore(store, "default").unwrap();

        // Manually set state to ShuttingDown (simulating the shutdown window)
        {
            let mut state = engine.state.write();
            *state = SchedulerState::ShuttingDown;
        }

        // Now try to add a job -- should fail with SchedulerShuttingDown
        let spec = ScheduleSpec::new("late_job", sample_task(), sample_trigger());
        let result = engine.add_job(spec).await;
        assert!(
            matches!(result, Err(SchedulerError::SchedulerShuttingDown)),
            "add_job during shutdown should return SchedulerShuttingDown error, got: {:?}",
            result
        );
    }
}
