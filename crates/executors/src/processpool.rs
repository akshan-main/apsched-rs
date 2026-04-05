//! Process-pool based executor implementation (scaffold).
//!
//! In the Python context this would delegate to multiprocessing for true
//! process isolation. For now the Rust implementation mirrors
//! [`ThreadPoolExecutor`] — actual process spawning is handled at the
//! Python boundary in the `pyext` crate.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use apsched_core::error::ExecutorError;
use apsched_core::model::{JobOutcome, JobResultEnvelope, JobSpec};
use apsched_core::traits::Executor;

use crate::base::CallableHandle;

/// A process-pool executor scaffold.
///
/// Structurally identical to [`super::ThreadPoolExecutor`]. In the full
/// Python-backed system, actual process isolation is provided by the pyext
/// layer. This struct keeps the same accounting (running count, shutdown
/// flag, etc.) so the scheduler can treat both executor types uniformly.
pub struct ProcessPoolExecutor {
    max_workers: usize,
    running_count: Arc<AtomicU32>,
    started: Arc<AtomicBool>,
    shutdown_flag: Arc<AtomicBool>,
    active_jobs: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl std::fmt::Debug for ProcessPoolExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessPoolExecutor")
            .field("max_workers", &self.max_workers)
            .field("running_count", &self.running_count.load(Ordering::Relaxed))
            .field("started", &self.started.load(Ordering::Relaxed))
            .field("shutdown_flag", &self.shutdown_flag.load(Ordering::Relaxed))
            .finish()
    }
}

impl ProcessPoolExecutor {
    /// Create a new `ProcessPoolExecutor` with the given maximum worker count.
    pub fn new(max_workers: usize) -> Self {
        Self {
            max_workers,
            running_count: Arc::new(AtomicU32::new(0)),
            started: Arc::new(AtomicBool::new(false)),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            active_jobs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Submit a job together with its callable handle and a result channel.
    pub async fn submit_job_with_callable(
        &self,
        job: JobSpec,
        callable: CallableHandle,
        result_tx: tokio::sync::mpsc::Sender<JobResultEnvelope>,
    ) -> Result<(), ExecutorError> {
        if self.shutdown_flag.load(Ordering::Relaxed) {
            return Err(ExecutorError::ShutdownInProgress);
        }
        if !self.started.load(Ordering::Relaxed) {
            return Err(ExecutorError::NotStarted);
        }

        let current = self.running_count.load(Ordering::Relaxed) as usize;
        if current >= self.max_workers {
            return Err(ExecutorError::PoolExhausted);
        }

        self.running_count.fetch_add(1, Ordering::Relaxed);

        let running_count = Arc::clone(&self.running_count);
        let job_id = job.id;
        let schedule_id = job.schedule_id.clone();

        let handle = tokio::spawn(async move {
            let outcome = match callable {
                CallableHandle::RustFn(f) => f(job).await,
                CallableHandle::PythonRef(ref_str) => {
                    warn!(
                        callable = %ref_str,
                        "PythonRef callable invoked in Rust executor; returning error"
                    );
                    JobOutcome::Error(format!(
                        "PythonRef callable '{}' cannot be executed in pure-Rust executor",
                        ref_str
                    ))
                }
            };

            let envelope = JobResultEnvelope {
                job_id,
                schedule_id,
                outcome,
                completed_at: Utc::now(),
            };

            if let Err(e) = result_tx.send(envelope).await {
                warn!("failed to send job result: {}", e);
            }

            running_count.fetch_sub(1, Ordering::Relaxed);
            debug!(job_id = %job_id, "job completed (processpool)");
        });

        self.active_jobs.lock().await.push(handle);
        Ok(())
    }
}

#[async_trait]
impl Executor for ProcessPoolExecutor {
    async fn start(&self) -> Result<(), ExecutorError> {
        self.started.store(true, Ordering::Relaxed);
        debug!(
            "ProcessPoolExecutor started with {} max workers",
            self.max_workers
        );
        Ok(())
    }

    async fn shutdown(&self, wait: bool) -> Result<(), ExecutorError> {
        self.shutdown_flag.store(true, Ordering::Relaxed);
        debug!(wait = wait, "ProcessPoolExecutor shutting down");

        if wait {
            let mut jobs = self.active_jobs.lock().await;
            let handles: Vec<_> = jobs.drain(..).collect();
            for handle in handles {
                let _ = handle.await;
            }
        }

        self.started.store(false, Ordering::Relaxed);
        Ok(())
    }

    async fn submit_job(
        &self,
        job: JobSpec,
        result_tx: tokio::sync::mpsc::Sender<JobResultEnvelope>,
    ) -> Result<(), ExecutorError> {
        if self.shutdown_flag.load(Ordering::Relaxed) {
            return Err(ExecutorError::ShutdownInProgress);
        }
        if !self.started.load(Ordering::Relaxed) {
            return Err(ExecutorError::NotStarted);
        }

        let current = self.running_count.load(Ordering::Relaxed) as usize;
        if current >= self.max_workers {
            return Err(ExecutorError::PoolExhausted);
        }

        self.running_count.fetch_add(1, Ordering::Relaxed);

        let running_count = Arc::clone(&self.running_count);
        let job_id = job.id;
        let schedule_id = job.schedule_id.clone();

        let handle = tokio::spawn(async move {
            let envelope = JobResultEnvelope {
                job_id,
                schedule_id,
                outcome: JobOutcome::Success,
                completed_at: Utc::now(),
            };

            if let Err(e) = result_tx.send(envelope).await {
                warn!("failed to send job result: {}", e);
            }

            running_count.fetch_sub(1, Ordering::Relaxed);
        });

        self.active_jobs.lock().await.push(handle);
        Ok(())
    }

    async fn running_job_count(&self) -> usize {
        self.running_count.load(Ordering::Relaxed) as usize
    }

    fn executor_type(&self) -> &'static str {
        "processpool"
    }
}
