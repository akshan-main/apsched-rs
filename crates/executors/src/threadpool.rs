//! Thread-pool based executor implementation.

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

/// A Rust-native thread pool executor for running jobs.
///
/// Jobs are spawned as tokio tasks. The `max_workers` limit controls the
/// maximum number of concurrently running jobs.
pub struct ThreadPoolExecutor {
    max_workers: usize,
    running_count: Arc<AtomicU32>,
    started: Arc<AtomicBool>,
    shutdown_flag: Arc<AtomicBool>,
    active_jobs: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl std::fmt::Debug for ThreadPoolExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadPoolExecutor")
            .field("max_workers", &self.max_workers)
            .field("running_count", &self.running_count.load(Ordering::Relaxed))
            .field("started", &self.started.load(Ordering::Relaxed))
            .field("shutdown_flag", &self.shutdown_flag.load(Ordering::Relaxed))
            .finish()
    }
}

impl ThreadPoolExecutor {
    /// Create a new `ThreadPoolExecutor` with the given maximum worker count.
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
    ///
    /// This is the primary entry point used by the scheduler. The `callable`
    /// determines how the job is actually invoked.
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
            debug!(job_id = %job_id, "job completed");
        });

        self.active_jobs.lock().await.push(handle);
        Ok(())
    }
}

#[async_trait]
impl Executor for ThreadPoolExecutor {
    async fn start(&self) -> Result<(), ExecutorError> {
        self.started.store(true, Ordering::Relaxed);
        debug!(
            "ThreadPoolExecutor started with {} max workers",
            self.max_workers
        );
        Ok(())
    }

    async fn shutdown(&self, wait: bool) -> Result<(), ExecutorError> {
        self.shutdown_flag.store(true, Ordering::Relaxed);
        debug!(wait = wait, "ThreadPoolExecutor shutting down");

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
            // Default implementation: mark as success (the real callable
            // invocation goes through submit_job_with_callable).
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
        "threadpool"
    }
}
