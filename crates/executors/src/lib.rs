//! Executor implementations for apscheduler-rs.
//!
//! This crate provides concrete executor backends that implement the
//! [`apsched_core::traits::Executor`] trait:
//!
//! - [`ThreadPoolExecutor`] — spawns jobs as tokio tasks with a configurable
//!   concurrency limit.
//! - [`ProcessPoolExecutor`] — scaffold for process-based execution (actual
//!   process isolation is provided by the pyext layer).
//!
//! The [`CallableHandle`] enum is the bridge between the scheduler and the
//! actual function invocation: the `RustFn` variant works in pure Rust, while
//! `PythonRef` is resolved by the pyext crate.

pub mod base;
pub mod processpool;
pub mod threadpool;

pub use base::CallableHandle;
pub use processpool::ProcessPoolExecutor;
pub use threadpool::ThreadPoolExecutor;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::Utc;
    use futures::FutureExt;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use apsched_core::error::ExecutorError;
    use apsched_core::model::{CallableRef, JobOutcome, JobSpec, TaskSpec};
    use apsched_core::traits::Executor;

    use crate::base::CallableHandle;
    use crate::processpool::ProcessPoolExecutor;
    use crate::threadpool::ThreadPoolExecutor;

    /// Helper: create a minimal JobSpec for testing.
    fn test_job() -> JobSpec {
        JobSpec {
            id: Uuid::new_v4(),
            schedule_id: "test-schedule".to_string(),
            task: TaskSpec::new(CallableRef::ImportPath("test:func".to_string())),
            executor: "threadpool".to_string(),
            scheduled_fire_time: Utc::now(),
            actual_fire_time: None,
            deadline: None,
            attempt: 1,
        }
    }

    /// Helper: create a RustFn callable that returns Success after a short delay.
    fn success_callable() -> CallableHandle {
        CallableHandle::RustFn(Arc::new(|_job| async { JobOutcome::Success }.boxed()))
    }

    /// Helper: callable that sleeps for the given duration then returns Success.
    fn slow_callable(dur: Duration) -> CallableHandle {
        CallableHandle::RustFn(Arc::new(move |_job| {
            async move {
                tokio::time::sleep(dur).await;
                JobOutcome::Success
            }
            .boxed()
        }))
    }

    // -----------------------------------------------------------------------
    // ThreadPoolExecutor tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn threadpool_start_shutdown_lifecycle() {
        let exec = ThreadPoolExecutor::new(4);
        assert_eq!(exec.executor_type(), "threadpool");

        exec.start().await.unwrap();
        exec.shutdown(false).await.unwrap();
    }

    #[tokio::test]
    async fn threadpool_submit_and_receive_result() {
        let exec = ThreadPoolExecutor::new(4);
        exec.start().await.unwrap();

        let (tx, mut rx) = mpsc::channel(16);
        let job = test_job();
        let job_id = job.id;

        exec.submit_job_with_callable(job, success_callable(), tx)
            .await
            .unwrap();

        let result = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timeout waiting for result")
            .expect("channel closed");

        assert_eq!(result.job_id, job_id);
        assert!(matches!(result.outcome, JobOutcome::Success));

        exec.shutdown(true).await.unwrap();
    }

    #[tokio::test]
    async fn threadpool_max_workers_enforced() {
        let exec = ThreadPoolExecutor::new(1);
        exec.start().await.unwrap();

        let (tx, _rx) = mpsc::channel(16);

        // First job should succeed.
        exec.submit_job_with_callable(
            test_job(),
            slow_callable(Duration::from_secs(5)),
            tx.clone(),
        )
        .await
        .unwrap();

        // Second job should fail — pool exhausted.
        let err = exec
            .submit_job_with_callable(test_job(), success_callable(), tx)
            .await
            .unwrap_err();

        assert!(matches!(err, ExecutorError::PoolExhausted));

        exec.shutdown(false).await.unwrap();
    }

    #[tokio::test]
    async fn threadpool_running_count_tracks() {
        let exec = ThreadPoolExecutor::new(4);
        exec.start().await.unwrap();

        assert_eq!(exec.running_job_count().await, 0);

        let (tx, _rx) = mpsc::channel(16);
        exec.submit_job_with_callable(test_job(), slow_callable(Duration::from_millis(200)), tx)
            .await
            .unwrap();

        // Give tokio a moment to start the task.
        tokio::task::yield_now().await;
        assert_eq!(exec.running_job_count().await, 1);

        // Wait for it to finish.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(exec.running_job_count().await, 0);

        exec.shutdown(true).await.unwrap();
    }

    #[tokio::test]
    async fn threadpool_shutdown_wait_true_waits_for_jobs() {
        let exec = ThreadPoolExecutor::new(4);
        exec.start().await.unwrap();

        let (tx, mut rx) = mpsc::channel(16);
        exec.submit_job_with_callable(test_job(), slow_callable(Duration::from_millis(100)), tx)
            .await
            .unwrap();

        // shutdown(wait=true) should wait for the job to finish.
        exec.shutdown(true).await.unwrap();

        // Result should be available.
        let result = rx
            .try_recv()
            .expect("should have result after shutdown(wait=true)");
        assert!(matches!(result.outcome, JobOutcome::Success));
    }

    #[tokio::test]
    async fn threadpool_shutdown_wait_false_returns_immediately() {
        let exec = ThreadPoolExecutor::new(4);
        exec.start().await.unwrap();

        let (tx, _rx) = mpsc::channel(16);
        exec.submit_job_with_callable(test_job(), slow_callable(Duration::from_secs(10)), tx)
            .await
            .unwrap();

        // shutdown(wait=false) should return quickly.
        let start = std::time::Instant::now();
        exec.shutdown(false).await.unwrap();
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn threadpool_submit_after_shutdown() {
        let exec = ThreadPoolExecutor::new(4);
        exec.start().await.unwrap();
        exec.shutdown(false).await.unwrap();

        let (tx, _rx) = mpsc::channel(16);
        let err = exec
            .submit_job_with_callable(test_job(), success_callable(), tx)
            .await
            .unwrap_err();

        assert!(matches!(err, ExecutorError::ShutdownInProgress));
    }

    #[tokio::test]
    async fn threadpool_submit_before_start() {
        let exec = ThreadPoolExecutor::new(4);

        let (tx, _rx) = mpsc::channel(16);
        let err = exec
            .submit_job_with_callable(test_job(), success_callable(), tx)
            .await
            .unwrap_err();

        assert!(matches!(err, ExecutorError::NotStarted));
    }

    #[tokio::test]
    async fn threadpool_multiple_concurrent_jobs() {
        let exec = ThreadPoolExecutor::new(4);
        exec.start().await.unwrap();

        let (tx, mut rx) = mpsc::channel(16);

        for _ in 0..4 {
            exec.submit_job_with_callable(
                test_job(),
                slow_callable(Duration::from_millis(50)),
                tx.clone(),
            )
            .await
            .unwrap();
        }

        // Collect all 4 results.
        for _ in 0..4 {
            let result = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("timeout")
                .expect("channel closed");
            assert!(matches!(result.outcome, JobOutcome::Success));
        }

        exec.shutdown(true).await.unwrap();
    }

    #[tokio::test]
    async fn threadpool_trait_submit_job() {
        // Test the Executor trait's submit_job method (without callable).
        let exec = ThreadPoolExecutor::new(4);
        exec.start().await.unwrap();

        let (tx, mut rx) = mpsc::channel(16);
        exec.submit_job(test_job(), tx).await.unwrap();

        let result = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");
        assert!(matches!(result.outcome, JobOutcome::Success));

        exec.shutdown(true).await.unwrap();
    }

    // -----------------------------------------------------------------------
    // ProcessPoolExecutor tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn processpool_start_shutdown_lifecycle() {
        let exec = ProcessPoolExecutor::new(4);
        assert_eq!(exec.executor_type(), "processpool");

        exec.start().await.unwrap();
        exec.shutdown(false).await.unwrap();
    }

    #[tokio::test]
    async fn processpool_submit_and_receive_result() {
        let exec = ProcessPoolExecutor::new(4);
        exec.start().await.unwrap();

        let (tx, mut rx) = mpsc::channel(16);
        let job = test_job();
        let job_id = job.id;

        exec.submit_job_with_callable(job, success_callable(), tx)
            .await
            .unwrap();

        let result = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");

        assert_eq!(result.job_id, job_id);
        assert!(matches!(result.outcome, JobOutcome::Success));

        exec.shutdown(true).await.unwrap();
    }

    #[tokio::test]
    async fn processpool_max_workers_enforced() {
        let exec = ProcessPoolExecutor::new(1);
        exec.start().await.unwrap();

        let (tx, _rx) = mpsc::channel(16);
        exec.submit_job_with_callable(
            test_job(),
            slow_callable(Duration::from_secs(5)),
            tx.clone(),
        )
        .await
        .unwrap();

        let err = exec
            .submit_job_with_callable(test_job(), success_callable(), tx)
            .await
            .unwrap_err();

        assert!(matches!(err, ExecutorError::PoolExhausted));
        exec.shutdown(false).await.unwrap();
    }

    #[tokio::test]
    async fn processpool_submit_after_shutdown() {
        let exec = ProcessPoolExecutor::new(4);
        exec.start().await.unwrap();
        exec.shutdown(false).await.unwrap();

        let (tx, _rx) = mpsc::channel(16);
        let err = exec
            .submit_job_with_callable(test_job(), success_callable(), tx)
            .await
            .unwrap_err();

        assert!(matches!(err, ExecutorError::ShutdownInProgress));
    }
}
