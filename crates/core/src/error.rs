use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum SchedulerError {
    #[error("scheduler is already running")]
    AlreadyRunning,
    #[error("scheduler is not running")]
    NotRunning,
    #[error("job not found: {job_id}")]
    JobNotFound { job_id: String },
    #[error("job store not found: {alias}")]
    JobStoreNotFound { alias: String },
    #[error("executor not found: {alias}")]
    ExecutorNotFound { alias: String },
    #[error("duplicate job ID: {job_id}")]
    DuplicateJobId { job_id: String },
    #[error("duplicate job store: {alias}")]
    DuplicateJobStore { alias: String },
    #[error("duplicate executor: {alias}")]
    DuplicateExecutor { alias: String },
    #[error("trigger error: {0}")]
    TriggerError(#[from] TriggerError),
    #[error("store error: {0}")]
    StoreError(#[from] StoreError),
    #[error("executor error: {0}")]
    ExecutorError(#[from] ExecutorError),
    #[error("serialization error: {0}")]
    SerializationError(String),
    #[error("callable not found: {ref_str}")]
    CallableNotFound { ref_str: String },
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error("shutdown timeout")]
    ShutdownTimeout,
    #[error("scheduler is paused")]
    SchedulerPaused,
    #[error("scheduler is shutting down")]
    SchedulerShuttingDown,
}

#[derive(Error, Debug, Clone)]
pub enum TriggerError {
    #[error("invalid cron expression: {0}")]
    InvalidCronExpression(String),
    #[error("invalid date: {0}")]
    InvalidDate(String),
    #[error("invalid interval: {0}")]
    InvalidInterval(String),
    #[error("no next fire time")]
    NoNextFireTime,
    #[error("invalid calendar interval: {0}")]
    InvalidCalendarInterval(String),
}

#[derive(Error, Debug, Clone)]
pub enum StoreError {
    #[error("connection failed: {0}")]
    ConnectionFailed(String),
    #[error("query failed: {0}")]
    QueryFailed(String),
    #[error("serialization failed: {0}")]
    SerializationFailed(String),
    #[error("deserialization failed: {0}")]
    DeserializationFailed(String),
    #[error("lease conflict for job: {job_id}")]
    LeaseConflict { job_id: String },
    #[error("corrupted data for job {job_id}: {detail}")]
    CorruptedData { job_id: String, detail: String },
    #[error("job not found: {job_id}")]
    JobNotFound { job_id: String },
    #[error("duplicate job: {job_id}")]
    DuplicateJob { job_id: String },
}

#[derive(Error, Debug, Clone)]
pub enum ExecutorError {
    #[error("executor pool exhausted")]
    PoolExhausted,
    #[error("job failed: {job_id}: {error}")]
    JobFailed { job_id: String, error: String },
    #[error("job timeout: {job_id}")]
    Timeout { job_id: String },
    #[error("shutdown in progress")]
    ShutdownInProgress,
    #[error("executor not started")]
    NotStarted,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scheduler_error_display() {
        let err = SchedulerError::AlreadyRunning;
        assert_eq!(err.to_string(), "scheduler is already running");

        let err = SchedulerError::JobNotFound {
            job_id: "test-123".to_string(),
        };
        assert_eq!(err.to_string(), "job not found: test-123");
    }

    #[test]
    fn test_trigger_error_display() {
        let err = TriggerError::InvalidCronExpression("bad cron".to_string());
        assert_eq!(err.to_string(), "invalid cron expression: bad cron");

        let err = TriggerError::NoNextFireTime;
        assert_eq!(err.to_string(), "no next fire time");
    }

    #[test]
    fn test_store_error_display() {
        let err = StoreError::LeaseConflict {
            job_id: "j1".to_string(),
        };
        assert_eq!(err.to_string(), "lease conflict for job: j1");

        let err = StoreError::CorruptedData {
            job_id: "j2".to_string(),
            detail: "missing field".to_string(),
        };
        assert_eq!(err.to_string(), "corrupted data for job j2: missing field");
    }

    #[test]
    fn test_executor_error_display() {
        let err = ExecutorError::PoolExhausted;
        assert_eq!(err.to_string(), "executor pool exhausted");

        let err = ExecutorError::JobFailed {
            job_id: "j1".to_string(),
            error: "panic".to_string(),
        };
        assert_eq!(err.to_string(), "job failed: j1: panic");
    }

    #[test]
    fn test_scheduler_error_from_trigger_error() {
        let trigger_err = TriggerError::NoNextFireTime;
        let scheduler_err: SchedulerError = trigger_err.into();
        assert!(matches!(scheduler_err, SchedulerError::TriggerError(_)));
    }

    #[test]
    fn test_scheduler_error_from_store_error() {
        let store_err = StoreError::ConnectionFailed("timeout".to_string());
        let scheduler_err: SchedulerError = store_err.into();
        assert!(matches!(scheduler_err, SchedulerError::StoreError(_)));
    }

    #[test]
    fn test_scheduler_error_from_executor_error() {
        let exec_err = ExecutorError::PoolExhausted;
        let scheduler_err: SchedulerError = exec_err.into();
        assert!(matches!(scheduler_err, SchedulerError::ExecutorError(_)));
    }

    #[test]
    fn test_error_clone() {
        let err = SchedulerError::AlreadyRunning;
        let cloned = err.clone();
        assert_eq!(err.to_string(), cloned.to_string());
    }
}
