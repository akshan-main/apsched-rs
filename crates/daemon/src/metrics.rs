//! Prometheus-compatible metrics tracking.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Thread-safe metrics collector for the daemon.
#[derive(Debug)]
pub struct Metrics {
    /// Total number of scheduled jobs (gauge).
    pub jobs_total: AtomicI64,
    /// Currently running/active jobs (gauge).
    pub jobs_active: AtomicI64,
    /// Total successful executions (counter).
    pub executions_success: AtomicU64,
    /// Total errored executions (counter).
    pub executions_error: AtomicU64,
    /// Total missed executions (counter).
    pub executions_missed: AtomicU64,
    /// Sum of job durations in milliseconds (for computing average).
    pub job_duration_sum_ms: AtomicU64,
    /// Count of job duration observations.
    pub job_duration_count: AtomicU64,
    /// Sum of scheduler loop durations in microseconds.
    pub loop_duration_sum_us: AtomicU64,
    /// Count of scheduler loop iterations.
    pub loop_duration_count: AtomicU64,
    /// Sum of store operation durations in microseconds.
    pub store_op_duration_sum_us: AtomicU64,
    /// Count of store operations.
    pub store_op_count: AtomicU64,
    /// Sum of wakeup latency in microseconds.
    pub wakeup_latency_sum_us: AtomicU64,
    /// Count of wakeup latency observations.
    pub wakeup_latency_count: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Create a new zeroed metrics instance.
    pub fn new() -> Self {
        Self {
            jobs_total: AtomicI64::new(0),
            jobs_active: AtomicI64::new(0),
            executions_success: AtomicU64::new(0),
            executions_error: AtomicU64::new(0),
            executions_missed: AtomicU64::new(0),
            job_duration_sum_ms: AtomicU64::new(0),
            job_duration_count: AtomicU64::new(0),
            loop_duration_sum_us: AtomicU64::new(0),
            loop_duration_count: AtomicU64::new(0),
            store_op_duration_sum_us: AtomicU64::new(0),
            store_op_count: AtomicU64::new(0),
            wakeup_latency_sum_us: AtomicU64::new(0),
            wakeup_latency_count: AtomicU64::new(0),
        }
    }

    /// Record a successful execution with the given duration in milliseconds.
    pub fn record_success(&self, duration_ms: u64) {
        self.executions_success.fetch_add(1, Ordering::Relaxed);
        self.job_duration_sum_ms
            .fetch_add(duration_ms, Ordering::Relaxed);
        self.job_duration_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed execution with the given duration in milliseconds.
    pub fn record_error(&self, duration_ms: u64) {
        self.executions_error.fetch_add(1, Ordering::Relaxed);
        self.job_duration_sum_ms
            .fetch_add(duration_ms, Ordering::Relaxed);
        self.job_duration_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a missed execution.
    pub fn record_missed(&self) {
        self.executions_missed.fetch_add(1, Ordering::Relaxed);
    }

    /// Format metrics in Prometheus text exposition format.
    pub fn to_prometheus(&self) -> String {
        let jobs_total = self.jobs_total.load(Ordering::Relaxed);
        let jobs_active = self.jobs_active.load(Ordering::Relaxed);
        let exec_success = self.executions_success.load(Ordering::Relaxed);
        let exec_error = self.executions_error.load(Ordering::Relaxed);
        let exec_missed = self.executions_missed.load(Ordering::Relaxed);
        let dur_sum = self.job_duration_sum_ms.load(Ordering::Relaxed);
        let dur_count = self.job_duration_count.load(Ordering::Relaxed);
        let loop_sum = self.loop_duration_sum_us.load(Ordering::Relaxed);
        let loop_count = self.loop_duration_count.load(Ordering::Relaxed);
        let store_sum = self.store_op_duration_sum_us.load(Ordering::Relaxed);
        let store_count = self.store_op_count.load(Ordering::Relaxed);
        let wakeup_sum = self.wakeup_latency_sum_us.load(Ordering::Relaxed);
        let wakeup_count = self.wakeup_latency_count.load(Ordering::Relaxed);

        let dur_sum_secs = dur_sum as f64 / 1000.0;
        let loop_sum_secs = loop_sum as f64 / 1_000_000.0;
        let store_sum_secs = store_sum as f64 / 1_000_000.0;
        let wakeup_sum_secs = wakeup_sum as f64 / 1_000_000.0;

        format!(
            "\
# HELP apscheduler_jobs_total Total number of scheduled jobs
# TYPE apscheduler_jobs_total gauge
apscheduler_jobs_total {jobs_total}

# HELP apscheduler_jobs_active Number of currently running jobs
# TYPE apscheduler_jobs_active gauge
apscheduler_jobs_active {jobs_active}

# HELP apscheduler_job_executions_total Total job executions by status
# TYPE apscheduler_job_executions_total counter
apscheduler_job_executions_total{{status=\"success\"}} {exec_success}
apscheduler_job_executions_total{{status=\"error\"}} {exec_error}
apscheduler_job_executions_total{{status=\"missed\"}} {exec_missed}

# HELP apscheduler_job_duration_seconds Job execution duration
# TYPE apscheduler_job_duration_seconds summary
apscheduler_job_duration_seconds_sum {dur_sum_secs}
apscheduler_job_duration_seconds_count {dur_count}

# HELP apscheduler_scheduler_loop_duration_seconds Scheduler loop iteration duration
# TYPE apscheduler_scheduler_loop_duration_seconds summary
apscheduler_scheduler_loop_duration_seconds_sum {loop_sum_secs}
apscheduler_scheduler_loop_duration_seconds_count {loop_count}

# HELP apscheduler_store_operation_duration_seconds Store operation duration
# TYPE apscheduler_store_operation_duration_seconds summary
apscheduler_store_operation_duration_seconds_sum {store_sum_secs}
apscheduler_store_operation_duration_seconds_count {store_count}

# HELP apscheduler_wakeup_latency_seconds Wakeup latency
# TYPE apscheduler_wakeup_latency_seconds summary
apscheduler_wakeup_latency_seconds_sum {wakeup_sum_secs}
apscheduler_wakeup_latency_seconds_count {wakeup_count}
"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_success() {
        let m = Metrics::new();
        m.record_success(150);
        assert_eq!(m.executions_success.load(Ordering::Relaxed), 1);
        assert_eq!(m.job_duration_sum_ms.load(Ordering::Relaxed), 150);
        assert_eq!(m.job_duration_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_prometheus_format() {
        let m = Metrics::new();
        m.jobs_total.store(5, Ordering::Relaxed);
        m.record_success(100);
        m.record_error(200);
        m.record_missed();

        let output = m.to_prometheus();
        assert!(output.contains("apscheduler_jobs_total 5"));
        assert!(output.contains("apscheduler_job_executions_total{status=\"success\"} 1"));
        assert!(output.contains("apscheduler_job_executions_total{status=\"error\"} 1"));
        assert!(output.contains("apscheduler_job_executions_total{status=\"missed\"} 1"));
    }
}
