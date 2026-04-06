//! Execution history tracking via a ring buffer per job.

use std::collections::VecDeque;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

/// Tracks execution history for all jobs.
#[derive(Debug)]
pub struct ExecutionHistory {
    entries: DashMap<String, VecDeque<ExecutionRecord>>,
    max_per_job: usize,
}

/// A single execution record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRecord {
    pub job_id: String,
    pub scheduled_time: DateTime<Utc>,
    pub actual_time: DateTime<Utc>,
    pub duration_ms: u64,
    pub outcome: String,
    pub error_message: Option<String>,
    pub output: Option<String>,
}

impl ExecutionHistory {
    /// Create a new history tracker with the given max entries per job.
    pub fn new(max_per_job: usize) -> Self {
        Self {
            entries: DashMap::new(),
            max_per_job,
        }
    }

    /// Record a new execution.
    pub fn record(&self, record: ExecutionRecord) {
        let job_id = record.job_id.clone();

        let mut entry = self.entries.entry(job_id).or_default();
        if entry.len() >= self.max_per_job {
            entry.pop_front();
        }
        entry.push_back(record);
    }

    /// Get the execution history for a specific job.
    pub fn get(&self, job_id: &str) -> Vec<ExecutionRecord> {
        self.entries
            .get(job_id)
            .map(|e| e.value().iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get the total number of executions across all jobs.
    #[allow(dead_code)]
    pub fn total_executions(&self) -> usize {
        self.entries.iter().map(|e| e.value().len()).sum()
    }

    /// Remove all history for a specific job.
    pub fn remove(&self, job_id: &str) {
        self.entries.remove(job_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_get() {
        let history = ExecutionHistory::new(10);
        let now = Utc::now();
        history.record(ExecutionRecord {
            job_id: "job1".to_string(),
            scheduled_time: now,
            actual_time: now,
            duration_ms: 100,
            outcome: "success".to_string(),
            error_message: None,
            output: Some("ok".to_string()),
        });

        let records = history.get("job1");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].outcome, "success");
    }

    #[test]
    fn test_ring_buffer_overflow() {
        let history = ExecutionHistory::new(3);
        let now = Utc::now();

        for i in 0..5 {
            history.record(ExecutionRecord {
                job_id: "job1".to_string(),
                scheduled_time: now,
                actual_time: now,
                duration_ms: i * 10,
                outcome: "success".to_string(),
                error_message: None,
                output: None,
            });
        }

        let records = history.get("job1");
        assert_eq!(records.len(), 3);
        // Oldest entries should have been dropped
        assert_eq!(records[0].duration_ms, 20);
    }

    #[test]
    fn test_empty_history() {
        let history = ExecutionHistory::new(10);
        assert!(history.get("nonexistent").is_empty());
        assert_eq!(history.total_executions(), 0);
    }
}
