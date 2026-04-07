use chrono::{DateTime, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use uuid::Uuid;

/// A single log entry appended by a job via `ctx.log(...)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobLogEntry {
    pub timestamp: DateTime<Utc>,
    pub message: String,
    pub run_id: String,
}

/// In-memory key/value storage scoped per job, with output for DAG passing.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct JobMemory {
    /// Per-job key/value storage. Persists across runs of the same job.
    pub state: HashMap<String, serde_json::Value>,
    /// The "output" that downstream DAG jobs can read.
    pub last_output: Option<serde_json::Value>,
    /// Timestamp of last successful output.
    pub last_output_at: Option<DateTime<Utc>>,
    /// Per-run logs (last N entries, ring buffer).
    pub logs: Vec<JobLogEntry>,
}

/// An entry in the dead letter queue, representing a job that failed after
/// exhausting all retries (or with no retry policy).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterEntry {
    pub job_id: String,
    pub schedule_id: String,
    pub failed_at: DateTime<Utc>,
    pub scheduled_fire_time: DateTime<Utc>,
    pub error_type: String,
    pub error_message: String,
    pub traceback: Option<String>,
    pub attempt: u32,
    pub task: TaskSpec,
}

/// Rate limit configuration for a job or group: max N executions within a
/// sliding window of W seconds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimit {
    pub max_executions: u32,
    pub window_seconds: u64,
}

/// Represents a serialized value that can be passed as an argument to a callable.
/// Supports multiple serialization formats for cross-language compatibility.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SerializedValue {
    /// Python pickle bytes (for backward compat with APScheduler 3.x)
    Pickle(Vec<u8>),
    /// JSON value
    Json(serde_json::Value),
    /// CBOR bytes
    Cbor(Vec<u8>),
    /// No value / None / null
    None,
}

impl SerializedValue {
    /// Create a JSON serialized value from any serializable type.
    pub fn from_json<T: Serialize>(value: &T) -> Result<Self, serde_json::Error> {
        Ok(SerializedValue::Json(serde_json::to_value(value)?))
    }

    /// Try to deserialize a JSON value into a concrete type.
    pub fn to_json<T: for<'de> Deserialize<'de>>(&self) -> Option<T> {
        match self {
            SerializedValue::Json(v) => serde_json::from_value(v.clone()).ok(),
            _ => None,
        }
    }

    /// Returns true if this is a None value.
    pub fn is_none(&self) -> bool {
        matches!(self, SerializedValue::None)
    }
}

/// Reference to a callable function/task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum CallableRef {
    /// A dotted import path (e.g., "mymodule:my_function") for cross-language callables.
    ImportPath(String),
    /// An in-memory handle (pointer/id) for Rust-native callables.
    InMemoryHandle(u64),
}

impl CallableRef {
    /// Returns a human-readable string for error messages.
    pub fn ref_string(&self) -> String {
        match self {
            CallableRef::ImportPath(path) => path.clone(),
            CallableRef::InMemoryHandle(handle) => format!("handle:{}", handle),
        }
    }
}

/// Specifies a task to be executed, including the callable and its arguments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSpec {
    pub callable_ref: CallableRef,
    pub args: Vec<SerializedValue>,
    pub kwargs: HashMap<String, SerializedValue>,
}

impl TaskSpec {
    pub fn new(callable_ref: CallableRef) -> Self {
        Self {
            callable_ref,
            args: Vec::new(),
            kwargs: HashMap::new(),
        }
    }

    pub fn with_args(mut self, args: Vec<SerializedValue>) -> Self {
        self.args = args;
        self
    }

    pub fn with_kwargs(mut self, kwargs: HashMap<String, SerializedValue>) -> Self {
        self.kwargs = kwargs;
        self
    }
}

/// Controls whether multiple pending executions of the same job are combined into one.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CoalescePolicy {
    /// Do not coalesce; run every pending execution.
    Off,
    /// Coalesce all pending executions into a single run.
    #[default]
    On,
}

/// Controls what happens when a job misses its scheduled fire time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MisfirePolicy {
    /// Always run the job, regardless of how late it is.
    RunAlways,
    /// Run the job only if it is within the grace period.
    GracePeriod(#[serde(with = "duration_serde")] Duration),
    /// Skip the missed execution entirely.
    Skip,
}

impl Default for MisfirePolicy {
    fn default() -> Self {
        MisfirePolicy::GracePeriod(Duration::from_secs(1))
    }
}

/// Serde helper for std::time::Duration (as seconds f64).
mod duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        duration.as_secs_f64().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let secs = f64::deserialize(deserializer)?;
        Ok(Duration::from_secs_f64(secs))
    }
}

/// Serde helper for Option<Duration>.
mod option_duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(
        opt: &Option<Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match opt {
            Some(d) => d.as_secs_f64().serialize(serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Duration>, D::Error> {
        let opt: Option<f64> = Option::deserialize(deserializer)?;
        Ok(opt.map(Duration::from_secs_f64))
    }
}

/// Configuration for automatic retries on job failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_retries: u32,
    #[serde(with = "duration_serde")]
    pub retry_delay: Duration,
    pub backoff_factor: f64,
    #[serde(with = "duration_serde")]
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 0,
            retry_delay: Duration::from_secs(1),
            backoff_factor: 2.0,
            max_delay: Duration::from_secs(300),
        }
    }
}

impl RetryPolicy {
    /// Compute the delay before the given attempt (0-indexed).
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return self.retry_delay;
        }
        let factor = self.backoff_factor.powi(attempt as i32);
        let delay_secs = self.retry_delay.as_secs_f64() * factor;
        let capped = delay_secs.min(self.max_delay.as_secs_f64());
        Duration::from_secs_f64(capped)
    }
}

/// Limits concurrent execution of jobs sharing a concurrency group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcurrencyGroup {
    pub name: String,
    pub max_concurrent: u32,
}

/// Serialized trigger state. Defined in core so that the triggers crate (which depends on core)
/// can construct and consume these variants.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TriggerState {
    Date {
        run_date: DateTime<Utc>,
        timezone: String,
    },
    Interval {
        weeks: i64,
        days: i64,
        hours: i64,
        minutes: i64,
        seconds: i64,
        start_date: Option<DateTime<Utc>>,
        end_date: Option<DateTime<Utc>>,
        timezone: String,
        /// Jitter in seconds.
        jitter: Option<f64>,
        /// Optional sub-second precision: total interval in microseconds.
        /// When `Some`, it overrides the integer component fields above for
        /// the actual interval computation. Used to support fractional
        /// (sub-second) intervals like 100ms.
        #[serde(default)]
        interval_micros: Option<i64>,
    },
    Cron {
        year: Option<String>,
        month: Option<String>,
        day: Option<String>,
        week: Option<String>,
        day_of_week: Option<String>,
        hour: Option<String>,
        minute: Option<String>,
        second: Option<String>,
        start_date: Option<DateTime<Utc>>,
        end_date: Option<DateTime<Utc>>,
        timezone: String,
        jitter: Option<f64>,
    },
    CalendarInterval {
        years: i32,
        months: i32,
        weeks: i32,
        days: i32,
        hour: u32,
        minute: u32,
        second: u32,
        start_date: Option<DateTime<Utc>>,
        end_date: Option<DateTime<Utc>>,
        timezone: String,
    },
    /// A custom plugin trigger whose scheduling logic lives in user code (e.g. Python).
    /// The description is a human-readable label; the actual fire-time computation
    /// is handled by the wrapper that holds the trigger object reference.
    Plugin { description: String },
}

impl TriggerState {
    /// Compute the next fire time after a job has been executed.
    /// `previous_fire_time` is the fire time that just occurred.
    /// `now` is the current time.
    /// Returns None for one-shot triggers (Date) or if past end_date.
    pub fn compute_next_fire_time(
        &self,
        previous_fire_time: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        match self {
            TriggerState::Date { .. } => {
                // Date triggers are one-shot
                None
            }
            TriggerState::Interval {
                weeks,
                days,
                hours,
                minutes,
                seconds,
                end_date,
                jitter,
                interval_micros,
                ..
            } => {
                let interval = if let Some(micros) = interval_micros {
                    if *micros <= 0 {
                        return None;
                    }
                    chrono::Duration::microseconds(*micros)
                } else {
                    let total_secs = (*weeks) * 7 * 86400
                        + (*days) * 86400
                        + (*hours) * 3600
                        + (*minutes) * 60
                        + (*seconds);
                    if total_secs <= 0 {
                        return None;
                    }
                    chrono::Duration::seconds(total_secs)
                };
                // Advance by one interval step only.  The caller is
                // responsible for skipping ahead further when
                // coalescing is active.
                let next = previous_fire_time + interval;
                // Check end_date
                if let Some(end) = end_date {
                    if next > *end {
                        return None;
                    }
                }
                // Apply jitter
                let next = if let Some(j) = jitter {
                    if *j > 0.0 {
                        let offset_ms = rand::thread_rng().gen_range(0..=(*j * 1000.0) as i64);
                        next + chrono::Duration::milliseconds(offset_ms)
                    } else {
                        next
                    }
                } else {
                    next
                };
                Some(next)
            }
            TriggerState::Cron { end_date, .. } => {
                // For cron, we can't easily compute the next fire time without
                // a full cron parser. Return now + 1 second as a placeholder;
                // the actual cron next-fire-time calculation happens when the
                // trigger is reconstructed. This ensures the job stays active.
                let next = now + chrono::Duration::seconds(1);
                if let Some(end) = end_date {
                    if next > *end {
                        return None;
                    }
                }
                Some(next)
            }
            TriggerState::CalendarInterval { end_date, .. } => {
                // Similar placeholder for calendar interval
                let next = now + chrono::Duration::seconds(1);
                if let Some(end) = end_date {
                    if next > *end {
                        return None;
                    }
                }
                Some(next)
            }
            TriggerState::Plugin { .. } => {
                // Plugin triggers compute their next fire time via the
                // wrapper that holds the user callback.  When we only have
                // the serialized state we use a 1-second placeholder so the
                // job stays active in the scheduler loop.
                Some(now + chrono::Duration::seconds(1))
            }
        }
    }

    /// Advance from `previous_fire_time` to the next fire time that is
    /// strictly after `now`.  Used when coalescing missed executions so that
    /// all past fire times are skipped in one shot.  Jitter is applied only
    /// to the final result, not to intermediate steps.
    pub fn compute_next_future_fire_time(
        &self,
        previous_fire_time: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        match self {
            TriggerState::Interval {
                weeks,
                days,
                hours,
                minutes,
                seconds,
                end_date,
                jitter,
                interval_micros,
                ..
            } => {
                let interval = if let Some(micros) = interval_micros {
                    if *micros <= 0 {
                        return None;
                    }
                    chrono::Duration::microseconds(*micros)
                } else {
                    let total_secs = (*weeks) * 7 * 86400
                        + (*days) * 86400
                        + (*hours) * 3600
                        + (*minutes) * 60
                        + (*seconds);
                    if total_secs <= 0 {
                        return None;
                    }
                    chrono::Duration::seconds(total_secs)
                };
                let mut next = previous_fire_time + interval;
                while next <= now {
                    next += interval;
                }
                if let Some(end) = end_date {
                    if next > *end {
                        return None;
                    }
                }
                // Apply jitter to the final result only
                if let Some(j) = jitter {
                    if *j > 0.0 {
                        let offset_ms = rand::thread_rng().gen_range(0..=(*j * 1000.0) as i64);
                        next += chrono::Duration::milliseconds(offset_ms);
                    }
                }
                Some(next)
            }
            // For other trigger types, fall back to iterative advancement
            _ => {
                let mut prev = previous_fire_time;
                loop {
                    match self.compute_next_fire_time(prev, now) {
                        Some(next) if next <= now => {
                            prev = next;
                        }
                        other => return other,
                    }
                }
            }
        }
    }
}

/// The main schedule record. Represents a recurring or one-shot scheduled task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleSpec {
    pub id: String,
    pub task: TaskSpec,
    pub trigger_state: TriggerState,
    pub executor: String,
    pub jobstore: String,
    pub name: Option<String>,
    #[serde(with = "option_duration_serde")]
    pub misfire_grace_time: Option<Duration>,
    pub coalesce: CoalescePolicy,
    pub max_instances: u32,
    #[serde(with = "option_duration_serde")]
    pub jitter: Option<Duration>,
    pub next_run_time: Option<DateTime<Utc>>,
    pub paused: bool,
    pub replace_existing: bool,
    pub version: u64,
    /// Optional rate limit for this job.
    #[serde(default)]
    pub rate_limit: Option<RateLimit>,
    /// Optional concurrency group name. Jobs sharing the same group name
    /// share a concurrency limit defined by `max_group_instances`.
    #[serde(default)]
    pub concurrency_group: Option<String>,
    /// Maximum concurrent instances across the concurrency group.
    #[serde(default = "default_max_group_instances")]
    pub max_group_instances: u32,
    /// Optional wall-clock timeout for a single execution of this job.
    /// When the timeout elapses before the callable returns, the scheduler
    /// reports a `TimeoutError` via `JobOutcome::Error` and frees the
    /// execution slot.
    ///
    /// Note: for Python callables, the underlying interpreter cannot be
    /// force-terminated mid-execution due to GIL constraints.  The Python
    /// code continues running to completion in the background, but the
    /// scheduler considers the slot freed so that subsequent fires can
    /// proceed on schedule.
    #[serde(default, with = "option_duration_serde")]
    pub timeout: Option<Duration>,
    /// Job IDs that must complete before this job runs.  When non-empty,
    /// the job is triggered by upstream completion rather than by its own
    /// time trigger.  The `trigger_state` is ignored for dependency-driven
    /// jobs (typically set to a placeholder by the caller).
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// If true, the dependency trigger fires even when one or more upstream
    /// jobs failed (dependency becomes "any completion").  Default: false,
    /// which requires all upstream dependencies to succeed.
    #[serde(default)]
    pub run_on_failure: bool,
    /// If true, the executor should pass a `JobContext` as the first positional
    /// argument when invoking the callable. Detected via signature inspection
    /// or an explicit `wants_context=True` at add_job time.
    #[serde(default)]
    pub wants_context: bool,
}

fn default_max_group_instances() -> u32 {
    1
}

impl ScheduleSpec {
    /// Create a new ScheduleSpec with sensible defaults.
    pub fn new(id: impl Into<String>, task: TaskSpec, trigger_state: TriggerState) -> Self {
        Self {
            id: id.into(),
            task,
            trigger_state,
            executor: "default".to_string(),
            jobstore: "default".to_string(),
            name: None,
            misfire_grace_time: Some(Duration::from_secs(1)),
            coalesce: CoalescePolicy::On,
            max_instances: 1,
            jitter: None,
            next_run_time: None,
            paused: false,
            replace_existing: false,
            version: 1,
            rate_limit: None,
            concurrency_group: None,
            max_group_instances: 1,
            timeout: None,
            depends_on: Vec::new(),
            run_on_failure: false,
            wants_context: false,
        }
    }

    /// Check if this job is due to run at or before `now`.
    pub fn is_due(&self, now: DateTime<Utc>) -> bool {
        if self.paused {
            return false;
        }
        match self.next_run_time {
            Some(nrt) => nrt <= now,
            None => false,
        }
    }

    /// Check whether the job has misfired given the current time.
    pub fn is_misfired(&self, now: DateTime<Utc>) -> bool {
        match (self.next_run_time, self.misfire_grace_time) {
            (Some(nrt), Some(grace)) => {
                let deadline =
                    nrt + chrono::Duration::from_std(grace).unwrap_or(chrono::Duration::zero());
                now > deadline
            }
            (Some(nrt), None) => now > nrt,
            _ => false,
        }
    }
}

/// A concrete instance of work to be executed by an executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSpec {
    pub id: Uuid,
    pub schedule_id: String,
    pub task: TaskSpec,
    pub executor: String,
    pub scheduled_fire_time: DateTime<Utc>,
    pub actual_fire_time: Option<DateTime<Utc>>,
    pub deadline: Option<DateTime<Utc>>,
    pub attempt: u32,
    /// Optional execution timeout copied from the schedule.  The executor
    /// is expected to report a `TimeoutError` via `JobOutcome::Error` if
    /// the call does not complete within this duration.
    #[serde(default, with = "option_duration_serde")]
    pub timeout: Option<Duration>,
    /// Whether this job wants a `JobContext` injected as the first positional
    /// argument when invoked.
    #[serde(default)]
    pub wants_context: bool,
}

impl JobSpec {
    /// Create a new job instance from a schedule.
    pub fn from_schedule(schedule: &ScheduleSpec, fire_time: DateTime<Utc>) -> Self {
        let deadline = schedule
            .misfire_grace_time
            .and_then(|g| chrono::Duration::from_std(g).ok())
            .map(|g| fire_time + g);

        Self {
            id: Uuid::new_v4(),
            schedule_id: schedule.id.clone(),
            task: schedule.task.clone(),
            executor: schedule.executor.clone(),
            scheduled_fire_time: fire_time,
            actual_fire_time: None,
            deadline,
            attempt: 1,
            timeout: schedule.timeout,
            wants_context: schedule.wants_context,
        }
    }
}

/// The outcome of a job execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobOutcome {
    Success,
    Error(String),
    Missed,
    Skipped(SkipReason),
}

/// Reason a job execution was skipped.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SkipReason {
    MaxInstancesReached,
    CoalescedAway,
    Paused,
}

/// Status of the last completed run of a job, used for dependency
/// evaluation in DAG-style workflows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompletionStatus {
    Success,
    Failure,
}

/// Record of the most recent completion for a job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobCompletion {
    pub schedule_id: String,
    pub last_run: DateTime<Utc>,
    pub status: CompletionStatus,
}

/// A lease on a job, used for distributed locking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobLease {
    pub job_id: String,
    pub scheduler_id: String,
    pub acquired_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub version: u64,
}

/// Context provided to a running job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunContext {
    pub job_id: Uuid,
    pub schedule_id: String,
    pub scheduled_fire_time: DateTime<Utc>,
    pub attempt: u32,
    pub scheduler_id: String,
}

/// Envelope wrapping a job's execution result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResultEnvelope {
    pub job_id: Uuid,
    pub schedule_id: String,
    pub outcome: JobOutcome,
    pub completed_at: DateTime<Utc>,
}

/// The state of the scheduler.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchedulerState {
    #[default]
    Stopped,
    Starting,
    Running,
    Paused,
    ShuttingDown,
}

/// Default values for job parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDefaults {
    #[serde(with = "duration_serde")]
    pub misfire_grace_time: Duration,
    pub coalesce: CoalescePolicy,
    pub max_instances: u32,
}

impl Default for JobDefaults {
    fn default() -> Self {
        Self {
            misfire_grace_time: Duration::from_secs(1),
            coalesce: CoalescePolicy::On,
            max_instances: 1,
        }
    }
}

/// Top-level scheduler configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerConfig {
    pub timezone: String,
    pub job_defaults: JobDefaults,
    pub daemon: bool,
    #[serde(with = "duration_serde")]
    pub misfire_grace_time_default: Duration,
    pub coalesce_default: CoalescePolicy,
    pub max_instances_default: u32,
    /// Base directory for per-job artifact storage (`ctx.artifact_dir`).
    /// Defaults to `{tempdir}/apscheduler-rs/artifacts` when not set.
    #[serde(default)]
    pub artifact_root: Option<PathBuf>,
}

impl SchedulerConfig {
    /// Resolve the artifact root, falling back to the system default.
    pub fn resolved_artifact_root(&self) -> PathBuf {
        self.artifact_root
            .clone()
            .unwrap_or_else(default_artifact_root)
    }
}

/// The default artifact root used when `SchedulerConfig::artifact_root` is None.
pub fn default_artifact_root() -> PathBuf {
    std::env::temp_dir()
        .join("apscheduler-rs")
        .join("artifacts")
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            timezone: "UTC".to_string(),
            job_defaults: JobDefaults::default(),
            daemon: true,
            misfire_grace_time_default: Duration::from_secs(1),
            coalesce_default: CoalescePolicy::On,
            max_instances_default: 1,
            artifact_root: None,
        }
    }
}

/// Optional field updates for modifying a job.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobChanges {
    pub name: Option<String>,
    pub misfire_grace_time: Option<Option<Duration>>,
    pub coalesce: Option<CoalescePolicy>,
    pub max_instances: Option<u32>,
    pub jitter: Option<Option<Duration>>,
    pub next_run_time: Option<Option<DateTime<Utc>>>,
    pub paused: Option<bool>,
    pub executor: Option<String>,
    pub trigger_state: Option<TriggerState>,
    /// Fully replace the task spec (used when args/kwargs change at runtime).
    #[serde(default)]
    pub task: Option<TaskSpec>,
    /// Replace the per-job execution timeout.  Outer `Some` means "apply";
    /// inner `None` clears the timeout.
    #[serde(default)]
    pub timeout: Option<Option<Duration>>,
}

impl JobChanges {
    /// Apply changes to a schedule spec, incrementing the version.
    pub fn apply(self, spec: &mut ScheduleSpec) {
        if let Some(name) = self.name {
            spec.name = Some(name);
        }
        if let Some(grace) = self.misfire_grace_time {
            spec.misfire_grace_time = grace;
        }
        if let Some(coalesce) = self.coalesce {
            spec.coalesce = coalesce;
        }
        if let Some(max) = self.max_instances {
            spec.max_instances = max;
        }
        if let Some(jitter) = self.jitter {
            spec.jitter = jitter;
        }
        if let Some(nrt) = self.next_run_time {
            spec.next_run_time = nrt;
        }
        if let Some(paused) = self.paused {
            spec.paused = paused;
        }
        if let Some(executor) = self.executor {
            spec.executor = executor;
        }
        if let Some(trigger_state) = self.trigger_state {
            spec.trigger_state = trigger_state;
        }
        if let Some(task) = self.task {
            spec.task = task;
        }
        if let Some(timeout) = self.timeout {
            spec.timeout = timeout;
        }
        spec.version += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_task() -> TaskSpec {
        TaskSpec::new(CallableRef::ImportPath("mymodule:func".to_string()))
    }

    fn sample_trigger() -> TriggerState {
        TriggerState::Date {
            run_date: Utc::now(),
            timezone: "UTC".to_string(),
        }
    }

    #[test]
    fn test_serialized_value_json_roundtrip() {
        let val = SerializedValue::from_json(&42i32).unwrap();
        let back: i32 = val.to_json().unwrap();
        assert_eq!(back, 42);
    }

    #[test]
    fn test_serialized_value_none() {
        let val = SerializedValue::None;
        assert!(val.is_none());
        assert!(!SerializedValue::Json(serde_json::Value::Null).is_none());
    }

    #[test]
    fn test_callable_ref_string() {
        let path = CallableRef::ImportPath("a:b".to_string());
        assert_eq!(path.ref_string(), "a:b");
        let handle = CallableRef::InMemoryHandle(123);
        assert_eq!(handle.ref_string(), "handle:123");
    }

    #[test]
    fn test_schedule_spec_defaults() {
        let spec = ScheduleSpec::new("job1", sample_task(), sample_trigger());
        assert_eq!(spec.executor, "default");
        assert_eq!(spec.jobstore, "default");
        assert_eq!(spec.coalesce, CoalescePolicy::On);
        assert_eq!(spec.max_instances, 1);
        assert!(!spec.paused);
        assert_eq!(spec.version, 1);
    }

    #[test]
    fn test_schedule_spec_is_due() {
        let mut spec = ScheduleSpec::new("job1", sample_task(), sample_trigger());
        let now = Utc::now();

        // No next_run_time -> not due
        assert!(!spec.is_due(now));

        // Future next_run_time -> not due
        spec.next_run_time = Some(now + chrono::Duration::hours(1));
        assert!(!spec.is_due(now));

        // Past next_run_time -> due
        spec.next_run_time = Some(now - chrono::Duration::hours(1));
        assert!(spec.is_due(now));

        // Paused -> not due
        spec.paused = true;
        assert!(!spec.is_due(now));
    }

    #[test]
    fn test_job_spec_from_schedule() {
        let now = Utc::now();
        let spec = ScheduleSpec::new("job1", sample_task(), sample_trigger());
        let job = JobSpec::from_schedule(&spec, now);
        assert_eq!(job.schedule_id, "job1");
        assert_eq!(job.scheduled_fire_time, now);
        assert_eq!(job.attempt, 1);
        assert!(job.deadline.is_some());
    }

    #[test]
    fn test_job_changes_apply() {
        let mut spec = ScheduleSpec::new("job1", sample_task(), sample_trigger());
        assert_eq!(spec.version, 1);

        let changes = JobChanges {
            name: Some("renamed".to_string()),
            max_instances: Some(5),
            paused: Some(true),
            ..Default::default()
        };
        changes.apply(&mut spec);
        assert_eq!(spec.name, Some("renamed".to_string()));
        assert_eq!(spec.max_instances, 5);
        assert!(spec.paused);
        assert_eq!(spec.version, 2);
    }

    #[test]
    fn test_retry_policy_delay() {
        let policy = RetryPolicy {
            max_retries: 3,
            retry_delay: Duration::from_secs(1),
            backoff_factor: 2.0,
            max_delay: Duration::from_secs(10),
        };
        assert_eq!(policy.delay_for_attempt(0), Duration::from_secs(1));
        assert_eq!(policy.delay_for_attempt(1), Duration::from_secs(2));
        assert_eq!(policy.delay_for_attempt(2), Duration::from_secs(4));
        assert_eq!(policy.delay_for_attempt(3), Duration::from_secs(8));
        // Capped at max_delay
        assert_eq!(policy.delay_for_attempt(4), Duration::from_secs(10));
    }

    #[test]
    fn test_scheduler_config_default() {
        let config = SchedulerConfig::default();
        assert_eq!(config.timezone, "UTC");
        assert!(config.daemon);
        assert_eq!(config.max_instances_default, 1);
        assert_eq!(config.coalesce_default, CoalescePolicy::On);
    }

    #[test]
    fn test_coalesce_policy_serde() {
        let json = serde_json::to_string(&CoalescePolicy::On).unwrap();
        let back: CoalescePolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(back, CoalescePolicy::On);
    }

    #[test]
    fn test_trigger_state_serde() {
        let state = TriggerState::Interval {
            weeks: 0,
            days: 0,
            hours: 1,
            minutes: 0,
            seconds: 0,
            start_date: None,
            end_date: None,
            timezone: "UTC".to_string(),
            jitter: Some(5.0),
            interval_micros: None,
        };
        let json = serde_json::to_string(&state).unwrap();
        let back: TriggerState = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, TriggerState::Interval { hours: 1, .. }));
    }

    #[test]
    fn test_scheduler_state_default() {
        assert_eq!(SchedulerState::default(), SchedulerState::Stopped);
    }

    #[test]
    fn test_misfire_policy_default() {
        let policy = MisfirePolicy::default();
        assert!(matches!(
            policy,
            MisfirePolicy::GracePeriod(d) if d == Duration::from_secs(1)
        ));
    }

    #[test]
    fn test_schedule_spec_serde_roundtrip() {
        let spec = ScheduleSpec::new("job1", sample_task(), sample_trigger());
        let json = serde_json::to_string(&spec).unwrap();
        let back: ScheduleSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "job1");
        assert_eq!(back.executor, "default");
    }

    #[test]
    fn test_is_misfired() {
        let now = Utc::now();
        let mut spec = ScheduleSpec::new("job1", sample_task(), sample_trigger());

        // No next_run_time -> not misfired
        assert!(!spec.is_misfired(now));

        // Within grace period -> not misfired
        spec.next_run_time = Some(now - chrono::Duration::milliseconds(500));
        spec.misfire_grace_time = Some(Duration::from_secs(1));
        assert!(!spec.is_misfired(now));

        // Beyond grace period -> misfired
        spec.next_run_time = Some(now - chrono::Duration::seconds(5));
        assert!(spec.is_misfired(now));
    }
}
