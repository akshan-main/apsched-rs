use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

use crate::model::JobOutcome;

// ---------------------------------------------------------------------------
// Event mask constants (bitflags)
// ---------------------------------------------------------------------------

pub const EVENT_SCHEDULER_STARTED: u32 = 1 << 0;
pub const EVENT_SCHEDULER_SHUTDOWN: u32 = 1 << 1;
pub const EVENT_SCHEDULER_PAUSED: u32 = 1 << 2;
pub const EVENT_SCHEDULER_RESUMED: u32 = 1 << 3;
pub const EVENT_EXECUTOR_ADDED: u32 = 1 << 4;
pub const EVENT_EXECUTOR_REMOVED: u32 = 1 << 5;
pub const EVENT_JOBSTORE_ADDED: u32 = 1 << 6;
pub const EVENT_JOBSTORE_REMOVED: u32 = 1 << 7;
pub const EVENT_ALL_JOBS_REMOVED: u32 = 1 << 8;
pub const EVENT_JOB_ADDED: u32 = 1 << 9;
pub const EVENT_JOB_REMOVED: u32 = 1 << 10;
pub const EVENT_JOB_MODIFIED: u32 = 1 << 11;
pub const EVENT_JOB_EXECUTED: u32 = 1 << 12;
pub const EVENT_JOB_ERROR: u32 = 1 << 13;
pub const EVENT_JOB_MISSED: u32 = 1 << 14;
pub const EVENT_JOB_SUBMITTED: u32 = 1 << 15;
pub const EVENT_JOB_MAX_INSTANCES: u32 = 1 << 16;
pub const EVENT_ALL: u32 = 0xFFFF_FFFF;

// ---------------------------------------------------------------------------
// SchedulerEvent
// ---------------------------------------------------------------------------

/// Events emitted by the scheduler, mirroring APScheduler 3.x event types.
#[derive(Debug, Clone)]
pub enum SchedulerEvent {
    SchedulerStarted,
    SchedulerShutdown,
    SchedulerPaused,
    SchedulerResumed,

    ExecutorAdded {
        alias: String,
    },
    ExecutorRemoved {
        alias: String,
    },

    JobStoreAdded {
        alias: String,
    },
    JobStoreRemoved {
        alias: String,
    },
    AllJobsRemoved {
        jobstore: Option<String>,
    },

    JobAdded {
        job_id: String,
        jobstore: String,
    },
    JobRemoved {
        job_id: String,
        jobstore: String,
    },
    JobModified {
        job_id: String,
        jobstore: String,
    },

    JobExecuted {
        job_id: Uuid,
        schedule_id: String,
        jobstore: String,
        scheduled_run_time: DateTime<Utc>,
        retval: Option<String>,
    },
    JobError {
        job_id: Uuid,
        schedule_id: String,
        jobstore: String,
        scheduled_run_time: DateTime<Utc>,
        exception: String,
    },
    JobMissed {
        job_id: String,
        jobstore: String,
        scheduled_run_time: DateTime<Utc>,
    },
    JobSubmitted {
        job_id: Uuid,
        schedule_id: String,
        jobstore: String,
        scheduled_run_time: DateTime<Utc>,
    },
    JobMaxInstances {
        job_id: String,
        jobstore: String,
    },
}

impl SchedulerEvent {
    /// Returns the bitmask identifying this event type.
    pub fn event_mask(&self) -> u32 {
        match self {
            SchedulerEvent::SchedulerStarted => EVENT_SCHEDULER_STARTED,
            SchedulerEvent::SchedulerShutdown => EVENT_SCHEDULER_SHUTDOWN,
            SchedulerEvent::SchedulerPaused => EVENT_SCHEDULER_PAUSED,
            SchedulerEvent::SchedulerResumed => EVENT_SCHEDULER_RESUMED,
            SchedulerEvent::ExecutorAdded { .. } => EVENT_EXECUTOR_ADDED,
            SchedulerEvent::ExecutorRemoved { .. } => EVENT_EXECUTOR_REMOVED,
            SchedulerEvent::JobStoreAdded { .. } => EVENT_JOBSTORE_ADDED,
            SchedulerEvent::JobStoreRemoved { .. } => EVENT_JOBSTORE_REMOVED,
            SchedulerEvent::AllJobsRemoved { .. } => EVENT_ALL_JOBS_REMOVED,
            SchedulerEvent::JobAdded { .. } => EVENT_JOB_ADDED,
            SchedulerEvent::JobRemoved { .. } => EVENT_JOB_REMOVED,
            SchedulerEvent::JobModified { .. } => EVENT_JOB_MODIFIED,
            SchedulerEvent::JobExecuted { .. } => EVENT_JOB_EXECUTED,
            SchedulerEvent::JobError { .. } => EVENT_JOB_ERROR,
            SchedulerEvent::JobMissed { .. } => EVENT_JOB_MISSED,
            SchedulerEvent::JobSubmitted { .. } => EVENT_JOB_SUBMITTED,
            SchedulerEvent::JobMaxInstances { .. } => EVENT_JOB_MAX_INSTANCES,
        }
    }

    /// Helper: create a JobExecuted or JobError event from a JobOutcome.
    pub fn from_outcome(
        job_id: Uuid,
        schedule_id: String,
        jobstore: String,
        scheduled_run_time: DateTime<Utc>,
        outcome: &JobOutcome,
    ) -> Self {
        match outcome {
            JobOutcome::Success => SchedulerEvent::JobExecuted {
                job_id,
                schedule_id,
                jobstore,
                scheduled_run_time,
                retval: None,
            },
            JobOutcome::Error(msg) => SchedulerEvent::JobError {
                job_id,
                schedule_id,
                jobstore,
                scheduled_run_time,
                exception: msg.clone(),
            },
            JobOutcome::Missed => SchedulerEvent::JobMissed {
                job_id: schedule_id,
                jobstore,
                scheduled_run_time,
            },
            JobOutcome::Skipped(_) => SchedulerEvent::JobMissed {
                job_id: schedule_id,
                jobstore,
                scheduled_run_time,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Listener ID
// ---------------------------------------------------------------------------

/// Opaque identifier for a registered event listener.
pub type ListenerId = u64;

// ---------------------------------------------------------------------------
// EventBus
// ---------------------------------------------------------------------------

struct Listener {
    id: ListenerId,
    mask: u32,
    callback: Arc<dyn Fn(&SchedulerEvent) + Send + Sync>,
}

/// Thread-safe event bus that dispatches scheduler events to registered listeners.
///
/// Listeners are called synchronously on the thread that calls `emit()`.
/// If a listener panics, the panic is caught, logged, and remaining listeners
/// continue to be notified.
pub struct EventBus {
    listeners: RwLock<Vec<Listener>>,
    next_id: AtomicU64,
}

impl std::fmt::Debug for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.listeners.read().len();
        f.debug_struct("EventBus")
            .field("listener_count", &count)
            .finish()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl EventBus {
    /// Create a new empty event bus.
    pub fn new() -> Self {
        Self {
            listeners: RwLock::new(Vec::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// Register a listener that will be called for events matching the given mask.
    ///
    /// Returns a `ListenerId` that can be used to remove the listener later.
    pub fn add_listener(
        &self,
        callback: Arc<dyn Fn(&SchedulerEvent) + Send + Sync>,
        mask: u32,
    ) -> ListenerId {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let listener = Listener { id, mask, callback };
        self.listeners.write().push(listener);
        id
    }

    /// Remove a previously registered listener by its ID.
    ///
    /// Returns `true` if the listener was found and removed.
    pub fn remove_listener(&self, id: ListenerId) -> bool {
        let mut listeners = self.listeners.write();
        let before = listeners.len();
        listeners.retain(|l| l.id != id);
        listeners.len() < before
    }

    /// Emit an event, calling all listeners whose mask matches.
    ///
    /// Listener panics are caught and logged; other listeners still receive the event.
    pub fn emit(&self, event: &SchedulerEvent) {
        let event_mask = event.event_mask();
        let listeners = self.listeners.read();
        for listener in listeners.iter() {
            if listener.mask & event_mask != 0 {
                let cb = Arc::clone(&listener.callback);
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    cb(event);
                }));
                if let Err(e) = result {
                    let msg = if let Some(s) = e.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = e.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "unknown panic".to_string()
                    };
                    tracing::error!(
                        listener_id = listener.id,
                        "event listener panicked: {}",
                        msg
                    );
                }
            }
        }
    }

    /// Returns the number of currently registered listeners.
    pub fn listener_count(&self) -> usize {
        self.listeners.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn test_event_mask_values() {
        assert_eq!(EVENT_SCHEDULER_STARTED, 1);
        assert_eq!(EVENT_SCHEDULER_SHUTDOWN, 2);
        assert_eq!(EVENT_JOB_ADDED, 1 << 9);
        assert_eq!(EVENT_JOB_MAX_INSTANCES, 1 << 16);
        assert_eq!(EVENT_ALL, 0xFFFF_FFFF);
    }

    #[test]
    fn test_event_mask_method() {
        assert_eq!(
            SchedulerEvent::SchedulerStarted.event_mask(),
            EVENT_SCHEDULER_STARTED
        );
        assert_eq!(
            SchedulerEvent::SchedulerShutdown.event_mask(),
            EVENT_SCHEDULER_SHUTDOWN
        );
        assert_eq!(
            SchedulerEvent::JobAdded {
                job_id: "j1".into(),
                jobstore: "default".into()
            }
            .event_mask(),
            EVENT_JOB_ADDED
        );
    }

    #[test]
    fn test_add_and_emit_listener() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicU32::new(0));
        let counter2 = Arc::clone(&counter);

        bus.add_listener(
            Arc::new(move |_event| {
                counter2.fetch_add(1, Ordering::Relaxed);
            }),
            EVENT_ALL,
        );

        bus.emit(&SchedulerEvent::SchedulerStarted);
        bus.emit(&SchedulerEvent::SchedulerShutdown);

        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_listener_mask_filtering() {
        let bus = EventBus::new();
        let started_count = Arc::new(AtomicU32::new(0));
        let shutdown_count = Arc::new(AtomicU32::new(0));

        let c1 = Arc::clone(&started_count);
        bus.add_listener(
            Arc::new(move |_| {
                c1.fetch_add(1, Ordering::Relaxed);
            }),
            EVENT_SCHEDULER_STARTED,
        );

        let c2 = Arc::clone(&shutdown_count);
        bus.add_listener(
            Arc::new(move |_| {
                c2.fetch_add(1, Ordering::Relaxed);
            }),
            EVENT_SCHEDULER_SHUTDOWN,
        );

        bus.emit(&SchedulerEvent::SchedulerStarted);
        bus.emit(&SchedulerEvent::SchedulerShutdown);
        bus.emit(&SchedulerEvent::SchedulerStarted);

        assert_eq!(started_count.load(Ordering::Relaxed), 2);
        assert_eq!(shutdown_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_combined_mask() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&counter);

        bus.add_listener(
            Arc::new(move |_| {
                c.fetch_add(1, Ordering::Relaxed);
            }),
            EVENT_SCHEDULER_STARTED | EVENT_SCHEDULER_SHUTDOWN,
        );

        bus.emit(&SchedulerEvent::SchedulerStarted);
        bus.emit(&SchedulerEvent::SchedulerShutdown);
        bus.emit(&SchedulerEvent::SchedulerPaused); // Should NOT match

        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_remove_listener() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&counter);

        let id = bus.add_listener(
            Arc::new(move |_| {
                c.fetch_add(1, Ordering::Relaxed);
            }),
            EVENT_ALL,
        );

        bus.emit(&SchedulerEvent::SchedulerStarted);
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        assert!(bus.remove_listener(id));
        bus.emit(&SchedulerEvent::SchedulerStarted);
        assert_eq!(counter.load(Ordering::Relaxed), 1); // Not incremented

        // Removing again returns false
        assert!(!bus.remove_listener(id));
    }

    #[test]
    fn test_listener_panic_does_not_break_bus() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&counter);

        // First listener panics
        bus.add_listener(
            Arc::new(|_| {
                panic!("test panic");
            }),
            EVENT_ALL,
        );

        // Second listener should still fire
        bus.add_listener(
            Arc::new(move |_| {
                c.fetch_add(1, Ordering::Relaxed);
            }),
            EVENT_ALL,
        );

        bus.emit(&SchedulerEvent::SchedulerStarted);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_listener_count() {
        let bus = EventBus::new();
        assert_eq!(bus.listener_count(), 0);

        let id1 = bus.add_listener(Arc::new(|_| {}), EVENT_ALL);
        assert_eq!(bus.listener_count(), 1);

        let _id2 = bus.add_listener(Arc::new(|_| {}), EVENT_ALL);
        assert_eq!(bus.listener_count(), 2);

        bus.remove_listener(id1);
        assert_eq!(bus.listener_count(), 1);
    }

    #[test]
    fn test_multiple_listeners_same_event() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicU32::new(0));

        for _ in 0..5 {
            let c = Arc::clone(&counter);
            bus.add_listener(
                Arc::new(move |_| {
                    c.fetch_add(1, Ordering::Relaxed);
                }),
                EVENT_JOB_ADDED,
            );
        }

        bus.emit(&SchedulerEvent::JobAdded {
            job_id: "j1".into(),
            jobstore: "default".into(),
        });

        assert_eq!(counter.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn test_event_bus_thread_safety() {
        use std::thread;

        let bus = Arc::new(EventBus::new());
        let counter = Arc::new(AtomicU32::new(0));

        let c = Arc::clone(&counter);
        bus.add_listener(
            Arc::new(move |_| {
                c.fetch_add(1, Ordering::Relaxed);
            }),
            EVENT_ALL,
        );

        let mut handles = vec![];
        for _ in 0..10 {
            let bus_clone = Arc::clone(&bus);
            handles.push(thread::spawn(move || {
                bus_clone.emit(&SchedulerEvent::SchedulerStarted);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(counter.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn test_from_outcome_success() {
        let id = Uuid::new_v4();
        let now = Utc::now();
        let event = SchedulerEvent::from_outcome(
            id,
            "sched1".into(),
            "default".into(),
            now,
            &JobOutcome::Success,
        );
        assert!(matches!(event, SchedulerEvent::JobExecuted { .. }));
    }

    #[test]
    fn test_from_outcome_error() {
        let id = Uuid::new_v4();
        let now = Utc::now();
        let event = SchedulerEvent::from_outcome(
            id,
            "sched1".into(),
            "default".into(),
            now,
            &JobOutcome::Error("boom".into()),
        );
        assert!(matches!(event, SchedulerEvent::JobError { exception, .. } if exception == "boom"));
    }

    #[test]
    fn test_event_bus_default() {
        let bus = EventBus::default();
        assert_eq!(bus.listener_count(), 0);
    }

    #[test]
    fn test_no_listeners_emit_ok() {
        let bus = EventBus::new();
        // Should not panic even with zero listeners
        bus.emit(&SchedulerEvent::SchedulerStarted);
    }
}
