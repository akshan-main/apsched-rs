//! HTTP admin API for the daemon, built with axum.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use apsched_core::model::{JobChanges, ScheduleSpec, SchedulerState, TriggerState};
use apsched_core::traits::JobStore;
use apsched_core::SchedulerEngine;

use crate::config::{ActionConfig, TriggerConfig};
use crate::executor::execute_action;
use crate::history::ExecutionHistory;
use crate::metrics::Metrics;

/// Shared application state for API handlers.
#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<SchedulerEngine>,
    pub store: Arc<dyn JobStore>,
    pub history: Arc<ExecutionHistory>,
    pub metrics: Arc<Metrics>,
    /// Map of job_id -> ActionConfig for the daemon's job actions.
    pub actions: Arc<RwLock<HashMap<String, ActionConfig>>>,
}

/// Create the API router with all endpoints.
pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/jobs", get(list_jobs).post(add_job))
        .route(
            "/api/v1/jobs/{id}",
            get(get_job).put(modify_job).delete(remove_job),
        )
        .route("/api/v1/jobs/{id}/pause", post(pause_job))
        .route("/api/v1/jobs/{id}/resume", post(resume_job))
        .route("/api/v1/jobs/{id}/run", post(run_job))
        .route("/api/v1/jobs/{id}/history", get(get_history))
        .route("/api/v1/jobs/{id}/next", get(next_fire_times))
        .route("/api/v1/scheduler", get(scheduler_status))
        .route("/api/v1/scheduler/pause", post(pause_scheduler))
        .route("/api/v1/scheduler/resume", post(resume_scheduler))
        .route("/api/v1/health", get(health))
        .route("/api/v1/metrics", get(metrics))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct JobResponse {
    id: String,
    name: Option<String>,
    trigger: String,
    next_run_time: Option<DateTime<Utc>>,
    paused: bool,
    max_instances: u32,
    version: u64,
}

impl From<ScheduleSpec> for JobResponse {
    fn from(spec: ScheduleSpec) -> Self {
        let trigger_desc = match &spec.trigger_state {
            TriggerState::Date { run_date, .. } => format!("date[{}]", run_date),
            TriggerState::Interval {
                hours,
                minutes,
                seconds,
                ..
            } => {
                format!("interval[{}h {}m {}s]", hours, minutes, seconds)
            }
            TriggerState::Cron {
                hour,
                minute,
                second,
                ..
            } => {
                format!(
                    "cron[{} {} {}]",
                    hour.as_deref().unwrap_or("*"),
                    minute.as_deref().unwrap_or("*"),
                    second.as_deref().unwrap_or("0"),
                )
            }
            TriggerState::CalendarInterval { years, months, .. } => {
                format!("calendar[{}y {}m]", years, months)
            }
            TriggerState::Plugin { description } => format!("plugin[{}]", description),
        };
        Self {
            id: spec.id,
            name: spec.name,
            trigger: trigger_desc,
            next_run_time: spec.next_run_time,
            paused: spec.paused,
            max_instances: spec.max_instances,
            version: spec.version,
        }
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

fn error_response(status: StatusCode, msg: impl Into<String>) -> (StatusCode, Json<ErrorResponse>) {
    (status, Json(ErrorResponse { error: msg.into() }))
}

// ---------------------------------------------------------------------------
// Helper: run engine operations in a spawned task to satisfy Send bounds.
// The SchedulerEngine uses parking_lot::RwLock internally which makes some
// futures !Send when locks are held across await points. Spawning a task
// sidesteps this constraint.
// ---------------------------------------------------------------------------

async fn engine_get_job(
    engine: Arc<SchedulerEngine>,
    id: String,
) -> Result<ScheduleSpec, apsched_core::error::SchedulerError> {
    let handle = tokio::task::spawn(async move { engine.get_job(&id, Some("default")).await });
    handle.await.unwrap()
}

async fn engine_add_job(
    engine: Arc<SchedulerEngine>,
    spec: ScheduleSpec,
) -> Result<(), apsched_core::error::SchedulerError> {
    let handle = tokio::task::spawn(async move { engine.add_job(spec).await });
    handle.await.unwrap()
}

async fn engine_modify_job(
    engine: Arc<SchedulerEngine>,
    id: String,
    changes: JobChanges,
) -> Result<ScheduleSpec, apsched_core::error::SchedulerError> {
    let handle =
        tokio::task::spawn(async move { engine.modify_job(&id, Some("default"), changes).await });
    handle.await.unwrap()
}

async fn engine_remove_job(
    engine: Arc<SchedulerEngine>,
    id: String,
) -> Result<(), apsched_core::error::SchedulerError> {
    let handle = tokio::task::spawn(async move { engine.remove_job(&id, Some("default")).await });
    handle.await.unwrap()
}

async fn engine_pause_job(
    engine: Arc<SchedulerEngine>,
    id: String,
) -> Result<ScheduleSpec, apsched_core::error::SchedulerError> {
    let handle = tokio::task::spawn(async move { engine.pause_job(&id, Some("default")).await });
    handle.await.unwrap()
}

async fn engine_resume_job(
    engine: Arc<SchedulerEngine>,
    id: String,
) -> Result<ScheduleSpec, apsched_core::error::SchedulerError> {
    let handle = tokio::task::spawn(async move { engine.resume_job(&id, Some("default")).await });
    handle.await.unwrap()
}

// ---------------------------------------------------------------------------
// Job endpoints
// ---------------------------------------------------------------------------

async fn list_jobs(State(state): State<AppState>) -> impl IntoResponse {
    match state.store.get_all_jobs().await {
        Ok(jobs) => {
            let responses: Vec<JobResponse> = jobs.into_iter().map(JobResponse::from).collect();
            (
                StatusCode::OK,
                Json(serde_json::to_value(responses).unwrap()),
            )
                .into_response()
        }
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_job(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match engine_get_job(state.engine, id).await {
        Ok(spec) => (
            StatusCode::OK,
            Json(serde_json::to_value(JobResponse::from(spec)).unwrap()),
        )
            .into_response(),
        Err(e) => error_response(StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

/// Request body for adding a new job.
#[derive(Deserialize)]
struct AddJobRequest {
    id: String,
    #[serde(default)]
    name: Option<String>,
    trigger: TriggerConfig,
    action: ActionConfig,
    #[serde(default)]
    misfire_grace_time: Option<u64>,
    #[serde(default)]
    max_instances: Option<u32>,
}

async fn add_job(
    State(state): State<AppState>,
    Json(req): Json<AddJobRequest>,
) -> impl IntoResponse {
    let trigger_state = match config_to_trigger_state(&req.trigger) {
        Ok(ts) => ts,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, e).into_response(),
    };

    let now = Utc::now();
    let next_fire_time = trigger_state.compute_next_fire_time(now, now);

    let task = apsched_core::model::TaskSpec::new(apsched_core::model::CallableRef::ImportPath(
        format!("daemon:{}", req.id),
    ));

    let mut spec = ScheduleSpec::new(req.id.clone(), task, trigger_state);
    spec.name = req.name;
    spec.next_run_time = next_fire_time;
    spec.replace_existing = true;

    if let Some(grace) = req.misfire_grace_time {
        spec.misfire_grace_time = Some(Duration::from_secs(grace));
    }
    if let Some(max) = req.max_instances {
        spec.max_instances = max;
    }

    // Store the action config
    state
        .actions
        .write()
        .await
        .insert(req.id.clone(), req.action);

    match engine_add_job(state.engine, spec).await {
        Ok(()) => {
            state
                .metrics
                .jobs_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            (
                StatusCode::CREATED,
                Json(serde_json::json!({"status": "created", "id": req.id})),
            )
                .into_response()
        }
        Err(e) => error_response(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

/// Request body for modifying a job.
#[derive(Deserialize)]
struct ModifyJobRequest {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    trigger: Option<TriggerConfig>,
    #[serde(default)]
    action: Option<ActionConfig>,
    #[serde(default)]
    max_instances: Option<u32>,
    #[serde(default)]
    misfire_grace_time: Option<u64>,
}

async fn modify_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<ModifyJobRequest>,
) -> impl IntoResponse {
    let trigger_state = if let Some(trigger) = &req.trigger {
        match config_to_trigger_state(trigger) {
            Ok(ts) => Some(ts),
            Err(e) => return error_response(StatusCode::BAD_REQUEST, e).into_response(),
        }
    } else {
        None
    };

    let changes = JobChanges {
        name: req.name,
        max_instances: req.max_instances,
        misfire_grace_time: req.misfire_grace_time.map(|g| Some(Duration::from_secs(g))),
        trigger_state,
        ..Default::default()
    };

    // Update action if provided
    if let Some(action) = req.action {
        state.actions.write().await.insert(id.clone(), action);
    }

    match engine_modify_job(state.engine, id, changes).await {
        Ok(spec) => (
            StatusCode::OK,
            Json(serde_json::to_value(JobResponse::from(spec)).unwrap()),
        )
            .into_response(),
        Err(e) => error_response(StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

async fn remove_job(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match engine_remove_job(state.engine, id.clone()).await {
        Ok(()) => {
            state
                .metrics
                .jobs_total
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            state.actions.write().await.remove(&id);
            state.history.remove(&id);
            (
                StatusCode::OK,
                Json(serde_json::json!({"status": "removed", "id": id})),
            )
                .into_response()
        }
        Err(e) => error_response(StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

async fn pause_job(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match engine_pause_job(state.engine, id).await {
        Ok(spec) => (
            StatusCode::OK,
            Json(serde_json::to_value(JobResponse::from(spec)).unwrap()),
        )
            .into_response(),
        Err(e) => error_response(StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

async fn resume_job(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match engine_resume_job(state.engine, id).await {
        Ok(spec) => (
            StatusCode::OK,
            Json(serde_json::to_value(JobResponse::from(spec)).unwrap()),
        )
            .into_response(),
        Err(e) => error_response(StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

async fn run_job(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    // Get the action config for this job
    let action = {
        let actions = state.actions.read().await;
        actions.get(&id).cloned()
    };

    match action {
        Some(action) => {
            let history = state.history.clone();
            let metrics = state.metrics.clone();
            let job_id = id.clone();

            // Spawn the execution in the background
            tokio::spawn(async move {
                execute_action(&job_id, &action, &history, &metrics).await;
            });

            (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({"status": "triggered", "id": id})),
            )
                .into_response()
        }
        None => error_response(
            StatusCode::NOT_FOUND,
            format!("no action configured for job '{}'", id),
        )
        .into_response(),
    }
}

async fn get_history(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    let records = state.history.get(&id);
    (StatusCode::OK, Json(serde_json::to_value(records).unwrap())).into_response()
}

#[derive(Deserialize)]
struct NextFireTimesQuery {
    #[serde(default = "default_count")]
    count: usize,
}

fn default_count() -> usize {
    5
}

async fn next_fire_times(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<NextFireTimesQuery>,
) -> impl IntoResponse {
    match engine_get_job(state.engine, id).await {
        Ok(spec) => {
            let now = Utc::now();
            let mut times = Vec::new();
            let mut prev = spec.next_run_time;

            for _ in 0..query.count {
                match prev {
                    Some(t) => {
                        times.push(t);
                        prev = spec.trigger_state.compute_next_fire_time(t, now);
                    }
                    None => break,
                }
            }

            (StatusCode::OK, Json(serde_json::to_value(times).unwrap())).into_response()
        }
        Err(e) => error_response(StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Scheduler endpoints
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct SchedulerStatusResponse {
    state: String,
    scheduler_id: String,
    timezone: String,
    job_count: usize,
    uptime_description: String,
}

async fn scheduler_status(State(state): State<AppState>) -> impl IntoResponse {
    let engine_state = state.engine.state();
    let state_str = match engine_state {
        SchedulerState::Running => "running",
        SchedulerState::Paused => "paused",
        SchedulerState::Stopped => "stopped",
        SchedulerState::Starting => "starting",
        SchedulerState::ShuttingDown => "shutting_down",
    };

    let job_count = state
        .store
        .get_all_jobs()
        .await
        .map(|j| j.len())
        .unwrap_or(0);

    let resp = SchedulerStatusResponse {
        state: state_str.to_string(),
        scheduler_id: state.engine.scheduler_id().to_string(),
        timezone: state.engine.config().timezone.clone(),
        job_count,
        uptime_description: "running".to_string(),
    };

    (StatusCode::OK, Json(serde_json::to_value(resp).unwrap()))
}

async fn pause_scheduler(State(state): State<AppState>) -> impl IntoResponse {
    match state.engine.pause() {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"status": "paused"})),
        )
            .into_response(),
        Err(e) => error_response(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn resume_scheduler(State(state): State<AppState>) -> impl IntoResponse {
    match state.engine.resume() {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"status": "resumed"})),
        )
            .into_response(),
        Err(e) => error_response(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Health and metrics
// ---------------------------------------------------------------------------

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let engine_state = state.engine.state();
    let healthy = matches!(
        engine_state,
        SchedulerState::Running | SchedulerState::Paused
    );

    let status = if healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let state_str = format!("{:?}", engine_state);

    (
        status,
        Json(serde_json::json!({
            "healthy": healthy,
            "state": state_str,
        })),
    )
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let body = state.metrics.to_prometheus();
    (
        StatusCode::OK,
        [("content-type", "text/plain; charset=utf-8")],
        body,
    )
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a TriggerConfig from the config/API into a core TriggerState.
pub fn config_to_trigger_state(trigger: &TriggerConfig) -> Result<TriggerState, String> {
    match trigger {
        TriggerConfig::Cron {
            year,
            month,
            day,
            week,
            day_of_week,
            hour,
            minute,
            second,
        } => Ok(TriggerState::Cron {
            year: year.clone(),
            month: month.clone(),
            day: day.clone(),
            week: week.clone(),
            day_of_week: day_of_week.clone(),
            hour: hour.clone(),
            minute: minute.clone(),
            second: second.clone(),
            start_date: None,
            end_date: None,
            timezone: "UTC".to_string(),
            jitter: None,
        }),
        TriggerConfig::Interval {
            weeks,
            days,
            hours,
            minutes,
            seconds,
        } => {
            let w = weeks.unwrap_or(0);
            let d = days.unwrap_or(0);
            let h = hours.unwrap_or(0);
            let m = minutes.unwrap_or(0);
            let s = seconds.unwrap_or(0);
            let total = w * 7 * 86400 + d * 86400 + h * 3600 + m * 60 + s;
            if total <= 0 {
                return Err("interval must be positive".to_string());
            }
            Ok(TriggerState::Interval {
                weeks: w,
                days: d,
                hours: h,
                minutes: m,
                seconds: s,
                start_date: None,
                end_date: None,
                timezone: "UTC".to_string(),
                jitter: None,
            })
        }
        TriggerConfig::Date { run_date } => {
            let dt = run_date
                .parse::<DateTime<Utc>>()
                .map_err(|e| format!("invalid date '{}': {}", run_date, e))?;
            Ok(TriggerState::Date {
                run_date: dt,
                timezone: "UTC".to_string(),
            })
        }
    }
}
