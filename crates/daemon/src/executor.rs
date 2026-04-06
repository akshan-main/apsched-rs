//! Daemon-specific job executors for shell commands and HTTP webhooks.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tracing::debug;

use crate::config::ActionConfig;
use crate::history::ExecutionHistory;
use crate::metrics::Metrics;

/// Outcome of executing a daemon job action.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ActionOutcome {
    pub success: bool,
    pub output: Option<String>,
    pub error: Option<String>,
    pub duration: Duration,
}

/// Execute a job action (shell command or HTTP call).
pub async fn execute_action(
    job_id: &str,
    action: &ActionConfig,
    history: &Arc<ExecutionHistory>,
    metrics: &Arc<Metrics>,
) -> ActionOutcome {
    let scheduled_time = Utc::now();
    let start = std::time::Instant::now();

    let result = match action {
        ActionConfig::Shell {
            command,
            timeout,
            working_dir,
            env,
        } => {
            execute_shell(
                command,
                timeout
                    .map(Duration::from_secs)
                    .unwrap_or(Duration::from_secs(300)),
                working_dir.as_deref(),
                env.as_ref(),
            )
            .await
        }
        ActionConfig::Http {
            url,
            method,
            headers,
            body,
            timeout,
        } => {
            execute_http(
                url,
                method,
                headers.as_ref().cloned().unwrap_or_default(),
                body.as_deref(),
                timeout
                    .map(Duration::from_secs)
                    .unwrap_or(Duration::from_secs(30)),
            )
            .await
        }
    };

    let duration = start.elapsed();
    let actual_time = Utc::now();

    // Record metrics
    let outcome_str = if result.success { "success" } else { "error" };
    if result.success {
        metrics.record_success(duration.as_millis() as u64);
    } else {
        metrics.record_error(duration.as_millis() as u64);
    }

    // Record history
    history.record(crate::history::ExecutionRecord {
        job_id: job_id.to_string(),
        scheduled_time,
        actual_time,
        duration_ms: duration.as_millis() as u64,
        outcome: outcome_str.to_string(),
        error_message: result.error.clone(),
        output: result.output.clone(),
    });

    debug!(
        job_id = job_id,
        outcome = outcome_str,
        duration_ms = duration.as_millis() as u64,
        "job execution complete"
    );

    result
}

/// Execute a shell command with timeout.
async fn execute_shell(
    command: &str,
    timeout: Duration,
    working_dir: Option<&str>,
    env: Option<&HashMap<String, String>>,
) -> ActionOutcome {
    let start = std::time::Instant::now();

    let mut cmd = tokio::process::Command::new("sh");
    cmd.arg("-c").arg(command);

    if let Some(dir) = working_dir {
        cmd.current_dir(dir);
    }

    if let Some(env_vars) = env {
        for (key, value) in env_vars {
            cmd.env(key, value);
        }
    }

    let result = tokio::time::timeout(timeout, cmd.output()).await;

    let duration = start.elapsed();

    match result {
        Ok(Ok(output)) => {
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();

            if output.status.success() {
                ActionOutcome {
                    success: true,
                    output: if stdout.is_empty() {
                        None
                    } else {
                        Some(stdout)
                    },
                    error: None,
                    duration,
                }
            } else {
                let error_msg = if stderr.is_empty() {
                    format!("exit code: {}", output.status.code().unwrap_or(-1))
                } else {
                    stderr
                };
                ActionOutcome {
                    success: false,
                    output: if stdout.is_empty() {
                        None
                    } else {
                        Some(stdout)
                    },
                    error: Some(error_msg),
                    duration,
                }
            }
        }
        Ok(Err(e)) => ActionOutcome {
            success: false,
            output: None,
            error: Some(format!("failed to spawn command: {}", e)),
            duration,
        },
        Err(_) => ActionOutcome {
            success: false,
            output: None,
            error: Some(format!("command timed out after {:?}", timeout)),
            duration,
        },
    }
}

/// Execute an HTTP request with timeout.
async fn execute_http(
    url: &str,
    method: &str,
    headers: HashMap<String, String>,
    body: Option<&str>,
    timeout: Duration,
) -> ActionOutcome {
    let start = std::time::Instant::now();

    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .unwrap_or_default();

    let method = match method.to_uppercase().as_str() {
        "GET" => reqwest::Method::GET,
        "POST" => reqwest::Method::POST,
        "PUT" => reqwest::Method::PUT,
        "DELETE" => reqwest::Method::DELETE,
        "PATCH" => reqwest::Method::PATCH,
        "HEAD" => reqwest::Method::HEAD,
        other => {
            return ActionOutcome {
                success: false,
                output: None,
                error: Some(format!("unsupported HTTP method: {}", other)),
                duration: start.elapsed(),
            };
        }
    };

    let mut req = client.request(method, url);

    for (key, value) in &headers {
        req = req.header(key.as_str(), value.as_str());
    }

    if let Some(body) = body {
        req = req.body(body.to_string());
    }

    match req.send().await {
        Ok(resp) => {
            let status = resp.status();
            let body_text = resp.text().await.unwrap_or_default();
            let duration = start.elapsed();

            if status.is_success() {
                ActionOutcome {
                    success: true,
                    output: Some(format!("HTTP {} - {}", status.as_u16(), body_text)),
                    error: None,
                    duration,
                }
            } else {
                ActionOutcome {
                    success: false,
                    output: Some(body_text.clone()),
                    error: Some(format!("HTTP {}", status.as_u16())),
                    duration,
                }
            }
        }
        Err(e) => {
            let duration = start.elapsed();
            ActionOutcome {
                success: false,
                output: None,
                error: Some(format!("HTTP request failed: {}", e)),
                duration,
            }
        }
    }
}
