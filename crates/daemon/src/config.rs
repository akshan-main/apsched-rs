//! Configuration file parsing for the daemon.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Top-level daemon configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonConfig {
    #[serde(default)]
    pub scheduler: SchedulerSection,
    #[serde(default)]
    pub store: StoreSection,
    #[serde(default)]
    pub api: ApiSection,
    #[serde(default)]
    pub jobs: Vec<JobConfig>,
}

/// Scheduler-level settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerSection {
    #[serde(default = "default_timezone")]
    pub timezone: String,
    #[serde(default = "default_misfire_grace_time")]
    pub misfire_grace_time: u64,
    #[serde(default = "default_coalesce")]
    pub coalesce: bool,
}

impl Default for SchedulerSection {
    fn default() -> Self {
        Self {
            timezone: default_timezone(),
            misfire_grace_time: default_misfire_grace_time(),
            coalesce: default_coalesce(),
        }
    }
}

fn default_timezone() -> String {
    "UTC".to_string()
}
fn default_misfire_grace_time() -> u64 {
    60
}
fn default_coalesce() -> bool {
    true
}

/// Store backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreSection {
    #[serde(default = "default_store_type", rename = "type")]
    pub store_type: String,
    #[serde(default)]
    pub url: Option<String>,
}

impl Default for StoreSection {
    fn default() -> Self {
        Self {
            store_type: default_store_type(),
            url: None,
        }
    }
}

fn default_store_type() -> String {
    "memory".to_string()
}

/// HTTP API configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiSection {
    #[serde(default = "default_api_enabled")]
    pub enabled: bool,
    #[serde(default = "default_api_bind")]
    pub bind: String,
}

impl Default for ApiSection {
    fn default() -> Self {
        Self {
            enabled: default_api_enabled(),
            bind: default_api_bind(),
        }
    }
}

fn default_api_enabled() -> bool {
    true
}
fn default_api_bind() -> String {
    "0.0.0.0:8080".to_string()
}

/// A single job definition from the config file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobConfig {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    pub trigger: TriggerConfig,
    pub action: ActionConfig,
    #[serde(default)]
    pub misfire_grace_time: Option<u64>,
    #[serde(default)]
    pub max_instances: Option<u32>,
}

/// Trigger configuration (cron, interval, or date).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TriggerConfig {
    #[serde(rename = "cron")]
    Cron {
        #[serde(default)]
        year: Option<String>,
        #[serde(default)]
        month: Option<String>,
        #[serde(default)]
        day: Option<String>,
        #[serde(default)]
        week: Option<String>,
        #[serde(default)]
        day_of_week: Option<String>,
        #[serde(default)]
        hour: Option<String>,
        #[serde(default)]
        minute: Option<String>,
        #[serde(default)]
        second: Option<String>,
    },
    #[serde(rename = "interval")]
    Interval {
        #[serde(default)]
        weeks: Option<i64>,
        #[serde(default)]
        days: Option<i64>,
        #[serde(default)]
        hours: Option<i64>,
        #[serde(default)]
        minutes: Option<i64>,
        #[serde(default)]
        seconds: Option<i64>,
    },
    #[serde(rename = "date")]
    Date { run_date: String },
}

/// Job action: what to do when the job fires.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ActionConfig {
    #[serde(rename = "shell")]
    Shell {
        command: String,
        #[serde(default)]
        timeout: Option<u64>,
        #[serde(default)]
        working_dir: Option<String>,
        #[serde(default)]
        env: Option<HashMap<String, String>>,
    },
    #[serde(rename = "http")]
    Http {
        url: String,
        #[serde(default = "default_http_method")]
        method: String,
        #[serde(default)]
        headers: Option<HashMap<String, String>>,
        #[serde(default)]
        body: Option<String>,
        #[serde(default)]
        timeout: Option<u64>,
    },
}

fn default_http_method() -> String {
    "GET".to_string()
}

impl DaemonConfig {
    /// Load configuration from a YAML or JSON file.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            anyhow::anyhow!("failed to read config file '{}': {}", path.display(), e)
        })?;

        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("yaml");

        let config: DaemonConfig = match ext {
            "json" => serde_json::from_str(&content)
                .map_err(|e| anyhow::anyhow!("failed to parse JSON config: {}", e))?,
            _ => serde_yaml::from_str(&content)
                .map_err(|e| anyhow::anyhow!("failed to parse YAML config: {}", e))?,
        };

        Ok(config)
    }

    /// Validate the configuration, returning errors for invalid fields.
    pub fn validate(&self) -> anyhow::Result<()> {
        // Validate timezone
        if self.scheduler.timezone != "UTC" && self.scheduler.timezone != "utc" {
            self.scheduler
                .timezone
                .parse::<chrono_tz::Tz>()
                .map_err(|_| anyhow::anyhow!("invalid timezone: '{}'", self.scheduler.timezone))?;
        }

        // Validate store type
        match self.store.store_type.as_str() {
            "memory" | "sqlite" | "postgres" | "redis" => {}
            other => {
                anyhow::bail!(
                    "unsupported store type: '{}'. Use memory, sqlite, postgres, or redis",
                    other
                );
            }
        }

        // Validate each job
        let mut seen_ids = std::collections::HashSet::new();
        for job in &self.jobs {
            if job.id.is_empty() {
                anyhow::bail!("job ID must not be empty");
            }
            if !seen_ids.insert(&job.id) {
                anyhow::bail!("duplicate job ID: '{}'", job.id);
            }

            // Validate trigger
            match &job.trigger {
                TriggerConfig::Interval {
                    weeks,
                    days,
                    hours,
                    minutes,
                    seconds,
                } => {
                    let total = weeks.unwrap_or(0) * 7 * 86400
                        + days.unwrap_or(0) * 86400
                        + hours.unwrap_or(0) * 3600
                        + minutes.unwrap_or(0) * 60
                        + seconds.unwrap_or(0);
                    if total <= 0 {
                        anyhow::bail!(
                            "job '{}': interval trigger must have a positive duration",
                            job.id
                        );
                    }
                }
                TriggerConfig::Cron { .. } => {
                    // Cron fields are optional strings; basic validation is done at trigger creation
                }
                TriggerConfig::Date { run_date } => {
                    if run_date.is_empty() {
                        anyhow::bail!("job '{}': date trigger must have a run_date", job.id);
                    }
                }
            }

            // Validate action
            match &job.action {
                ActionConfig::Shell { command, .. } => {
                    if command.is_empty() {
                        anyhow::bail!("job '{}': shell action must have a command", job.id);
                    }
                }
                ActionConfig::Http { url, .. } => {
                    if url.is_empty() {
                        anyhow::bail!("job '{}': http action must have a url", job.id);
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_yaml() {
        let yaml = r#"
scheduler:
  timezone: "UTC"
  misfire_grace_time: 60
  coalesce: true

store:
  type: memory

api:
  enabled: true
  bind: "0.0.0.0:8080"

jobs:
  - id: test_job
    name: "Test Job"
    trigger:
      type: interval
      seconds: 30
    action:
      type: shell
      command: "echo hello"
"#;
        let config: DaemonConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.jobs.len(), 1);
        assert_eq!(config.jobs[0].id, "test_job");
        config.validate().unwrap();
    }

    #[test]
    fn test_validate_duplicate_job_ids() {
        let yaml = r#"
jobs:
  - id: dup
    trigger:
      type: interval
      seconds: 10
    action:
      type: shell
      command: "echo 1"
  - id: dup
    trigger:
      type: interval
      seconds: 20
    action:
      type: shell
      command: "echo 2"
"#;
        let config: DaemonConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_zero_interval() {
        let yaml = r#"
jobs:
  - id: bad
    trigger:
      type: interval
      seconds: 0
    action:
      type: shell
      command: "echo bad"
"#;
        let config: DaemonConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_defaults() {
        let yaml = "{}";
        let config: DaemonConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.scheduler.timezone, "UTC");
        assert_eq!(config.store.store_type, "memory");
        assert!(config.api.enabled);
    }
}
