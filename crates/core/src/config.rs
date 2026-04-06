use std::time::Duration;

use crate::error::SchedulerError;
use crate::model::{CoalescePolicy, JobDefaults, SchedulerConfig};

/// Builder for constructing and validating a `SchedulerConfig`.
#[derive(Debug, Clone)]
pub struct SchedulerConfigBuilder {
    timezone: Option<String>,
    job_defaults: Option<JobDefaults>,
    daemon: Option<bool>,
    misfire_grace_time_default: Option<Duration>,
    coalesce_default: Option<CoalescePolicy>,
    max_instances_default: Option<u32>,
}

impl Default for SchedulerConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SchedulerConfigBuilder {
    /// Create a new builder with no values set (defaults will be applied on build).
    pub fn new() -> Self {
        Self {
            timezone: None,
            job_defaults: None,
            daemon: None,
            misfire_grace_time_default: None,
            coalesce_default: None,
            max_instances_default: None,
        }
    }

    /// Set the timezone (IANA timezone string, e.g. "America/New_York").
    pub fn timezone(mut self, tz: impl Into<String>) -> Self {
        self.timezone = Some(tz.into());
        self
    }

    /// Set custom job defaults.
    pub fn job_defaults(mut self, defaults: JobDefaults) -> Self {
        self.job_defaults = Some(defaults);
        self
    }

    /// Set whether the scheduler runs as a daemon thread.
    pub fn daemon(mut self, daemon: bool) -> Self {
        self.daemon = Some(daemon);
        self
    }

    /// Set the default misfire grace time.
    pub fn misfire_grace_time(mut self, duration: Duration) -> Self {
        self.misfire_grace_time_default = Some(duration);
        self
    }

    /// Set the default coalesce policy.
    pub fn coalesce(mut self, policy: CoalescePolicy) -> Self {
        self.coalesce_default = Some(policy);
        self
    }

    /// Set the default max instances.
    pub fn max_instances(mut self, max: u32) -> Self {
        self.max_instances_default = Some(max);
        self
    }

    /// Validate the configuration and build a `SchedulerConfig`.
    pub fn build(self) -> Result<SchedulerConfig, SchedulerError> {
        let defaults = SchedulerConfig::default();

        let timezone = self.timezone.unwrap_or(defaults.timezone);

        // Validate timezone string
        if timezone != "UTC" && timezone != "utc" {
            // Attempt to parse as IANA timezone
            if timezone.parse::<chrono_tz::Tz>().is_err() {
                return Err(SchedulerError::InvalidConfiguration(format!(
                    "invalid timezone: '{}'. Use an IANA timezone name like 'America/New_York'",
                    timezone
                )));
            }
        }

        let max_instances = self
            .max_instances_default
            .unwrap_or(defaults.max_instances_default);
        if max_instances == 0 {
            return Err(SchedulerError::InvalidConfiguration(
                "max_instances must be at least 1".to_string(),
            ));
        }

        let job_defaults = self.job_defaults.unwrap_or(defaults.job_defaults);
        if job_defaults.max_instances == 0 {
            return Err(SchedulerError::InvalidConfiguration(
                "job_defaults.max_instances must be at least 1".to_string(),
            ));
        }

        Ok(SchedulerConfig {
            timezone,
            job_defaults,
            daemon: self.daemon.unwrap_or(defaults.daemon),
            misfire_grace_time_default: self
                .misfire_grace_time_default
                .unwrap_or(defaults.misfire_grace_time_default),
            coalesce_default: self.coalesce_default.unwrap_or(defaults.coalesce_default),
            max_instances_default: max_instances,
        })
    }
}

impl SchedulerConfig {
    /// Create a builder for this config.
    pub fn builder() -> SchedulerConfigBuilder {
        SchedulerConfigBuilder::new()
    }

    /// Validate an existing config.
    pub fn validate(&self) -> Result<(), SchedulerError> {
        if self.timezone != "UTC"
            && self.timezone != "utc"
            && self.timezone.parse::<chrono_tz::Tz>().is_err()
        {
            return Err(SchedulerError::InvalidConfiguration(format!(
                "invalid timezone: '{}'",
                self.timezone
            )));
        }
        if self.max_instances_default == 0 {
            return Err(SchedulerError::InvalidConfiguration(
                "max_instances must be at least 1".to_string(),
            ));
        }
        if self.job_defaults.max_instances == 0 {
            return Err(SchedulerError::InvalidConfiguration(
                "job_defaults.max_instances must be at least 1".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_defaults() {
        let config = SchedulerConfigBuilder::new().build().unwrap();
        assert_eq!(config.timezone, "UTC");
        assert!(config.daemon);
        assert_eq!(config.max_instances_default, 1);
        assert_eq!(config.coalesce_default, CoalescePolicy::On);
        assert_eq!(config.misfire_grace_time_default, Duration::from_secs(1));
    }

    #[test]
    fn test_builder_custom_timezone() {
        let config = SchedulerConfigBuilder::new()
            .timezone("America/New_York")
            .build()
            .unwrap();
        assert_eq!(config.timezone, "America/New_York");
    }

    #[test]
    fn test_builder_invalid_timezone() {
        let result = SchedulerConfigBuilder::new()
            .timezone("Not/A/Timezone")
            .build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, SchedulerError::InvalidConfiguration(_)));
    }

    #[test]
    fn test_builder_max_instances_zero() {
        let result = SchedulerConfigBuilder::new().max_instances(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_builder_all_fields() {
        let config = SchedulerConfigBuilder::new()
            .timezone("Europe/London")
            .daemon(false)
            .misfire_grace_time(Duration::from_secs(5))
            .coalesce(CoalescePolicy::Off)
            .max_instances(3)
            .build()
            .unwrap();

        assert_eq!(config.timezone, "Europe/London");
        assert!(!config.daemon);
        assert_eq!(config.misfire_grace_time_default, Duration::from_secs(5));
        assert_eq!(config.coalesce_default, CoalescePolicy::Off);
        assert_eq!(config.max_instances_default, 3);
    }

    #[test]
    fn test_config_validate_ok() {
        let config = SchedulerConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validate_bad_timezone() {
        let mut config = SchedulerConfig::default();
        config.timezone = "Fake/Zone".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_zero_instances() {
        let mut config = SchedulerConfig::default();
        config.max_instances_default = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_builder_from_config() {
        let config = SchedulerConfig::builder()
            .timezone("Asia/Tokyo")
            .build()
            .unwrap();
        assert_eq!(config.timezone, "Asia/Tokyo");
    }

    #[test]
    fn test_builder_default_trait() {
        let builder = SchedulerConfigBuilder::default();
        let config = builder.build().unwrap();
        assert_eq!(config.timezone, "UTC");
    }

    #[test]
    fn test_builder_custom_job_defaults() {
        let jd = JobDefaults {
            misfire_grace_time: Duration::from_secs(10),
            coalesce: CoalescePolicy::Off,
            max_instances: 5,
        };
        let config = SchedulerConfigBuilder::new()
            .job_defaults(jd)
            .build()
            .unwrap();
        assert_eq!(config.job_defaults.max_instances, 5);
        assert_eq!(config.job_defaults.coalesce, CoalescePolicy::Off);
    }

    #[test]
    fn test_builder_job_defaults_zero_instances() {
        let jd = JobDefaults {
            misfire_grace_time: Duration::from_secs(1),
            coalesce: CoalescePolicy::On,
            max_instances: 0,
        };
        let result = SchedulerConfigBuilder::new().job_defaults(jd).build();
        assert!(result.is_err());
    }
}
