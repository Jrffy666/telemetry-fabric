use crate::error::PipelineError;
use crate::telemetry::SignalKind;
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineConfig {
    pub tenant_id: String,
    pub name: String,
    pub receivers: Vec<ReceiverConfig>,
    pub processors: Vec<ProcessorConfig>,
    pub exporters: Vec<ExporterConfig>,
    pub routes: Vec<RouteConfig>,
    pub limits: TenantLimits,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverConfig {
    pub name: String,
    pub protocol: ReceiverProtocol,
    pub endpoint: String,
    pub tls: TlsConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiverProtocol {
    OtlpGrpc,
    OtlpHttp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessorConfig {
    pub name: String,
    pub kind: ProcessorKind,
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessorKind {
    MemoryLimiter,
    Batch,
    Redact,
    TenantRateLimit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExporterConfig {
    pub name: String,
    pub protocol: ExporterProtocol,
    pub endpoint: String,
    pub tls: TlsConfig,
    pub retry: ExporterRetryConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExporterRetryConfig {
    pub max_attempts: usize,
    pub timeout_ms: u64,
    pub initial_backoff_ms: u64,
}

impl Default for ExporterRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            timeout_ms: 30_000,
            initial_backoff_ms: 100,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TlsConfig {
    pub enabled: bool,
    pub ca_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub server_name: Option<String>,
    pub require_client_auth: bool,
}

impl TlsConfig {
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExporterProtocol {
    OtlpGrpc,
    OtlpHttp,
    OtlpHttpJson,
    PrometheusRemoteWrite,
    Stdout,
    File,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteConfig {
    pub signal: SignalKind,
    pub exporters: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantLimits {
    pub max_ingest_bytes_per_second: u64,
    pub max_queue_bytes: u64,
    pub max_record_bytes: u32,
}

impl Default for TenantLimits {
    fn default() -> Self {
        Self {
            max_ingest_bytes_per_second: 64 * 1024 * 1024,
            max_queue_bytes: 10 * 1024 * 1024 * 1024,
            max_record_bytes: 4 * 1024 * 1024,
        }
    }
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            tenant_id: "default".to_string(),
            name: "default".to_string(),
            receivers: vec![
                ReceiverConfig {
                    name: "otlp-grpc".to_string(),
                    protocol: ReceiverProtocol::OtlpGrpc,
                    endpoint: "0.0.0.0:4317".to_string(),
                    tls: TlsConfig::disabled(),
                },
                ReceiverConfig {
                    name: "otlp-http".to_string(),
                    protocol: ReceiverProtocol::OtlpHttp,
                    endpoint: "0.0.0.0:4318".to_string(),
                    tls: TlsConfig::disabled(),
                },
            ],
            processors: vec![
                ProcessorConfig {
                    name: "memory-limiter".to_string(),
                    kind: ProcessorKind::MemoryLimiter,
                    enabled: true,
                },
                ProcessorConfig {
                    name: "batch".to_string(),
                    kind: ProcessorKind::Batch,
                    enabled: true,
                },
            ],
            exporters: vec![ExporterConfig {
                name: "stdout".to_string(),
                protocol: ExporterProtocol::Stdout,
                endpoint: "stdout://local".to_string(),
                tls: TlsConfig::disabled(),
                retry: ExporterRetryConfig::default(),
            }],
            routes: vec![
                RouteConfig {
                    signal: SignalKind::Trace,
                    exporters: vec!["stdout".to_string()],
                },
                RouteConfig {
                    signal: SignalKind::Metric,
                    exporters: vec!["stdout".to_string()],
                },
                RouteConfig {
                    signal: SignalKind::Log,
                    exporters: vec!["stdout".to_string()],
                },
            ],
            limits: TenantLimits::default(),
        }
    }
}

impl PipelineConfig {
    pub fn validate(&self) -> Result<(), PipelineError> {
        require_non_empty("tenant_id", &self.tenant_id)?;
        require_non_empty("name", &self.name)?;

        if self.receivers.is_empty() {
            return Err(PipelineError::InvalidConfig(
                "at least one receiver is required".to_string(),
            ));
        }
        if self.exporters.is_empty() {
            return Err(PipelineError::InvalidConfig(
                "at least one exporter is required".to_string(),
            ));
        }
        if self.routes.is_empty() {
            return Err(PipelineError::InvalidConfig(
                "at least one route is required".to_string(),
            ));
        }
        if self.limits.max_record_bytes == 0 {
            return Err(PipelineError::InvalidConfig(
                "max_record_bytes must be greater than zero".to_string(),
            ));
        }
        if self.limits.max_queue_bytes == 0 {
            return Err(PipelineError::InvalidConfig(
                "max_queue_bytes must be greater than zero".to_string(),
            ));
        }
        if self.limits.max_ingest_bytes_per_second == 0 {
            return Err(PipelineError::InvalidConfig(
                "max_ingest_bytes_per_second must be greater than zero".to_string(),
            ));
        }
        for exporter in &self.exporters {
            if exporter.retry.max_attempts == 0 {
                return Err(PipelineError::InvalidConfig(format!(
                    "exporter {} retry.max_attempts must be greater than zero",
                    exporter.name
                )));
            }
            if exporter.retry.timeout_ms == 0 {
                return Err(PipelineError::InvalidConfig(format!(
                    "exporter {} retry.timeout_ms must be greater than zero",
                    exporter.name
                )));
            }
            if exporter.retry.initial_backoff_ms == 0 {
                return Err(PipelineError::InvalidConfig(format!(
                    "exporter {} retry.initial_backoff_ms must be greater than zero",
                    exporter.name
                )));
            }
        }

        validate_unique_names(
            "receiver",
            self.receivers.iter().map(|item| item.name.as_str()),
        )?;
        validate_unique_names(
            "exporter",
            self.exporters.iter().map(|item| item.name.as_str()),
        )?;
        validate_unique_names(
            "processor",
            self.processors.iter().map(|item| item.name.as_str()),
        )?;

        let exporter_names: HashSet<&str> = self
            .exporters
            .iter()
            .map(|exporter| exporter.name.as_str())
            .collect();

        for route in &self.routes {
            if route.exporters.is_empty() {
                return Err(PipelineError::InvalidConfig(format!(
                    "route for {} has no exporters",
                    route.signal
                )));
            }

            for exporter in &route.exporters {
                if !exporter_names.contains(exporter.as_str()) {
                    return Err(PipelineError::UnknownExporter(exporter.clone()));
                }
            }
        }

        Ok(())
    }
}

fn require_non_empty(field: &str, value: &str) -> Result<(), PipelineError> {
    if value.trim().is_empty() {
        return Err(PipelineError::InvalidConfig(format!(
            "{field} must not be empty"
        )));
    }
    Ok(())
}

fn validate_unique_names<'a>(
    kind: &str,
    names: impl Iterator<Item = &'a str>,
) -> Result<(), PipelineError> {
    let mut seen = HashSet::new();
    for name in names {
        require_non_empty(kind, name)?;
        if !seen.insert(name) {
            return Err(PipelineError::InvalidConfig(format!(
                "duplicate {kind} name: {name}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let config = PipelineConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn rejects_unknown_exporter_in_route() {
        let mut config = PipelineConfig::default();
        config.routes[0].exporters = vec!["missing".to_string()];

        let error = config
            .validate()
            .err()
            .unwrap_or_else(|| PipelineError::InvalidConfig("expected error".to_string()));

        assert_eq!(error, PipelineError::UnknownExporter("missing".to_string()));
    }

    #[test]
    fn rejects_zero_ingest_rate_limit() {
        let mut config = PipelineConfig::default();
        config.limits.max_ingest_bytes_per_second = 0;

        let error = config
            .validate()
            .err()
            .unwrap_or_else(|| PipelineError::InvalidConfig("expected error".to_string()));

        assert_eq!(
            error,
            PipelineError::InvalidConfig(
                "max_ingest_bytes_per_second must be greater than zero".to_string()
            )
        );
    }

    #[test]
    fn rejects_invalid_exporter_retry_policy() {
        let mut config = PipelineConfig::default();
        config.exporters[0].retry.max_attempts = 0;

        let error = config
            .validate()
            .err()
            .unwrap_or_else(|| PipelineError::InvalidConfig("expected error".to_string()));

        assert_eq!(
            error,
            PipelineError::InvalidConfig(
                "exporter stdout retry.max_attempts must be greater than zero".to_string()
            )
        );
    }
}
