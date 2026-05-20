use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use telemetry_core::{
    ExporterConfig, ExporterProtocol, PipelineConfig, ProcessorConfig, ProcessorKind,
    ReceiverConfig, ReceiverProtocol, RouteConfig, SignalKind, TenantLimits, TlsConfig,
};

type DynError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Deserialize)]
struct FilePipelineConfig {
    tenant: String,
    pipeline: Option<String>,
    receivers: BTreeMap<String, FileReceiverConfig>,
    processors: Option<BTreeMap<String, FileProcessorConfig>>,
    exporters: BTreeMap<String, FileExporterConfig>,
    routes: BTreeMap<String, FileRouteConfig>,
    limits: Option<FileTenantLimits>,
}

#[derive(Debug, Deserialize)]
struct FileReceiverConfig {
    protocol: String,
    endpoint: String,
    tls: Option<FileTlsConfig>,
}

#[derive(Debug, Deserialize)]
struct FileProcessorConfig {
    enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct FileExporterConfig {
    protocol: String,
    endpoint: String,
    tls: Option<FileTlsConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FileTlsConfig {
    Enabled(bool),
    Detailed {
        enabled: Option<bool>,
        ca_file: Option<String>,
        cert_file: Option<String>,
        key_file: Option<String>,
        server_name: Option<String>,
        require_client_auth: Option<bool>,
    },
}

#[derive(Debug, Deserialize)]
struct FileRouteConfig {
    exporters: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct FileTenantLimits {
    max_ingest_bytes_per_second: Option<u64>,
    max_queue_bytes: Option<u64>,
    max_record_bytes: Option<u32>,
}

pub fn load_pipeline_config(path: &Path) -> Result<PipelineConfig, DynError> {
    let content = fs::read_to_string(path)?;
    parse_pipeline_config(&content)
}

pub fn parse_pipeline_config(content: &str) -> Result<PipelineConfig, DynError> {
    let file_config: FilePipelineConfig = serde_yaml::from_str(content)?;
    file_config.into_pipeline_config()
}

impl FilePipelineConfig {
    fn into_pipeline_config(self) -> Result<PipelineConfig, DynError> {
        let mut config = PipelineConfig {
            tenant_id: self.tenant,
            name: self.pipeline.unwrap_or_else(|| "default".to_string()),
            receivers: self
                .receivers
                .into_iter()
                .map(|(name, receiver)| receiver.into_receiver_config(name))
                .collect::<Result<Vec<_>, _>>()?,
            processors: self
                .processors
                .unwrap_or_default()
                .into_iter()
                .map(|(name, processor)| processor.into_processor_config(name))
                .collect::<Result<Vec<_>, _>>()?,
            exporters: self
                .exporters
                .into_iter()
                .map(|(name, exporter)| exporter.into_exporter_config(name))
                .collect::<Result<Vec<_>, _>>()?,
            routes: self
                .routes
                .into_iter()
                .map(|(signal, route)| route.into_route_config(signal))
                .collect::<Result<Vec<_>, _>>()?,
            limits: self
                .limits
                .map_or_else(TenantLimits::default, |limits| limits.into_tenant_limits()),
        };

        if config.processors.is_empty() {
            config.processors = PipelineConfig::default().processors;
        }

        config.validate()?;
        Ok(config)
    }
}

impl FileReceiverConfig {
    fn into_receiver_config(self, name: String) -> Result<ReceiverConfig, DynError> {
        Ok(ReceiverConfig {
            name,
            protocol: parse_receiver_protocol(&self.protocol)?,
            endpoint: self.endpoint,
            tls: self.tls.map_or_else(TlsConfig::disabled, Into::into),
        })
    }
}

impl FileProcessorConfig {
    fn into_processor_config(self, name: String) -> Result<ProcessorConfig, DynError> {
        Ok(ProcessorConfig {
            kind: parse_processor_kind(&name)?,
            name,
            enabled: self.enabled.unwrap_or(true),
        })
    }
}

impl FileExporterConfig {
    fn into_exporter_config(self, name: String) -> Result<ExporterConfig, DynError> {
        Ok(ExporterConfig {
            name,
            protocol: parse_exporter_protocol(&self.protocol)?,
            endpoint: self.endpoint,
            tls: self.tls.map_or_else(TlsConfig::disabled, Into::into),
        })
    }
}

impl From<FileTlsConfig> for TlsConfig {
    fn from(value: FileTlsConfig) -> Self {
        match value {
            FileTlsConfig::Enabled(enabled) => TlsConfig {
                enabled,
                ..TlsConfig::default()
            },
            FileTlsConfig::Detailed {
                enabled,
                ca_file,
                cert_file,
                key_file,
                server_name,
                require_client_auth,
            } => TlsConfig {
                enabled: enabled.unwrap_or(true),
                ca_file,
                cert_file,
                key_file,
                server_name,
                require_client_auth: require_client_auth.unwrap_or(false),
            },
        }
    }
}

impl FileRouteConfig {
    fn into_route_config(self, signal: String) -> Result<RouteConfig, DynError> {
        Ok(RouteConfig {
            signal: parse_signal(&signal)?,
            exporters: self.exporters,
        })
    }
}

impl FileTenantLimits {
    fn into_tenant_limits(self) -> TenantLimits {
        let defaults = TenantLimits::default();
        TenantLimits {
            max_ingest_bytes_per_second: self
                .max_ingest_bytes_per_second
                .unwrap_or(defaults.max_ingest_bytes_per_second),
            max_queue_bytes: self.max_queue_bytes.unwrap_or(defaults.max_queue_bytes),
            max_record_bytes: self.max_record_bytes.unwrap_or(defaults.max_record_bytes),
        }
    }
}

fn parse_receiver_protocol(value: &str) -> Result<ReceiverProtocol, DynError> {
    match normalize_token(value).as_str() {
        "otlp-grpc" => Ok(ReceiverProtocol::OtlpGrpc),
        "otlp-http" => Ok(ReceiverProtocol::OtlpHttp),
        "prometheus-remote-write" => Ok(ReceiverProtocol::PrometheusRemoteWrite),
        "file-log" => Ok(ReceiverProtocol::FileLog),
        "tf-line" => Ok(ReceiverProtocol::TfLine),
        _ => Err(format!("unknown receiver protocol: {value}").into()),
    }
}

fn parse_processor_kind(value: &str) -> Result<ProcessorKind, DynError> {
    match normalize_token(value).as_str() {
        "memory-limiter" => Ok(ProcessorKind::MemoryLimiter),
        "batch" => Ok(ProcessorKind::Batch),
        "redact" => Ok(ProcessorKind::Redact),
        "tail-sampling" => Ok(ProcessorKind::TailSampling),
        "tenant-rate-limit" => Ok(ProcessorKind::TenantRateLimit),
        _ => Err(format!("unknown processor kind: {value}").into()),
    }
}

fn parse_exporter_protocol(value: &str) -> Result<ExporterProtocol, DynError> {
    match normalize_token(value).as_str() {
        "otlp-grpc" => Ok(ExporterProtocol::OtlpGrpc),
        "otlp-http" => Ok(ExporterProtocol::OtlpHttp),
        "otlp-http-json" => Ok(ExporterProtocol::OtlpHttpJson),
        "prometheus-remote-write" => Ok(ExporterProtocol::PrometheusRemoteWrite),
        "kafka" => Ok(ExporterProtocol::Kafka),
        "s3" => Ok(ExporterProtocol::S3),
        "stdout" => Ok(ExporterProtocol::Stdout),
        "file" => Ok(ExporterProtocol::File),
        _ => Err(format!("unknown exporter protocol: {value}").into()),
    }
}

fn parse_signal(value: &str) -> Result<SignalKind, DynError> {
    SignalKind::from_token(&normalize_token(value))
        .ok_or_else(|| format!("unknown route signal: {value}").into())
}

fn normalize_token(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('_', "-")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_yaml_pipeline_config() -> Result<(), DynError> {
        let raw = r#"
tenant: payments-prod
pipeline: default
receivers:
  tf-line:
    protocol: tf_line
    endpoint: 127.0.0.1:4319
  otlp-http:
    protocol: otlp_http
    endpoint: 0.0.0.0:4318
    tls:
      enabled: true
      cert_file: certs/server.pem
      key_file: certs/server-key.pem
      ca_file: certs/ca.pem
      require_client_auth: true
processors:
  memory-limiter:
    enabled: true
  redact:
    enabled: true
exporters:
  stdout:
    protocol: otlp_http_json
    endpoint: http://collector:4318
    tls:
      enabled: true
      ca_file: certs/ca.pem
      cert_file: certs/client.pem
      key_file: certs/client-key.pem
      server_name: collector.internal
routes:
  traces:
    exporters: [stdout]
limits:
  max_queue_bytes: 1024
"#;

        let file_config: FilePipelineConfig = serde_yaml::from_str(raw)?;
        let config = file_config.into_pipeline_config()?;

        assert_eq!(config.tenant_id, "payments-prod");
        assert!(
            config
                .receivers
                .iter()
                .any(|receiver| receiver.protocol == ReceiverProtocol::TfLine)
        );
        let otlp_http = config
            .receivers
            .iter()
            .find(|receiver| receiver.protocol == ReceiverProtocol::OtlpHttp)
            .ok_or("missing otlp-http receiver")?;
        assert!(otlp_http.tls.enabled);
        assert!(otlp_http.tls.require_client_auth);
        assert_eq!(config.exporters[0].protocol, ExporterProtocol::OtlpHttpJson);
        assert_eq!(
            config.exporters[0].tls.server_name.as_deref(),
            Some("collector.internal")
        );
        assert_eq!(config.routes[0].signal, SignalKind::Trace);
        assert_eq!(config.limits.max_queue_bytes, 1024);
        Ok(())
    }

    #[test]
    fn rejects_unknown_protocol() -> Result<(), DynError> {
        let raw = r#"
tenant: payments-prod
receivers:
  custom:
    protocol: not_real
    endpoint: 127.0.0.1:4319
exporters:
  stdout:
    protocol: stdout
    endpoint: stdout://local
routes:
  traces:
    exporters: [stdout]
"#;

        let file_config: FilePipelineConfig = serde_yaml::from_str(raw)?;
        let result = file_config.into_pipeline_config();

        assert!(
            result
                .err()
                .map(|err| err.to_string().contains("unknown receiver protocol"))
                .unwrap_or(false)
        );
        Ok(())
    }
}
