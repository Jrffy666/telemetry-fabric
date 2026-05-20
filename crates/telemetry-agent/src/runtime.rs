use std::error::Error;
use std::fmt::Write;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use telemetry_buffer::{DiskQueue, DiskQueueError, DiskQueueOptions, storage_bytes_for_payload};
use telemetry_core::{PipelineConfig, RecordBatch, Router, TelemetryRecord};
use telemetry_exporters::{ExporterMap, build_exporters};
use telemetry_processors::ProcessorChain;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FlushReport {
    pub drained_records: usize,
    pub exported_records: usize,
    pub dropped_records: usize,
    pub exported_bytes: usize,
}

pub struct AgentRuntime {
    config: PipelineConfig,
    queue: DiskQueue,
    processors: ProcessorChain,
    exporters: ExporterMap,
    metrics: RuntimeMetrics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    QueueFull {
        queued_bytes: u64,
        record_bytes: u64,
        max_queue_bytes: u64,
    },
}

impl Display for RuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QueueFull {
                queued_bytes,
                record_bytes,
                max_queue_bytes,
            } => write!(
                f,
                "queue is full: queued_bytes={queued_bytes} record_bytes={record_bytes} max_queue_bytes={max_queue_bytes}"
            ),
        }
    }
}

impl Error for RuntimeError {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeHealth {
    pub queued_bytes: u64,
    pub cursor_segment_id: u64,
    pub cursor_offset: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeMetrics {
    pub ingested_records_total: u64,
    pub ingest_rejected_records_total: u64,
    pub flush_attempts_total: u64,
    pub flush_successes_total: u64,
    pub flush_failures_total: u64,
    pub drained_records_total: u64,
    pub exported_records_total: u64,
    pub dropped_records_total: u64,
    pub exported_bytes_total: u64,
    pub config_reloads_total: u64,
    pub config_reload_failures_total: u64,
    pub control_registrations_total: u64,
    pub control_registration_failures_total: u64,
    pub control_heartbeats_total: u64,
    pub control_heartbeat_failures_total: u64,
    pub control_config_fetches_total: u64,
    pub control_config_fetch_failures_total: u64,
}

impl AgentRuntime {
    pub fn open(
        config: PipelineConfig,
        queue_dir: PathBuf,
        options: DiskQueueOptions,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        config.validate()?;
        let queue = DiskQueue::open(queue_dir, options)?;
        let processors = ProcessorChain::from_config(&config);
        let exporters = build_exporters(&config.exporters)?;
        Ok(Self {
            config,
            queue,
            processors,
            exporters,
            metrics: RuntimeMetrics::default(),
        })
    }

    pub fn ingest(&mut self, record: TelemetryRecord) -> Result<(), Box<dyn Error + Send + Sync>> {
        let result = self.ingest_inner(record);
        if result.is_ok() {
            self.metrics.ingested_records_total += 1;
        } else {
            self.metrics.ingest_rejected_records_total += 1;
        }
        result
    }

    fn ingest_inner(
        &mut self,
        record: TelemetryRecord,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        record.validate(self.config.limits.max_record_bytes)?;
        let encoded = record.encode()?;
        let record_bytes = storage_bytes_for_payload(encoded.len())?;
        let queued_bytes = self.queue.queued_bytes()?;
        if queued_bytes.saturating_add(record_bytes) > self.config.limits.max_queue_bytes {
            return Err(Box::new(RuntimeError::QueueFull {
                queued_bytes,
                record_bytes,
                max_queue_bytes: self.config.limits.max_queue_bytes,
            }));
        }

        self.queue.enqueue(&encoded)?;
        Ok(())
    }

    pub fn health(&self) -> Result<RuntimeHealth, DiskQueueError> {
        let (cursor_segment_id, cursor_offset) = self.queue.cursor_position();
        Ok(RuntimeHealth {
            queued_bytes: self.queue.queued_bytes()?,
            cursor_segment_id,
            cursor_offset,
        })
    }

    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    pub fn metrics(&self) -> RuntimeMetrics {
        self.metrics
    }

    pub fn prometheus_metrics(&self) -> Result<String, DiskQueueError> {
        let health = self.health()?;
        Ok(format_prometheus_metrics(health, self.metrics))
    }

    pub fn record_control_registration(&mut self, success: bool) {
        if success {
            self.metrics.control_registrations_total += 1;
        } else {
            self.metrics.control_registration_failures_total += 1;
        }
    }

    pub fn record_control_heartbeat(&mut self, success: bool) {
        if success {
            self.metrics.control_heartbeats_total += 1;
        } else {
            self.metrics.control_heartbeat_failures_total += 1;
        }
    }

    pub fn record_control_config_fetch(&mut self, success: bool) {
        if success {
            self.metrics.control_config_fetches_total += 1;
        } else {
            self.metrics.control_config_fetch_failures_total += 1;
        }
    }

    pub fn reload_config(
        &mut self,
        config: PipelineConfig,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let result = self.reload_config_inner(config);
        if result.is_ok() {
            self.metrics.config_reloads_total += 1;
        } else {
            self.metrics.config_reload_failures_total += 1;
        }
        result
    }

    fn reload_config_inner(
        &mut self,
        config: PipelineConfig,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        config.validate()?;
        let processors = ProcessorChain::from_config(&config);
        let exporters = build_exporters(&config.exporters)?;
        self.config = config;
        self.processors = processors;
        self.exporters = exporters;
        Ok(())
    }

    pub async fn flush(&mut self, max_items: usize) -> Result<FlushReport, DiskQueueError> {
        self.metrics.flush_attempts_total += 1;
        let result = self.flush_inner(max_items).await;
        match result {
            Ok(report) => {
                self.metrics.flush_successes_total += 1;
                self.metrics.drained_records_total += report.drained_records as u64;
                self.metrics.exported_records_total += report.exported_records as u64;
                self.metrics.dropped_records_total += report.dropped_records as u64;
                self.metrics.exported_bytes_total += report.exported_bytes as u64;
                Ok(report)
            }
            Err(err) => {
                self.metrics.flush_failures_total += 1;
                Err(err)
            }
        }
    }

    async fn flush_inner(&mut self, max_items: usize) -> Result<FlushReport, DiskQueueError> {
        let config = &self.config;
        let processors = &mut self.processors;
        let exporters = &mut self.exporters;
        let batch = self.queue.read_batch(max_items)?;
        let mut report = FlushReport {
            drained_records: batch.len(),
            ..FlushReport::default()
        };

        if batch.is_empty() {
            self.queue.commit_batch(batch)?;
            return Ok(report);
        }

        let mut records = Vec::with_capacity(batch.len());

        for payload in batch.payloads() {
            let record = TelemetryRecord::decode(payload)
                .map_err(|err| DiskQueueError::Sink(err.to_string()))?;

            match processors
                .process(record)
                .map_err(|err| DiskQueueError::Sink(err.to_string()))?
            {
                Some(record) => records.push(record),
                None => report.dropped_records += 1,
            }
        }

        let export_report = export_records(config, exporters, records).await?;
        report.exported_records += export_report.exported_records;
        report.exported_bytes += export_report.exported_bytes;
        self.queue.commit_batch(batch)?;

        Ok(report)
    }

    pub async fn flush_to_stdout(&mut self, max_items: usize) -> Result<usize, DiskQueueError> {
        self.flush(max_items)
            .await
            .map(|report| report.drained_records)
    }
}

fn format_prometheus_metrics(health: RuntimeHealth, metrics: RuntimeMetrics) -> String {
    let mut output = String::new();

    write_gauge(
        &mut output,
        "telemetry_agent_queue_bytes",
        "Current durable queue size in bytes.",
        health.queued_bytes,
    );
    write_gauge(
        &mut output,
        "telemetry_agent_queue_cursor_segment_id",
        "Current durable queue cursor segment id.",
        health.cursor_segment_id,
    );
    write_gauge(
        &mut output,
        "telemetry_agent_queue_cursor_offset_bytes",
        "Current durable queue cursor offset in bytes.",
        health.cursor_offset,
    );

    write_counter(
        &mut output,
        "telemetry_agent_ingested_records_total",
        "Records accepted into the durable queue.",
        metrics.ingested_records_total,
    );
    write_counter(
        &mut output,
        "telemetry_agent_ingest_rejected_records_total",
        "Records rejected before durable queue enqueue.",
        metrics.ingest_rejected_records_total,
    );
    write_counter(
        &mut output,
        "telemetry_agent_flush_attempts_total",
        "Flush attempts started by the runtime.",
        metrics.flush_attempts_total,
    );
    write_counter(
        &mut output,
        "telemetry_agent_flush_successes_total",
        "Flush attempts that completed successfully.",
        metrics.flush_successes_total,
    );
    write_counter(
        &mut output,
        "telemetry_agent_flush_failures_total",
        "Flush attempts that failed before committing the queue cursor.",
        metrics.flush_failures_total,
    );
    write_counter(
        &mut output,
        "telemetry_agent_drained_records_total",
        "Records drained from the durable queue.",
        metrics.drained_records_total,
    );
    write_counter(
        &mut output,
        "telemetry_agent_exported_records_total",
        "Records exported successfully.",
        metrics.exported_records_total,
    );
    write_counter(
        &mut output,
        "telemetry_agent_dropped_records_total",
        "Records dropped by processors.",
        metrics.dropped_records_total,
    );
    write_counter(
        &mut output,
        "telemetry_agent_exported_bytes_total",
        "Approximate bytes exported successfully.",
        metrics.exported_bytes_total,
    );
    write_counter(
        &mut output,
        "telemetry_agent_config_reloads_total",
        "Control-plane config reloads applied successfully.",
        metrics.config_reloads_total,
    );
    write_counter(
        &mut output,
        "telemetry_agent_config_reload_failures_total",
        "Control-plane config reloads that failed validation or runtime application.",
        metrics.config_reload_failures_total,
    );
    write_counter(
        &mut output,
        "telemetry_agent_control_registrations_total",
        "Successful control-plane registrations.",
        metrics.control_registrations_total,
    );
    write_counter(
        &mut output,
        "telemetry_agent_control_registration_failures_total",
        "Failed control-plane registration attempts.",
        metrics.control_registration_failures_total,
    );
    write_counter(
        &mut output,
        "telemetry_agent_control_heartbeats_total",
        "Successful control-plane heartbeat calls.",
        metrics.control_heartbeats_total,
    );
    write_counter(
        &mut output,
        "telemetry_agent_control_heartbeat_failures_total",
        "Failed control-plane heartbeat calls.",
        metrics.control_heartbeat_failures_total,
    );
    write_counter(
        &mut output,
        "telemetry_agent_control_config_fetches_total",
        "Successful control-plane config fetch calls.",
        metrics.control_config_fetches_total,
    );
    write_counter(
        &mut output,
        "telemetry_agent_control_config_fetch_failures_total",
        "Failed control-plane config fetch calls.",
        metrics.control_config_fetch_failures_total,
    );

    output
}

fn write_counter(output: &mut String, name: &str, help: &str, value: u64) {
    write_metric(output, name, help, "counter", value);
}

fn write_gauge(output: &mut String, name: &str, help: &str, value: u64) {
    write_metric(output, name, help, "gauge", value);
}

fn write_metric(output: &mut String, name: &str, help: &str, kind: &str, value: u64) {
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} {kind}");
    let _ = writeln!(output, "{name} {value}");
}

async fn export_records(
    config: &PipelineConfig,
    exporters: &mut ExporterMap,
    records: Vec<TelemetryRecord>,
) -> Result<FlushReport, DiskQueueError> {
    let router = Router::new(config);
    let mut grouped = std::collections::BTreeMap::<String, Vec<TelemetryRecord>>::new();
    let mut report = FlushReport::default();

    for record in records {
        let destinations = router
            .exporters_for(record.signal)
            .map_err(|err| DiskQueueError::Sink(err.to_string()))?;

        for destination in destinations {
            grouped
                .entry(destination.name.clone())
                .or_default()
                .push(record.clone());
        }
    }

    for (name, records) in grouped {
        let exporter = exporters
            .get_mut(&name)
            .ok_or_else(|| DiskQueueError::Sink(format!("exporter not initialized: {name}")))?;
        let batch = RecordBatch::new(records);
        let export_report = exporter
            .export(&batch)
            .await
            .map_err(|err| DiskQueueError::Sink(err.to_string()))?;
        report.exported_records += export_report.records;
        report.exported_bytes += export_report.bytes;
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};
    use telemetry_core::{
        ExporterConfig, ExporterProtocol, RouteConfig, SignalKind, TenantLimits, TlsConfig,
    };

    #[tokio::test]
    async fn runtime_flushes_records_to_file_exporter() -> Result<(), Box<dyn Error + Send + Sync>>
    {
        let dir = test_dir("runtime-file-export");
        let queue_dir = dir.join("queue");
        let export_path = dir.join("export.log");
        let config = PipelineConfig {
            exporters: vec![ExporterConfig {
                name: "file".to_string(),
                protocol: ExporterProtocol::File,
                endpoint: export_path.to_string_lossy().to_string(),
                tls: TlsConfig::disabled(),
            }],
            routes: vec![RouteConfig {
                signal: SignalKind::Trace,
                exporters: vec!["file".to_string()],
            }],
            ..PipelineConfig::default()
        };

        let mut runtime = AgentRuntime::open(config, queue_dir, DiskQueueOptions::default())?;
        runtime.ingest(TelemetryRecord::new(
            "tenant-a",
            SignalKind::Trace,
            b"span".to_vec(),
        ))?;

        let report = runtime.flush(10).await?;

        assert_eq!(report.drained_records, 1);
        assert_eq!(report.exported_records, 1);
        let metrics = runtime.metrics();
        assert_eq!(metrics.ingested_records_total, 1);
        assert_eq!(metrics.flush_attempts_total, 1);
        assert_eq!(metrics.flush_successes_total, 1);
        assert_eq!(metrics.drained_records_total, 1);
        assert_eq!(metrics.exported_records_total, 1);
        assert_eq!(
            fs::read_to_string(&export_path)?,
            "tenant=tenant-a signal=trace attrs=0 bytes=4\n"
        );

        cleanup(dir);
        Ok(())
    }

    #[tokio::test]
    async fn runtime_rejects_records_when_queue_quota_would_be_exceeded()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let dir = test_dir("runtime-queue-quota");
        let queue_dir = dir.join("queue");
        let config = PipelineConfig {
            limits: TenantLimits {
                max_queue_bytes: 32,
                ..TenantLimits::default()
            },
            ..PipelineConfig::default()
        };

        let mut runtime = AgentRuntime::open(config, queue_dir, DiskQueueOptions::default())?;
        let result = runtime.ingest(TelemetryRecord::new(
            "tenant-a",
            SignalKind::Trace,
            b"this-record-is-too-large-for-the-queue".to_vec(),
        ));

        assert!(result.is_err());
        assert!(
            result
                .err()
                .map(|err| err.to_string().contains("queue is full"))
                .unwrap_or(false)
        );
        assert_eq!(runtime.metrics().ingest_rejected_records_total, 1);

        cleanup(dir);
        Ok(())
    }

    #[tokio::test]
    async fn runtime_formats_prometheus_metrics() -> Result<(), Box<dyn Error + Send + Sync>> {
        let dir = test_dir("runtime-prometheus-metrics");
        let queue_dir = dir.join("queue");
        let mut runtime = AgentRuntime::open(
            PipelineConfig::default(),
            queue_dir,
            DiskQueueOptions::default(),
        )?;

        runtime.ingest(TelemetryRecord::new(
            "tenant-a",
            SignalKind::Metric,
            b"42".to_vec(),
        ))?;
        runtime.record_control_registration(true);
        runtime.record_control_heartbeat(false);

        let metrics = runtime.prometheus_metrics()?;

        assert!(metrics.contains("# TYPE telemetry_agent_queue_bytes gauge"));
        assert!(metrics.contains("telemetry_agent_ingested_records_total 1"));
        assert!(metrics.contains("telemetry_agent_control_registrations_total 1"));
        assert!(metrics.contains("telemetry_agent_control_heartbeat_failures_total 1"));

        cleanup(dir);
        Ok(())
    }

    fn test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!("telemetry_agent_{name}_{nanos}"));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn cleanup(path: PathBuf) {
        let _ = fs::remove_dir_all(path);
    }
}
