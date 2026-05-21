use std::error::Error;
use std::fmt::Write;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use telemetry_buffer::{
    DiskQueue, DiskQueueError, DiskQueueOptions, PendingBatch, storage_bytes_for_payload,
};
use telemetry_core::{ExporterRetryConfig, PipelineConfig, RecordBatch, Router, TelemetryRecord};
use telemetry_exporters::{ExportReport, Exporter, ExporterMap, build_exporters};
use telemetry_processors::ProcessorChain;
use tokio::sync::Mutex;

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
    exports_paused: bool,
    config_generation: u64,
    flush_in_progress: bool,
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
    pub exports_paused: bool,
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
            exports_paused: false,
            config_generation: 0,
            flush_in_progress: false,
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
            exports_paused: self.exports_paused,
        })
    }

    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    #[cfg(test)]
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

    pub fn pause_exports(&mut self) {
        self.exports_paused = true;
    }

    pub fn resume_exports(&mut self) {
        self.exports_paused = false;
    }

    #[cfg(test)]
    pub fn exports_paused(&self) -> bool {
        self.exports_paused
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
        self.config_generation = self.config_generation.wrapping_add(1);
        Ok(())
    }

    pub async fn flush_shared(
        runtime: Arc<Mutex<Self>>,
        max_items: usize,
    ) -> Result<FlushReport, DiskQueueError> {
        Self::flush_shared_inner(runtime, max_items, false).await
    }

    pub async fn flush_for_shutdown_shared(
        runtime: Arc<Mutex<Self>>,
        max_items: usize,
    ) -> Result<FlushReport, DiskQueueError> {
        Self::flush_shared_inner(runtime, max_items, true).await
    }

    async fn flush_shared_inner(
        runtime: Arc<Mutex<Self>>,
        max_items: usize,
        force_unpaused: bool,
    ) -> Result<FlushReport, DiskQueueError> {
        let prepared = Self::prepare_shared_flush(&runtime, max_items, force_unpaused).await?;
        let Some(prepared) = prepared else {
            return Ok(FlushReport::default());
        };

        let PreparedFlush {
            batch,
            mut report,
            config,
            mut exporters,
            config_generation,
            records,
        } = prepared;

        let export_result = export_records(&config, &mut exporters, records).await;
        let mut runtime = runtime.lock().await;

        if runtime.config_generation == config_generation {
            runtime.exporters = exporters;
        }
        runtime.flush_in_progress = false;

        match export_result {
            Ok(export_report) => {
                report.exported_records += export_report.exported_records;
                report.exported_bytes += export_report.exported_bytes;
                match runtime.queue.commit_batch(batch) {
                    Ok(_) => {
                        runtime.record_flush_success(report);
                        Ok(report)
                    }
                    Err(err) => {
                        runtime.record_flush_failure();
                        Err(err)
                    }
                }
            }
            Err(err) => {
                runtime.record_flush_failure();
                Err(err)
            }
        }
    }

    async fn prepare_shared_flush(
        runtime: &Arc<Mutex<Self>>,
        max_items: usize,
        force_unpaused: bool,
    ) -> Result<Option<PreparedFlush>, DiskQueueError> {
        loop {
            let mut runtime = runtime.lock().await;
            if runtime.exports_paused && !force_unpaused {
                return Ok(None);
            }
            if runtime.flush_in_progress {
                drop(runtime);
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            runtime.metrics.flush_attempts_total += 1;
            let result = runtime.prepare_flush_work(max_items);
            match result {
                Ok(PreparedFlushState::Empty(report)) => {
                    runtime.record_flush_success(report);
                    return Ok(None);
                }
                Ok(PreparedFlushState::Ready(prepared)) => {
                    runtime.flush_in_progress = true;
                    return Ok(Some(*prepared));
                }
                Err(err) => {
                    runtime.record_flush_failure();
                    return Err(err);
                }
            }
        }
    }

    fn prepare_flush_work(
        &mut self,
        max_items: usize,
    ) -> Result<PreparedFlushState, DiskQueueError> {
        let batch = self.queue.read_batch(max_items)?;
        let mut report = FlushReport {
            drained_records: batch.len(),
            ..FlushReport::default()
        };

        if batch.is_empty() {
            self.queue.commit_batch(batch)?;
            return Ok(PreparedFlushState::Empty(report));
        }

        let mut records = Vec::with_capacity(batch.len());

        for payload in batch.payloads() {
            let record = TelemetryRecord::decode(payload)
                .map_err(|err| DiskQueueError::Sink(err.to_string()))?;

            match self
                .processors
                .process(record)
                .map_err(|err| DiskQueueError::Sink(err.to_string()))?
            {
                Some(record) => records.push(record),
                None => report.dropped_records += 1,
            }
        }

        Ok(PreparedFlushState::Ready(Box::new(PreparedFlush {
            batch,
            report,
            config: self.config.clone(),
            exporters: std::mem::take(&mut self.exporters),
            config_generation: self.config_generation,
            records,
        })))
    }

    pub async fn flush(&mut self, max_items: usize) -> Result<FlushReport, DiskQueueError> {
        if self.exports_paused {
            return Ok(FlushReport::default());
        }

        self.flush_unpaused(max_items).await
    }

    #[cfg(test)]
    pub async fn flush_for_shutdown(
        &mut self,
        max_items: usize,
    ) -> Result<FlushReport, DiskQueueError> {
        let was_paused = self.exports_paused;
        self.exports_paused = false;
        let result = self.flush_unpaused(max_items).await;
        self.exports_paused = was_paused;
        result
    }

    async fn flush_unpaused(&mut self, max_items: usize) -> Result<FlushReport, DiskQueueError> {
        self.metrics.flush_attempts_total += 1;
        let result = self.flush_inner(max_items).await;
        match result {
            Ok(report) => {
                self.record_flush_success(report);
                Ok(report)
            }
            Err(err) => {
                self.record_flush_failure();
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

    fn record_flush_success(&mut self, report: FlushReport) {
        self.metrics.flush_successes_total += 1;
        self.metrics.drained_records_total += report.drained_records as u64;
        self.metrics.exported_records_total += report.exported_records as u64;
        self.metrics.dropped_records_total += report.dropped_records as u64;
        self.metrics.exported_bytes_total += report.exported_bytes as u64;
    }

    fn record_flush_failure(&mut self) {
        self.metrics.flush_failures_total += 1;
    }
}

struct PreparedFlush {
    batch: PendingBatch,
    report: FlushReport,
    config: PipelineConfig,
    exporters: ExporterMap,
    config_generation: u64,
    records: Vec<TelemetryRecord>,
}

enum PreparedFlushState {
    Empty(FlushReport),
    Ready(Box<PreparedFlush>),
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
    write_gauge(
        &mut output,
        "telemetry_agent_exports_paused",
        "Whether exporter flushing is paused by the control plane.",
        if health.exports_paused { 1 } else { 0 },
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
        let retry = config
            .exporters
            .iter()
            .find(|exporter| exporter.name == name)
            .map(|exporter| exporter.retry)
            .unwrap_or_default();
        let export_report =
            export_with_retry(name.as_str(), exporter.as_mut(), &batch, retry).await?;
        report.exported_records += export_report.records;
        report.exported_bytes += export_report.bytes;
    }

    Ok(report)
}

async fn export_with_retry(
    exporter_name: &str,
    exporter: &mut dyn Exporter,
    batch: &RecordBatch,
    retry: ExporterRetryConfig,
) -> Result<ExportReport, DiskQueueError> {
    let mut attempt = 1;

    loop {
        let result = tokio::time::timeout(
            Duration::from_millis(retry.timeout_ms),
            exporter.export(batch),
        )
        .await;

        match result {
            Ok(Ok(report)) => return Ok(report),
            Ok(Err(err)) if attempt < retry.max_attempts => {
                sleep_before_retry(attempt, retry).await;
                attempt += 1;
                eprintln!(
                    "exporter retrying after failure: exporter={} attempt={} error={}",
                    exporter_name, attempt, err
                );
            }
            Ok(Err(err)) => {
                return Err(DiskQueueError::Sink(format!(
                    "exporter {exporter_name} failed after {attempt} attempts: {err}"
                )));
            }
            Err(_) if attempt < retry.max_attempts => {
                sleep_before_retry(attempt, retry).await;
                attempt += 1;
                eprintln!(
                    "exporter retrying after timeout: exporter={} attempt={}",
                    exporter_name, attempt
                );
            }
            Err(_) => {
                return Err(DiskQueueError::Sink(format!(
                    "exporter {exporter_name} timed out after {attempt} attempts"
                )));
            }
        }
    }
}

async fn sleep_before_retry(attempt: usize, retry: ExporterRetryConfig) {
    let multiplier = 1_u32.checked_shl((attempt - 1) as u32).unwrap_or(1);
    tokio::time::sleep(Duration::from_millis(retry.initial_backoff_ms) * multiplier).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use telemetry_core::{
        ExporterConfig, ExporterProtocol, ExporterRetryConfig, RouteConfig, SignalKind,
        TenantLimits, TlsConfig,
    };
    use telemetry_exporters::ExporterError;
    use tokio::sync::Notify;

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
                retry: ExporterRetryConfig::default(),
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
        assert!(metrics.contains("telemetry_agent_exports_paused 0"));
        assert!(metrics.contains("telemetry_agent_ingested_records_total 1"));
        assert!(metrics.contains("telemetry_agent_control_registrations_total 1"));
        assert!(metrics.contains("telemetry_agent_control_heartbeat_failures_total 1"));

        cleanup(dir);
        Ok(())
    }

    #[tokio::test]
    async fn runtime_pause_exports_keeps_records_queued() -> Result<(), Box<dyn Error + Send + Sync>>
    {
        let dir = test_dir("runtime-pause-exports");
        let queue_dir = dir.join("queue");
        let export_path = dir.join("export.log");
        let config = PipelineConfig {
            exporters: vec![ExporterConfig {
                name: "file".to_string(),
                protocol: ExporterProtocol::File,
                endpoint: export_path.to_string_lossy().to_string(),
                tls: TlsConfig::disabled(),
                retry: ExporterRetryConfig::default(),
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
        runtime.pause_exports();

        let paused_report = runtime.flush(10).await?;

        assert_eq!(paused_report.drained_records, 0);
        assert!(runtime.exports_paused());
        assert!(runtime.health()?.queued_bytes > 0);
        assert_eq!(fs::read_to_string(&export_path)?, "");

        runtime.resume_exports();
        let resumed_report = runtime.flush(10).await?;

        assert_eq!(resumed_report.drained_records, 1);
        assert_eq!(
            fs::read_to_string(&export_path)?,
            "tenant=tenant-a signal=trace attrs=0 bytes=4\n"
        );

        cleanup(dir);
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_flush_bypasses_paused_exports() -> Result<(), Box<dyn Error + Send + Sync>> {
        let dir = test_dir("runtime-shutdown-flush");
        let queue_dir = dir.join("queue");
        let export_path = dir.join("export.log");
        let config = PipelineConfig {
            exporters: vec![ExporterConfig {
                name: "file".to_string(),
                protocol: ExporterProtocol::File,
                endpoint: export_path.to_string_lossy().to_string(),
                tls: TlsConfig::disabled(),
                retry: ExporterRetryConfig::default(),
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
        runtime.pause_exports();

        let report = runtime.flush_for_shutdown(10).await?;

        assert_eq!(report.drained_records, 1);
        assert!(runtime.exports_paused());
        assert_eq!(
            fs::read_to_string(&export_path)?,
            "tenant=tenant-a signal=trace attrs=0 bytes=4\n"
        );

        cleanup(dir);
        Ok(())
    }

    #[tokio::test]
    async fn export_records_retries_transient_exporter_failures()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let attempts = Arc::new(AtomicUsize::new(0));
        let mut exporters: ExporterMap = std::collections::BTreeMap::new();
        exporters.insert(
            "flaky".to_string(),
            Box::new(FlakyExporter {
                attempts: Arc::clone(&attempts),
                fail_until_attempt: 2,
            }),
        );
        let config = PipelineConfig {
            exporters: vec![ExporterConfig {
                name: "flaky".to_string(),
                protocol: ExporterProtocol::Stdout,
                endpoint: "stdout://local".to_string(),
                tls: TlsConfig::disabled(),
                retry: ExporterRetryConfig {
                    max_attempts: 4,
                    timeout_ms: 1000,
                    initial_backoff_ms: 1,
                },
            }],
            routes: vec![RouteConfig {
                signal: SignalKind::Trace,
                exporters: vec!["flaky".to_string()],
            }],
            ..PipelineConfig::default()
        };
        let records = vec![TelemetryRecord::new(
            "tenant-a",
            SignalKind::Trace,
            b"span".to_vec(),
        )];

        let report = export_records(&config, &mut exporters, records).await?;

        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert_eq!(report.exported_records, 1);
        Ok(())
    }

    #[tokio::test]
    async fn shared_flush_releases_runtime_lock_while_exporting()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let dir = test_dir("runtime-shared-flush-lock");
        let queue_dir = dir.join("queue");
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let config = PipelineConfig {
            exporters: vec![ExporterConfig {
                name: "blocking".to_string(),
                protocol: ExporterProtocol::Stdout,
                endpoint: "stdout://local".to_string(),
                tls: TlsConfig::disabled(),
                retry: ExporterRetryConfig {
                    max_attempts: 1,
                    timeout_ms: 10_000,
                    initial_backoff_ms: 1,
                },
            }],
            routes: vec![RouteConfig {
                signal: SignalKind::Trace,
                exporters: vec!["blocking".to_string()],
            }],
            ..PipelineConfig::default()
        };
        let mut runtime = AgentRuntime::open(config, queue_dir, DiskQueueOptions::default())?;
        runtime.exporters.clear();
        runtime.exporters.insert(
            "blocking".to_string(),
            Box::new(BlockingExporter {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
            }),
        );
        runtime.ingest(TelemetryRecord::new(
            "tenant-a",
            SignalKind::Trace,
            b"span".to_vec(),
        ))?;
        let runtime = Arc::new(Mutex::new(runtime));
        let flush_task = tokio::spawn(AgentRuntime::flush_shared(Arc::clone(&runtime), 10));

        started.notified().await;
        let guard = tokio::time::timeout(Duration::from_millis(100), runtime.lock()).await?;
        drop(guard);
        release.notify_one();

        let report = flush_task.await??;

        assert_eq!(report.drained_records, 1);
        assert_eq!(report.exported_records, 1);
        cleanup(dir);
        Ok(())
    }

    struct FlakyExporter {
        attempts: Arc<AtomicUsize>,
        fail_until_attempt: usize,
    }

    impl Exporter for FlakyExporter {
        fn export<'a>(
            &'a mut self,
            batch: &'a RecordBatch,
        ) -> Pin<
            Box<dyn std::future::Future<Output = Result<ExportReport, ExporterError>> + Send + 'a>,
        > {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            let fail_until_attempt = self.fail_until_attempt;

            Box::pin(async move {
                if attempt <= fail_until_attempt {
                    return Err(ExporterError::Io(io::Error::other(
                        "temporary exporter failure",
                    )));
                }

                Ok(ExportReport {
                    records: batch.records.len(),
                    bytes: batch.records.iter().map(|record| record.body.len()).sum(),
                })
            })
        }
    }

    struct BlockingExporter {
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl Exporter for BlockingExporter {
        fn export<'a>(
            &'a mut self,
            batch: &'a RecordBatch,
        ) -> Pin<
            Box<dyn std::future::Future<Output = Result<ExportReport, ExporterError>> + Send + 'a>,
        > {
            let started = Arc::clone(&self.started);
            let release = Arc::clone(&self.release);

            Box::pin(async move {
                started.notify_one();
                release.notified().await;
                Ok(ExportReport {
                    records: batch.records.len(),
                    bytes: batch.records.iter().map(|record| record.body.len()).sum(),
                })
            })
        }
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
