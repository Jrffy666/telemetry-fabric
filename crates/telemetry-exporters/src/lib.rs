use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{
    LogRecord, ResourceLogs as LogResourceLogs, ScopeLogs as LogScopeLogs, SeverityNumber,
};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge,
    Histogram, HistogramDataPoint, Metric, NumberDataPoint,
    ResourceMetrics as MetricResourceMetrics, ScopeMetrics as MetricScopeMetrics, Sum, Summary,
    SummaryDataPoint, exemplar, exponential_histogram_data_point, metric, number_data_point,
    summary_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::{self, OpenOptions};
use std::future::Future;
use std::io::{self, BufReader, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use telemetry_core::{
    ExporterConfig, ExporterProtocol, RecordBatch, SignalKind, TelemetryRecord, TlsConfig,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};
use tonic::transport::Channel;

#[derive(Debug)]
pub enum ExporterError {
    Io(io::Error),
    Grpc(String),
    UnsupportedExporter(String),
    InvalidEndpoint(String),
    Tls(String),
}

impl Display for ExporterError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "exporter I/O error: {err}"),
            Self::Grpc(message) => write!(f, "exporter gRPC error: {message}"),
            Self::UnsupportedExporter(name) => write!(f, "unsupported exporter: {name}"),
            Self::InvalidEndpoint(endpoint) => write!(f, "invalid exporter endpoint: {endpoint}"),
            Self::Tls(message) => write!(f, "exporter TLS error: {message}"),
        }
    }
}

impl Error for ExporterError {}

impl From<io::Error> for ExporterError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExportReport {
    pub records: usize,
    pub bytes: usize,
}

pub trait Exporter: Send {
    fn export<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
    ) -> Pin<Box<dyn Future<Output = Result<ExportReport, ExporterError>> + Send + 'a>>;
}

pub type ExporterMap = BTreeMap<String, Box<dyn Exporter>>;

pub fn build_exporters(configs: &[ExporterConfig]) -> Result<ExporterMap, ExporterError> {
    let mut exporters: ExporterMap = BTreeMap::new();

    for config in configs {
        let exporter: Box<dyn Exporter> = match config.protocol {
            ExporterProtocol::Stdout => Box::new(StdoutExporter::new()),
            ExporterProtocol::File => {
                Box::new(FileExporter::open(endpoint_to_path(&config.endpoint)?)?)
            }
            ExporterProtocol::OtlpGrpc => Box::new(OtlpGrpcExporter::new(&config.endpoint)?),
            ExporterProtocol::OtlpHttp => {
                Box::new(OtlpHttpExporter::new(&config.endpoint, config.tls.clone())?)
            }
            ExporterProtocol::OtlpHttpJson => Box::new(OtlpHttpExporter::new_json(
                &config.endpoint,
                config.tls.clone(),
            )?),
            ExporterProtocol::PrometheusRemoteWrite => Box::new(
                PrometheusRemoteWriteExporter::new(&config.endpoint, config.tls.clone())?,
            ),
            ExporterProtocol::Kafka => Box::new(KafkaExporter::new(config.endpoint.clone())),
            ExporterProtocol::S3 => Box::new(S3Exporter::new(config.endpoint.clone())),
            ExporterProtocol::ClickHouse => {
                Box::new(ClickHouseExporter::new(config.endpoint.clone()))
            }
        };

        exporters.insert(config.name.clone(), exporter);
    }

    Ok(exporters)
}

pub struct KafkaExporter {
    endpoint: String,
}

impl KafkaExporter {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }
}

impl Exporter for KafkaExporter {
    fn export<'a>(
        &'a mut self,
        _batch: &'a RecordBatch,
    ) -> Pin<Box<dyn Future<Output = Result<ExportReport, ExporterError>> + Send + 'a>> {
        Box::pin(async move {
            Err(ExporterError::UnsupportedExporter(format!(
                "kafka exporter is a skeleton; endpoint={}",
                self.endpoint
            )))
        })
    }
}

pub struct S3Exporter {
    endpoint: String,
}

impl S3Exporter {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }
}

impl Exporter for S3Exporter {
    fn export<'a>(
        &'a mut self,
        _batch: &'a RecordBatch,
    ) -> Pin<Box<dyn Future<Output = Result<ExportReport, ExporterError>> + Send + 'a>> {
        Box::pin(async move {
            Err(ExporterError::UnsupportedExporter(format!(
                "s3 exporter is a skeleton; endpoint={}",
                self.endpoint
            )))
        })
    }
}

pub struct ClickHouseExporter {
    endpoint: String,
}

impl ClickHouseExporter {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }
}

impl Exporter for ClickHouseExporter {
    fn export<'a>(
        &'a mut self,
        _batch: &'a RecordBatch,
    ) -> Pin<Box<dyn Future<Output = Result<ExportReport, ExporterError>> + Send + 'a>> {
        Box::pin(async move {
            Err(ExporterError::UnsupportedExporter(format!(
                "clickhouse exporter is a skeleton; endpoint={}",
                self.endpoint
            )))
        })
    }
}

pub struct StdoutExporter {
    writer: Box<dyn Write + Send>,
}

impl StdoutExporter {
    pub fn new() -> Self {
        Self {
            writer: Box::new(io::stdout()),
        }
    }

    pub fn with_writer(writer: impl Write + Send + 'static) -> Self {
        Self {
            writer: Box::new(writer),
        }
    }
}

impl Default for StdoutExporter {
    fn default() -> Self {
        Self::new()
    }
}

impl Exporter for StdoutExporter {
    fn export<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
    ) -> Pin<Box<dyn Future<Output = Result<ExportReport, ExporterError>> + Send + 'a>> {
        Box::pin(async move {
            let mut bytes = 0;

            for record in &batch.records {
                let line = format_record(record);
                bytes += line.len();
                self.writer.write_all(line.as_bytes())?;
                self.writer.write_all(b"\n")?;
            }

            self.writer.flush()?;
            Ok(ExportReport {
                records: batch.records.len(),
                bytes,
            })
        })
    }
}

pub struct FileExporter {
    writer: Box<dyn Write + Send>,
}

impl FileExporter {
    pub fn open(path: PathBuf) -> Result<Self, ExporterError> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            writer: Box::new(file),
        })
    }

    pub fn with_writer(writer: impl Write + Send + 'static) -> Self {
        Self {
            writer: Box::new(writer),
        }
    }
}

impl Exporter for FileExporter {
    fn export<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
    ) -> Pin<Box<dyn Future<Output = Result<ExportReport, ExporterError>> + Send + 'a>> {
        Box::pin(async move {
            let mut bytes = 0;

            for record in &batch.records {
                let line = format_record(record);
                bytes += line.len();
                self.writer.write_all(line.as_bytes())?;
                self.writer.write_all(b"\n")?;
            }

            self.writer.flush()?;
            Ok(ExportReport {
                records: batch.records.len(),
                bytes,
            })
        })
    }
}

pub struct PrometheusRemoteWriteExporter {
    endpoint: HttpEndpoint,
    tls: TlsConfig,
}

impl PrometheusRemoteWriteExporter {
    pub fn new(endpoint: &str, tls: TlsConfig) -> Result<Self, ExporterError> {
        Ok(Self {
            endpoint: parse_http_endpoint_with_default_port(endpoint, 9090)?,
            tls,
        })
    }
}

impl Exporter for PrometheusRemoteWriteExporter {
    fn export<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
    ) -> Pin<Box<dyn Future<Output = Result<ExportReport, ExporterError>> + Send + 'a>> {
        Box::pin(async move {
            let request = batch_to_prometheus_remote_write_request(batch);
            if request.timeseries.is_empty() {
                return Ok(ExportReport::default());
            }

            let records = request.timeseries.len();
            let encoded = request.encode_to_vec();
            let compressed = snappy_compress_literal(&encoded);
            let path = self.endpoint.path_or("/api/v1/write");
            self.endpoint
                .post_body(
                    &path,
                    "application/x-protobuf",
                    compressed,
                    &self.tls,
                    &[
                        ("Content-Encoding", "snappy"),
                        ("X-Prometheus-Remote-Write-Version", "0.1.0"),
                        ("User-Agent", "telemetry-fabric/0.1.0"),
                    ],
                )
                .await?;

            Ok(ExportReport { records, bytes: 0 })
        })
    }
}

pub struct OtlpHttpExporter {
    endpoint: HttpEndpoint,
    encoding: OtlpHttpEncoding,
    tls: TlsConfig,
}

impl OtlpHttpExporter {
    pub fn new(endpoint: &str, tls: TlsConfig) -> Result<Self, ExporterError> {
        Ok(Self {
            endpoint: parse_http_endpoint(endpoint)?,
            encoding: OtlpHttpEncoding::Protobuf,
            tls,
        })
    }

    pub fn new_json(endpoint: &str, tls: TlsConfig) -> Result<Self, ExporterError> {
        Ok(Self {
            endpoint: parse_http_endpoint(endpoint)?,
            encoding: OtlpHttpEncoding::Json,
            tls,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OtlpHttpEncoding {
    Protobuf,
    Json,
}

impl Exporter for OtlpHttpExporter {
    fn export<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
    ) -> Pin<Box<dyn Future<Output = Result<ExportReport, ExporterError>> + Send + 'a>> {
        Box::pin(async move {
            let trace_request = batch_to_trace_request(batch);
            let span_count = count_spans(&trace_request);
            let metrics_request = batch_to_metrics_request(batch);
            let data_point_count = count_metric_data_points(&metrics_request);
            let logs_request = batch_to_logs_request(batch);
            let log_record_count = count_log_records(&logs_request);

            if span_count == 0 && data_point_count == 0 && log_record_count == 0 {
                return Ok(ExportReport::default());
            }

            if span_count > 0 {
                self.post_otlp_http("/v1/traces", &trace_request).await?;
            }
            if data_point_count > 0 {
                self.post_otlp_http("/v1/metrics", &metrics_request).await?;
            }
            if log_record_count > 0 {
                self.post_otlp_http("/v1/logs", &logs_request).await?;
            }

            Ok(ExportReport {
                records: span_count + data_point_count + log_record_count,
                bytes: 0,
            })
        })
    }
}

impl OtlpHttpExporter {
    async fn post_otlp_http<T>(&self, signal_path: &str, request: &T) -> Result<(), ExporterError>
    where
        T: Message + serde::Serialize,
    {
        match self.encoding {
            OtlpHttpEncoding::Protobuf => {
                let path = self.endpoint.path(signal_path);
                self.endpoint
                    .post_body(
                        &path,
                        "application/x-protobuf",
                        request.encode_to_vec(),
                        &self.tls,
                        &[],
                    )
                    .await
            }
            OtlpHttpEncoding::Json => {
                let body = serde_json::to_vec(request)
                    .map_err(|err| ExporterError::Grpc(err.to_string()))?;
                let path = self.endpoint.path(signal_path);
                self.endpoint
                    .post_body(&path, "application/json", body, &self.tls, &[])
                    .await
            }
        }
    }
}

pub struct OtlpGrpcExporter {
    endpoint: String,
    trace_client: Option<TraceServiceClient<Channel>>,
    metrics_client: Option<MetricsServiceClient<Channel>>,
    logs_client: Option<LogsServiceClient<Channel>>,
}

impl OtlpGrpcExporter {
    pub fn new(endpoint: &str) -> Result<Self, ExporterError> {
        Ok(Self {
            endpoint: normalize_grpc_endpoint(endpoint)?,
            trace_client: None,
            metrics_client: None,
            logs_client: None,
        })
    }
}

impl Exporter for OtlpGrpcExporter {
    fn export<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
    ) -> Pin<Box<dyn Future<Output = Result<ExportReport, ExporterError>> + Send + 'a>> {
        Box::pin(async move {
            let trace_request = batch_to_trace_request(batch);
            let span_count = count_spans(&trace_request);
            let metrics_request = batch_to_metrics_request(batch);
            let data_point_count = count_metric_data_points(&metrics_request);
            let logs_request = batch_to_logs_request(batch);
            let log_record_count = count_log_records(&logs_request);

            if span_count == 0 && data_point_count == 0 && log_record_count == 0 {
                return Ok(ExportReport::default());
            }

            if span_count > 0 {
                self.export_traces(trace_request).await?;
            }
            if data_point_count > 0 {
                self.export_metrics(metrics_request).await?;
            }
            if log_record_count > 0 {
                self.export_logs(logs_request).await?;
            }

            Ok(ExportReport {
                records: span_count + data_point_count + log_record_count,
                bytes: 0,
            })
        })
    }
}

impl OtlpGrpcExporter {
    async fn export_traces(
        &mut self,
        request: ExportTraceServiceRequest,
    ) -> Result<(), ExporterError> {
        if self.trace_client.is_none() {
            let client = TraceServiceClient::connect(self.endpoint.clone())
                .await
                .map_err(|err| ExporterError::Grpc(err.to_string()))?;
            self.trace_client = Some(client);
        }

        let result = {
            let Some(client) = self.trace_client.as_mut() else {
                return Err(ExporterError::Grpc(
                    "OTLP gRPC trace client was not initialized".to_string(),
                ));
            };
            client.export(request).await
        };

        match result {
            Ok(_) => Ok(()),
            Err(status) => {
                self.trace_client = None;
                Err(ExporterError::Grpc(status.to_string()))
            }
        }
    }

    async fn export_metrics(
        &mut self,
        request: ExportMetricsServiceRequest,
    ) -> Result<(), ExporterError> {
        if self.metrics_client.is_none() {
            let client = MetricsServiceClient::connect(self.endpoint.clone())
                .await
                .map_err(|err| ExporterError::Grpc(err.to_string()))?;
            self.metrics_client = Some(client);
        }

        let result = {
            let Some(client) = self.metrics_client.as_mut() else {
                return Err(ExporterError::Grpc(
                    "OTLP gRPC metrics client was not initialized".to_string(),
                ));
            };
            client.export(request).await
        };

        match result {
            Ok(_) => Ok(()),
            Err(status) => {
                self.metrics_client = None;
                Err(ExporterError::Grpc(status.to_string()))
            }
        }
    }

    async fn export_logs(
        &mut self,
        request: ExportLogsServiceRequest,
    ) -> Result<(), ExporterError> {
        if self.logs_client.is_none() {
            let client = LogsServiceClient::connect(self.endpoint.clone())
                .await
                .map_err(|err| ExporterError::Grpc(err.to_string()))?;
            self.logs_client = Some(client);
        }

        let result = {
            let Some(client) = self.logs_client.as_mut() else {
                return Err(ExporterError::Grpc(
                    "OTLP gRPC logs client was not initialized".to_string(),
                ));
            };
            client.export(request).await
        };

        match result {
            Ok(_) => Ok(()),
            Err(status) => {
                self.logs_client = None;
                Err(ExporterError::Grpc(status.to_string()))
            }
        }
    }
}

pub fn batch_to_logs_request(batch: &RecordBatch) -> ExportLogsServiceRequest {
    let resource_logs = batch
        .records
        .iter()
        .filter(|record| record.signal == SignalKind::Log)
        .filter_map(record_to_resource_log)
        .collect::<Vec<_>>();

    if resource_logs.is_empty() {
        return ExportLogsServiceRequest {
            resource_logs: Vec::new(),
        };
    }

    ExportLogsServiceRequest { resource_logs }
}

pub fn batch_to_prometheus_remote_write_request(batch: &RecordBatch) -> PromWriteRequest {
    let timeseries = batch
        .records
        .iter()
        .filter(|record| record.signal == SignalKind::Metric)
        .flat_map(record_to_prometheus_timeseries)
        .collect();

    PromWriteRequest { timeseries }
}

pub fn batch_to_metrics_request(batch: &RecordBatch) -> ExportMetricsServiceRequest {
    let resource_metrics = batch
        .records
        .iter()
        .filter(|record| record.signal == SignalKind::Metric)
        .filter_map(record_to_resource_metric)
        .collect::<Vec<_>>();

    if resource_metrics.is_empty() {
        return ExportMetricsServiceRequest {
            resource_metrics: Vec::new(),
        };
    }

    ExportMetricsServiceRequest { resource_metrics }
}

pub fn batch_to_trace_request(batch: &RecordBatch) -> ExportTraceServiceRequest {
    let spans = batch
        .records
        .iter()
        .filter(|record| record.signal == SignalKind::Trace)
        .map(record_to_span)
        .collect::<Vec<_>>();

    if spans.is_empty() {
        return ExportTraceServiceRequest {
            resource_spans: Vec::new(),
        };
    }

    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: Vec::new(),
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn record_to_resource_log(record: &TelemetryRecord) -> Option<LogResourceLogs> {
    Some(LogResourceLogs {
        resource: Some(Resource {
            attributes: record_to_resource_attributes(record),
            dropped_attributes_count: 0,
            entity_refs: Vec::new(),
        }),
        scope_logs: vec![LogScopeLogs {
            scope: record_scope(record),
            log_records: vec![record_to_log_record(record)],
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    })
}

fn record_to_prometheus_timeseries(record: &TelemetryRecord) -> Vec<PromTimeSeries> {
    let Some(metric_name) = metric_name(record).map(|name| sanitize_prometheus_metric_name(&name))
    else {
        return Vec::new();
    };

    match find_attribute(record, "otel.metric.data_type").as_deref() {
        Some("histogram") => record_to_prometheus_histogram_timeseries(record, &metric_name),
        Some("exponential_histogram") => {
            record_to_prometheus_exponential_histogram_timeseries(record, &metric_name)
        }
        Some("summary") => record_to_prometheus_summary_timeseries(record, &metric_name),
        _ => record_to_prometheus_scalar_timeseries(record, &metric_name)
            .into_iter()
            .collect(),
    }
}

fn record_to_prometheus_scalar_timeseries(
    record: &TelemetryRecord,
    metric_name: &str,
) -> Option<PromTimeSeries> {
    let value = metric_value_to_f64(metric_value(record)?)?;
    Some(prometheus_timeseries(
        record,
        metric_name,
        &[],
        value,
        metric_timestamp_millis(record),
        prometheus_exemplars(record),
    ))
}

fn record_to_prometheus_histogram_timeseries(
    record: &TelemetryRecord,
    metric_name: &str,
) -> Vec<PromTimeSeries> {
    let timestamp = metric_timestamp_millis(record);
    let mut timeseries = Vec::new();

    if let Some(count) = parse_u64_attribute(record, "otel.metric.count") {
        timeseries.push(prometheus_timeseries(
            record,
            &format!("{metric_name}_count"),
            &[],
            count as f64,
            timestamp,
            Vec::new(),
        ));
    }
    if let Some(sum) = parse_f64_attribute(record, "otel.metric.sum") {
        timeseries.push(prometheus_timeseries(
            record,
            &format!("{metric_name}_sum"),
            &[],
            sum,
            timestamp,
            prometheus_exemplars(record),
        ));
    }

    let bucket_counts = parse_u64_list_attribute(record, "otel.metric.bucket_counts");
    let explicit_bounds = parse_f64_list_attribute(record, "otel.metric.explicit_bounds");
    let mut cumulative_count = 0_u64;

    for (index, bucket_count) in bucket_counts.iter().enumerate() {
        cumulative_count = cumulative_count.saturating_add(*bucket_count);
        let le = explicit_bounds
            .get(index)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "+Inf".to_string());
        timeseries.push(prometheus_timeseries(
            record,
            &format!("{metric_name}_bucket"),
            &[("le", le)],
            cumulative_count as f64,
            timestamp,
            Vec::new(),
        ));
    }

    timeseries
}

fn record_to_prometheus_exponential_histogram_timeseries(
    record: &TelemetryRecord,
    metric_name: &str,
) -> Vec<PromTimeSeries> {
    let timestamp = metric_timestamp_millis(record);
    let mut timeseries = Vec::new();

    if let Some(count) = parse_u64_attribute(record, "otel.metric.count") {
        timeseries.push(prometheus_timeseries(
            record,
            &format!("{metric_name}_count"),
            &[],
            count as f64,
            timestamp,
            Vec::new(),
        ));
    }
    if let Some(sum) = parse_f64_attribute(record, "otel.metric.sum") {
        timeseries.push(prometheus_timeseries(
            record,
            &format!("{metric_name}_sum"),
            &[],
            sum,
            timestamp,
            prometheus_exemplars(record),
        ));
    }

    let buckets = exponential_histogram_prometheus_buckets(record);
    let mut cumulative_count = 0_u64;
    for (le, bucket_count) in buckets {
        cumulative_count = cumulative_count.saturating_add(bucket_count);
        timeseries.push(prometheus_timeseries(
            record,
            &format!("{metric_name}_bucket"),
            &[("le", le)],
            cumulative_count as f64,
            timestamp,
            Vec::new(),
        ));
    }

    if let Some(count) = parse_u64_attribute(record, "otel.metric.count") {
        timeseries.push(prometheus_timeseries(
            record,
            &format!("{metric_name}_bucket"),
            &[("le", "+Inf".to_string())],
            count.max(cumulative_count) as f64,
            timestamp,
            Vec::new(),
        ));
    }

    timeseries
}

fn record_to_prometheus_summary_timeseries(
    record: &TelemetryRecord,
    metric_name: &str,
) -> Vec<PromTimeSeries> {
    let timestamp = metric_timestamp_millis(record);
    let mut timeseries = Vec::new();

    if let Some(count) = parse_u64_attribute(record, "otel.metric.count") {
        timeseries.push(prometheus_timeseries(
            record,
            &format!("{metric_name}_count"),
            &[],
            count as f64,
            timestamp,
            Vec::new(),
        ));
    }
    if let Some(sum) = parse_f64_attribute(record, "otel.metric.sum") {
        timeseries.push(prometheus_timeseries(
            record,
            &format!("{metric_name}_sum"),
            &[],
            sum,
            timestamp,
            Vec::new(),
        ));
    }

    for (quantile, value) in parse_quantile_values_attribute(record, "otel.metric.quantile_values")
    {
        timeseries.push(prometheus_timeseries(
            record,
            metric_name,
            &[("quantile", quantile.to_string())],
            value,
            timestamp,
            Vec::new(),
        ));
    }

    timeseries
}

fn prometheus_timeseries(
    record: &TelemetryRecord,
    metric_name: &str,
    extra_labels: &[(&str, String)],
    value: f64,
    timestamp: i64,
    exemplars: Vec<PromExemplar>,
) -> PromTimeSeries {
    let labels = if extra_labels.is_empty() {
        prometheus_labels(record, metric_name)
    } else {
        prometheus_labels_with_extra(record, metric_name, extra_labels)
    };

    PromTimeSeries {
        labels,
        samples: vec![PromSample { value, timestamp }],
        exemplars,
    }
}

fn prometheus_exemplars(record: &TelemetryRecord) -> Vec<PromExemplar> {
    record_to_exemplars(record)
        .into_iter()
        .filter_map(|exemplar| {
            let value = match exemplar.value? {
                exemplar::Value::AsDouble(value) if value.is_finite() => value,
                exemplar::Value::AsDouble(_) => return None,
                exemplar::Value::AsInt(value) => value as f64,
            };
            let timestamp =
                (u128::from(exemplar.time_unix_nano) / 1_000_000).min(i64::MAX as u128) as i64;
            let mut labels = Vec::new();
            if !exemplar.trace_id.is_empty() {
                labels.push(PromLabel {
                    name: "trace_id".to_string(),
                    value: hex_encode(&exemplar.trace_id),
                });
            }
            if !exemplar.span_id.is_empty() {
                labels.push(PromLabel {
                    name: "span_id".to_string(),
                    value: hex_encode(&exemplar.span_id),
                });
            }
            labels.extend(
                exemplar
                    .filtered_attributes
                    .into_iter()
                    .filter_map(|attribute| {
                        attribute.value.map(|value| PromLabel {
                            name: sanitize_prometheus_label_name(&attribute.key),
                            value: any_value_to_string(&value),
                        })
                    }),
            );

            Some(PromExemplar {
                labels,
                value,
                timestamp,
            })
        })
        .collect()
}

fn record_to_log_record(record: &TelemetryRecord) -> LogRecord {
    LogRecord {
        time_unix_nano: find_attribute(record, "otel.log.time_unix_nano")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(record.timestamp_unix_nanos as u64),
        observed_time_unix_nano: find_attribute(record, "otel.log.observed_time_unix_nano")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(record.timestamp_unix_nanos as u64),
        severity_number: find_attribute(record, "otel.log.severity_number")
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(SeverityNumber::Unspecified as i32),
        severity_text: find_attribute(record, "otel.log.severity_text").unwrap_or_default(),
        body: Some(AnyValue {
            value: Some(any_value::Value::StringValue(
                String::from_utf8_lossy(&record.body).to_string(),
            )),
        }),
        attributes: record_to_log_attributes(record),
        dropped_attributes_count: 0,
        flags: find_attribute(record, "otel.log.flags")
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or_default(),
        trace_id: find_attribute(record, "otel.trace_id")
            .and_then(|value| hex_decode_fixed(&value, 16))
            .unwrap_or_default(),
        span_id: find_attribute(record, "otel.span_id")
            .and_then(|value| hex_decode_fixed(&value, 8))
            .unwrap_or_default(),
        event_name: find_attribute(record, "otel.log.event_name").unwrap_or_default(),
    }
}

fn record_to_resource_metric(record: &TelemetryRecord) -> Option<MetricResourceMetrics> {
    Some(MetricResourceMetrics {
        resource: Some(Resource {
            attributes: record_to_resource_attributes(record),
            dropped_attributes_count: 0,
            entity_refs: Vec::new(),
        }),
        scope_metrics: vec![MetricScopeMetrics {
            scope: record_scope(record),
            metrics: vec![record_to_metric(record)?],
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    })
}

fn record_to_metric(record: &TelemetryRecord) -> Option<Metric> {
    let name = metric_name(record)?;
    let data = match find_attribute(record, "otel.metric.data_type").as_deref() {
        Some("sum") => metric::Data::Sum(Sum {
            data_points: vec![record_to_number_data_point(record, metric_value(record)?)],
            aggregation_temporality: metric_aggregation_temporality(record),
            is_monotonic: find_attribute(record, "otel.metric.is_monotonic")
                .and_then(|value| value.parse::<bool>().ok())
                .unwrap_or(false),
        }),
        Some("histogram") => metric::Data::Histogram(Histogram {
            data_points: vec![record_to_histogram_data_point(record)?],
            aggregation_temporality: metric_aggregation_temporality(record),
        }),
        Some("exponential_histogram") => metric::Data::ExponentialHistogram(ExponentialHistogram {
            data_points: vec![record_to_exponential_histogram_data_point(record)?],
            aggregation_temporality: metric_aggregation_temporality(record),
        }),
        Some("summary") => metric::Data::Summary(Summary {
            data_points: vec![record_to_summary_data_point(record)?],
        }),
        _ => metric::Data::Gauge(Gauge {
            data_points: vec![record_to_number_data_point(record, metric_value(record)?)],
        }),
    };

    Some(Metric {
        name,
        description: find_attribute(record, "otel.metric.description").unwrap_or_default(),
        unit: find_attribute(record, "otel.metric.unit").unwrap_or_default(),
        metadata: Vec::new(),
        data: Some(data),
    })
}

fn record_to_number_data_point(
    record: &TelemetryRecord,
    value: number_data_point::Value,
) -> NumberDataPoint {
    NumberDataPoint {
        attributes: record_to_metric_attributes(record),
        start_time_unix_nano: metric_start_time_unix_nano(record),
        time_unix_nano: metric_time_unix_nano(record),
        value: Some(value),
        exemplars: record_to_exemplars(record),
        flags: metric_flags(record),
    }
}

fn record_to_histogram_data_point(record: &TelemetryRecord) -> Option<HistogramDataPoint> {
    Some(HistogramDataPoint {
        attributes: record_to_metric_attributes(record),
        start_time_unix_nano: metric_start_time_unix_nano(record),
        time_unix_nano: metric_time_unix_nano(record),
        count: parse_u64_attribute(record, "otel.metric.count")?,
        sum: parse_f64_attribute(record, "otel.metric.sum"),
        bucket_counts: parse_u64_list_attribute(record, "otel.metric.bucket_counts"),
        explicit_bounds: parse_f64_list_attribute(record, "otel.metric.explicit_bounds"),
        exemplars: record_to_exemplars(record),
        flags: metric_flags(record),
        min: parse_f64_attribute(record, "otel.metric.min"),
        max: parse_f64_attribute(record, "otel.metric.max"),
    })
}

fn record_to_exponential_histogram_data_point(
    record: &TelemetryRecord,
) -> Option<ExponentialHistogramDataPoint> {
    Some(ExponentialHistogramDataPoint {
        attributes: record_to_metric_attributes(record),
        start_time_unix_nano: metric_start_time_unix_nano(record),
        time_unix_nano: metric_time_unix_nano(record),
        count: parse_u64_attribute(record, "otel.metric.count")?,
        sum: parse_f64_attribute(record, "otel.metric.sum"),
        scale: parse_i32_attribute(record, "otel.metric.scale").unwrap_or_default(),
        zero_count: parse_u64_attribute(record, "otel.metric.zero_count").unwrap_or_default(),
        positive: record_to_exponential_buckets(
            record,
            "otel.metric.positive_offset",
            "otel.metric.positive_bucket_counts",
        ),
        negative: record_to_exponential_buckets(
            record,
            "otel.metric.negative_offset",
            "otel.metric.negative_bucket_counts",
        ),
        flags: metric_flags(record),
        exemplars: record_to_exemplars(record),
        min: parse_f64_attribute(record, "otel.metric.min"),
        max: parse_f64_attribute(record, "otel.metric.max"),
        zero_threshold: parse_f64_attribute(record, "otel.metric.zero_threshold")
            .unwrap_or_default(),
    })
}

fn record_to_summary_data_point(record: &TelemetryRecord) -> Option<SummaryDataPoint> {
    Some(SummaryDataPoint {
        attributes: record_to_metric_attributes(record),
        start_time_unix_nano: metric_start_time_unix_nano(record),
        time_unix_nano: metric_time_unix_nano(record),
        count: parse_u64_attribute(record, "otel.metric.count")?,
        sum: parse_f64_attribute(record, "otel.metric.sum")?,
        quantile_values: parse_quantile_values_attribute(record, "otel.metric.quantile_values")
            .into_iter()
            .map(|(quantile, value)| summary_data_point::ValueAtQuantile { quantile, value })
            .collect(),
        flags: metric_flags(record),
    })
}

fn metric_name(record: &TelemetryRecord) -> Option<String> {
    find_attribute(record, "otel.metric.name")
        .or_else(|| find_attribute(record, "metric.name"))
        .or_else(|| find_attribute(record, "service.name"))
        .or_else(|| {
            let rendered = String::from_utf8_lossy(&record.body).to_string();
            (!rendered.trim().is_empty()).then_some(rendered)
        })
}

fn metric_value(record: &TelemetryRecord) -> Option<number_data_point::Value> {
    let value = find_attribute(record, "otel.metric.value")
        .unwrap_or_else(|| String::from_utf8_lossy(&record.body).to_string());

    if find_attribute(record, "otel.metric.value_type").as_deref() == Some("int") {
        return value
            .parse::<i64>()
            .ok()
            .map(number_data_point::Value::AsInt);
    }

    value
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .map(number_data_point::Value::AsDouble)
}

fn metric_value_to_f64(value: number_data_point::Value) -> Option<f64> {
    match value {
        number_data_point::Value::AsDouble(value) if value.is_finite() => Some(value),
        number_data_point::Value::AsDouble(_) => None,
        number_data_point::Value::AsInt(value) => Some(value as f64),
    }
}

fn metric_start_time_unix_nano(record: &TelemetryRecord) -> u64 {
    find_attribute(record, "otel.metric.start_time_unix_nano")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_default()
}

fn metric_time_unix_nano(record: &TelemetryRecord) -> u64 {
    find_attribute(record, "otel.metric.time_unix_nano")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(record.timestamp_unix_nanos as u64)
}

fn metric_flags(record: &TelemetryRecord) -> u32 {
    find_attribute(record, "otel.metric.flags")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or_default()
}

fn metric_aggregation_temporality(record: &TelemetryRecord) -> i32 {
    find_attribute(record, "otel.metric.aggregation_temporality")
        .and_then(|value| value.parse::<i32>().ok())
        .or_else(|| {
            find_attribute(record, "otel.metric.aggregation_temporality_name")
                .and_then(|value| aggregation_temporality_from_name(&value))
        })
        .unwrap_or(AggregationTemporality::Cumulative as i32)
}

fn aggregation_temporality_from_name(value: &str) -> Option<i32> {
    match value {
        "AGGREGATION_TEMPORALITY_UNSPECIFIED" | "unspecified" => {
            Some(AggregationTemporality::Unspecified as i32)
        }
        "AGGREGATION_TEMPORALITY_DELTA" | "delta" => Some(AggregationTemporality::Delta as i32),
        "AGGREGATION_TEMPORALITY_CUMULATIVE" | "cumulative" => {
            Some(AggregationTemporality::Cumulative as i32)
        }
        _ => None,
    }
}

fn record_to_exponential_buckets(
    record: &TelemetryRecord,
    offset_key: &str,
    bucket_counts_key: &str,
) -> Option<exponential_histogram_data_point::Buckets> {
    let bucket_counts = parse_u64_list_attribute(record, bucket_counts_key);
    if bucket_counts.is_empty() {
        return None;
    }

    Some(exponential_histogram_data_point::Buckets {
        offset: parse_i32_attribute(record, offset_key).unwrap_or_default(),
        bucket_counts,
    })
}

fn record_to_exemplars(record: &TelemetryRecord) -> Vec<Exemplar> {
    let count = parse_u64_attribute(record, "otel.metric.exemplar.count").unwrap_or_default();
    (0..count)
        .filter_map(|index| record_to_exemplar(record, index))
        .collect()
}

fn record_to_exemplar(record: &TelemetryRecord, index: u64) -> Option<Exemplar> {
    let prefix = format!("otel.metric.exemplar.{index}");
    Some(Exemplar {
        filtered_attributes: record_to_exemplar_filtered_attributes(record, &prefix),
        time_unix_nano: find_attribute(record, &format!("{prefix}.time_unix_nano"))
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_default(),
        span_id: find_attribute(record, &format!("{prefix}.span_id"))
            .and_then(|value| hex_decode_fixed(&value, 8))
            .unwrap_or_default(),
        trace_id: find_attribute(record, &format!("{prefix}.trace_id"))
            .and_then(|value| hex_decode_fixed(&value, 16))
            .unwrap_or_default(),
        value: record_to_exemplar_value(record, &prefix),
    })
}

fn record_to_exemplar_value(record: &TelemetryRecord, prefix: &str) -> Option<exemplar::Value> {
    let value = find_attribute(record, &format!("{prefix}.value"))?;
    if find_attribute(record, &format!("{prefix}.value_type")).as_deref() == Some("int") {
        return value.parse::<i64>().ok().map(exemplar::Value::AsInt);
    }

    value
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .map(exemplar::Value::AsDouble)
}

fn record_to_exemplar_filtered_attributes(record: &TelemetryRecord, prefix: &str) -> Vec<KeyValue> {
    let filtered_prefix = format!("{prefix}.filtered.");
    record
        .attributes
        .iter()
        .filter_map(|attribute| {
            attribute
                .key
                .strip_prefix(&filtered_prefix)
                .map(|key| KeyValue {
                    key: key.to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(attribute.value.clone())),
                    }),
                    key_strindex: 0,
                })
        })
        .collect()
}

fn parse_u64_attribute(record: &TelemetryRecord, key: &str) -> Option<u64> {
    find_attribute(record, key).and_then(|value| value.parse::<u64>().ok())
}

fn parse_f64_attribute(record: &TelemetryRecord, key: &str) -> Option<f64> {
    find_attribute(record, key)
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite())
}

fn parse_i32_attribute(record: &TelemetryRecord, key: &str) -> Option<i32> {
    find_attribute(record, key).and_then(|value| value.parse::<i32>().ok())
}

fn parse_u64_list_attribute(record: &TelemetryRecord, key: &str) -> Vec<u64> {
    find_attribute(record, key)
        .map(|value| {
            value
                .split(',')
                .filter_map(|item| item.trim().parse::<u64>().ok())
                .collect()
        })
        .unwrap_or_default()
}

fn parse_f64_list_attribute(record: &TelemetryRecord, key: &str) -> Vec<f64> {
    find_attribute(record, key)
        .map(|value| {
            value
                .split(',')
                .filter_map(|item| {
                    item.trim()
                        .parse::<f64>()
                        .ok()
                        .filter(|value| value.is_finite())
                })
                .collect()
        })
        .unwrap_or_default()
}

fn exponential_histogram_prometheus_buckets(record: &TelemetryRecord) -> Vec<(String, u64)> {
    let scale = parse_i32_attribute(record, "otel.metric.scale").unwrap_or_default();
    let base = 2_f64.powf(2_f64.powi(-scale));
    let mut buckets = Vec::new();

    let negative_offset =
        parse_i32_attribute(record, "otel.metric.negative_offset").unwrap_or_default();
    for (index, count) in parse_u64_list_attribute(record, "otel.metric.negative_bucket_counts")
        .into_iter()
        .enumerate()
    {
        let bucket_index = negative_offset.saturating_add(index as i32);
        let upper_bound = -base.powi(bucket_index);
        if upper_bound.is_finite() {
            buckets.push((upper_bound, count));
        }
    }

    if let Some(zero_count) = parse_u64_attribute(record, "otel.metric.zero_count")
        && zero_count > 0
    {
        let zero_threshold =
            parse_f64_attribute(record, "otel.metric.zero_threshold").unwrap_or_default();
        buckets.push((zero_threshold, zero_count));
    }

    let positive_offset =
        parse_i32_attribute(record, "otel.metric.positive_offset").unwrap_or_default();
    for (index, count) in parse_u64_list_attribute(record, "otel.metric.positive_bucket_counts")
        .into_iter()
        .enumerate()
    {
        let bucket_index = positive_offset
            .saturating_add(index as i32)
            .saturating_add(1);
        let upper_bound = base.powi(bucket_index);
        if upper_bound.is_finite() {
            buckets.push((upper_bound, count));
        }
    }

    buckets.sort_by(|left, right| left.0.total_cmp(&right.0));
    buckets
        .into_iter()
        .map(|(upper_bound, count)| (upper_bound.to_string(), count))
        .collect()
}

fn parse_quantile_values_attribute(record: &TelemetryRecord, key: &str) -> Vec<(f64, f64)> {
    find_attribute(record, key)
        .map(|value| {
            value
                .split(',')
                .filter_map(|item| {
                    let (quantile, value) = item.trim().split_once(':')?;
                    let quantile = quantile.parse::<f64>().ok()?;
                    let value = value.parse::<f64>().ok()?;
                    (quantile.is_finite() && value.is_finite()).then_some((quantile, value))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn metric_timestamp_millis(record: &TelemetryRecord) -> i64 {
    let nanos = find_attribute(record, "otel.metric.time_unix_nano")
        .and_then(|value| value.parse::<u128>().ok())
        .unwrap_or(record.timestamp_unix_nanos);
    (nanos / 1_000_000).min(i64::MAX as u128) as i64
}

fn prometheus_labels(record: &TelemetryRecord, metric_name: &str) -> Vec<PromLabel> {
    prometheus_labels_with_extra(record, metric_name, &[])
}

fn prometheus_labels_with_extra(
    record: &TelemetryRecord,
    metric_name: &str,
    extra_labels: &[(&str, String)],
) -> Vec<PromLabel> {
    let mut labels = BTreeMap::new();
    labels.insert("__name__".to_string(), metric_name.to_string());
    labels.insert("tenant_id".to_string(), record.tenant_id.clone());

    for attribute in &record.attributes {
        if attribute.key.starts_with("otel.") {
            continue;
        }

        let label_name = sanitize_prometheus_label_name(&attribute.key);
        if label_name == "__name__" || label_name.is_empty() {
            continue;
        }
        labels.entry(label_name).or_insert(attribute.value.clone());
    }

    for (name, value) in extra_labels {
        labels.insert((*name).to_string(), value.clone());
    }

    labels
        .into_iter()
        .map(|(name, value)| PromLabel { name, value })
        .collect()
}

fn sanitize_prometheus_metric_name(value: &str) -> String {
    sanitize_prometheus_identifier(value, true)
}

fn sanitize_prometheus_label_name(value: &str) -> String {
    sanitize_prometheus_identifier(value, false)
}

fn sanitize_prometheus_identifier(value: &str, allow_colon: bool) -> String {
    let mut output = String::with_capacity(value.len().max(1));

    for (index, character) in value.chars().enumerate() {
        let valid = character == '_'
            || (allow_colon && character == ':')
            || character.is_ascii_alphanumeric();
        let mut character = if valid { character } else { '_' };
        if index == 0 && character.is_ascii_digit() {
            output.push('_');
        }
        if !allow_colon && character == ':' {
            character = '_';
        }
        output.push(character);
    }

    if output.is_empty() {
        output.push('_');
    }
    output
}

fn record_to_metric_attributes(record: &TelemetryRecord) -> Vec<KeyValue> {
    record
        .attributes
        .iter()
        .filter(|attribute| {
            !attribute.key.starts_with("otel.") && !attribute.key.starts_with("resource.")
        })
        .map(|attribute| KeyValue {
            key: attribute.key.clone(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(attribute.value.clone())),
            }),
            key_strindex: 0,
        })
        .collect()
}

fn record_to_log_attributes(record: &TelemetryRecord) -> Vec<KeyValue> {
    record
        .attributes
        .iter()
        .filter(|attribute| {
            !attribute.key.starts_with("otel.") && !attribute.key.starts_with("resource.")
        })
        .map(|attribute| KeyValue {
            key: attribute.key.clone(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(attribute.value.clone())),
            }),
            key_strindex: 0,
        })
        .collect()
}

fn record_to_resource_attributes(record: &TelemetryRecord) -> Vec<KeyValue> {
    record
        .attributes
        .iter()
        .filter_map(|attribute| {
            attribute
                .key
                .strip_prefix("resource.")
                .filter(|key| !key.trim().is_empty())
                .map(|key| KeyValue {
                    key: key.to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(attribute.value.clone())),
                    }),
                    key_strindex: 0,
                })
        })
        .collect()
}

fn record_scope(record: &TelemetryRecord) -> Option<InstrumentationScope> {
    find_attribute(record, "otel.scope.name")
        .filter(|name| !name.trim().is_empty())
        .map(|name| InstrumentationScope {
            name,
            version: String::new(),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
        })
}

fn record_to_span(record: &TelemetryRecord) -> Span {
    let name = find_attribute(record, "otel.span.name")
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| String::from_utf8_lossy(&record.body).to_string());

    Span {
        trace_id: find_attribute(record, "otel.trace_id")
            .and_then(|value| hex_decode_fixed(&value, 16))
            .unwrap_or_else(|| fallback_id(&record.body, 16)),
        span_id: find_attribute(record, "otel.span_id")
            .and_then(|value| hex_decode_fixed(&value, 8))
            .unwrap_or_else(|| fallback_id(name.as_bytes(), 8)),
        parent_span_id: find_attribute(record, "otel.parent_span_id")
            .and_then(|value| hex_decode_fixed(&value, 8))
            .unwrap_or_default(),
        name,
        kind: find_attribute(record, "otel.span.kind")
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or_default(),
        start_time_unix_nano: find_attribute(record, "otel.start_unix_nano")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(record.timestamp_unix_nanos as u64),
        end_time_unix_nano: find_attribute(record, "otel.end_unix_nano")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(record.timestamp_unix_nanos as u64),
        attributes: record
            .attributes
            .iter()
            .filter(|attribute| !attribute.key.starts_with("otel."))
            .map(|attribute| KeyValue {
                key: attribute.key.clone(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(attribute.value.clone())),
                }),
                key_strindex: 0,
            })
            .collect(),
        ..Span::default()
    }
}

fn find_attribute(record: &TelemetryRecord, key: &str) -> Option<String> {
    record
        .attributes
        .iter()
        .find(|attribute| attribute.key == key)
        .map(|attribute| attribute.value.clone())
}

fn count_spans(request: &ExportTraceServiceRequest) -> usize {
    request
        .resource_spans
        .iter()
        .flat_map(|resource_spans| &resource_spans.scope_spans)
        .map(|scope_spans| scope_spans.spans.len())
        .sum()
}

fn count_metric_data_points(request: &ExportMetricsServiceRequest) -> usize {
    request
        .resource_metrics
        .iter()
        .flat_map(|resource_metrics| &resource_metrics.scope_metrics)
        .flat_map(|scope_metrics| &scope_metrics.metrics)
        .map(|metric| match &metric.data {
            Some(metric::Data::Gauge(gauge)) => gauge.data_points.len(),
            Some(metric::Data::Sum(sum)) => sum.data_points.len(),
            Some(metric::Data::Histogram(histogram)) => histogram.data_points.len(),
            Some(metric::Data::Summary(summary)) => summary.data_points.len(),
            Some(metric::Data::ExponentialHistogram(histogram)) => histogram.data_points.len(),
            None => 0,
        })
        .sum()
}

fn count_log_records(request: &ExportLogsServiceRequest) -> usize {
    request
        .resource_logs
        .iter()
        .flat_map(|resource_logs| &resource_logs.scope_logs)
        .map(|scope_logs| scope_logs.log_records.len())
        .sum()
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PromWriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<PromTimeSeries>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PromTimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<PromLabel>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<PromSample>,
    #[prost(message, repeated, tag = "3")]
    pub exemplars: Vec<PromExemplar>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PromLabel {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PromSample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PromExemplar {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<PromLabel>,
    #[prost(double, tag = "2")]
    pub value: f64,
    #[prost(int64, tag = "3")]
    pub timestamp: i64,
}

fn snappy_compress_literal(input: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(input.len() + 16);
    write_snappy_varint(input.len(), &mut output);

    for chunk in input.chunks(65_536) {
        write_snappy_literal_tag(chunk.len(), &mut output);
        output.extend_from_slice(chunk);
    }

    output
}

fn write_snappy_varint(mut value: usize, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn write_snappy_literal_tag(length: usize, output: &mut Vec<u8>) {
    if length == 0 {
        return;
    }

    let length_minus_one = length - 1;
    if length_minus_one < 60 {
        output.push((length_minus_one as u8) << 2);
        return;
    }

    let length_bytes = (length_minus_one as u32).to_le_bytes();
    let bytes_used = length_bytes
        .iter()
        .rposition(|byte| *byte != 0)
        .map(|index| index + 1)
        .unwrap_or(1);
    output.push(((59 + bytes_used) as u8) << 2);
    output.extend_from_slice(&length_bytes[..bytes_used]);
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpEndpoint {
    scheme: HttpScheme,
    host: String,
    port: u16,
    base_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HttpScheme {
    Http,
    Https,
}

impl HttpEndpoint {
    async fn post_body(
        &self,
        path: &str,
        content_type: &str,
        body: Vec<u8>,
        tls: &TlsConfig,
        extra_headers: &[(&str, &str)],
    ) -> Result<(), ExporterError> {
        let mut stream = self.connect(tls).await?;
        let rendered_extra_headers = extra_headers
            .iter()
            .map(|(name, value)| format!("{name}: {value}\r\n"))
            .collect::<String>();
        let headers = format!(
            "POST {path} HTTP/1.1\r\nHost: {}:{}\r\nContent-Type: {content_type}\r\n{rendered_extra_headers}Content-Length: {}\r\nConnection: close\r\n\r\n",
            self.host,
            self.port,
            body.len()
        );

        stream.write_all(headers.as_bytes()).await?;
        stream.write_all(&body).await?;
        stream.shutdown().await?;

        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        let status = parse_http_status(&response)?;
        if !(200..300).contains(&status) {
            return Err(ExporterError::Grpc(format!(
                "OTLP/HTTP export failed with HTTP status {status}"
            )));
        }

        Ok(())
    }

    async fn connect(&self, tls: &TlsConfig) -> Result<HttpStream, ExporterError> {
        let tcp = TcpStream::connect((self.host.as_str(), self.port)).await?;
        match self.scheme {
            HttpScheme::Http => Ok(HttpStream::Plain(tcp)),
            HttpScheme::Https => {
                let connector = TlsConnector::from(Arc::new(client_tls_config(tls)?));
                let server_name = tls
                    .server_name
                    .as_deref()
                    .unwrap_or(self.host.as_str())
                    .to_string();
                let server_name: ServerName<'static> = ServerName::try_from(server_name).map_err(
                    |err: rustls::pki_types::InvalidDnsNameError| {
                        ExporterError::Tls(err.to_string())
                    },
                )?;
                let stream = connector.connect(server_name, tcp).await?;
                Ok(HttpStream::Tls(Box::new(stream)))
            }
        }
    }

    fn path_or(&self, default_path: &str) -> String {
        if self.base_path.is_empty() {
            return default_path.to_string();
        }
        self.base_path.clone()
    }

    fn path(&self, signal_path: &str) -> String {
        if self.base_path.is_empty() {
            return signal_path.to_string();
        }
        if self.base_path.ends_with(signal_path) {
            return self.base_path.clone();
        }
        format!(
            "{}/{}",
            self.base_path.trim_end_matches('/'),
            signal_path.trim_start_matches('/')
        )
    }
}

enum HttpStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl HttpStream {
    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        match self {
            Self::Plain(stream) => stream.write_all(bytes).await,
            Self::Tls(stream) => stream.write_all(bytes).await,
        }
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(stream) => stream.shutdown().await,
            Self::Tls(stream) => stream.shutdown().await,
        }
    }

    async fn read_to_end(&mut self, buffer: &mut Vec<u8>) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read_to_end(buffer).await,
            Self::Tls(stream) => stream.read_to_end(buffer).await,
        }
    }
}

fn parse_http_endpoint(endpoint: &str) -> Result<HttpEndpoint, ExporterError> {
    parse_http_endpoint_with_default_port(endpoint, 4318)
}

fn client_tls_config(tls: &TlsConfig) -> Result<ClientConfig, ExporterError> {
    let mut roots = RootCertStore::empty();
    if let Some(ca_file) = &tls.ca_file {
        for cert in load_certificates(ca_file)? {
            roots
                .add(cert)
                .map_err(|err| ExporterError::Tls(err.to_string()))?;
        }
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let builder = ClientConfig::builder().with_root_certificates(roots);
    if let (Some(cert_file), Some(key_file)) = (&tls.cert_file, &tls.key_file) {
        builder
            .with_client_auth_cert(load_certificates(cert_file)?, load_private_key(key_file)?)
            .map_err(|err| ExporterError::Tls(err.to_string()))
    } else {
        Ok(builder.with_no_client_auth())
    }
}

fn load_certificates(path: &str) -> Result<Vec<CertificateDer<'static>>, ExporterError> {
    let file = fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| ExporterError::Tls(format!("failed to read certificate {path}: {err}")))
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, ExporterError> {
    let file = fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|err| ExporterError::Tls(format!("failed to read private key {path}: {err}")))?
        .ok_or_else(|| ExporterError::Tls(format!("private key not found in {path}")))
}

fn parse_http_endpoint_with_default_port(
    endpoint: &str,
    default_port: u16,
) -> Result<HttpEndpoint, ExporterError> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Err(ExporterError::InvalidEndpoint(endpoint.to_string()));
    }
    let (scheme, without_scheme) = if let Some(rest) = endpoint.strip_prefix("https://") {
        (HttpScheme::Https, rest)
    } else {
        (
            HttpScheme::Http,
            endpoint.strip_prefix("http://").unwrap_or(endpoint),
        )
    };
    let (authority, path) = match without_scheme.split_once('/') {
        Some((authority, path)) => (authority, format!("/{}", path.trim_matches('/'))),
        None => (without_scheme, String::new()),
    };
    if authority.trim().is_empty() {
        return Err(ExporterError::InvalidEndpoint(endpoint.to_string()));
    }

    let (host, port) = parse_http_authority(authority, default_port)?;
    let base_path = if path == "/" { String::new() } else { path };
    Ok(HttpEndpoint {
        scheme,
        host,
        port,
        base_path,
    })
}

fn parse_http_authority(
    authority: &str,
    default_port: u16,
) -> Result<(String, u16), ExporterError> {
    if let Some((host, port)) = authority.rsplit_once(':') {
        if host.trim().is_empty() {
            return Err(ExporterError::InvalidEndpoint(authority.to_string()));
        }
        let port = port
            .parse::<u16>()
            .map_err(|_| ExporterError::InvalidEndpoint(authority.to_string()))?;
        return Ok((host.to_string(), port));
    }

    Ok((authority.to_string(), default_port))
}

fn parse_http_status(response: &[u8]) -> Result<u16, ExporterError> {
    let response = String::from_utf8_lossy(response);
    let line = response.lines().next().ok_or_else(|| {
        ExporterError::Grpc("OTLP/HTTP response did not include a status line".to_string())
    })?;
    let mut parts = line.split_whitespace();
    let _version = parts.next().ok_or_else(|| {
        ExporterError::Grpc("OTLP/HTTP response status line was incomplete".to_string())
    })?;
    let status = parts.next().ok_or_else(|| {
        ExporterError::Grpc("OTLP/HTTP response status line was incomplete".to_string())
    })?;
    status
        .parse::<u16>()
        .map_err(|_| ExporterError::Grpc(format!("invalid OTLP/HTTP status code: {status}")))
}

fn normalize_grpc_endpoint(endpoint: &str) -> Result<String, ExporterError> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Err(ExporterError::InvalidEndpoint(endpoint.to_string()));
    }
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        return Ok(endpoint.to_string());
    }
    Ok(format!("http://{endpoint}"))
}

fn endpoint_to_path(endpoint: &str) -> Result<PathBuf, ExporterError> {
    if let Some(path) = endpoint.strip_prefix("file://") {
        if path.trim().is_empty() {
            return Err(ExporterError::InvalidEndpoint(endpoint.to_string()));
        }
        return Ok(PathBuf::from(path));
    }

    if endpoint.trim().is_empty() {
        return Err(ExporterError::InvalidEndpoint(endpoint.to_string()));
    }

    Ok(PathBuf::from(endpoint))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn hex_decode_fixed(value: &str, expected_len: usize) -> Option<Vec<u8>> {
    if value.len() != expected_len * 2 {
        return None;
    }

    let mut output = Vec::with_capacity(expected_len);
    let bytes = value.as_bytes();
    for pair in bytes.chunks_exact(2) {
        let high = hex_value(pair[0])?;
        let low = hex_value(pair[1])?;
        output.push((high << 4) | low);
    }

    if output.iter().all(|byte| *byte == 0) {
        return None;
    }

    Some(output)
}

fn any_value_to_string(value: &AnyValue) -> String {
    match &value.value {
        Some(any_value::Value::StringValue(value)) => value.clone(),
        Some(any_value::Value::BoolValue(value)) => value.to_string(),
        Some(any_value::Value::IntValue(value)) => value.to_string(),
        Some(any_value::Value::DoubleValue(value)) => value.to_string(),
        Some(any_value::Value::BytesValue(value)) => hex_encode(value),
        Some(any_value::Value::ArrayValue(value)) => value
            .values
            .iter()
            .map(any_value_to_string)
            .collect::<Vec<_>>()
            .join(","),
        Some(any_value::Value::KvlistValue(value)) => value
            .values
            .iter()
            .filter(|item| !item.key.trim().is_empty())
            .map(|item| {
                let rendered = item
                    .value
                    .as_ref()
                    .map(any_value_to_string)
                    .unwrap_or_default();
                format!("{}={rendered}", item.key)
            })
            .collect::<Vec<_>>()
            .join(","),
        Some(any_value::Value::StringValueStrindex(value)) => value.to_string(),
        None => String::new(),
    }
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn fallback_id(seed: &[u8], len: usize) -> Vec<u8> {
    let mut hash = 0x811c9dc5_u32;
    for byte in seed {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x01000193);
    }

    let mut output = Vec::with_capacity(len);
    while output.len() < len {
        hash ^= hash.rotate_left(13);
        hash = hash.wrapping_mul(0x01000193);
        output.extend_from_slice(&hash.to_le_bytes());
    }
    output.truncate(len);

    if output.iter().all(|byte| *byte == 0) {
        output[0] = 1;
    }

    output
}

fn format_record(record: &TelemetryRecord) -> String {
    format!(
        "tenant={} signal={} attrs={} bytes={}",
        record.tenant_id,
        record.signal,
        record.attributes.len(),
        record.body.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
        LogsService, LogsServiceServer,
    };
    use opentelemetry_proto::tonic::collector::logs::v1::{
        ExportLogsServiceRequest, ExportLogsServiceResponse,
    };
    use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
        MetricsService, MetricsServiceServer,
    };
    use opentelemetry_proto::tonic::collector::metrics::v1::{
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    };
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
        TraceService, TraceServiceServer,
    };
    use opentelemetry_proto::tonic::collector::trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
    };
    use std::sync::{Arc, Mutex};
    use telemetry_core::SignalKind;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;
    use tonic::{Request, Response, Status};

    #[derive(Clone, Default)]
    struct SharedWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl SharedWriter {
        fn as_string(&self) -> Result<String, Box<dyn Error>> {
            let bytes = self
                .buffer
                .lock()
                .map_err(|_| "shared writer mutex poisoned")?
                .clone();
            Ok(String::from_utf8(bytes)?)
        }
    }

    impl Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut buffer = self
                .buffer
                .lock()
                .map_err(|_| io::Error::other("shared writer mutex poisoned"))?;
            buffer.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn stdout_exporter_writes_stable_lines() -> Result<(), Box<dyn Error>> {
        let writer = SharedWriter::default();
        let output = writer.clone();
        let mut exporter = StdoutExporter::with_writer(writer);
        let record = TelemetryRecord::new("tenant-a", SignalKind::Metric, b"42".to_vec());
        let batch = RecordBatch::new(vec![record]);

        let report = exporter.export(&batch).await?;

        assert_eq!(report.records, 1);
        assert_eq!(
            output.as_string()?,
            "tenant=tenant-a signal=metric attrs=0 bytes=2\n"
        );
        Ok(())
    }

    #[test]
    fn converts_internal_trace_records_to_otlp_request() -> Result<(), Box<dyn Error>> {
        let record = TelemetryRecord::new("tenant-a", SignalKind::Trace, b"GET /checkout".to_vec())
            .with_attribute("otel.trace_id", "01010101010101010101010101010101")
            .with_attribute("otel.span_id", "0202020202020202")
            .with_attribute("otel.span.name", "GET /checkout")
            .with_attribute("http.method", "GET");
        let batch = RecordBatch::new(vec![record]);

        let request = batch_to_trace_request(&batch);
        let span = &request.resource_spans[0].scope_spans[0].spans[0];

        assert_eq!(span.name, "GET /checkout");
        assert_eq!(span.trace_id, vec![1; 16]);
        assert_eq!(span.span_id, vec![2; 8]);
        assert_eq!(span.attributes[0].key, "http.method");
        Ok(())
    }

    #[test]
    fn converts_internal_metric_records_to_otlp_request() -> Result<(), Box<dyn Error>> {
        let record = TelemetryRecord::new("tenant-a", SignalKind::Metric, b"42".to_vec())
            .with_attribute("resource.service.name", "checkout")
            .with_attribute("service.name", "checkout.latency_ms")
            .with_attribute("otel.scope.name", "checkout-scope")
            .with_attribute("otel.metric.name", "checkout.latency_ms")
            .with_attribute("otel.metric.value", "42")
            .with_attribute("otel.metric.value_type", "int")
            .with_attribute("otel.metric.data_type", "sum")
            .with_attribute("otel.metric.aggregation_temporality", "1")
            .with_attribute(
                "otel.metric.aggregation_temporality_name",
                "AGGREGATION_TEMPORALITY_DELTA",
            )
            .with_attribute("otel.metric.is_monotonic", "true")
            .with_attribute("otel.metric.exemplar.count", "1")
            .with_attribute("otel.metric.exemplar.0.time_unix_nano", "15")
            .with_attribute("otel.metric.exemplar.0.value", "7")
            .with_attribute("otel.metric.exemplar.0.value_type", "int")
            .with_attribute(
                "otel.metric.exemplar.0.trace_id",
                "01010101010101010101010101010101",
            )
            .with_attribute("otel.metric.exemplar.0.span_id", "0202020202020202")
            .with_attribute("otel.metric.exemplar.0.filtered.sampled", "true")
            .with_attribute("route", "/checkout");
        let batch = RecordBatch::new(vec![record]);

        let request = batch_to_metrics_request(&batch);
        let resource_metrics = &request.resource_metrics[0];
        let scope_metrics = &resource_metrics.scope_metrics[0];
        let metric = &scope_metrics.metrics[0];

        assert_eq!(
            resource_metrics
                .resource
                .as_ref()
                .and_then(|resource| resource.attributes.first())
                .map(|attr| (attr.key.as_str(), attr.value.is_some())),
            Some(("service.name", true))
        );
        assert_eq!(
            scope_metrics
                .scope
                .as_ref()
                .map(|scope| scope.name.as_str()),
            Some("checkout-scope")
        );
        assert_eq!(metric.name, "checkout.latency_ms");
        match metric.data.as_ref() {
            Some(metric::Data::Sum(sum)) => {
                assert_eq!(
                    sum.aggregation_temporality,
                    AggregationTemporality::Delta as i32
                );
                assert!(sum.is_monotonic);
                let point = &sum.data_points[0];
                assert!(point.time_unix_nano > 0);
                assert!(matches!(
                    point.value,
                    Some(number_data_point::Value::AsInt(42))
                ));
                assert_eq!(point.exemplars.len(), 1);
                assert_eq!(point.exemplars[0].trace_id, vec![1; 16]);
                assert!(matches!(
                    point.exemplars[0].value,
                    Some(exemplar::Value::AsInt(7))
                ));
                assert!(point.attributes.iter().any(|attr| attr.key == "route"));
                assert!(
                    !point
                        .attributes
                        .iter()
                        .any(|attr| attr.key == "resource.service.name")
                );
            }
            _ => return Err("expected OTLP sum metric".into()),
        }

        Ok(())
    }

    #[test]
    fn converts_internal_histogram_metric_records_to_otlp_request() -> Result<(), Box<dyn Error>> {
        let record = TelemetryRecord::new(
            "tenant-a",
            SignalKind::Metric,
            b"checkout.latency_ms".to_vec(),
        )
        .with_attribute("otel.metric.name", "checkout.latency_ms")
        .with_attribute("otel.metric.data_type", "histogram")
        .with_attribute("otel.metric.aggregation_temporality", "2")
        .with_attribute("otel.metric.start_time_unix_nano", "10")
        .with_attribute("otel.metric.time_unix_nano", "20")
        .with_attribute("otel.metric.count", "6")
        .with_attribute("otel.metric.sum", "125.5")
        .with_attribute("otel.metric.bucket_counts", "1,2,3")
        .with_attribute("otel.metric.explicit_bounds", "10,50")
        .with_attribute("otel.metric.min", "2")
        .with_attribute("otel.metric.max", "90")
        .with_attribute("route", "/checkout");
        let batch = RecordBatch::new(vec![record]);

        let request = batch_to_metrics_request(&batch);
        let metric = &request.resource_metrics[0].scope_metrics[0].metrics[0];

        match metric.data.as_ref() {
            Some(metric::Data::Histogram(histogram)) => {
                assert_eq!(
                    histogram.aggregation_temporality,
                    AggregationTemporality::Cumulative as i32
                );
                let point = &histogram.data_points[0];
                assert_eq!(point.start_time_unix_nano, 10);
                assert_eq!(point.time_unix_nano, 20);
                assert_eq!(point.count, 6);
                assert_eq!(point.sum, Some(125.5));
                assert_eq!(point.bucket_counts, vec![1, 2, 3]);
                assert_eq!(point.explicit_bounds, vec![10.0, 50.0]);
                assert_eq!(point.min, Some(2.0));
                assert_eq!(point.max, Some(90.0));
                assert!(point.attributes.iter().any(|attr| attr.key == "route"));
            }
            _ => return Err("expected OTLP histogram metric".into()),
        }

        Ok(())
    }

    #[test]
    fn converts_internal_summary_metric_records_to_otlp_request() -> Result<(), Box<dyn Error>> {
        let record = TelemetryRecord::new(
            "tenant-a",
            SignalKind::Metric,
            b"checkout.latency_ms".to_vec(),
        )
        .with_attribute("otel.metric.name", "checkout.latency_ms")
        .with_attribute("otel.metric.data_type", "summary")
        .with_attribute("otel.metric.start_time_unix_nano", "10")
        .with_attribute("otel.metric.time_unix_nano", "20")
        .with_attribute("otel.metric.count", "10")
        .with_attribute("otel.metric.sum", "250.5")
        .with_attribute("otel.metric.quantile_values", "0.5:20,0.95:80")
        .with_attribute("route", "/checkout");
        let batch = RecordBatch::new(vec![record]);

        let request = batch_to_metrics_request(&batch);
        let metric = &request.resource_metrics[0].scope_metrics[0].metrics[0];

        match metric.data.as_ref() {
            Some(metric::Data::Summary(summary)) => {
                let point = &summary.data_points[0];
                assert_eq!(point.start_time_unix_nano, 10);
                assert_eq!(point.time_unix_nano, 20);
                assert_eq!(point.count, 10);
                assert_eq!(point.sum, 250.5);
                assert_eq!(point.quantile_values.len(), 2);
                assert_eq!(point.quantile_values[0].quantile, 0.5);
                assert_eq!(point.quantile_values[0].value, 20.0);
                assert_eq!(point.quantile_values[1].quantile, 0.95);
                assert_eq!(point.quantile_values[1].value, 80.0);
                assert!(point.attributes.iter().any(|attr| attr.key == "route"));
            }
            _ => return Err("expected OTLP summary metric".into()),
        }

        Ok(())
    }

    #[test]
    fn converts_internal_exponential_histogram_metric_records_to_otlp_request()
    -> Result<(), Box<dyn Error>> {
        let record = TelemetryRecord::new(
            "tenant-a",
            SignalKind::Metric,
            b"checkout.latency_ms".to_vec(),
        )
        .with_attribute("otel.metric.name", "checkout.latency_ms")
        .with_attribute("otel.metric.data_type", "exponential_histogram")
        .with_attribute(
            "otel.metric.aggregation_temporality_name",
            "AGGREGATION_TEMPORALITY_DELTA",
        )
        .with_attribute("otel.metric.start_time_unix_nano", "10")
        .with_attribute("otel.metric.time_unix_nano", "20")
        .with_attribute("otel.metric.count", "8")
        .with_attribute("otel.metric.sum", "160")
        .with_attribute("otel.metric.scale", "2")
        .with_attribute("otel.metric.zero_count", "1")
        .with_attribute("otel.metric.zero_threshold", "0.01")
        .with_attribute("otel.metric.positive_offset", "-1")
        .with_attribute("otel.metric.positive_bucket_counts", "2,3")
        .with_attribute("otel.metric.negative_offset", "0")
        .with_attribute("otel.metric.negative_bucket_counts", "1")
        .with_attribute("otel.metric.min", "-2")
        .with_attribute("otel.metric.max", "16")
        .with_attribute("otel.metric.exemplar.count", "1")
        .with_attribute("otel.metric.exemplar.0.time_unix_nano", "15")
        .with_attribute("otel.metric.exemplar.0.value", "7")
        .with_attribute("otel.metric.exemplar.0.value_type", "int")
        .with_attribute(
            "otel.metric.exemplar.0.trace_id",
            "01010101010101010101010101010101",
        )
        .with_attribute("otel.metric.exemplar.0.span_id", "0202020202020202")
        .with_attribute("route", "/checkout");
        let batch = RecordBatch::new(vec![record]);

        let request = batch_to_metrics_request(&batch);
        let metric = &request.resource_metrics[0].scope_metrics[0].metrics[0];

        match metric.data.as_ref() {
            Some(metric::Data::ExponentialHistogram(histogram)) => {
                assert_eq!(
                    histogram.aggregation_temporality,
                    AggregationTemporality::Delta as i32
                );
                let point = &histogram.data_points[0];
                assert_eq!(point.count, 8);
                assert_eq!(point.sum, Some(160.0));
                assert_eq!(point.scale, 2);
                assert_eq!(point.zero_count, 1);
                assert_eq!(
                    point
                        .positive
                        .as_ref()
                        .map(|buckets| (buckets.offset, buckets.bucket_counts.as_slice())),
                    Some((-1, [2, 3].as_slice()))
                );
                assert_eq!(
                    point
                        .negative
                        .as_ref()
                        .map(|buckets| (buckets.offset, buckets.bucket_counts.as_slice())),
                    Some((0, [1].as_slice()))
                );
                assert_eq!(point.exemplars.len(), 1);
            }
            _ => return Err("expected OTLP exponential histogram metric".into()),
        }

        Ok(())
    }

    #[test]
    fn converts_internal_log_records_to_otlp_request() -> Result<(), Box<dyn Error>> {
        let record = TelemetryRecord::new("tenant-a", SignalKind::Log, b"worker-ready".to_vec())
            .with_attribute("resource.service.name", "checkout")
            .with_attribute("otel.scope.name", "checkout-scope")
            .with_attribute("otel.log.time_unix_nano", "10")
            .with_attribute("otel.log.observed_time_unix_nano", "20")
            .with_attribute("otel.log.severity_number", "9")
            .with_attribute("otel.log.severity_text", "INFO")
            .with_attribute("otel.log.event_name", "checkout.worker")
            .with_attribute("otel.trace_id", "01010101010101010101010101010101")
            .with_attribute("otel.span_id", "0202020202020202")
            .with_attribute("thread.name", "worker-1");
        let batch = RecordBatch::new(vec![record]);

        let request = batch_to_logs_request(&batch);
        let resource_logs = &request.resource_logs[0];
        let scope_logs = &resource_logs.scope_logs[0];
        let log_record = &scope_logs.log_records[0];

        assert_eq!(
            resource_logs
                .resource
                .as_ref()
                .and_then(|resource| resource.attributes.first())
                .map(|attr| (attr.key.as_str(), attr.value.is_some())),
            Some(("service.name", true))
        );
        assert_eq!(
            scope_logs.scope.as_ref().map(|scope| scope.name.as_str()),
            Some("checkout-scope")
        );
        assert_eq!(log_record.time_unix_nano, 10);
        assert_eq!(log_record.observed_time_unix_nano, 20);
        assert_eq!(log_record.severity_number, SeverityNumber::Info as i32);
        assert_eq!(log_record.severity_text, "INFO");
        assert_eq!(log_record.event_name, "checkout.worker");
        assert_eq!(log_record.trace_id, vec![1; 16]);
        assert_eq!(log_record.span_id, vec![2; 8]);
        assert!(
            log_record
                .attributes
                .iter()
                .any(|attr| attr.key == "thread.name")
        );
        assert!(
            !log_record
                .attributes
                .iter()
                .any(|attr| attr.key == "resource.service.name")
        );
        Ok(())
    }

    #[test]
    fn converts_internal_metric_records_to_prometheus_remote_write_request()
    -> Result<(), Box<dyn Error>> {
        let record = TelemetryRecord::new("tenant-a", SignalKind::Metric, b"42.5".to_vec())
            .with_attribute("resource.service.name", "checkout")
            .with_attribute("otel.metric.name", "checkout.latency.ms")
            .with_attribute("otel.metric.value", "42.5")
            .with_attribute("otel.metric.time_unix_nano", "2000000")
            .with_attribute("otel.metric.exemplar.count", "1")
            .with_attribute("otel.metric.exemplar.0.time_unix_nano", "1500000")
            .with_attribute("otel.metric.exemplar.0.value", "41")
            .with_attribute("otel.metric.exemplar.0.value_type", "int")
            .with_attribute(
                "otel.metric.exemplar.0.trace_id",
                "01010101010101010101010101010101",
            )
            .with_attribute("route", "/checkout");
        let batch = RecordBatch::new(vec![record]);

        let request = batch_to_prometheus_remote_write_request(&batch);
        let timeseries = &request.timeseries[0];

        assert_eq!(request.timeseries.len(), 1);
        assert_eq!(timeseries.samples[0].value, 42.5);
        assert_eq!(timeseries.samples[0].timestamp, 2);
        assert_eq!(
            find_prom_label(timeseries, "__name__"),
            Some("checkout_latency_ms")
        );
        assert_eq!(find_prom_label(timeseries, "tenant_id"), Some("tenant-a"));
        assert_eq!(
            find_prom_label(timeseries, "resource_service_name"),
            Some("checkout")
        );
        assert_eq!(find_prom_label(timeseries, "route"), Some("/checkout"));
        assert_eq!(timeseries.exemplars.len(), 1);
        assert_eq!(timeseries.exemplars[0].value, 41.0);
        assert_eq!(
            find_prom_label_value(&timeseries.exemplars[0].labels, "trace_id"),
            Some("01010101010101010101010101010101")
        );
        Ok(())
    }

    #[test]
    fn converts_histogram_metric_records_to_prometheus_remote_write_request()
    -> Result<(), Box<dyn Error>> {
        let record = TelemetryRecord::new(
            "tenant-a",
            SignalKind::Metric,
            b"checkout.latency_ms".to_vec(),
        )
        .with_attribute("otel.metric.name", "checkout.latency_ms")
        .with_attribute("otel.metric.data_type", "histogram")
        .with_attribute("otel.metric.count", "6")
        .with_attribute("otel.metric.sum", "125.5")
        .with_attribute("otel.metric.bucket_counts", "1,2,3")
        .with_attribute("otel.metric.explicit_bounds", "10,50")
        .with_attribute("otel.metric.time_unix_nano", "3000000")
        .with_attribute("route", "/checkout");
        let batch = RecordBatch::new(vec![record]);

        let request = batch_to_prometheus_remote_write_request(&batch);

        assert_eq!(request.timeseries.len(), 5);
        assert_eq!(
            find_prom_timeseries(&request, "checkout_latency_ms_count", None)
                .ok_or("missing histogram count")?
                .samples[0]
                .value,
            6.0
        );
        assert_eq!(
            find_prom_timeseries(&request, "checkout_latency_ms_sum", None)
                .ok_or("missing histogram sum")?
                .samples[0]
                .value,
            125.5
        );
        assert_eq!(
            find_prom_timeseries(&request, "checkout_latency_ms_bucket", Some(("le", "10")))
                .ok_or("missing first histogram bucket")?
                .samples[0]
                .value,
            1.0
        );
        assert_eq!(
            find_prom_timeseries(&request, "checkout_latency_ms_bucket", Some(("le", "50")))
                .ok_or("missing second histogram bucket")?
                .samples[0]
                .value,
            3.0
        );
        assert_eq!(
            find_prom_timeseries(&request, "checkout_latency_ms_bucket", Some(("le", "+Inf")))
                .ok_or("missing final histogram bucket")?
                .samples[0]
                .value,
            6.0
        );
        Ok(())
    }

    #[test]
    fn converts_exponential_histogram_metric_records_to_prometheus_remote_write_request()
    -> Result<(), Box<dyn Error>> {
        let record = TelemetryRecord::new(
            "tenant-a",
            SignalKind::Metric,
            b"checkout.latency_ms".to_vec(),
        )
        .with_attribute("otel.metric.name", "checkout.latency_ms")
        .with_attribute("otel.metric.data_type", "exponential_histogram")
        .with_attribute("otel.metric.count", "4")
        .with_attribute("otel.metric.sum", "20")
        .with_attribute("otel.metric.scale", "0")
        .with_attribute("otel.metric.zero_count", "1")
        .with_attribute("otel.metric.zero_threshold", "0")
        .with_attribute("otel.metric.positive_offset", "0")
        .with_attribute("otel.metric.positive_bucket_counts", "2")
        .with_attribute("otel.metric.negative_offset", "0")
        .with_attribute("otel.metric.negative_bucket_counts", "1")
        .with_attribute("otel.metric.time_unix_nano", "5000000")
        .with_attribute("route", "/checkout");
        let batch = RecordBatch::new(vec![record]);

        let request = batch_to_prometheus_remote_write_request(&batch);

        assert_eq!(request.timeseries.len(), 6);
        assert_eq!(
            find_prom_timeseries(&request, "checkout_latency_ms_count", None)
                .ok_or("missing exponential histogram count")?
                .samples[0]
                .value,
            4.0
        );
        assert_eq!(
            find_prom_timeseries(&request, "checkout_latency_ms_sum", None)
                .ok_or("missing exponential histogram sum")?
                .samples[0]
                .value,
            20.0
        );
        assert!(
            find_prom_timeseries(&request, "checkout_latency_ms_bucket", Some(("le", "-1")))
                .is_some()
        );
        assert!(
            find_prom_timeseries(&request, "checkout_latency_ms_bucket", Some(("le", "0")))
                .is_some()
        );
        assert!(
            find_prom_timeseries(&request, "checkout_latency_ms_bucket", Some(("le", "2")))
                .is_some()
        );
        assert_eq!(
            find_prom_timeseries(&request, "checkout_latency_ms_bucket", Some(("le", "+Inf")))
                .ok_or("missing final exponential histogram bucket")?
                .samples[0]
                .value,
            4.0
        );
        Ok(())
    }

    #[test]
    fn converts_summary_metric_records_to_prometheus_remote_write_request()
    -> Result<(), Box<dyn Error>> {
        let record = TelemetryRecord::new(
            "tenant-a",
            SignalKind::Metric,
            b"checkout.latency_ms".to_vec(),
        )
        .with_attribute("otel.metric.name", "checkout.latency_ms")
        .with_attribute("otel.metric.data_type", "summary")
        .with_attribute("otel.metric.count", "10")
        .with_attribute("otel.metric.sum", "250.5")
        .with_attribute("otel.metric.quantile_values", "0.5:20,0.95:80")
        .with_attribute("otel.metric.time_unix_nano", "4000000")
        .with_attribute("route", "/checkout");
        let batch = RecordBatch::new(vec![record]);

        let request = batch_to_prometheus_remote_write_request(&batch);

        assert_eq!(request.timeseries.len(), 4);
        assert_eq!(
            find_prom_timeseries(&request, "checkout_latency_ms_count", None)
                .ok_or("missing summary count")?
                .samples[0]
                .value,
            10.0
        );
        assert_eq!(
            find_prom_timeseries(&request, "checkout_latency_ms_sum", None)
                .ok_or("missing summary sum")?
                .samples[0]
                .value,
            250.5
        );
        assert_eq!(
            find_prom_timeseries(&request, "checkout_latency_ms", Some(("quantile", "0.5")))
                .ok_or("missing p50 summary quantile")?
                .samples[0]
                .value,
            20.0
        );
        assert_eq!(
            find_prom_timeseries(&request, "checkout_latency_ms", Some(("quantile", "0.95")))
                .ok_or("missing p95 summary quantile")?
                .samples[0]
                .value,
            80.0
        );
        Ok(())
    }

    #[test]
    fn snappy_literal_compression_round_trips() -> Result<(), Box<dyn Error>> {
        let input = b"prometheus remote write payload";
        let compressed = snappy_compress_literal(input);
        let decompressed = snappy_decompress_literal(&compressed)?;

        assert_eq!(decompressed, input);
        Ok(())
    }

    #[test]
    fn parses_otlp_http_endpoints() -> Result<(), Box<dyn Error>> {
        assert_eq!(
            parse_http_endpoint("127.0.0.1")?,
            HttpEndpoint {
                scheme: HttpScheme::Http,
                host: "127.0.0.1".to_string(),
                port: 4318,
                base_path: String::new(),
            }
        );
        assert_eq!(
            parse_http_endpoint("http://collector:4318/otlp")?.path("/v1/traces"),
            "/otlp/v1/traces"
        );
        assert_eq!(
            parse_http_endpoint("https://collector:4318")?,
            HttpEndpoint {
                scheme: HttpScheme::Https,
                host: "collector".to_string(),
                port: 4318,
                base_path: String::new(),
            }
        );
        assert_eq!(
            parse_http_endpoint_with_default_port("prometheus.local", 9090)?,
            HttpEndpoint {
                scheme: HttpScheme::Http,
                host: "prometheus.local".to_string(),
                port: 9090,
                base_path: String::new(),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn prometheus_remote_write_exporter_sends_metrics_to_server() -> Result<(), Box<dyn Error>>
    {
        let received = Arc::new(tokio::sync::Mutex::new((String::new(), 0_usize)));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let received_for_server = Arc::clone(&received);
        let server = tokio::spawn(async move {
            let (mut stream, _addr) = listener.accept().await?;
            let (path, body) = read_http_request(&mut stream).await?;
            let decompressed = snappy_decompress_literal(&body)
                .map_err(|err| io::Error::other(err.to_string()))?;
            let request = PromWriteRequest::decode(decompressed.as_slice())
                .map_err(|err| io::Error::other(err.to_string()))?;
            {
                let mut received = received_for_server.lock().await;
                *received = (path, request.timeseries.len());
            }
            stream
                .write_all(
                    b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await?;
            Ok::<(), io::Error>(())
        });

        let mut exporter = PrometheusRemoteWriteExporter::new(
            &format!("http://{addr}/api/v1/write"),
            TlsConfig::disabled(),
        )?;
        let record = TelemetryRecord::new("tenant-a", SignalKind::Metric, b"42.5".to_vec())
            .with_attribute("otel.metric.name", "checkout.latency.ms")
            .with_attribute("otel.metric.value", "42.5");
        let batch = RecordBatch::new(vec![record]);

        let report = exporter.export(&batch).await?;
        server.await??;

        assert_eq!(report.records, 1);
        assert_eq!(*received.lock().await, ("/api/v1/write".to_string(), 1));
        Ok(())
    }

    #[tokio::test]
    async fn otlp_http_exporter_sends_logs_to_server() -> Result<(), Box<dyn Error>> {
        let received = Arc::new(tokio::sync::Mutex::new((String::new(), 0_usize)));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let received_for_server = Arc::clone(&received);
        let server = tokio::spawn(async move {
            let (mut stream, _addr) = listener.accept().await?;
            let (path, body) = read_http_request(&mut stream).await?;
            let request = ExportLogsServiceRequest::decode(body.as_slice())
                .map_err(|err| io::Error::other(err.to_string()))?;
            {
                let mut received = received_for_server.lock().await;
                *received = (path, count_log_records(&request));
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await?;
            Ok::<(), io::Error>(())
        });

        let mut exporter = OtlpHttpExporter::new(&format!("http://{addr}"), TlsConfig::disabled())?;
        let record = TelemetryRecord::new("tenant-a", SignalKind::Log, b"worker-ready".to_vec())
            .with_attribute("otel.log.severity_text", "INFO");
        let batch = RecordBatch::new(vec![record]);

        let report = exporter.export(&batch).await?;
        server.await??;

        assert_eq!(report.records, 1);
        assert_eq!(*received.lock().await, ("/v1/logs".to_string(), 1));
        Ok(())
    }

    #[tokio::test]
    async fn otlp_http_json_exporter_sends_logs_to_server() -> Result<(), Box<dyn Error>> {
        let received = Arc::new(tokio::sync::Mutex::new((String::new(), 0_usize)));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let received_for_server = Arc::clone(&received);
        let server = tokio::spawn(async move {
            let (mut stream, _addr) = listener.accept().await?;
            let (path, body) = read_http_request(&mut stream).await?;
            let request: ExportLogsServiceRequest = serde_json::from_slice(body.as_slice())
                .map_err(|err| io::Error::other(err.to_string()))?;
            {
                let mut received = received_for_server.lock().await;
                *received = (path, count_log_records(&request));
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await?;
            Ok::<(), io::Error>(())
        });

        let mut exporter =
            OtlpHttpExporter::new_json(&format!("http://{addr}"), TlsConfig::disabled())?;
        let record = TelemetryRecord::new("tenant-a", SignalKind::Log, b"worker-ready".to_vec())
            .with_attribute("otel.log.severity_text", "INFO");
        let batch = RecordBatch::new(vec![record]);

        let report = exporter.export(&batch).await?;
        server.await??;

        assert_eq!(report.records, 1);
        assert_eq!(*received.lock().await, ("/v1/logs".to_string(), 1));
        Ok(())
    }

    #[tokio::test]
    async fn otlp_grpc_exporter_sends_traces_to_server() -> Result<(), Box<dyn Error>> {
        let received_spans = Arc::new(tokio::sync::Mutex::new(0_usize));
        let service = RecordingTraceService {
            received_spans: Arc::clone(&received_spans),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let incoming = TcpListenerStream::new(listener);
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(TraceServiceServer::new(service))
                .serve_with_incoming(incoming)
                .await
        });

        let mut exporter = OtlpGrpcExporter::new(&format!("http://{addr}"))?;
        let record = TelemetryRecord::new("tenant-a", SignalKind::Trace, b"GET /checkout".to_vec())
            .with_attribute("otel.trace_id", "01010101010101010101010101010101")
            .with_attribute("otel.span_id", "0202020202020202")
            .with_attribute("otel.span.name", "GET /checkout");
        let batch = RecordBatch::new(vec![record]);

        let report = exporter.export(&batch).await?;

        assert_eq!(report.records, 1);
        assert_eq!(*received_spans.lock().await, 1);
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn otlp_grpc_exporter_sends_metrics_to_server() -> Result<(), Box<dyn Error>> {
        let received_points = Arc::new(tokio::sync::Mutex::new(0_usize));
        let service = RecordingMetricsService {
            received_points: Arc::clone(&received_points),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let incoming = TcpListenerStream::new(listener);
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(MetricsServiceServer::new(service))
                .serve_with_incoming(incoming)
                .await
        });

        let mut exporter = OtlpGrpcExporter::new(&format!("http://{addr}"))?;
        let record = TelemetryRecord::new("tenant-a", SignalKind::Metric, b"42.5".to_vec())
            .with_attribute("service.name", "checkout.latency_ms")
            .with_attribute("otel.metric.name", "checkout.latency_ms")
            .with_attribute("otel.metric.value", "42.5");
        let batch = RecordBatch::new(vec![record]);

        let report = exporter.export(&batch).await?;

        assert_eq!(report.records, 1);
        assert_eq!(*received_points.lock().await, 1);
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn otlp_grpc_exporter_sends_logs_to_server() -> Result<(), Box<dyn Error>> {
        let received_logs = Arc::new(tokio::sync::Mutex::new(0_usize));
        let service = RecordingLogsService {
            received_logs: Arc::clone(&received_logs),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let incoming = TcpListenerStream::new(listener);
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(LogsServiceServer::new(service))
                .serve_with_incoming(incoming)
                .await
        });

        let mut exporter = OtlpGrpcExporter::new(&format!("http://{addr}"))?;
        let record = TelemetryRecord::new("tenant-a", SignalKind::Log, b"worker-ready".to_vec())
            .with_attribute("otel.log.severity_text", "INFO");
        let batch = RecordBatch::new(vec![record]);

        let report = exporter.export(&batch).await?;

        assert_eq!(report.records, 1);
        assert_eq!(*received_logs.lock().await, 1);
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn kafka_s3_and_clickhouse_exporters_are_buildable_skeletons()
    -> Result<(), Box<dyn Error>> {
        let configs = vec![
            ExporterConfig {
                name: "kafka".to_string(),
                protocol: ExporterProtocol::Kafka,
                endpoint: "kafka://broker/topic".to_string(),
                tls: TlsConfig::disabled(),
                retry: telemetry_core::ExporterRetryConfig::default(),
            },
            ExporterConfig {
                name: "s3".to_string(),
                protocol: ExporterProtocol::S3,
                endpoint: "s3://bucket/prefix".to_string(),
                tls: TlsConfig::disabled(),
                retry: telemetry_core::ExporterRetryConfig::default(),
            },
            ExporterConfig {
                name: "clickhouse".to_string(),
                protocol: ExporterProtocol::ClickHouse,
                endpoint: "clickhouse://localhost:9000/events".to_string(),
                tls: TlsConfig::disabled(),
                retry: telemetry_core::ExporterRetryConfig::default(),
            },
        ];
        let mut exporters = build_exporters(&configs)?;
        let batch = RecordBatch::new(vec![TelemetryRecord::new(
            "tenant-a",
            SignalKind::Log,
            b"module-event".to_vec(),
        )]);

        assert_eq!(exporters.len(), 3);
        for (name, expected) in [
            ("kafka", "kafka exporter is a skeleton"),
            ("s3", "s3 exporter is a skeleton"),
            ("clickhouse", "clickhouse exporter is a skeleton"),
        ] {
            let exporter = exporters
                .get_mut(name)
                .ok_or_else(|| format!("missing exporter {name}"))?;
            let error = exporter
                .export(&batch)
                .await
                .err()
                .ok_or_else(|| format!("exporter {name} unexpectedly succeeded"))?;
            assert!(error.to_string().contains(expected));
        }
        Ok(())
    }

    #[derive(Clone)]
    struct RecordingTraceService {
        received_spans: Arc<tokio::sync::Mutex<usize>>,
    }

    #[tonic::async_trait]
    impl TraceService for RecordingTraceService {
        async fn export(
            &self,
            request: Request<ExportTraceServiceRequest>,
        ) -> Result<Response<ExportTraceServiceResponse>, Status> {
            let count = count_spans(&request.into_inner());
            let mut received = self.received_spans.lock().await;
            *received += count;
            Ok(Response::new(ExportTraceServiceResponse {
                partial_success: None,
            }))
        }
    }

    #[derive(Clone)]
    struct RecordingMetricsService {
        received_points: Arc<tokio::sync::Mutex<usize>>,
    }

    #[tonic::async_trait]
    impl MetricsService for RecordingMetricsService {
        async fn export(
            &self,
            request: Request<ExportMetricsServiceRequest>,
        ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
            let count = count_metric_data_points(&request.into_inner());
            let mut received = self.received_points.lock().await;
            *received += count;
            Ok(Response::new(ExportMetricsServiceResponse {
                partial_success: None,
            }))
        }
    }

    #[derive(Clone)]
    struct RecordingLogsService {
        received_logs: Arc<tokio::sync::Mutex<usize>>,
    }

    #[tonic::async_trait]
    impl LogsService for RecordingLogsService {
        async fn export(
            &self,
            request: Request<ExportLogsServiceRequest>,
        ) -> Result<Response<ExportLogsServiceResponse>, Status> {
            let count = count_log_records(&request.into_inner());
            let mut received = self.received_logs.lock().await;
            *received += count;
            Ok(Response::new(ExportLogsServiceResponse {
                partial_success: None,
            }))
        }
    }

    fn find_prom_label<'a>(timeseries: &'a PromTimeSeries, name: &str) -> Option<&'a str> {
        find_prom_label_value(&timeseries.labels, name)
    }

    fn find_prom_label_value<'a>(labels: &'a [PromLabel], name: &str) -> Option<&'a str> {
        labels
            .iter()
            .find(|label| label.name == name)
            .map(|label| label.value.as_str())
    }

    fn find_prom_timeseries<'a>(
        request: &'a PromWriteRequest,
        name: &str,
        extra_label: Option<(&str, &str)>,
    ) -> Option<&'a PromTimeSeries> {
        request.timeseries.iter().find(|timeseries| {
            find_prom_label(timeseries, "__name__") == Some(name)
                && extra_label
                    .map(|(label_name, label_value)| {
                        find_prom_label(timeseries, label_name) == Some(label_value)
                    })
                    .unwrap_or(true)
        })
    }

    fn snappy_decompress_literal(input: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        let (expected_len, mut offset) = read_snappy_varint(input)?;
        let mut output = Vec::with_capacity(expected_len);

        while offset < input.len() {
            let tag = input[offset];
            offset += 1;
            if tag & 0x03 != 0 {
                return Err("test decoder only supports snappy literal tags".into());
            }

            let literal_len_marker = (tag >> 2) as usize;
            let literal_len = if literal_len_marker < 60 {
                literal_len_marker + 1
            } else {
                let bytes_used = literal_len_marker - 59;
                if bytes_used == 0 || bytes_used > 4 || offset + bytes_used > input.len() {
                    return Err("invalid snappy literal length".into());
                }
                let mut length_minus_one = 0_usize;
                for index in 0..bytes_used {
                    length_minus_one |= usize::from(input[offset + index]) << (index * 8);
                }
                offset += bytes_used;
                length_minus_one + 1
            };

            if offset + literal_len > input.len() {
                return Err("snappy literal overruns input".into());
            }
            output.extend_from_slice(&input[offset..offset + literal_len]);
            offset += literal_len;
        }

        if output.len() != expected_len {
            return Err("snappy decompressed length mismatch".into());
        }

        Ok(output)
    }

    fn read_snappy_varint(input: &[u8]) -> Result<(usize, usize), Box<dyn Error>> {
        let mut value = 0_usize;
        let mut shift = 0_usize;

        for (index, byte) in input.iter().enumerate() {
            value |= usize::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok((value, index + 1));
            }
            shift += 7;
            if shift > 28 {
                return Err("snappy varint is too large".into());
            }
        }

        Err("snappy varint ended unexpectedly".into())
    }

    async fn read_http_request(
        stream: &mut tokio::net::TcpStream,
    ) -> io::Result<(String, Vec<u8>)> {
        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 1024];

        loop {
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..read]);

            if let Some((header_end, content_length)) = http_body_bounds(&buffer)? {
                let body_start = header_end + 4;
                if buffer.len() >= body_start + content_length {
                    let headers = String::from_utf8_lossy(&buffer[..header_end]);
                    let path = http_request_path(&headers)?;
                    let body = buffer[body_start..body_start + content_length].to_vec();
                    return Ok((path, body));
                }
            }
        }

        Err(io::Error::other("HTTP request ended before full body"))
    }

    fn http_body_bounds(buffer: &[u8]) -> io::Result<Option<(usize, usize)>> {
        let Some(header_end) = find_header_end(buffer) else {
            return Ok(None);
        };
        let headers = String::from_utf8_lossy(&buffer[..header_end]);
        let content_length = http_content_length(&headers)?;
        Ok(Some((header_end, content_length)))
    }

    fn find_header_end(buffer: &[u8]) -> Option<usize> {
        buffer.windows(4).position(|window| window == b"\r\n\r\n")
    }

    fn http_request_path(headers: &str) -> io::Result<String> {
        headers
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .map(str::to_string)
            .ok_or_else(|| io::Error::other("HTTP request line was incomplete"))
    }

    fn http_content_length(headers: &str) -> io::Result<usize> {
        headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>())
            })
            .transpose()
            .map_err(|err| io::Error::other(err.to_string()))?
            .ok_or_else(|| io::Error::other("HTTP request did not include Content-Length"))
    }
}
