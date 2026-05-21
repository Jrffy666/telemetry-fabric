use crate::runtime::AgentRuntime;
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
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use telemetry_otlp::{
    logs_request_to_records, metrics_request_to_records, trace_request_to_records,
};
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub async fn serve_otlp_grpc(
    bind: String,
    runtime: Arc<Mutex<AgentRuntime>>,
    tenant_id: String,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let addr = bind.parse::<SocketAddr>()?;
    let trace_service = OtlpTraceService {
        runtime: Arc::clone(&runtime),
        tenant_id: tenant_id.clone(),
    };
    let metrics_service = OtlpMetricsService {
        runtime: Arc::clone(&runtime),
        tenant_id: tenant_id.clone(),
    };
    let logs_service = OtlpLogsService { runtime, tenant_id };

    println!("telemetry-agent listening on {addr} using OTLP/gRPC traces, metrics, and logs");
    Server::builder()
        .add_service(TraceServiceServer::new(trace_service))
        .add_service(MetricsServiceServer::new(metrics_service))
        .add_service(LogsServiceServer::new(logs_service))
        .serve(addr)
        .await?;

    Ok(())
}

#[derive(Clone)]
struct OtlpTraceService {
    runtime: Arc<Mutex<AgentRuntime>>,
    tenant_id: String,
}

#[tonic::async_trait]
impl TraceService for OtlpTraceService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let records = trace_request_to_records(&self.tenant_id, request.into_inner());
        let mut runtime = self.runtime.lock().await;

        for record in records {
            runtime
                .ingest(record)
                .map_err(|err| Status::resource_exhausted(err.to_string()))?;
        }

        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

#[derive(Clone)]
struct OtlpMetricsService {
    runtime: Arc<Mutex<AgentRuntime>>,
    tenant_id: String,
}

#[tonic::async_trait]
impl MetricsService for OtlpMetricsService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let records = metrics_request_to_records(&self.tenant_id, request.into_inner());
        let mut runtime = self.runtime.lock().await;

        for record in records {
            runtime
                .ingest(record)
                .map_err(|err| Status::resource_exhausted(err.to_string()))?;
        }

        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

#[derive(Clone)]
struct OtlpLogsService {
    runtime: Arc<Mutex<AgentRuntime>>,
    tenant_id: String,
}

#[tonic::async_trait]
impl LogsService for OtlpLogsService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let records = logs_request_to_records(&self.tenant_id, request.into_inner());
        let mut runtime = self.runtime.lock().await;

        for record in records {
            runtime
                .ingest(record)
                .map_err(|err| Status::resource_exhausted(err.to_string()))?;
        }

        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::AgentRuntime;
    use opentelemetry_proto::tonic::common::v1::{
        AnyValue, InstrumentationScope, KeyValue, any_value,
    };
    use opentelemetry_proto::tonic::logs::v1::{
        LogRecord, ResourceLogs as OtlpResourceLogs, ScopeLogs as OtlpScopeLogs, SeverityNumber,
    };
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics as OtlpResourceMetrics,
        ScopeMetrics as OtlpScopeMetrics, metric, number_data_point,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};
    use telemetry_buffer::DiskQueueOptions;
    use telemetry_core::{
        ExporterConfig, ExporterProtocol, ExporterRetryConfig, PipelineConfig, RouteConfig,
        SignalKind, TlsConfig,
    };

    #[tokio::test]
    async fn otlp_trace_service_ingests_and_flushes_records()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let dir = test_dir("otlp-trace-service");
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
        let runtime = AgentRuntime::open(config, queue_dir, DiskQueueOptions::default())?;
        let service = OtlpTraceService {
            runtime: Arc::new(Mutex::new(runtime)),
            tenant_id: "payments-prod".to_string(),
        };

        service.export(Request::new(sample_trace_request())).await?;
        service.runtime.lock().await.flush(32).await?;

        assert_eq!(
            fs::read_to_string(&export_path)?,
            "tenant=payments-prod signal=trace attrs=11 bytes=13\n"
        );

        cleanup(dir);
        Ok(())
    }

    #[tokio::test]
    async fn otlp_metrics_service_ingests_and_flushes_records()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let dir = test_dir("otlp-metrics-service");
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
                signal: SignalKind::Metric,
                exporters: vec!["file".to_string()],
            }],
            ..PipelineConfig::default()
        };
        let runtime = AgentRuntime::open(config, queue_dir, DiskQueueOptions::default())?;
        let service = OtlpMetricsService {
            runtime: Arc::new(Mutex::new(runtime)),
            tenant_id: "payments-prod".to_string(),
        };

        service
            .export(Request::new(sample_metrics_request()))
            .await?;
        service.runtime.lock().await.flush(32).await?;

        assert_eq!(
            fs::read_to_string(&export_path)?,
            "tenant=payments-prod signal=metric attrs=11 bytes=19\n"
        );

        cleanup(dir);
        Ok(())
    }

    #[tokio::test]
    async fn otlp_logs_service_ingests_and_flushes_records()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let dir = test_dir("otlp-logs-service");
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
                signal: SignalKind::Log,
                exporters: vec!["file".to_string()],
            }],
            ..PipelineConfig::default()
        };
        let runtime = AgentRuntime::open(config, queue_dir, DiskQueueOptions::default())?;
        let service = OtlpLogsService {
            runtime: Arc::new(Mutex::new(runtime)),
            tenant_id: "payments-prod".to_string(),
        };

        service.export(Request::new(sample_logs_request())).await?;
        service.runtime.lock().await.flush(32).await?;

        assert_eq!(
            fs::read_to_string(&export_path)?,
            "tenant=payments-prod signal=log attrs=12 bytes=12\n"
        );

        cleanup(dir);
        Ok(())
    }

    fn sample_trace_request() -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![string_attr("service.name", "checkout")],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "test-scope".to_string(),
                        version: "1.0.0".to_string(),
                        attributes: Vec::new(),
                        dropped_attributes_count: 0,
                    }),
                    spans: vec![Span {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        name: "GET /checkout".to_string(),
                        start_time_unix_nano: 10,
                        end_time_unix_nano: 20,
                        attributes: vec![string_attr("http.method", "GET")],
                        ..Span::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn sample_metrics_request() -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![OtlpResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![string_attr("service.name", "checkout")],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_metrics: vec![OtlpScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "test-scope".to_string(),
                        version: "1.0.0".to_string(),
                        attributes: Vec::new(),
                        dropped_attributes_count: 0,
                    }),
                    metrics: vec![Metric {
                        name: "checkout.latency_ms".to_string(),
                        description: String::new(),
                        unit: String::new(),
                        metadata: Vec::new(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![string_attr("route", "/checkout")],
                                start_time_unix_nano: 10,
                                time_unix_nano: 20,
                                value: Some(number_data_point::Value::AsDouble(42.5)),
                                exemplars: Vec::new(),
                                flags: 0,
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn sample_logs_request() -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![OtlpResourceLogs {
                resource: Some(Resource {
                    attributes: vec![string_attr("service.name", "checkout")],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_logs: vec![OtlpScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "test-scope".to_string(),
                        version: "1.0.0".to_string(),
                        attributes: Vec::new(),
                        dropped_attributes_count: 0,
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: 10,
                        observed_time_unix_nano: 20,
                        severity_number: SeverityNumber::Info as i32,
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("worker-ready".to_string())),
                        }),
                        attributes: vec![string_attr("thread.name", "worker-1")],
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        event_name: "checkout.worker".to_string(),
                        ..LogRecord::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn string_attr(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
            key_strindex: 0,
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
