use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, Exemplar, ExponentialHistogramDataPoint, HistogramDataPoint, Metric,
    NumberDataPoint, SummaryDataPoint, exemplar, metric, number_data_point,
};
use opentelemetry_proto::tonic::trace::v1::Span;
use telemetry_core::{SignalKind, TelemetryRecord};

pub fn trace_request_to_records(
    tenant_id: &str,
    request: ExportTraceServiceRequest,
) -> Vec<TelemetryRecord> {
    let mut records = Vec::new();

    for resource_spans in request.resource_spans {
        let resource_attributes = resource_spans
            .resource
            .as_ref()
            .map(|resource| attributes_from_key_values("resource.", &resource.attributes))
            .unwrap_or_default();

        for scope_spans in resource_spans.scope_spans {
            let scope_name = scope_spans
                .scope
                .as_ref()
                .map(|scope| scope.name.as_str())
                .unwrap_or("");

            for span in scope_spans.spans {
                records.push(span_to_record(
                    tenant_id,
                    &resource_attributes,
                    scope_name,
                    span,
                ));
            }
        }
    }

    records
}

pub fn metrics_request_to_records(
    tenant_id: &str,
    request: ExportMetricsServiceRequest,
) -> Vec<TelemetryRecord> {
    let mut records = Vec::new();

    for resource_metrics in request.resource_metrics {
        let resource_attributes = resource_metrics
            .resource
            .as_ref()
            .map(|resource| attributes_from_key_values("resource.", &resource.attributes))
            .unwrap_or_default();

        for scope_metrics in resource_metrics.scope_metrics {
            let scope_name = scope_metrics
                .scope
                .as_ref()
                .map(|scope| scope.name.as_str())
                .unwrap_or("");

            for metric in scope_metrics.metrics {
                records.extend(metric_to_records(
                    tenant_id,
                    &resource_attributes,
                    scope_name,
                    metric,
                ));
            }
        }
    }

    records
}

pub fn logs_request_to_records(
    tenant_id: &str,
    request: ExportLogsServiceRequest,
) -> Vec<TelemetryRecord> {
    let mut records = Vec::new();

    for resource_logs in request.resource_logs {
        let resource_attributes = resource_logs
            .resource
            .as_ref()
            .map(|resource| attributes_from_key_values("resource.", &resource.attributes))
            .unwrap_or_default();

        for scope_logs in resource_logs.scope_logs {
            let scope_name = scope_logs
                .scope
                .as_ref()
                .map(|scope| scope.name.as_str())
                .unwrap_or("");

            for log_record in scope_logs.log_records {
                records.push(log_record_to_record(
                    tenant_id,
                    &resource_attributes,
                    scope_name,
                    log_record,
                ));
            }
        }
    }

    records
}

fn span_to_record(
    tenant_id: &str,
    resource_attributes: &[(String, String)],
    scope_name: &str,
    span: Span,
) -> TelemetryRecord {
    let mut record =
        TelemetryRecord::new(tenant_id, SignalKind::Trace, span.name.as_bytes().to_vec())
            .with_attribute("otel.signal", "trace")
            .with_attribute("otel.span.name", span.name)
            .with_attribute("otel.trace_id", hex_encode(&span.trace_id))
            .with_attribute("otel.span_id", hex_encode(&span.span_id))
            .with_attribute("otel.parent_span_id", hex_encode(&span.parent_span_id))
            .with_attribute("otel.span.kind", span.kind.to_string())
            .with_attribute(
                "otel.start_unix_nano",
                span.start_time_unix_nano.to_string(),
            )
            .with_attribute("otel.end_unix_nano", span.end_time_unix_nano.to_string());

    if !scope_name.is_empty() {
        record = record.with_attribute("otel.scope.name", scope_name);
    }

    for (key, value) in resource_attributes {
        record = record.with_attribute(key.clone(), value.clone());
    }

    for (key, value) in attributes_from_key_values("", &span.attributes) {
        record = record.with_attribute(key, value);
    }

    record
}

fn metric_to_records(
    tenant_id: &str,
    resource_attributes: &[(String, String)],
    scope_name: &str,
    metric: Metric,
) -> Vec<TelemetryRecord> {
    let mut records = Vec::new();
    let name = metric.name;
    let description = metric.description;
    let unit = metric.unit;

    match metric.data {
        Some(metric::Data::Gauge(gauge)) => {
            for data_point in gauge.data_points {
                if let Some(record) = number_data_point_to_record(
                    tenant_id,
                    resource_attributes,
                    scope_name,
                    MetricRecordMetadata {
                        name: &name,
                        description: &description,
                        unit: &unit,
                        data_type: "gauge",
                        aggregation_temporality: None,
                        is_monotonic: None,
                    },
                    data_point,
                ) {
                    records.push(record);
                }
            }
        }
        Some(metric::Data::Sum(sum)) => {
            for data_point in sum.data_points {
                if let Some(record) = number_data_point_to_record(
                    tenant_id,
                    resource_attributes,
                    scope_name,
                    MetricRecordMetadata {
                        name: &name,
                        description: &description,
                        unit: &unit,
                        data_type: "sum",
                        aggregation_temporality: Some(sum.aggregation_temporality),
                        is_monotonic: Some(sum.is_monotonic),
                    },
                    data_point,
                ) {
                    records.push(record);
                }
            }
        }
        Some(metric::Data::Histogram(histogram)) => {
            for data_point in histogram.data_points {
                records.push(histogram_data_point_to_record(
                    tenant_id,
                    resource_attributes,
                    scope_name,
                    MetricRecordMetadata {
                        name: &name,
                        description: &description,
                        unit: &unit,
                        data_type: "histogram",
                        aggregation_temporality: Some(histogram.aggregation_temporality),
                        is_monotonic: None,
                    },
                    data_point,
                ));
            }
        }
        Some(metric::Data::ExponentialHistogram(histogram)) => {
            for data_point in histogram.data_points {
                records.push(exponential_histogram_data_point_to_record(
                    tenant_id,
                    resource_attributes,
                    scope_name,
                    MetricRecordMetadata {
                        name: &name,
                        description: &description,
                        unit: &unit,
                        data_type: "exponential_histogram",
                        aggregation_temporality: Some(histogram.aggregation_temporality),
                        is_monotonic: None,
                    },
                    data_point,
                ));
            }
        }
        Some(metric::Data::Summary(summary)) => {
            for data_point in summary.data_points {
                records.push(summary_data_point_to_record(
                    tenant_id,
                    resource_attributes,
                    scope_name,
                    MetricRecordMetadata {
                        name: &name,
                        description: &description,
                        unit: &unit,
                        data_type: "summary",
                        aggregation_temporality: None,
                        is_monotonic: None,
                    },
                    data_point,
                ));
            }
        }
        None => {}
    }

    records
}

#[derive(Clone, Copy)]
struct MetricRecordMetadata<'a> {
    name: &'a str,
    description: &'a str,
    unit: &'a str,
    data_type: &'a str,
    aggregation_temporality: Option<i32>,
    is_monotonic: Option<bool>,
}

fn number_data_point_to_record(
    tenant_id: &str,
    resource_attributes: &[(String, String)],
    scope_name: &str,
    metadata: MetricRecordMetadata<'_>,
    data_point: NumberDataPoint,
) -> Option<TelemetryRecord> {
    let NumberDataPoint {
        attributes,
        start_time_unix_nano,
        time_unix_nano,
        value,
        exemplars,
        flags,
        ..
    } = data_point;
    let (value_type, value) = number_value_to_string(value?)?;

    let record = append_exemplar_attributes(
        metric_record_base(
            tenant_id,
            resource_attributes,
            scope_name,
            metadata,
            &attributes,
            start_time_unix_nano,
            time_unix_nano,
            flags,
        )
        .with_attribute("otel.metric.value", value)
        .with_attribute("otel.metric.value_type", value_type),
        &exemplars,
    );

    Some(record)
}

fn histogram_data_point_to_record(
    tenant_id: &str,
    resource_attributes: &[(String, String)],
    scope_name: &str,
    metadata: MetricRecordMetadata<'_>,
    data_point: HistogramDataPoint,
) -> TelemetryRecord {
    let HistogramDataPoint {
        attributes,
        start_time_unix_nano,
        time_unix_nano,
        count,
        sum,
        bucket_counts,
        explicit_bounds,
        exemplars,
        flags,
        min,
        max,
        ..
    } = data_point;

    let mut record = metric_record_base(
        tenant_id,
        resource_attributes,
        scope_name,
        metadata,
        &attributes,
        start_time_unix_nano,
        time_unix_nano,
        flags,
    )
    .with_attribute("otel.metric.count", count.to_string())
    .with_attribute("otel.metric.bucket_counts", join_u64_values(&bucket_counts))
    .with_attribute(
        "otel.metric.explicit_bounds",
        join_f64_values(&explicit_bounds),
    );

    if let Some(sum) = sum {
        record = record.with_attribute("otel.metric.sum", sum.to_string());
    }
    if let Some(min) = min {
        record = record.with_attribute("otel.metric.min", min.to_string());
    }
    if let Some(max) = max {
        record = record.with_attribute("otel.metric.max", max.to_string());
    }

    append_exemplar_attributes(record, &exemplars)
}

fn exponential_histogram_data_point_to_record(
    tenant_id: &str,
    resource_attributes: &[(String, String)],
    scope_name: &str,
    metadata: MetricRecordMetadata<'_>,
    data_point: ExponentialHistogramDataPoint,
) -> TelemetryRecord {
    let ExponentialHistogramDataPoint {
        attributes,
        start_time_unix_nano,
        time_unix_nano,
        count,
        sum,
        scale,
        zero_count,
        positive,
        negative,
        flags,
        exemplars,
        min,
        max,
        zero_threshold,
        ..
    } = data_point;

    let mut record = metric_record_base(
        tenant_id,
        resource_attributes,
        scope_name,
        metadata,
        &attributes,
        start_time_unix_nano,
        time_unix_nano,
        flags,
    )
    .with_attribute("otel.metric.count", count.to_string())
    .with_attribute("otel.metric.scale", scale.to_string())
    .with_attribute("otel.metric.zero_count", zero_count.to_string())
    .with_attribute("otel.metric.zero_threshold", zero_threshold.to_string());

    if let Some(sum) = sum {
        record = record.with_attribute("otel.metric.sum", sum.to_string());
    }
    if let Some(min) = min {
        record = record.with_attribute("otel.metric.min", min.to_string());
    }
    if let Some(max) = max {
        record = record.with_attribute("otel.metric.max", max.to_string());
    }
    if let Some(positive) = positive {
        record = record
            .with_attribute("otel.metric.positive_offset", positive.offset.to_string())
            .with_attribute(
                "otel.metric.positive_bucket_counts",
                join_u64_values(&positive.bucket_counts),
            );
    }
    if let Some(negative) = negative {
        record = record
            .with_attribute("otel.metric.negative_offset", negative.offset.to_string())
            .with_attribute(
                "otel.metric.negative_bucket_counts",
                join_u64_values(&negative.bucket_counts),
            );
    }

    append_exemplar_attributes(record, &exemplars)
}

fn summary_data_point_to_record(
    tenant_id: &str,
    resource_attributes: &[(String, String)],
    scope_name: &str,
    metadata: MetricRecordMetadata<'_>,
    data_point: SummaryDataPoint,
) -> TelemetryRecord {
    let SummaryDataPoint {
        attributes,
        start_time_unix_nano,
        time_unix_nano,
        count,
        sum,
        quantile_values,
        flags,
    } = data_point;

    metric_record_base(
        tenant_id,
        resource_attributes,
        scope_name,
        metadata,
        &attributes,
        start_time_unix_nano,
        time_unix_nano,
        flags,
    )
    .with_attribute("otel.metric.count", count.to_string())
    .with_attribute("otel.metric.sum", sum.to_string())
    .with_attribute(
        "otel.metric.quantile_values",
        join_quantile_values(&quantile_values),
    )
}

fn metric_record_base(
    tenant_id: &str,
    resource_attributes: &[(String, String)],
    scope_name: &str,
    metadata: MetricRecordMetadata<'_>,
    data_point_attributes: &[KeyValue],
    start_time_unix_nano: u64,
    time_unix_nano: u64,
    flags: u32,
) -> TelemetryRecord {
    let mut record = TelemetryRecord::new(
        tenant_id,
        SignalKind::Metric,
        metadata.name.as_bytes().to_vec(),
    )
    .with_attribute("otel.signal", "metric")
    .with_attribute("otel.metric.name", metadata.name)
    .with_attribute("otel.metric.data_type", metadata.data_type)
    .with_attribute("otel.metric.time_unix_nano", time_unix_nano.to_string())
    .with_attribute(
        "otel.metric.start_time_unix_nano",
        start_time_unix_nano.to_string(),
    )
    .with_attribute("otel.metric.flags", flags.to_string());

    if !metadata.description.is_empty() {
        record = record.with_attribute("otel.metric.description", metadata.description);
    }
    if !metadata.unit.is_empty() {
        record = record.with_attribute("otel.metric.unit", metadata.unit);
    }
    if let Some(temporality) = metadata.aggregation_temporality {
        record = record.with_attribute(
            "otel.metric.aggregation_temporality",
            temporality.to_string(),
        );
        if let Some(name) = aggregation_temporality_name(temporality) {
            record = record.with_attribute("otel.metric.aggregation_temporality_name", name);
        }
    }
    if let Some(is_monotonic) = metadata.is_monotonic {
        record = record.with_attribute("otel.metric.is_monotonic", is_monotonic.to_string());
    }
    if !scope_name.is_empty() {
        record = record.with_attribute("otel.scope.name", scope_name);
    }

    for (key, value) in resource_attributes {
        record = record.with_attribute(key.clone(), value.clone());
    }

    for (key, value) in attributes_from_key_values("", data_point_attributes) {
        record = record.with_attribute(key, value);
    }

    record
}

fn log_record_to_record(
    tenant_id: &str,
    resource_attributes: &[(String, String)],
    scope_name: &str,
    log_record: LogRecord,
) -> TelemetryRecord {
    let body = log_record
        .body
        .as_ref()
        .map(any_value_to_string)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| log_record.event_name.clone());

    let mut record = TelemetryRecord::new(tenant_id, SignalKind::Log, body.as_bytes().to_vec())
        .with_attribute("otel.signal", "log")
        .with_attribute(
            "otel.log.time_unix_nano",
            log_record.time_unix_nano.to_string(),
        )
        .with_attribute(
            "otel.log.observed_time_unix_nano",
            log_record.observed_time_unix_nano.to_string(),
        )
        .with_attribute(
            "otel.log.severity_number",
            log_record.severity_number.to_string(),
        )
        .with_attribute("otel.log.severity_text", log_record.severity_text)
        .with_attribute("otel.trace_id", hex_encode(&log_record.trace_id))
        .with_attribute("otel.span_id", hex_encode(&log_record.span_id))
        .with_attribute("otel.log.flags", log_record.flags.to_string());

    if !log_record.event_name.is_empty() {
        record = record.with_attribute("otel.log.event_name", log_record.event_name);
    }
    if !scope_name.is_empty() {
        record = record.with_attribute("otel.scope.name", scope_name);
    }

    for (key, value) in resource_attributes {
        record = record.with_attribute(key.clone(), value.clone());
    }

    for (key, value) in attributes_from_key_values("", &log_record.attributes) {
        record = record.with_attribute(key, value);
    }

    record
}

fn number_value_to_string(value: number_data_point::Value) -> Option<(&'static str, String)> {
    match value {
        number_data_point::Value::AsDouble(value) if value.is_finite() => {
            Some(("double", value.to_string()))
        }
        number_data_point::Value::AsDouble(_) => None,
        number_data_point::Value::AsInt(value) => Some(("int", value.to_string())),
    }
}

fn exemplar_value_to_string(value: exemplar::Value) -> Option<(&'static str, String)> {
    match value {
        exemplar::Value::AsDouble(value) if value.is_finite() => {
            Some(("double", value.to_string()))
        }
        exemplar::Value::AsDouble(_) => None,
        exemplar::Value::AsInt(value) => Some(("int", value.to_string())),
    }
}

fn append_exemplar_attributes(
    mut record: TelemetryRecord,
    exemplars: &[Exemplar],
) -> TelemetryRecord {
    if exemplars.is_empty() {
        return record;
    }

    record = record.with_attribute("otel.metric.exemplar.count", exemplars.len().to_string());

    for (index, exemplar) in exemplars.iter().enumerate() {
        let prefix = format!("otel.metric.exemplar.{index}");
        record = record
            .with_attribute(
                format!("{prefix}.time_unix_nano"),
                exemplar.time_unix_nano.to_string(),
            )
            .with_attribute(format!("{prefix}.trace_id"), hex_encode(&exemplar.trace_id))
            .with_attribute(format!("{prefix}.span_id"), hex_encode(&exemplar.span_id));

        if let Some((value_type, value)) = exemplar.value.and_then(exemplar_value_to_string) {
            record = record
                .with_attribute(format!("{prefix}.value"), value)
                .with_attribute(format!("{prefix}.value_type"), value_type);
        }

        for (key, value) in attributes_from_key_values(
            &format!("{prefix}.filtered."),
            &exemplar.filtered_attributes,
        ) {
            record = record.with_attribute(key, value);
        }
    }

    record
}

fn aggregation_temporality_name(value: i32) -> Option<&'static str> {
    AggregationTemporality::try_from(value)
        .ok()
        .map(|temporality| temporality.as_str_name())
}

fn join_u64_values(values: &[u64]) -> String {
    values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn join_f64_values(values: &[f64]) -> String {
    values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn join_quantile_values(
    values: &[opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile],
) -> String {
    values
        .iter()
        .map(|value| format!("{}:{}", value.quantile, value.value))
        .collect::<Vec<_>>()
        .join(",")
}

fn attributes_from_key_values(prefix: &str, values: &[KeyValue]) -> Vec<(String, String)> {
    values
        .iter()
        .filter(|value| !value.key.trim().is_empty())
        .map(|value| {
            let key = if prefix.is_empty() {
                value.key.clone()
            } else {
                format!("{prefix}{}", value.key)
            };
            let rendered = value
                .value
                .as_ref()
                .map(any_value_to_string)
                .unwrap_or_default();
            (key, rendered)
        })
        .collect()
}

fn any_value_to_string(value: &AnyValue) -> String {
    match &value.value {
        Some(any_value::Value::StringValue(value)) => value.clone(),
        Some(any_value::Value::BoolValue(value)) => value.to_string(),
        Some(any_value::Value::IntValue(value)) => value.to_string(),
        Some(any_value::Value::DoubleValue(value)) => value.to_string(),
        Some(any_value::Value::BytesValue(value)) => hex_encode(value),
        Some(any_value::Value::ArrayValue(value)) => {
            let items = value
                .values
                .iter()
                .map(any_value_to_string)
                .collect::<Vec<_>>()
                .join(",");
            format!("[{items}]")
        }
        Some(any_value::Value::KvlistValue(value)) => {
            let items = value
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
                .join(",");
            format!("{{{items}}}")
        }
        Some(any_value::Value::StringValueStrindex(value)) => value.to_string(),
        None => String::new(),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{
        AnyValue, InstrumentationScope, KeyValue, any_value,
    };
    use opentelemetry_proto::tonic::logs::v1::{
        LogRecord, ResourceLogs as OtlpResourceLogs, ScopeLogs as OtlpScopeLogs, SeverityNumber,
    };
    use opentelemetry_proto::tonic::metrics::v1::{
        AggregationTemporality, Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint,
        Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint,
        ResourceMetrics as OtlpResourceMetrics, ScopeMetrics as OtlpScopeMetrics, Summary,
        SummaryDataPoint, exemplar, exponential_histogram_data_point, metric, number_data_point,
        summary_data_point,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans};

    #[test]
    fn converts_trace_export_request_to_records() {
        let request = ExportTraceServiceRequest {
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
        };

        let records = trace_request_to_records("payments-prod", request);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].tenant_id, "payments-prod");
        assert_eq!(records[0].signal, SignalKind::Trace);
        assert_eq!(records[0].body, b"GET /checkout");
        assert!(
            records[0]
                .attributes
                .iter()
                .any(|attr| attr.key == "resource.service.name" && attr.value == "checkout")
        );
    }

    #[test]
    fn converts_metric_export_request_to_records() {
        let request = ExportMetricsServiceRequest {
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
                        unit: "ms".to_string(),
                        metadata: Vec::new(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![string_attr("route", "/checkout")],
                                start_time_unix_nano: 10,
                                time_unix_nano: 20,
                                value: Some(number_data_point::Value::AsDouble(42.5)),
                                exemplars: vec![sample_exemplar()],
                                flags: 0,
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let records = metrics_request_to_records("payments-prod", request);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].tenant_id, "payments-prod");
        assert_eq!(records[0].signal, SignalKind::Metric);
        assert_eq!(records[0].body, b"checkout.latency_ms");
        assert!(
            records[0]
                .attributes
                .iter()
                .any(|attr| attr.key == "otel.metric.value" && attr.value == "42.5")
        );
        assert!(
            records[0]
                .attributes
                .iter()
                .any(|attr| attr.key == "resource.service.name" && attr.value == "checkout")
        );
        assert_eq!(
            attr_value(&records[0], "otel.metric.exemplar.count"),
            Some("1")
        );
        assert_eq!(
            attr_value(&records[0], "otel.metric.exemplar.0.trace_id"),
            Some("01010101010101010101010101010101")
        );
    }

    #[test]
    fn converts_histogram_metric_export_request_to_records() {
        let request = ExportMetricsServiceRequest {
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
                        description: "request latency".to_string(),
                        unit: "ms".to_string(),
                        metadata: Vec::new(),
                        data: Some(metric::Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                attributes: vec![string_attr("route", "/checkout")],
                                start_time_unix_nano: 10,
                                time_unix_nano: 20,
                                count: 6,
                                sum: Some(125.5),
                                bucket_counts: vec![1, 2, 3],
                                explicit_bounds: vec![10.0, 50.0],
                                exemplars: vec![sample_exemplar()],
                                flags: 1,
                                min: Some(2.0),
                                max: Some(90.0),
                            }],
                            aggregation_temporality: AggregationTemporality::Cumulative as i32,
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let records = metrics_request_to_records("payments-prod", request);
        let record = &records[0];

        assert_eq!(records.len(), 1);
        assert_eq!(record.signal, SignalKind::Metric);
        assert_eq!(
            attr_value(record, "otel.metric.data_type"),
            Some("histogram")
        );
        assert_eq!(attr_value(record, "otel.metric.count"), Some("6"));
        assert_eq!(attr_value(record, "otel.metric.sum"), Some("125.5"));
        assert_eq!(
            attr_value(record, "otel.metric.bucket_counts"),
            Some("1,2,3")
        );
        assert_eq!(
            attr_value(record, "otel.metric.explicit_bounds"),
            Some("10,50")
        );
        assert_eq!(attr_value(record, "otel.metric.min"), Some("2"));
        assert_eq!(attr_value(record, "otel.metric.max"), Some("90"));
        assert_eq!(
            attr_value(record, "otel.metric.aggregation_temporality"),
            Some("2")
        );
        assert_eq!(
            attr_value(record, "otel.metric.aggregation_temporality_name"),
            Some("AGGREGATION_TEMPORALITY_CUMULATIVE")
        );
        assert_eq!(attr_value(record, "otel.metric.exemplar.count"), Some("1"));
        assert_eq!(attr_value(record, "route"), Some("/checkout"));
    }

    #[test]
    fn converts_exponential_histogram_metric_export_request_to_records() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![OtlpResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![string_attr("service.name", "checkout")],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_metrics: vec![OtlpScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "checkout.latency_ms".to_string(),
                        description: String::new(),
                        unit: "ms".to_string(),
                        metadata: Vec::new(),
                        data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                            data_points: vec![ExponentialHistogramDataPoint {
                                attributes: vec![string_attr("route", "/checkout")],
                                start_time_unix_nano: 10,
                                time_unix_nano: 20,
                                count: 8,
                                sum: Some(160.0),
                                scale: 2,
                                zero_count: 1,
                                positive: Some(exponential_histogram_data_point::Buckets {
                                    offset: -1,
                                    bucket_counts: vec![2, 3],
                                }),
                                negative: Some(exponential_histogram_data_point::Buckets {
                                    offset: 0,
                                    bucket_counts: vec![1],
                                }),
                                flags: 1,
                                exemplars: vec![sample_exemplar()],
                                min: Some(-2.0),
                                max: Some(16.0),
                                zero_threshold: 0.01,
                            }],
                            aggregation_temporality: AggregationTemporality::Delta as i32,
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let records = metrics_request_to_records("payments-prod", request);
        let record = &records[0];

        assert_eq!(records.len(), 1);
        assert_eq!(
            attr_value(record, "otel.metric.data_type"),
            Some("exponential_histogram")
        );
        assert_eq!(attr_value(record, "otel.metric.count"), Some("8"));
        assert_eq!(attr_value(record, "otel.metric.scale"), Some("2"));
        assert_eq!(attr_value(record, "otel.metric.zero_count"), Some("1"));
        assert_eq!(
            attr_value(record, "otel.metric.positive_bucket_counts"),
            Some("2,3")
        );
        assert_eq!(
            attr_value(record, "otel.metric.negative_bucket_counts"),
            Some("1")
        );
        assert_eq!(
            attr_value(record, "otel.metric.aggregation_temporality_name"),
            Some("AGGREGATION_TEMPORALITY_DELTA")
        );
        assert_eq!(
            attr_value(record, "otel.metric.exemplar.0.value"),
            Some("7")
        );
    }

    #[test]
    fn converts_summary_metric_export_request_to_records() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![OtlpResourceMetrics {
                resource: None,
                scope_metrics: vec![OtlpScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "checkout.latency_ms".to_string(),
                        description: String::new(),
                        unit: "ms".to_string(),
                        metadata: Vec::new(),
                        data: Some(metric::Data::Summary(Summary {
                            data_points: vec![SummaryDataPoint {
                                attributes: vec![string_attr("route", "/checkout")],
                                start_time_unix_nano: 10,
                                time_unix_nano: 20,
                                count: 10,
                                sum: 250.5,
                                quantile_values: vec![
                                    summary_data_point::ValueAtQuantile {
                                        quantile: 0.5,
                                        value: 20.0,
                                    },
                                    summary_data_point::ValueAtQuantile {
                                        quantile: 0.95,
                                        value: 80.0,
                                    },
                                ],
                                flags: 0,
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let records = metrics_request_to_records("payments-prod", request);
        let record = &records[0];

        assert_eq!(records.len(), 1);
        assert_eq!(record.signal, SignalKind::Metric);
        assert_eq!(attr_value(record, "otel.metric.data_type"), Some("summary"));
        assert_eq!(attr_value(record, "otel.metric.count"), Some("10"));
        assert_eq!(attr_value(record, "otel.metric.sum"), Some("250.5"));
        assert_eq!(
            attr_value(record, "otel.metric.quantile_values"),
            Some("0.5:20,0.95:80")
        );
        assert_eq!(attr_value(record, "route"), Some("/checkout"));
    }

    #[test]
    fn converts_log_export_request_to_records() {
        let request = ExportLogsServiceRequest {
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
        };

        let records = logs_request_to_records("payments-prod", request);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].tenant_id, "payments-prod");
        assert_eq!(records[0].signal, SignalKind::Log);
        assert_eq!(records[0].body, b"worker-ready");
        assert!(
            records[0]
                .attributes
                .iter()
                .any(|attr| attr.key == "otel.log.severity_text" && attr.value == "INFO")
        );
        assert!(
            records[0]
                .attributes
                .iter()
                .any(|attr| attr.key == "resource.service.name" && attr.value == "checkout")
        );
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

    fn sample_exemplar() -> Exemplar {
        Exemplar {
            filtered_attributes: vec![string_attr("trace.sampled", "true")],
            time_unix_nano: 15,
            span_id: vec![2; 8],
            trace_id: vec![1; 16],
            value: Some(exemplar::Value::AsInt(7)),
        }
    }

    fn attr_value<'a>(record: &'a TelemetryRecord, key: &str) -> Option<&'a str> {
        record
            .attributes
            .iter()
            .find(|attr| attr.key == key)
            .map(|attr| attr.value.as_str())
    }
}
