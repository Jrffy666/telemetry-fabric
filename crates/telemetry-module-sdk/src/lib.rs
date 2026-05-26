use std::error::Error;
use std::fmt::{Display, Formatter};
use std::time::{SystemTime, UNIX_EPOCH};
use telemetry_core::{Attribute, SignalKind, TelemetryRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum Priority {
    Low,
    #[default]
    Normal,
    Important,
    Critical,
}

impl Priority {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Normal => "normal",
            Self::Important => "important",
            Self::Critical => "critical",
        }
    }

    pub fn from_token(value: &str) -> Option<Self> {
        match normalize_token(value).as_str() {
            "low" => Some(Self::Low),
            "normal" | "default" => Some(Self::Normal),
            "important" | "high" => Some(Self::Important),
            "critical" | "urgent" => Some(Self::Critical),
            _ => None,
        }
    }
}

impl Display for Priority {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Payload {
    Bytes(Vec<u8>),
    Json(Vec<u8>),
}

impl Payload {
    pub fn bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self::Bytes(bytes.into())
    }

    pub fn json(json: impl Into<Vec<u8>>) -> Self {
        Self::Json(json.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Bytes(bytes) | Self::Json(bytes) => bytes,
        }
    }

    pub fn encoding(&self) -> &'static str {
        match self {
            Self::Bytes(_) => "application/octet-stream",
            Self::Json(_) => "application/json",
        }
    }

    pub fn is_empty(&self) -> bool {
        self.as_bytes().is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointMetadata {
    pub cursor: String,
    pub source: String,
    pub partition: String,
    pub offset: String,
    pub sequence: Option<String>,
    pub attributes: Vec<Attribute>,
}

impl CheckpointMetadata {
    pub fn new(
        cursor: impl Into<String>,
        source: impl Into<String>,
        partition: impl Into<String>,
        offset: impl Into<String>,
    ) -> Self {
        Self {
            cursor: cursor.into(),
            source: source.into(),
            partition: partition.into(),
            offset: offset.into(),
            sequence: None,
            attributes: Vec::new(),
        }
    }

    pub fn with_sequence(mut self, sequence: impl Into<String>) -> Self {
        self.sequence = Some(sequence.into());
        self
    }

    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.push(Attribute {
            key: key.into(),
            value: value.into(),
        });
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RoutingMetadata {
    pub route_key: Option<String>,
    pub partition_key: Option<String>,
    pub shard_key: Option<String>,
    pub preferred_exporters: Vec<String>,
    pub attributes: Vec<Attribute>,
}

impl RoutingMetadata {
    pub fn with_route_key(mut self, route_key: impl Into<String>) -> Self {
        self.route_key = Some(route_key.into());
        self
    }

    pub fn with_partition_key(mut self, partition_key: impl Into<String>) -> Self {
        self.partition_key = Some(partition_key.into());
        self
    }

    pub fn with_preferred_exporter(mut self, exporter: impl Into<String>) -> Self {
        self.preferred_exporters.push(exporter.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventEnvelope {
    pub tenant_id: String,
    pub module: String,
    pub source: String,
    pub event_type: String,
    pub priority: Priority,
    pub schema_version: Option<String>,
    pub event_time_unix_nanos: u128,
    pub ingest_time_unix_nanos: u128,
    pub dedupe_key: Option<String>,
    pub checkpoint: Option<CheckpointMetadata>,
    pub attributes: Vec<Attribute>,
    pub routing: RoutingMetadata,
    pub payload: Payload,
}

impl EventEnvelope {
    pub fn new(
        tenant_id: impl Into<String>,
        module: impl Into<String>,
        source: impl Into<String>,
        event_type: impl Into<String>,
        payload: Payload,
    ) -> Self {
        let now = now_unix_nanos();
        Self {
            tenant_id: tenant_id.into(),
            module: module.into(),
            source: source.into(),
            event_type: event_type.into(),
            priority: Priority::Normal,
            schema_version: None,
            event_time_unix_nanos: now,
            ingest_time_unix_nanos: now,
            dedupe_key: None,
            checkpoint: None,
            attributes: Vec::new(),
            routing: RoutingMetadata::default(),
            payload,
        }
    }

    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_schema_version(mut self, schema_version: impl Into<String>) -> Self {
        self.schema_version = Some(schema_version.into());
        self
    }

    pub fn with_dedupe_key(mut self, dedupe_key: impl Into<String>) -> Self {
        self.dedupe_key = Some(dedupe_key.into());
        self
    }

    pub fn with_checkpoint(mut self, checkpoint: CheckpointMetadata) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }

    pub fn with_routing(mut self, routing: RoutingMetadata) -> Self {
        self.routing = routing;
        self
    }

    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.push(Attribute {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    pub fn route_key(&self) -> String {
        self.routing.route_key.clone().unwrap_or_else(|| {
            format!(
                "{}/{}/{}",
                self.module.trim(),
                self.source.trim(),
                self.event_type.trim()
            )
        })
    }

    pub fn validate(&self) -> Result<(), EnvelopeError> {
        require_non_empty("tenant_id", &self.tenant_id)?;
        require_non_empty("module", &self.module)?;
        require_non_empty("source", &self.source)?;
        require_non_empty("event_type", &self.event_type)?;

        if self.payload.is_empty() {
            return Err(EnvelopeError::InvalidEnvelope(
                "payload must not be empty".to_string(),
            ));
        }
        for attribute in &self.attributes {
            require_non_empty("attribute.key", &attribute.key)?;
        }
        for attribute in &self.routing.attributes {
            require_non_empty("routing.attribute.key", &attribute.key)?;
        }
        if let Some(checkpoint) = &self.checkpoint {
            checkpoint.validate()?;
        }
        Ok(())
    }

    pub fn into_telemetry_record(self) -> Result<TelemetryRecord, EnvelopeError> {
        self.validate()?;

        let route_key = self.route_key();
        let payload_encoding = self.payload.encoding();
        let body = self.payload.as_bytes().to_vec();
        let mut record = TelemetryRecord::new(self.tenant_id, SignalKind::Log, body)
            .with_attribute("module.name", self.module)
            .with_attribute("module.source", self.source)
            .with_attribute("module.event_type", self.event_type)
            .with_attribute("module.priority", self.priority.as_str())
            .with_attribute("module.payload_encoding", payload_encoding)
            .with_attribute("module.route_key", route_key)
            .with_attribute(
                "module.event_time_unix_nanos",
                self.event_time_unix_nanos.to_string(),
            )
            .with_attribute(
                "module.ingest_time_unix_nanos",
                self.ingest_time_unix_nanos.to_string(),
            );

        if let Some(schema_version) = self.schema_version {
            record = record.with_attribute("module.schema_version", schema_version);
        }
        if let Some(dedupe_key) = self.dedupe_key {
            record = record.with_attribute("module.dedupe_key", dedupe_key);
        }
        if let Some(partition_key) = self.routing.partition_key {
            record = record.with_attribute("module.routing.partition_key", partition_key);
        }
        if let Some(shard_key) = self.routing.shard_key {
            record = record.with_attribute("module.routing.shard_key", shard_key);
        }
        for exporter in self.routing.preferred_exporters {
            record = record.with_attribute("module.routing.preferred_exporter", exporter);
        }
        for attribute in self.routing.attributes {
            record = record.with_attribute(
                format!("module.routing.attribute.{}", attribute.key),
                attribute.value,
            );
        }
        if let Some(checkpoint) = self.checkpoint {
            record = record
                .with_attribute("module.checkpoint.cursor", checkpoint.cursor)
                .with_attribute("module.checkpoint.source", checkpoint.source)
                .with_attribute("module.checkpoint.partition", checkpoint.partition)
                .with_attribute("module.checkpoint.offset", checkpoint.offset);
            if let Some(sequence) = checkpoint.sequence {
                record = record.with_attribute("module.checkpoint.sequence", sequence);
            }
            for attribute in checkpoint.attributes {
                record = record.with_attribute(
                    format!("module.checkpoint.attribute.{}", attribute.key),
                    attribute.value,
                );
            }
        }
        for attribute in self.attributes {
            record = record.with_attribute(attribute.key, attribute.value);
        }

        Ok(record)
    }
}

impl CheckpointMetadata {
    fn validate(&self) -> Result<(), EnvelopeError> {
        require_non_empty("checkpoint.cursor", &self.cursor)?;
        require_non_empty("checkpoint.source", &self.source)?;
        require_non_empty("checkpoint.partition", &self.partition)?;
        require_non_empty("checkpoint.offset", &self.offset)?;
        for attribute in &self.attributes {
            require_non_empty("checkpoint.attribute.key", &attribute.key)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvelopeError {
    InvalidEnvelope(String),
}

impl Display for EnvelopeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidEnvelope(message) => write!(f, "invalid event envelope: {message}"),
        }
    }
}

impl Error for EnvelopeError {}

fn require_non_empty(field: &str, value: &str) -> Result<(), EnvelopeError> {
    if value.trim().is_empty() {
        return Err(EnvelopeError::InvalidEnvelope(format!(
            "{field} must not be empty"
        )));
    }
    Ok(())
}

fn normalize_token(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('_', "-")
}

fn now_unix_nanos() -> u128 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos(),
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_converts_to_generic_telemetry_record() -> Result<(), Box<dyn Error>> {
        let envelope = EventEnvelope::new(
            "tenant-a",
            "payments",
            "checkout-worker",
            "payment.authorized",
            Payload::json(br#"{"amount":"42.00"}"#.to_vec()),
        )
        .with_priority(Priority::Important)
        .with_schema_version("payments.payment.v1.0.0")
        .with_dedupe_key("payments:authorized:123")
        .with_checkpoint(
            CheckpointMetadata::new("cursor-1", "payments", "partition-0", "123")
                .with_sequence("7")
                .with_attribute("region", "us-east-1"),
        )
        .with_routing(
            RoutingMetadata::default()
                .with_route_key("payments/authorized")
                .with_partition_key("merchant-1")
                .with_preferred_exporter("kafka-critical"),
        )
        .with_attribute("merchant_id", "merchant-1");

        let record = envelope.into_telemetry_record()?;

        assert_eq!(record.tenant_id, "tenant-a");
        assert_eq!(record.signal, SignalKind::Log);
        assert_eq!(record.body, br#"{"amount":"42.00"}"#);
        assert!(
            record
                .attributes
                .iter()
                .any(|attribute| attribute.key == "module.name" && attribute.value == "payments")
        );
        assert!(record.attributes.iter().any(|attribute| {
            attribute.key == "module.payload_encoding" && attribute.value == "application/json"
        }));
        assert!(record.attributes.iter().any(|attribute| {
            attribute.key == "module.routing.preferred_exporter"
                && attribute.value == "kafka-critical"
        }));
        Ok(())
    }

    #[test]
    fn envelope_rejects_empty_module() {
        let envelope = EventEnvelope::new("tenant-a", "", "source", "event", Payload::bytes([1]));

        assert!(matches!(
            envelope.validate(),
            Err(EnvelopeError::InvalidEnvelope(_))
        ));
    }

    #[test]
    fn priority_parses_stable_tokens() {
        assert_eq!(Priority::from_token("normal"), Some(Priority::Normal));
        assert_eq!(Priority::from_token("HIGH"), Some(Priority::Important));
        assert_eq!(Priority::from_token("urgent"), Some(Priority::Critical));
        assert_eq!(Priority::from_token("unknown"), None);
    }
}
