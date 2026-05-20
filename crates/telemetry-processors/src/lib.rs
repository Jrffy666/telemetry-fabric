use std::error::Error;
use std::fmt::{Display, Formatter};
use telemetry_core::{PipelineConfig, ProcessorKind, TelemetryRecord};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessingError {
    InvalidProcessor(String),
}

impl Display for ProcessingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidProcessor(message) => write!(f, "invalid processor: {message}"),
        }
    }
}

impl Error for ProcessingError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessDecision {
    Emit(TelemetryRecord),
    Drop { reason: String },
}

pub trait RecordProcessor: Send {
    fn name(&self) -> &str;
    fn process(&mut self, record: TelemetryRecord) -> Result<ProcessDecision, ProcessingError>;
}

#[derive(Default)]
pub struct ProcessorChain {
    processors: Vec<Box<dyn RecordProcessor>>,
    dropped_records: u64,
}

impl ProcessorChain {
    pub fn from_config(config: &PipelineConfig) -> Self {
        let mut chain = Self::default();

        for processor in config
            .processors
            .iter()
            .filter(|processor| processor.enabled)
        {
            match processor.kind {
                ProcessorKind::MemoryLimiter => {
                    chain.push(MemoryLimiterProcessor::new(
                        processor.name.clone(),
                        config.limits.max_record_bytes as usize,
                    ));
                }
                ProcessorKind::Redact => {
                    chain.push(RedactProcessor::new(
                        processor.name.clone(),
                        [
                            "authorization",
                            "http.request.header.authorization",
                            "db.statement",
                            "password",
                            "secret",
                            "token",
                        ],
                    ));
                }
                ProcessorKind::Batch
                | ProcessorKind::TailSampling
                | ProcessorKind::TenantRateLimit => {}
            }
        }

        chain
    }

    pub fn push(&mut self, processor: impl RecordProcessor + 'static) {
        self.processors.push(Box::new(processor));
    }

    pub fn process(
        &mut self,
        mut record: TelemetryRecord,
    ) -> Result<Option<TelemetryRecord>, ProcessingError> {
        for processor in &mut self.processors {
            match processor.process(record)? {
                ProcessDecision::Emit(processed) => {
                    record = processed;
                }
                ProcessDecision::Drop { reason: _ } => {
                    self.dropped_records += 1;
                    return Ok(None);
                }
            }
        }

        Ok(Some(record))
    }

    pub fn dropped_records(&self) -> u64 {
        self.dropped_records
    }
}

pub struct MemoryLimiterProcessor {
    name: String,
    max_record_bytes: usize,
}

impl MemoryLimiterProcessor {
    pub fn new(name: impl Into<String>, max_record_bytes: usize) -> Self {
        Self {
            name: name.into(),
            max_record_bytes,
        }
    }
}

impl RecordProcessor for MemoryLimiterProcessor {
    fn name(&self) -> &str {
        &self.name
    }

    fn process(&mut self, record: TelemetryRecord) -> Result<ProcessDecision, ProcessingError> {
        let estimated = estimate_record_bytes(&record);
        if estimated > self.max_record_bytes {
            return Ok(ProcessDecision::Drop {
                reason: format!(
                    "estimated record size {estimated} exceeds limit {}",
                    self.max_record_bytes
                ),
            });
        }

        Ok(ProcessDecision::Emit(record))
    }
}

pub struct RedactProcessor {
    name: String,
    sensitive_keys: Vec<String>,
}

impl RedactProcessor {
    pub fn new<I, S>(name: impl Into<String>, sensitive_keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            name: name.into(),
            sensitive_keys: sensitive_keys
                .into_iter()
                .map(|key| key.into().to_ascii_lowercase())
                .collect(),
        }
    }
}

impl RecordProcessor for RedactProcessor {
    fn name(&self) -> &str {
        &self.name
    }

    fn process(&mut self, mut record: TelemetryRecord) -> Result<ProcessDecision, ProcessingError> {
        for attribute in &mut record.attributes {
            let key = attribute.key.to_ascii_lowercase();
            if self
                .sensitive_keys
                .iter()
                .any(|sensitive| key == *sensitive || key.ends_with(sensitive))
            {
                attribute.value = "[REDACTED]".to_string();
            }
        }

        Ok(ProcessDecision::Emit(record))
    }
}

fn estimate_record_bytes(record: &TelemetryRecord) -> usize {
    let attributes = record
        .attributes
        .iter()
        .map(|attribute| attribute.key.len() + attribute.value.len())
        .sum::<usize>();

    record.tenant_id.len() + record.body.len() + attributes + 32
}

#[cfg(test)]
mod tests {
    use super::*;
    use telemetry_core::SignalKind;

    #[test]
    fn redact_processor_masks_sensitive_attributes() -> Result<(), Box<dyn Error>> {
        let record = TelemetryRecord::new("tenant-a", SignalKind::Trace, b"body".to_vec())
            .with_attribute("http.request.header.authorization", "Bearer secret")
            .with_attribute("service.name", "checkout");
        let mut processor = RedactProcessor::new("redact", ["authorization"]);

        let ProcessDecision::Emit(processed) = processor.process(record)? else {
            return Err("record was unexpectedly dropped".into());
        };

        assert_eq!(processed.attributes[0].value, "[REDACTED]");
        assert_eq!(processed.attributes[1].value, "checkout");
        Ok(())
    }

    #[test]
    fn memory_limiter_drops_oversized_records() -> Result<(), Box<dyn Error>> {
        let record = TelemetryRecord::new("tenant-a", SignalKind::Log, vec![0_u8; 128]);
        let mut processor = MemoryLimiterProcessor::new("memory-limiter", 64);

        let decision = processor.process(record)?;

        assert!(matches!(decision, ProcessDecision::Drop { .. }));
        Ok(())
    }
}
