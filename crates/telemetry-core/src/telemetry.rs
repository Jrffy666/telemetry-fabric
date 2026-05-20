use crate::error::TelemetryError;
use std::fmt::{Display, Formatter};
use std::time::{SystemTime, UNIX_EPOCH};

const RECORD_MAGIC: &[u8; 4] = b"TFB1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignalKind {
    Trace,
    Metric,
    Log,
}

impl SignalKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Metric => "metric",
            Self::Log => "log",
        }
    }

    pub fn from_token(value: &str) -> Option<Self> {
        match value {
            "trace" | "traces" => Some(Self::Trace),
            "metric" | "metrics" => Some(Self::Metric),
            "log" | "logs" => Some(Self::Log),
            _ => None,
        }
    }

    fn to_wire(self) -> u8 {
        match self {
            Self::Trace => 1,
            Self::Metric => 2,
            Self::Log => 3,
        }
    }

    fn from_wire(value: u8) -> Result<Self, TelemetryError> {
        match value {
            1 => Ok(Self::Trace),
            2 => Ok(Self::Metric),
            3 => Ok(Self::Log),
            other => Err(TelemetryError::DecodeError(format!(
                "unknown signal kind: {other}"
            ))),
        }
    }
}

impl Display for SignalKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attribute {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryRecord {
    pub tenant_id: String,
    pub signal: SignalKind,
    pub timestamp_unix_nanos: u128,
    pub attributes: Vec<Attribute>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatch {
    pub records: Vec<TelemetryRecord>,
}

impl TelemetryRecord {
    pub fn new(tenant_id: impl Into<String>, signal: SignalKind, body: impl Into<Vec<u8>>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            signal,
            timestamp_unix_nanos: now_unix_nanos(),
            attributes: Vec::new(),
            body: body.into(),
        }
    }

    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.push(Attribute {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    pub fn validate(&self, max_record_bytes: u32) -> Result<(), TelemetryError> {
        if self.tenant_id.trim().is_empty() {
            return Err(TelemetryError::InvalidRecord(
                "tenant_id must not be empty".to_string(),
            ));
        }
        if self.body.len() > max_record_bytes as usize {
            return Err(TelemetryError::PayloadTooLarge(self.body.len()));
        }
        for attribute in &self.attributes {
            if attribute.key.trim().is_empty() {
                return Err(TelemetryError::InvalidRecord(
                    "attribute keys must not be empty".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, TelemetryError> {
        self.validate(u32::MAX)?;

        let tenant = self.tenant_id.as_bytes();
        if tenant.len() > u16::MAX as usize {
            return Err(TelemetryError::PayloadTooLarge(tenant.len()));
        }
        if self.attributes.len() > u16::MAX as usize {
            return Err(TelemetryError::PayloadTooLarge(self.attributes.len()));
        }
        if self.body.len() > u32::MAX as usize {
            return Err(TelemetryError::PayloadTooLarge(self.body.len()));
        }

        let mut output = Vec::with_capacity(32 + tenant.len() + self.body.len());
        output.extend_from_slice(RECORD_MAGIC);
        output.push(self.signal.to_wire());
        output.extend_from_slice(&self.timestamp_unix_nanos.to_le_bytes());
        write_u16(&mut output, tenant.len() as u16);
        output.extend_from_slice(tenant);
        write_u16(&mut output, self.attributes.len() as u16);

        for attribute in &self.attributes {
            write_string(&mut output, &attribute.key)?;
            write_string(&mut output, &attribute.value)?;
        }

        write_u32(&mut output, self.body.len() as u32);
        output.extend_from_slice(&self.body);
        Ok(output)
    }

    pub fn decode(input: &[u8]) -> Result<Self, TelemetryError> {
        let mut cursor = Cursor::new(input);
        let magic = cursor.read_exact(4)?;
        if magic != RECORD_MAGIC {
            return Err(TelemetryError::DecodeError(
                "record magic does not match".to_string(),
            ));
        }

        let signal = SignalKind::from_wire(cursor.read_u8()?)?;
        let timestamp_unix_nanos = cursor.read_u128()?;
        let tenant_len = cursor.read_u16()? as usize;
        let tenant_id = cursor.read_string(tenant_len)?;
        let attribute_count = cursor.read_u16()? as usize;
        let mut attributes = Vec::with_capacity(attribute_count);

        for _ in 0..attribute_count {
            let key_len = cursor.read_u16()? as usize;
            let key = cursor.read_string(key_len)?;
            let value_len = cursor.read_u16()? as usize;
            let value = cursor.read_string(value_len)?;
            attributes.push(Attribute { key, value });
        }

        let body_len = cursor.read_u32()? as usize;
        let body = cursor.read_exact(body_len)?.to_vec();
        if !cursor.is_finished() {
            return Err(TelemetryError::DecodeError(
                "record has trailing bytes".to_string(),
            ));
        }

        Ok(Self {
            tenant_id,
            signal,
            timestamp_unix_nanos,
            attributes,
            body,
        })
    }
}

impl RecordBatch {
    pub fn new(records: Vec<TelemetryRecord>) -> Self {
        Self { records }
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }
}

fn now_unix_nanos() -> u128 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos(),
        Err(_) => 0,
    }
}

fn write_string(output: &mut Vec<u8>, value: &str) -> Result<(), TelemetryError> {
    let bytes = value.as_bytes();
    if bytes.len() > u16::MAX as usize {
        return Err(TelemetryError::PayloadTooLarge(bytes.len()));
    }
    write_u16(output, bytes.len() as u16);
    output.extend_from_slice(bytes);
    Ok(())
}

fn write_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn write_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

struct Cursor<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], TelemetryError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| TelemetryError::DecodeError("offset overflow".to_string()))?;
        if end > self.input.len() {
            return Err(TelemetryError::DecodeError(
                "record ended unexpectedly".to_string(),
            ));
        }
        let bytes = &self.input[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> Result<u8, TelemetryError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, TelemetryError> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> Result<u32, TelemetryError> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u128(&mut self) -> Result<u128, TelemetryError> {
        let bytes = self.read_exact(16)?;
        let mut output = [0_u8; 16];
        output.copy_from_slice(bytes);
        Ok(u128::from_le_bytes(output))
    }

    fn read_string(&mut self, len: usize) -> Result<String, TelemetryError> {
        let bytes = self.read_exact(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|err| TelemetryError::DecodeError(err.to_string()))
    }

    fn is_finished(&self) -> bool {
        self.offset == self.input.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_round_trip_preserves_data() -> Result<(), Box<dyn std::error::Error>> {
        let record = TelemetryRecord::new("payments-prod", SignalKind::Trace, b"hello".to_vec())
            .with_attribute("service.name", "checkout");

        let encoded = record.encode()?;
        let decoded = TelemetryRecord::decode(&encoded)?;

        assert_eq!(decoded.tenant_id, "payments-prod");
        assert_eq!(decoded.signal, SignalKind::Trace);
        assert_eq!(decoded.attributes[0].key, "service.name");
        assert_eq!(decoded.body, b"hello");
        Ok(())
    }

    #[test]
    fn signal_kind_parses_stable_tokens() {
        assert_eq!(SignalKind::from_token("trace"), Some(SignalKind::Trace));
        assert_eq!(SignalKind::from_token("metrics"), Some(SignalKind::Metric));
        assert_eq!(SignalKind::from_token("logs"), Some(SignalKind::Log));
        assert_eq!(SignalKind::from_token("span"), None);
    }
}
