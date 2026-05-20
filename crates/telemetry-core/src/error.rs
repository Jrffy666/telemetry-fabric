use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineError {
    InvalidConfig(String),
    MissingRoute(String),
    UnknownExporter(String),
}

impl Display for PipelineError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(f, "invalid pipeline config: {message}"),
            Self::MissingRoute(signal) => write!(f, "missing route for signal: {signal}"),
            Self::UnknownExporter(name) => {
                write!(f, "unknown exporter referenced by route: {name}")
            }
        }
    }
}

impl Error for PipelineError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelemetryError {
    InvalidRecord(String),
    DecodeError(String),
    PayloadTooLarge(usize),
}

impl Display for TelemetryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRecord(message) => write!(f, "invalid telemetry record: {message}"),
            Self::DecodeError(message) => write!(f, "failed to decode telemetry record: {message}"),
            Self::PayloadTooLarge(size) => write!(f, "payload is too large: {size} bytes"),
        }
    }
}

impl Error for TelemetryError {}
