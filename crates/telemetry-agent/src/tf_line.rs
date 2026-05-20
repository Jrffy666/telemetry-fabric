use std::error::Error;
use telemetry_core::{SignalKind, TelemetryRecord};

pub fn parse_line_record(line: &str) -> Result<TelemetryRecord, Box<dyn Error + Send + Sync>> {
    let mut parts = line.trim().splitn(4, char::is_whitespace);
    let signal = parts
        .next()
        .and_then(SignalKind::from_token)
        .ok_or_else(|| format!("invalid signal in line: {line}"))?;
    let tenant = parts
        .next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing tenant in line: {line}"))?;
    let service = parts
        .next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing service in line: {line}"))?;
    let body = parts
        .next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing body in line: {line}"))?;

    Ok(
        TelemetryRecord::new(tenant, signal, body.as_bytes().to_vec())
            .with_attribute("service.name", service),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_line_protocol_record() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let record = parse_line_record("trace payments-prod checkout request-started")?;

        assert_eq!(record.signal, SignalKind::Trace);
        assert_eq!(record.tenant_id, "payments-prod");
        assert_eq!(record.attributes[0].value, "checkout");
        assert_eq!(record.body, b"request-started");
        Ok(())
    }
}
