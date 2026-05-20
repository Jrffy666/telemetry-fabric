use serde_yaml::Value;
use std::error::Error;
use std::fmt::{Display, Formatter};
use telemetry_core::PipelineConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::config_file::parse_pipeline_config;

type DynError = Box<dyn Error + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlClient {
    endpoint: HttpEndpoint,
    agent_id: String,
    tenant_id: String,
    hostname: String,
    version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpEndpoint {
    host: String,
    port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterResponse {
    pub accepted: bool,
    pub config_version: u64,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlCommand {
    pub command_id: String,
    pub kind: ControlCommandKind,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlCommandKind {
    ReloadConfig,
    DrainAndRestart,
    PauseExports,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigUpdate {
    pub version: u64,
    pub pipeline_config: String,
    pub checksum: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatStats {
    pub config_version: u64,
    pub queue_depth_bytes: u64,
    pub ingest_bytes_per_second: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ControlClientError(String);

impl Display for ControlClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for ControlClientError {}

impl ControlClient {
    pub fn new(
        endpoint: &str,
        agent_id: String,
        tenant_id: String,
        hostname: String,
        version: String,
    ) -> Result<Self, DynError> {
        Ok(Self {
            endpoint: parse_http_endpoint(endpoint)?,
            agent_id,
            tenant_id,
            hostname,
            version,
        })
    }

    pub async fn register(&self) -> Result<RegisterResponse, DynError> {
        let body = format!(
            "{{\"agent_id\":\"{}\",\"tenant_id\":\"{}\",\"hostname\":\"{}\",\"version\":\"{}\"}}",
            json_escape(&self.agent_id),
            json_escape(&self.tenant_id),
            json_escape(&self.hostname),
            json_escape(&self.version)
        );
        let response = self.post_json("/v1/agents/register", &body).await?;
        parse_register_response(&response)
    }

    pub async fn heartbeat(&self, stats: HeartbeatStats) -> Result<Vec<ControlCommand>, DynError> {
        let body = format!(
            "{{\"agent_id\":\"{}\",\"tenant_id\":\"{}\",\"config_version\":{},\"queue_depth_bytes\":{},\"ingest_bytes_per_second\":{}}}",
            json_escape(&self.agent_id),
            json_escape(&self.tenant_id),
            stats.config_version,
            stats.queue_depth_bytes,
            stats.ingest_bytes_per_second
        );
        let response = self.post_json("/v1/agents/heartbeat", &body).await?;
        parse_commands_response(&response)
    }

    pub async fn config_update(
        &self,
        current_version: u64,
    ) -> Result<Option<ConfigUpdate>, DynError> {
        let body = format!(
            "{{\"agent_id\":\"{}\",\"tenant_id\":\"{}\",\"current_version\":{}}}",
            json_escape(&self.agent_id),
            json_escape(&self.tenant_id),
            current_version
        );
        let response = self.post_json("/v1/agents/config", &body).await?;
        parse_config_update_response(&response)
    }

    async fn post_json(&self, path: &str, body: &str) -> Result<Value, DynError> {
        let mut stream = TcpStream::connect((self.endpoint.host.as_str(), self.endpoint.port))
            .await
            .map_err(|err| {
                ControlClientError(format!(
                    "failed to connect to control endpoint {}:{}: {err}",
                    self.endpoint.host, self.endpoint.port
                ))
            })?;

        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            self.endpoint.host,
            body.len()
        );
        stream.write_all(request.as_bytes()).await?;

        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        let response = String::from_utf8(response)?;
        parse_http_json_response(&response)
    }
}

pub fn validate_config_update(update: &ConfigUpdate) -> Result<PipelineConfig, DynError> {
    let actual = sha256_hex(update.pipeline_config.as_bytes());
    if !actual.eq_ignore_ascii_case(&update.checksum) {
        return Err(ControlClientError(format!(
            "control config checksum mismatch: expected={} actual={actual}",
            update.checksum
        ))
        .into());
    }

    parse_pipeline_config(&update.pipeline_config)
}

fn parse_http_endpoint(endpoint: &str) -> Result<HttpEndpoint, DynError> {
    let endpoint = endpoint.trim();
    let Some(without_scheme) = endpoint.strip_prefix("http://") else {
        return Err(ControlClientError(
            "control endpoint must use http:// for the MVP client".to_string(),
        )
        .into());
    };

    if without_scheme.contains('/') {
        return Err(ControlClientError(
            "control endpoint path prefixes are not supported yet".to_string(),
        )
        .into());
    }

    let (host, port) = if let Some((host, port)) = without_scheme.rsplit_once(':') {
        (host.to_string(), port.parse::<u16>()?)
    } else {
        (without_scheme.to_string(), 80)
    };

    if host.trim().is_empty() {
        return Err(
            ControlClientError("control endpoint host must not be empty".to_string()).into(),
        );
    }

    Ok(HttpEndpoint { host, port })
}

fn parse_http_json_response(response: &str) -> Result<Value, DynError> {
    let (head, body) = response.split_once("\r\n\r\n").ok_or_else(|| {
        ControlClientError("control endpoint returned an invalid HTTP response".to_string())
    })?;
    let status_line = head.lines().next().ok_or_else(|| {
        ControlClientError("control endpoint returned an empty HTTP response".to_string())
    })?;
    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| ControlClientError("missing HTTP status code".to_string()))?
        .parse::<u16>()?;

    if !(200..300).contains(&status_code) {
        return Err(ControlClientError(format!(
            "control endpoint returned HTTP {status_code}: {body}"
        ))
        .into());
    }

    Ok(serde_yaml::from_str(body)?)
}

fn parse_register_response(value: &Value) -> Result<RegisterResponse, DynError> {
    let agent = get_field(value, "agent")?;
    Ok(RegisterResponse {
        accepted: get_bool(agent, "accepted")?,
        config_version: get_u64(agent, "config_version")?,
        message: get_string(agent, "message")?.to_string(),
    })
}

fn parse_commands_response(value: &Value) -> Result<Vec<ControlCommand>, DynError> {
    let commands = get_field(value, "commands")?
        .as_sequence()
        .ok_or_else(|| ControlClientError("commands must be an array".to_string()))?;
    commands.iter().map(parse_command).collect()
}

fn parse_command(value: &Value) -> Result<ControlCommand, DynError> {
    Ok(ControlCommand {
        command_id: get_string(value, "command_id")?.to_string(),
        kind: parse_command_kind(get_string(value, "kind")?),
        reason: get_string(value, "reason")?.to_string(),
    })
}

fn parse_config_update_response(value: &Value) -> Result<Option<ConfigUpdate>, DynError> {
    let update = get_field(value, "update")?;
    if update.is_null() {
        return Ok(None);
    }

    Ok(Some(ConfigUpdate {
        version: get_u64(update, "version")?,
        pipeline_config: get_string(update, "pipeline_config")?.to_string(),
        checksum: get_string(update, "checksum")?.to_string(),
    }))
}

fn parse_command_kind(value: &str) -> ControlCommandKind {
    match value {
        "reload_config" => ControlCommandKind::ReloadConfig,
        "drain_and_restart" => ControlCommandKind::DrainAndRestart,
        "pause_exports" => ControlCommandKind::PauseExports,
        _ => ControlCommandKind::Unknown,
    }
}

fn get_field<'a>(value: &'a Value, key: &str) -> Result<&'a Value, DynError> {
    let mapping = value
        .as_mapping()
        .ok_or_else(|| ControlClientError("expected JSON object".to_string()))?;
    mapping.get(Value::String(key.to_string())).ok_or_else(|| {
        ControlClientError(format!("missing field in control response: {key}")).into()
    })
}

fn get_string<'a>(value: &'a Value, key: &str) -> Result<&'a str, DynError> {
    get_field(value, key)?
        .as_str()
        .ok_or_else(|| ControlClientError(format!("field must be a string: {key}")).into())
}

fn get_bool(value: &Value, key: &str) -> Result<bool, DynError> {
    get_field(value, key)?
        .as_bool()
        .ok_or_else(|| ControlClientError(format!("field must be a boolean: {key}")).into())
}

fn get_u64(value: &Value, key: &str) -> Result<u64, DynError> {
    get_field(value, key)?.as_u64().ok_or_else(|| {
        ControlClientError(format!("field must be an unsigned integer: {key}")).into()
    })
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

fn sha256_hex(input: &[u8]) -> String {
    let mut state = [
        0x6a09e667_u32,
        0xbb67ae85,
        0x3c6ef372,
        0xa54ff53a,
        0x510e527f,
        0x9b05688c,
        0x1f83d9ab,
        0x5be0cd19,
    ];
    let mut message = input.to_vec();
    let bit_len = (message.len() as u64) * 8;
    message.push(0x80);
    while (message.len() % 64) != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in message.chunks_exact(64) {
        let mut words = [0_u32; 64];
        for (index, word) in words.iter_mut().take(16).enumerate() {
            let offset = index * 4;
            *word = u32::from_be_bytes([
                chunk[offset],
                chunk[offset + 1],
                chunk[offset + 2],
                chunk[offset + 3],
            ]);
        }
        for index in 16..64 {
            let s0 = words[index - 15].rotate_right(7)
                ^ words[index - 15].rotate_right(18)
                ^ (words[index - 15] >> 3);
            let s1 = words[index - 2].rotate_right(17)
                ^ words[index - 2].rotate_right(19)
                ^ (words[index - 2] >> 10);
            words[index] = words[index - 16]
                .wrapping_add(s0)
                .wrapping_add(words[index - 7])
                .wrapping_add(s1);
        }

        let mut a = state[0];
        let mut b = state[1];
        let mut c = state[2];
        let mut d = state[3];
        let mut e = state[4];
        let mut f = state[5];
        let mut g = state[6];
        let mut h = state[7];

        for index in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = h
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(SHA256_K[index])
                .wrapping_add(words[index]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
        state[4] = state[4].wrapping_add(e);
        state[5] = state[5].wrapping_add(f);
        state[6] = state[6].wrapping_add(g);
        state[7] = state[7].wrapping_add(h);
    }

    let mut output = String::with_capacity(64);
    for word in state {
        output.push_str(&format!("{word:08x}"));
    }
    output
}

const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_http_endpoint() -> Result<(), DynError> {
        assert_eq!(
            parse_http_endpoint("http://127.0.0.1:4001")?,
            HttpEndpoint {
                host: "127.0.0.1".to_string(),
                port: 4001
            }
        );
        assert!(parse_http_endpoint("https://127.0.0.1:4001").is_err());
        Ok(())
    }

    #[test]
    fn sha256_matches_known_vector() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn parses_config_update_response() -> Result<(), DynError> {
        let payload = r#"
tenant: "payments-prod"
pipeline: "default"
receivers:
  tf-line:
    protocol: "tf_line"
    endpoint: "127.0.0.1:4319"
processors:
  memory-limiter:
    enabled: true
exporters:
  stdout:
    protocol: "stdout"
    endpoint: "stdout://local"
    tls: false
routes:
  traces:
    exporters: ["stdout"]
"#;
        let checksum = sha256_hex(payload.as_bytes());
        let response = serde_yaml::from_str(&format!(
            "{{\"update\":{{\"version\":1,\"pipeline_config\":{},\"checksum\":\"{}\"}}}}",
            yaml_json_string(payload),
            checksum
        ))?;

        let update = parse_config_update_response(&response)?.ok_or_else(|| {
            ControlClientError("expected config update in test response".to_string())
        })?;
        let config = validate_config_update(&update)?;

        assert_eq!(config.tenant_id, "payments-prod");
        assert_eq!(config.name, "default");
        Ok(())
    }

    fn yaml_json_string(value: &str) -> String {
        format!("\"{}\"", json_escape(value))
    }
}
