use serde_yaml::Value;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs;
use std::io::BufReader;
use std::sync::Arc;
use telemetry_core::{PipelineConfig, TlsConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};

use crate::config_file::parse_pipeline_config;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};

type DynError = Box<dyn Error + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlClient {
    endpoint: HttpEndpoint,
    agent_id: String,
    tenant_id: String,
    hostname: String,
    version: String,
    security: ControlClientSecurity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpEndpoint {
    scheme: HttpScheme,
    host: String,
    port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HttpScheme {
    Http,
    Https,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ControlClientSecurity {
    pub auth_token: Option<String>,
    pub tls: TlsConfig,
    pub config_signing_key: Option<String>,
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
    pub status: ControlCommandStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlCommandKind {
    ReloadConfig,
    DrainAndRestart,
    PauseExports,
    ResumeExports,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlCommandStatus {
    Pending,
    Delivered,
    Succeeded,
    Failed,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigUpdate {
    pub version: u64,
    pub pipeline_config: String,
    pub checksum: String,
    pub signature: Option<String>,
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
    pub fn new_with_security(
        endpoint: &str,
        agent_id: String,
        tenant_id: String,
        hostname: String,
        version: String,
        security: ControlClientSecurity,
    ) -> Result<Self, DynError> {
        Ok(Self {
            endpoint: parse_http_endpoint(endpoint)?,
            agent_id,
            tenant_id,
            hostname,
            version,
            security,
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

    pub async fn ack_command(
        &self,
        command_id: &str,
        success: bool,
        error: Option<&str>,
    ) -> Result<(), DynError> {
        let error_field = match error {
            Some(error) => format!(",\"error\":\"{}\"", json_escape(error)),
            None => String::new(),
        };
        let body = format!(
            "{{\"agent_id\":\"{}\",\"tenant_id\":\"{}\",\"command_id\":\"{}\",\"success\":{}{error_field}}}",
            json_escape(&self.agent_id),
            json_escape(&self.tenant_id),
            json_escape(command_id),
            success
        );
        self.post_json("/v1/agents/commands/ack", &body).await?;
        Ok(())
    }

    pub fn validate_config_update(
        &self,
        update: &ConfigUpdate,
    ) -> Result<PipelineConfig, DynError> {
        validate_config_update(update, self.security.config_signing_key.as_deref())
    }

    async fn post_json(&self, path: &str, body: &str) -> Result<Value, DynError> {
        let request = self.render_post_request(path, body)?;
        let mut stream = self
            .endpoint
            .connect(&self.security.tls)
            .await
            .map_err(|err| {
                ControlClientError(format!(
                    "failed to connect to control endpoint {}:{}: {err}",
                    self.endpoint.host, self.endpoint.port
                ))
            })?;
        stream.write_all(request.as_bytes()).await?;
        stream.write_all(body.as_bytes()).await?;
        stream.shutdown().await?;

        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        let response = String::from_utf8(response)?;
        parse_http_json_response(&response)
    }

    fn render_post_request(&self, path: &str, body: &str) -> Result<String, DynError> {
        let mut request = format!(
            "POST {path} HTTP/1.1\r\nHost: {}:{}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
            self.endpoint.host,
            self.endpoint.port,
            body.len()
        );

        if let Some(token) = &self.security.auth_token {
            if token.contains('\r') || token.contains('\n') {
                return Err(ControlClientError(
                    "control auth token must not contain newline characters".to_string(),
                )
                .into());
            }
            request.push_str("Authorization: Bearer ");
            request.push_str(token);
            request.push_str("\r\n");
        }

        request.push_str("Connection: close\r\n\r\n");
        Ok(request)
    }
}

pub fn validate_config_update(
    update: &ConfigUpdate,
    signing_key: Option<&str>,
) -> Result<PipelineConfig, DynError> {
    let actual = sha256_hex(update.pipeline_config.as_bytes());
    if !actual.eq_ignore_ascii_case(&update.checksum) {
        return Err(ControlClientError(format!(
            "control config checksum mismatch: expected={} actual={actual}",
            update.checksum
        ))
        .into());
    }

    validate_config_signature(update, signing_key)?;
    parse_pipeline_config(&update.pipeline_config)
}

fn validate_config_signature(
    update: &ConfigUpdate,
    signing_key: Option<&str>,
) -> Result<(), DynError> {
    let Some(signing_key) = signing_key.filter(|value| !value.trim().is_empty()) else {
        return Ok(());
    };

    let signature = update.signature.as_deref().ok_or_else(|| {
        ControlClientError(
            "control config signature is required when a signing key is configured".to_string(),
        )
    })?;
    let expected = signature
        .strip_prefix("hmac-sha256=")
        .unwrap_or(signature)
        .trim();
    let actual = hmac_sha256_hex(signing_key.as_bytes(), update.pipeline_config.as_bytes());

    if !constant_time_equal(expected.as_bytes(), actual.as_bytes()) {
        return Err(ControlClientError("control config signature mismatch".to_string()).into());
    }

    Ok(())
}

fn parse_http_endpoint(endpoint: &str) -> Result<HttpEndpoint, DynError> {
    let endpoint = endpoint.trim();
    let (scheme, without_scheme) = if let Some(rest) = endpoint.strip_prefix("https://") {
        (HttpScheme::Https, rest)
    } else if let Some(rest) = endpoint.strip_prefix("http://") {
        (HttpScheme::Http, rest)
    } else {
        return Err(ControlClientError(
            "control endpoint must use http:// or https://".to_string(),
        )
        .into());
    };

    if without_scheme.contains('/') {
        return Err(ControlClientError(
            "control endpoint path prefixes are not supported yet".to_string(),
        )
        .into());
    }

    let default_port = match scheme {
        HttpScheme::Http => 80,
        HttpScheme::Https => 443,
    };

    let (host, port) = if let Some((host, port)) = without_scheme.rsplit_once(':') {
        (host.to_string(), port.parse::<u16>()?)
    } else {
        (without_scheme.to_string(), default_port)
    };

    if host.trim().is_empty() {
        return Err(
            ControlClientError("control endpoint host must not be empty".to_string()).into(),
        );
    }

    Ok(HttpEndpoint { scheme, host, port })
}

impl HttpEndpoint {
    async fn connect(&self, tls: &TlsConfig) -> Result<ControlStream, DynError> {
        let tcp = TcpStream::connect((self.host.as_str(), self.port)).await?;
        match self.scheme {
            HttpScheme::Http => Ok(ControlStream::Plain(tcp)),
            HttpScheme::Https => {
                let connector = TlsConnector::from(Arc::new(client_tls_config(tls)?));
                let server_name = tls
                    .server_name
                    .as_deref()
                    .unwrap_or(self.host.as_str())
                    .to_string();
                let server_name: ServerName<'static> = ServerName::try_from(server_name).map_err(
                    |err: rustls::pki_types::InvalidDnsNameError| {
                        ControlClientError(format!("invalid control TLS server name: {err}"))
                    },
                )?;
                let stream = connector.connect(server_name, tcp).await?;
                Ok(ControlStream::Tls(Box::new(stream)))
            }
        }
    }
}

enum ControlStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl ControlStream {
    async fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Plain(stream) => stream.write_all(bytes).await,
            Self::Tls(stream) => stream.write_all(bytes).await,
        }
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(stream) => stream.shutdown().await,
            Self::Tls(stream) => stream.shutdown().await,
        }
    }

    async fn read_to_end(&mut self, buffer: &mut Vec<u8>) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read_to_end(buffer).await,
            Self::Tls(stream) => stream.read_to_end(buffer).await,
        }
    }
}

fn client_tls_config(tls: &TlsConfig) -> Result<ClientConfig, DynError> {
    let mut roots = RootCertStore::empty();
    if let Some(ca_file) = &tls.ca_file {
        for cert in load_certificates(ca_file)? {
            roots
                .add(cert)
                .map_err(|err| ControlClientError(format!("invalid control CA file: {err}")))?;
        }
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let builder = ClientConfig::builder().with_root_certificates(roots);
    if let (Some(cert_file), Some(key_file)) = (&tls.cert_file, &tls.key_file) {
        builder
            .with_client_auth_cert(load_certificates(cert_file)?, load_private_key(key_file)?)
            .map_err(|err| ControlClientError(format!("invalid control client cert/key: {err}")))
            .map_err(Into::into)
    } else {
        Ok(builder.with_no_client_auth())
    }
}

fn load_certificates(path: &str) -> Result<Vec<CertificateDer<'static>>, DynError> {
    let file = fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            ControlClientError(format!(
                "failed to read control TLS certificate {path}: {err}"
            ))
            .into()
        })
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, DynError> {
    let file = fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|err| {
            ControlClientError(format!(
                "failed to read control TLS private key {path}: {err}"
            ))
        })?
        .ok_or_else(|| ControlClientError(format!("private key not found in {path}")).into())
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
        status: get_optional_string(value, "status")
            .map(parse_command_status)
            .unwrap_or(ControlCommandStatus::Unknown),
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
        signature: get_optional_string(update, "signature").map(ToString::to_string),
    }))
}

fn parse_command_kind(value: &str) -> ControlCommandKind {
    match value {
        "reload_config" => ControlCommandKind::ReloadConfig,
        "drain_and_restart" => ControlCommandKind::DrainAndRestart,
        "pause_exports" => ControlCommandKind::PauseExports,
        "resume_exports" => ControlCommandKind::ResumeExports,
        _ => ControlCommandKind::Unknown,
    }
}

fn parse_command_status(value: &str) -> ControlCommandStatus {
    match value {
        "pending" => ControlCommandStatus::Pending,
        "delivered" => ControlCommandStatus::Delivered,
        "succeeded" => ControlCommandStatus::Succeeded,
        "failed" => ControlCommandStatus::Failed,
        _ => ControlCommandStatus::Unknown,
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

fn get_optional_string<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    get_field(value, key).ok()?.as_str()
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
    hex_lower(&sha256_digest(input))
}

fn sha256_digest(input: &[u8]) -> [u8; 32] {
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

    let mut output = [0_u8; 32];
    for (index, word) in state.iter().enumerate() {
        output[index * 4..index * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    output
}

fn hmac_sha256_hex(key: &[u8], message: &[u8]) -> String {
    let mut key_block = [0_u8; 64];
    if key.len() > key_block.len() {
        key_block[..32].copy_from_slice(&sha256_digest(key));
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }

    let mut inner_pad = [0x36_u8; 64];
    let mut outer_pad = [0x5c_u8; 64];
    for index in 0..key_block.len() {
        inner_pad[index] ^= key_block[index];
        outer_pad[index] ^= key_block[index];
    }

    let mut inner = Vec::with_capacity(inner_pad.len() + message.len());
    inner.extend_from_slice(&inner_pad);
    inner.extend_from_slice(message);
    let inner_hash = sha256_digest(&inner);

    let mut outer = Vec::with_capacity(outer_pad.len() + inner_hash.len());
    outer.extend_from_slice(&outer_pad);
    outer.extend_from_slice(&inner_hash);

    hex_lower(&sha256_digest(&outer))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    left.iter()
        .zip(right.iter())
        .fold(0_u8, |acc, (left, right)| acc | (left ^ right))
        == 0
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
                scheme: HttpScheme::Http,
                host: "127.0.0.1".to_string(),
                port: 4001
            }
        );
        assert_eq!(
            parse_http_endpoint("https://control-plane:8443")?,
            HttpEndpoint {
                scheme: HttpScheme::Https,
                host: "control-plane".to_string(),
                port: 8443
            }
        );
        assert_eq!(parse_http_endpoint("https://control-plane")?.port, 443);
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
  otlp-grpc:
    protocol: "otlp_grpc"
    endpoint: "0.0.0.0:4317"
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
        let signature = hmac_sha256_hex(b"test-signing-key", payload.as_bytes());
        let response = serde_yaml::from_str(&format!(
            "{{\"update\":{{\"version\":1,\"pipeline_config\":{},\"checksum\":\"{}\",\"signature\":\"{}\"}}}}",
            yaml_json_string(payload),
            checksum,
            signature
        ))?;

        let update = parse_config_update_response(&response)?.ok_or_else(|| {
            ControlClientError("expected config update in test response".to_string())
        })?;
        assert_eq!(update.signature.as_deref(), Some(signature.as_str()));

        let config = validate_config_update(&update, Some("test-signing-key"))?;

        assert_eq!(config.tenant_id, "payments-prod");
        assert_eq!(config.name, "default");
        Ok(())
    }

    #[test]
    fn rejects_config_update_when_signature_is_missing_or_invalid() {
        let payload = r#"
tenant: "payments-prod"
pipeline: "default"
receivers:
  otlp-grpc:
    protocol: "otlp_grpc"
    endpoint: "0.0.0.0:4317"
exporters:
  stdout:
    protocol: "stdout"
    endpoint: "stdout://local"
routes:
  traces:
    exporters: ["stdout"]
"#;
        let checksum = sha256_hex(payload.as_bytes());
        let mut update = ConfigUpdate {
            version: 1,
            pipeline_config: payload.to_string(),
            checksum,
            signature: None,
        };

        let missing_signature = validate_config_update(&update, Some("test-signing-key"));
        assert!(
            missing_signature
                .err()
                .map(|err| err.to_string().contains("signature is required"))
                .unwrap_or(false)
        );

        update.signature = Some("not-a-valid-signature".to_string());
        let invalid_signature = validate_config_update(&update, Some("test-signing-key"));
        assert!(
            invalid_signature
                .err()
                .map(|err| err.to_string().contains("signature mismatch"))
                .unwrap_or(false)
        );
    }

    #[test]
    fn hmac_sha256_matches_known_vector() {
        assert_eq!(
            hmac_sha256_hex(b"key", b"The quick brown fox jumps over the lazy dog"),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    #[test]
    fn parses_control_commands_response() -> Result<(), DynError> {
        let response = serde_yaml::from_str(
            r#"{"commands":[{"command_id":"cmd-1","kind":"resume_exports","reason":"done","status":"delivered"}]}"#,
        )?;

        let commands = parse_commands_response(&response)?;

        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].kind, ControlCommandKind::ResumeExports);
        assert_eq!(commands[0].reason, "done");
        assert_eq!(commands[0].status, ControlCommandStatus::Delivered);
        Ok(())
    }

    fn yaml_json_string(value: &str) -> String {
        format!("\"{}\"", json_escape(value))
    }
}
