use crate::runtime::AgentRuntime;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use prost::Message;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use serde::{Serialize, de::DeserializeOwned};
use std::error::Error;
use std::fs;
use std::io::BufReader as StdBufReader;
use std::sync::Arc;
use telemetry_core::TlsConfig;
use telemetry_otlp::{
    logs_request_to_records, metrics_request_to_records, trace_request_to_records,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_rustls::TlsAcceptor;

type DynError = Box<dyn Error + Send + Sync>;

const MAX_OTLP_HTTP_BODY_BYTES: usize = 16 * 1024 * 1024;

pub async fn serve_otlp_http(
    bind: String,
    runtime: Arc<Mutex<AgentRuntime>>,
    tenant_id: String,
    tls: TlsConfig,
) -> Result<(), DynError> {
    let listener = TcpListener::bind(&bind).await?;
    let tls_acceptor = if tls.enabled {
        Some(build_tls_acceptor(&tls)?)
    } else {
        None
    };
    let mode = if tls_acceptor.is_some() {
        "OTLP/HTTP protobuf+JSON over TLS"
    } else {
        "OTLP/HTTP protobuf+JSON"
    };
    println!("telemetry-agent listening on {bind} using {mode}");

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let runtime = Arc::clone(&runtime);
                let tenant_id = tenant_id.clone();
                let tls_acceptor = tls_acceptor.clone();
                tokio::spawn(async move {
                    let result = if let Some(acceptor) = tls_acceptor {
                        match acceptor.accept(stream).await {
                            Ok(stream) => handle_otlp_http(stream, runtime, tenant_id).await,
                            Err(err) => Err(Box::new(err) as DynError),
                        }
                    } else {
                        handle_otlp_http(stream, runtime, tenant_id).await
                    };
                    if let Err(err) = result {
                        eprintln!("OTLP/HTTP request failed: {err}");
                    }
                });
            }
            Err(err) => eprintln!("OTLP/HTTP accept failed: {err}"),
        }
    }
}

async fn handle_otlp_http<S>(
    mut stream: S,
    runtime: Arc<Mutex<AgentRuntime>>,
    tenant_id: String,
) -> Result<(), DynError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = match read_otlp_http_request(&mut stream).await {
        Ok(request) => request,
        Err(message) => {
            write_http_response(
                &mut stream,
                "400 Bad Request",
                "text/plain",
                message.as_bytes(),
            )
            .await?;
            return Ok(());
        }
    };

    if request.method != "POST" {
        write_http_response(
            &mut stream,
            "405 Method Not Allowed",
            "text/plain",
            b"method must be POST",
        )
        .await?;
        return Ok(());
    }

    let response = match request.path.as_str() {
        "/v1/traces" => {
            let decoded = decode_otlp_http_body::<ExportTraceServiceRequest>(&request);
            match decoded {
                Ok(decoded) => {
                    let records = trace_request_to_records(&tenant_id, decoded);
                    ingest_records(&runtime, records).await?;
                    encode_otlp_http_response(
                        &request,
                        &ExportTraceServiceResponse {
                            partial_success: None,
                        },
                    )
                }
                Err(message) => Err(message),
            }
        }
        "/v1/metrics" => {
            let decoded = decode_otlp_http_body::<ExportMetricsServiceRequest>(&request);
            match decoded {
                Ok(decoded) => {
                    let records = metrics_request_to_records(&tenant_id, decoded);
                    ingest_records(&runtime, records).await?;
                    encode_otlp_http_response(
                        &request,
                        &ExportMetricsServiceResponse {
                            partial_success: None,
                        },
                    )
                }
                Err(message) => Err(message),
            }
        }
        "/v1/logs" => {
            let decoded = decode_otlp_http_body::<ExportLogsServiceRequest>(&request);
            match decoded {
                Ok(decoded) => {
                    let records = logs_request_to_records(&tenant_id, decoded);
                    ingest_records(&runtime, records).await?;
                    encode_otlp_http_response(
                        &request,
                        &ExportLogsServiceResponse {
                            partial_success: None,
                        },
                    )
                }
                Err(message) => Err(message),
            }
        }
        _ => {
            write_http_response(
                &mut stream,
                "404 Not Found",
                "text/plain",
                b"unknown OTLP/HTTP signal path",
            )
            .await?;
            return Ok(());
        }
    };

    match response {
        Ok((body, content_type)) => {
            write_http_response(&mut stream, "200 OK", content_type, body.as_slice()).await?;
        }
        Err(message) => {
            write_http_response(
                &mut stream,
                "400 Bad Request",
                "text/plain",
                message.as_bytes(),
            )
            .await?;
        }
    }

    Ok(())
}

async fn ingest_records(
    runtime: &Arc<Mutex<AgentRuntime>>,
    records: Vec<telemetry_core::TelemetryRecord>,
) -> Result<(), DynError> {
    let mut runtime = runtime.lock().await;
    for record in records {
        runtime.ingest(record)?;
    }
    Ok(())
}

fn build_tls_acceptor(tls: &TlsConfig) -> Result<TlsAcceptor, DynError> {
    let cert_file = tls
        .cert_file
        .as_deref()
        .ok_or("OTLP/HTTP TLS receiver requires tls.cert_file")?;
    let key_file = tls
        .key_file
        .as_deref()
        .ok_or("OTLP/HTTP TLS receiver requires tls.key_file")?;

    let builder = ServerConfig::builder();
    let config = if tls.require_client_auth {
        let ca_file = tls
            .ca_file
            .as_deref()
            .ok_or("OTLP/HTTP mTLS receiver requires tls.ca_file")?;
        let mut roots = RootCertStore::empty();
        for cert in load_certificates(ca_file)? {
            roots.add(cert)?;
        }
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots)).build()?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(load_certificates(cert_file)?, load_private_key(key_file)?)?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(load_certificates(cert_file)?, load_private_key(key_file)?)?
    };

    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn load_certificates(path: &str) -> Result<Vec<CertificateDer<'static>>, DynError> {
    let file = fs::File::open(path)?;
    let mut reader = StdBufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("failed to read certificate {path}: {err}").into())
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, DynError> {
    let file = fs::File::open(path)?;
    let mut reader = StdBufReader::new(file);
    rustls_pemfile::private_key(&mut reader)?
        .ok_or_else(|| format!("private key not found in {path}").into())
}

fn decode_otlp_http_body<T>(request: &OtlpHttpRequest) -> Result<T, String>
where
    T: Message + Default + DeserializeOwned,
{
    if request.is_json() {
        serde_json::from_slice(&request.body).map_err(|err| err.to_string())
    } else {
        T::decode(request.body.as_slice()).map_err(|err| err.to_string())
    }
}

fn encode_otlp_http_response<T>(
    request: &OtlpHttpRequest,
    response: &T,
) -> Result<(Vec<u8>, &'static str), String>
where
    T: Message + Serialize,
{
    if request.is_json() {
        serde_json::to_vec(response)
            .map(|body| (body, "application/json"))
            .map_err(|err| err.to_string())
    } else {
        Ok((response.encode_to_vec(), "application/x-protobuf"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OtlpHttpRequest {
    method: String,
    path: String,
    content_type: Option<String>,
    body: Vec<u8>,
}

impl OtlpHttpRequest {
    fn is_json(&self) -> bool {
        self.content_type
            .as_deref()
            .map(|value| {
                value
                    .split(';')
                    .next()
                    .map(|mime| mime.trim().eq_ignore_ascii_case("application/json"))
                    .unwrap_or(false)
            })
            .unwrap_or(false)
    }
}

async fn read_otlp_http_request<S>(stream: &mut S) -> Result<OtlpHttpRequest, String>
where
    S: AsyncRead + Unpin,
{
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 8192];

    loop {
        let read = stream
            .read(&mut chunk)
            .await
            .map_err(|err| err.to_string())?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);

        if let Some(request) = parse_buffered_http_request(&buffer)? {
            return Ok(request);
        }
        if buffer.len() > MAX_OTLP_HTTP_BODY_BYTES {
            return Err("OTLP/HTTP request exceeds maximum body size".to_string());
        }
    }

    Err("OTLP/HTTP request ended before full body".to_string())
}

fn parse_buffered_http_request(buffer: &[u8]) -> Result<Option<OtlpHttpRequest>, String> {
    let Some(header_end) = find_header_end(buffer) else {
        return Ok(None);
    };
    let headers = String::from_utf8_lossy(&buffer[..header_end]);
    let mut request_line = headers
        .lines()
        .next()
        .ok_or_else(|| "missing HTTP request line".to_string())?
        .split_whitespace();
    let method = request_line
        .next()
        .ok_or_else(|| "missing HTTP method".to_string())?
        .to_string();
    let path = request_line
        .next()
        .ok_or_else(|| "missing HTTP request path".to_string())?
        .to_string();
    let content_length = parse_content_length(&headers)?;
    let content_type = parse_header_value(&headers, "content-type");
    if content_length > MAX_OTLP_HTTP_BODY_BYTES {
        return Err("OTLP/HTTP request exceeds maximum body size".to_string());
    }

    let body_start = header_end + 4;
    let body_end = body_start + content_length;
    if buffer.len() < body_end {
        return Ok(None);
    }

    Ok(Some(OtlpHttpRequest {
        method,
        path,
        content_type,
        body: buffer[body_start..body_end].to_vec(),
    }))
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn parse_content_length(headers: &str) -> Result<usize, String> {
    parse_header_value(headers, "content-length")
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|err| err.to_string())?
        .ok_or_else(|| "missing Content-Length".to_string())
}

fn parse_header_value(headers: &str, expected_name: &str) -> Option<String> {
    headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case(expected_name)
            .then(|| value.trim().to_string())
    })
}

async fn write_http_response<S>(
    stream: &mut S,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> Result<(), DynError>
where
    S: AsyncWrite + Unpin,
{
    let headers = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes()).await?;
    stream.write_all(body).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, any_value};
    use opentelemetry_proto::tonic::logs::v1::{
        LogRecord, ResourceLogs as OtlpResourceLogs, ScopeLogs as OtlpScopeLogs,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};
    use telemetry_buffer::DiskQueueOptions;
    use telemetry_core::{
        ExporterConfig, ExporterProtocol, PipelineConfig, RouteConfig, SignalKind, TlsConfig,
    };
    use tokio::net::TcpStream;

    #[test]
    fn parses_buffered_otlp_http_request() -> Result<(), Box<dyn Error + Send + Sync>> {
        let body = vec![1_u8, 2, 3];
        let request = [
            b"POST /v1/logs HTTP/1.1\r\nContent-Length: 3\r\n\r\n".as_slice(),
            body.as_slice(),
        ]
        .concat();

        let parsed = parse_buffered_http_request(&request)?
            .ok_or_else(|| "expected complete request".to_string())?;

        assert_eq!(parsed.method, "POST");
        assert_eq!(parsed.path, "/v1/logs");
        assert_eq!(parsed.body, body);
        Ok(())
    }

    #[tokio::test]
    async fn otlp_http_logs_service_ingests_and_flushes_records()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let dir = test_dir("otlp-http-logs-service");
        let queue_dir = dir.join("queue");
        let export_path = dir.join("export.log");
        let config = PipelineConfig {
            exporters: vec![ExporterConfig {
                name: "file".to_string(),
                protocol: ExporterProtocol::File,
                endpoint: export_path.to_string_lossy().to_string(),
                tls: TlsConfig::disabled(),
            }],
            routes: vec![RouteConfig {
                signal: SignalKind::Log,
                exporters: vec!["file".to_string()],
            }],
            ..PipelineConfig::default()
        };
        let runtime = Arc::new(Mutex::new(AgentRuntime::open(
            config,
            queue_dir,
            DiskQueueOptions::default(),
        )?));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server_runtime = Arc::clone(&runtime);
        let server = tokio::spawn(async move {
            let (stream, _addr) = listener.accept().await?;
            handle_otlp_http(stream, server_runtime, "payments-prod".to_string()).await
        });

        let mut stream = TcpStream::connect(addr).await?;
        let body = sample_logs_request().encode_to_vec();
        let request = format!(
            "POST /v1/logs HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/x-protobuf\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(request.as_bytes()).await?;
        stream.write_all(&body).await?;
        stream.shutdown().await?;
        let mut response = String::new();
        stream.read_to_string(&mut response).await?;

        server.await??;
        runtime.lock().await.flush(32).await?;

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert_eq!(
            fs::read_to_string(&export_path)?,
            "tenant=payments-prod signal=log attrs=9 bytes=12\n"
        );

        cleanup(dir);
        Ok(())
    }

    #[tokio::test]
    async fn otlp_http_json_logs_service_ingests_and_flushes_records()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let dir = test_dir("otlp-http-json-logs-service");
        let queue_dir = dir.join("queue");
        let export_path = dir.join("export.log");
        let config = PipelineConfig {
            exporters: vec![ExporterConfig {
                name: "file".to_string(),
                protocol: ExporterProtocol::File,
                endpoint: export_path.to_string_lossy().to_string(),
                tls: TlsConfig::disabled(),
            }],
            routes: vec![RouteConfig {
                signal: SignalKind::Log,
                exporters: vec!["file".to_string()],
            }],
            ..PipelineConfig::default()
        };
        let runtime = Arc::new(Mutex::new(AgentRuntime::open(
            config,
            queue_dir,
            DiskQueueOptions::default(),
        )?));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server_runtime = Arc::clone(&runtime);
        let server = tokio::spawn(async move {
            let (stream, _addr) = listener.accept().await?;
            handle_otlp_http(stream, server_runtime, "payments-prod".to_string()).await
        });

        let mut stream = TcpStream::connect(addr).await?;
        let body = serde_json::to_vec(&sample_logs_request())?;
        let request = format!(
            "POST /v1/logs HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(request.as_bytes()).await?;
        stream.write_all(&body).await?;
        stream.shutdown().await?;
        let mut response = String::new();
        stream.read_to_string(&mut response).await?;

        server.await??;
        runtime.lock().await.flush(32).await?;

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("Content-Type: application/json"));
        assert_eq!(
            fs::read_to_string(&export_path)?,
            "tenant=payments-prod signal=log attrs=9 bytes=12\n"
        );

        cleanup(dir);
        Ok(())
    }

    fn sample_logs_request() -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![OtlpResourceLogs {
                resource: Some(Resource {
                    attributes: Vec::new(),
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_logs: vec![OtlpScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "test-scope".to_string(),
                        version: String::new(),
                        attributes: Vec::new(),
                        dropped_attributes_count: 0,
                    }),
                    log_records: vec![LogRecord {
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("worker-ready".to_string())),
                        }),
                        ..LogRecord::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
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
