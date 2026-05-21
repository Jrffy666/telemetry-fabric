mod config_file;
mod control_client;
mod otlp;
mod otlp_http;
mod runtime;
mod tf_line;

use config_file::load_pipeline_config;
use control_client::{
    ConfigUpdate, ControlClient, ControlCommandKind, HeartbeatStats, validate_config_update,
};
use otlp::serve_otlp_grpc;
use otlp_http::serve_otlp_http;
use runtime::{AgentRuntime, RuntimeHealth};
use std::env;
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use telemetry_buffer::DiskQueueOptions;
use telemetry_core::{
    ExporterConfig, ExporterProtocol, PipelineConfig, ReceiverProtocol, RouteConfig, SignalKind,
    TelemetryRecord, TlsConfig,
};
use tf_line::parse_line_record;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

type DynError = Box<dyn Error + Send + Sync>;

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("telemetry-agent failed: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), DynError> {
    let args = Args::parse(env::args().skip(1))?;

    if args.help {
        print_help();
        return Ok(());
    }

    let config = build_pipeline_config(&args)?;
    config.validate()?;

    let mut runtime =
        AgentRuntime::open(config, args.queue_dir.clone(), DiskQueueOptions::default())?;

    if args.self_test {
        let record = TelemetryRecord::new(
            runtime.config().tenant_id.clone(),
            SignalKind::Trace,
            b"agent self-test record".to_vec(),
        )
        .with_attribute("service.name", "telemetry-agent");
        runtime.ingest(record)?;
        runtime.flush_to_stdout(64).await?;
        return Ok(());
    }

    let runtime = Arc::new(Mutex::new(runtime));

    if let Some(bind) = args.health_listen.clone() {
        spawn_health_server(bind, Arc::clone(&runtime));
    }

    if let Some(endpoint) = args.control_endpoint.clone() {
        let (tenant_id, queue_health) = {
            let runtime = runtime.lock().await;
            (runtime.config().tenant_id.clone(), runtime.health().ok())
        };
        let agent_id = args.agent_id.clone().unwrap_or_else(default_agent_id);
        let hostname = default_hostname();
        let client = ControlClient::new(
            &endpoint,
            agent_id,
            tenant_id,
            hostname,
            env!("CARGO_PKG_VERSION").to_string(),
        )?;
        spawn_control_worker(
            Arc::clone(&runtime),
            client,
            Duration::from_millis(args.control_heartbeat_interval_ms),
            queue_health,
            args.flush_batch_size,
        );
    }

    let receiver_mode = {
        let runtime = runtime.lock().await;
        receiver_mode(&args, &runtime)?
    };
    if receiver_mode.is_some() {
        spawn_flush_worker(
            Arc::clone(&runtime),
            args.flush_batch_size,
            Duration::from_millis(args.flush_interval_ms),
        );
    }

    match receiver_mode {
        Some(ReceiverMode::TfLine(bind)) => {
            serve_line_protocol(bind, Arc::clone(&runtime)).await?;
            return Ok(());
        }
        Some(ReceiverMode::OtlpGrpc(bind)) => {
            let tenant_id = runtime.lock().await.config().tenant_id.clone();
            serve_otlp_grpc(bind, Arc::clone(&runtime), tenant_id).await?;
            return Ok(());
        }
        Some(ReceiverMode::OtlpHttp { bind, tls }) => {
            let tenant_id = runtime.lock().await.config().tenant_id.clone();
            serve_otlp_http(bind, Arc::clone(&runtime), tenant_id, tls).await?;
            return Ok(());
        }
        None => {}
    }

    runtime.lock().await.flush_to_stdout(1024).await?;
    Ok(())
}

async fn serve_line_protocol(
    bind: String,
    runtime: Arc<Mutex<AgentRuntime>>,
) -> Result<(), DynError> {
    let listener = TcpListener::bind(&bind).await?;
    println!("telemetry-agent listening on {bind} using tf-line protocol");

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let runtime = Arc::clone(&runtime);
                tokio::spawn(async move {
                    if let Err(err) = handle_client(stream, runtime).await {
                        eprintln!("client failed: {err}");
                    }
                });
            }
            Err(err) => eprintln!("accept failed: {err}"),
        }
    }
}

async fn handle_client(
    stream: TcpStream,
    runtime: Arc<Mutex<AgentRuntime>>,
) -> Result<(), DynError> {
    let peer = stream.peer_addr().ok();
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }

        let record = parse_line_record(&line)?;
        {
            let mut runtime = runtime.lock().await;
            runtime.ingest(record)?;
        }
        writer.write_all(b"ok\n").await?;
    }

    if let Some(peer) = peer {
        println!("client disconnected: {peer}");
    }

    Ok(())
}

fn spawn_flush_worker(
    runtime: Arc<Mutex<AgentRuntime>>,
    flush_batch_size: usize,
    flush_interval: Duration,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(flush_interval);
        loop {
            interval.tick().await;
            let mut runtime = runtime.lock().await;
            match runtime.flush(flush_batch_size).await {
                Ok(report) if report.drained_records > 0 => {
                    println!(
                        "flush completed: drained={} exported={} dropped={} bytes={}",
                        report.drained_records,
                        report.exported_records,
                        report.dropped_records,
                        report.exported_bytes
                    );
                }
                Ok(_) => {}
                Err(err) => eprintln!("flush failed: {err}"),
            }
        }
    });
}

fn spawn_control_worker(
    runtime: Arc<Mutex<AgentRuntime>>,
    client: ControlClient,
    heartbeat_interval: Duration,
    initial_health: Option<RuntimeHealth>,
    drain_batch_size: usize,
) {
    tokio::spawn(async move {
        let mut applied_config_version = 0_u64;
        let mut registered = false;
        let mut interval = tokio::time::interval(heartbeat_interval);

        if let Some(health) = initial_health {
            println!(
                "control worker starting: queued_bytes={} cursor={}:{}",
                health.queued_bytes, health.cursor_segment_id, health.cursor_offset
            );
        }

        loop {
            interval.tick().await;

            if !registered {
                match client.register().await {
                    Ok(response) if response.accepted => {
                        registered = true;
                        runtime.lock().await.record_control_registration(true);
                        println!(
                            "control registration accepted: latest_config_version={} message={}",
                            response.config_version, response.message
                        );
                    }
                    Ok(response) => {
                        runtime.lock().await.record_control_registration(false);
                        eprintln!("control registration rejected: {}", response.message);
                        continue;
                    }
                    Err(err) => {
                        runtime.lock().await.record_control_registration(false);
                        eprintln!("control registration failed: {err}");
                        continue;
                    }
                }
            }

            let health = {
                let runtime = runtime.lock().await;
                runtime.health().ok()
            };
            let queue_depth_bytes = health.map(|value| value.queued_bytes).unwrap_or(0);

            let commands = match client
                .heartbeat(HeartbeatStats {
                    config_version: applied_config_version,
                    queue_depth_bytes,
                    ingest_bytes_per_second: 0,
                })
                .await
            {
                Ok(commands) => commands,
                Err(err) => {
                    runtime.lock().await.record_control_heartbeat(false);
                    eprintln!("control heartbeat failed: {err}");
                    continue;
                }
            };
            runtime.lock().await.record_control_heartbeat(true);

            for command in commands {
                match command.kind {
                    ControlCommandKind::ReloadConfig => {
                        match fetch_and_apply_config(&runtime, &client, applied_config_version)
                            .await
                        {
                            Ok(Some(update)) => {
                                applied_config_version = update.version;
                                println!(
                                    "control config applied: version={} checksum={}",
                                    update.version, update.checksum
                                );
                            }
                            Ok(None) => {}
                            Err(err) => eprintln!("control config reload failed: {err}"),
                        }
                    }
                    ControlCommandKind::DrainAndRestart => {
                        println!(
                            "control command received: drain_and_restart id={} reason={}",
                            command.command_id, command.reason
                        );
                        if let Err(err) =
                            drain_and_exit(&runtime, drain_batch_size, &command.command_id).await
                        {
                            eprintln!(
                                "control drain_and_restart failed: id={} error={err}",
                                command.command_id
                            );
                        }
                    }
                    ControlCommandKind::PauseExports => {
                        runtime.lock().await.pause_exports();
                        println!(
                            "control exports paused: id={} reason={}",
                            command.command_id, command.reason
                        );
                    }
                    ControlCommandKind::ResumeExports => {
                        runtime.lock().await.resume_exports();
                        println!(
                            "control exports resumed: id={} reason={}",
                            command.command_id, command.reason
                        );
                    }
                    ControlCommandKind::Unknown => {
                        eprintln!(
                            "unknown control command ignored: id={} reason={}",
                            command.command_id, command.reason
                        );
                    }
                }
            }
        }
    });
}

async fn drain_and_exit(
    runtime: &Arc<Mutex<AgentRuntime>>,
    batch_size: usize,
    command_id: &str,
) -> Result<(), DynError> {
    loop {
        let report = runtime.lock().await.flush_for_shutdown(batch_size).await?;
        if report.drained_records == 0 {
            println!("control drain completed: id={command_id}; exiting for supervisor restart");
            std::process::exit(0);
        }

        println!(
            "control drain progress: id={} drained={} exported={} dropped={} bytes={}",
            command_id,
            report.drained_records,
            report.exported_records,
            report.dropped_records,
            report.exported_bytes
        );

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn fetch_and_apply_config(
    runtime: &Arc<Mutex<AgentRuntime>>,
    client: &ControlClient,
    current_version: u64,
) -> Result<Option<ConfigUpdate>, DynError> {
    let update = match client.config_update(current_version).await {
        Ok(update) => {
            runtime.lock().await.record_control_config_fetch(true);
            update
        }
        Err(err) => {
            runtime.lock().await.record_control_config_fetch(false);
            return Err(err);
        }
    };

    let Some(update) = update else {
        return Ok(None);
    };
    let config = validate_config_update(&update)?;
    runtime.lock().await.reload_config(config)?;
    Ok(Some(update))
}

fn spawn_health_server(bind: String, runtime: Arc<Mutex<AgentRuntime>>) {
    tokio::spawn(async move {
        if let Err(err) = serve_health(bind, runtime).await {
            eprintln!("health server failed: {err}");
        }
    });
}

async fn serve_health(bind: String, runtime: Arc<Mutex<AgentRuntime>>) -> Result<(), DynError> {
    let listener = TcpListener::bind(&bind).await?;
    println!("telemetry-agent health endpoint listening on {bind}");

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let runtime = Arc::clone(&runtime);
                tokio::spawn(async move {
                    if let Err(err) = handle_health(stream, runtime).await {
                        eprintln!("health request failed: {err}");
                    }
                });
            }
            Err(err) => eprintln!("health accept failed: {err}"),
        }
    }
}

async fn handle_health(
    mut stream: TcpStream,
    runtime: Arc<Mutex<AgentRuntime>>,
) -> Result<(), DynError> {
    let path = read_request_path(&mut stream)
        .await
        .unwrap_or_else(|| "/".to_string());
    let (status, content_type, body) = if path == "/metrics" {
        match runtime.lock().await.prometheus_metrics() {
            Ok(metrics) => ("200 OK", "text/plain; version=0.0.4", metrics),
            Err(err) => (
                "503 Service Unavailable",
                "application/json",
                format!(
                    "{{\"status\":\"unhealthy\",\"error\":\"{}\"}}",
                    json_escape(&err.to_string())
                ),
            ),
        }
    } else {
        let health = runtime.lock().await.health();
        match health {
            Ok(health) => (
                "200 OK",
                "application/json",
                format!(
                    "{{\"status\":\"ok\",\"queued_bytes\":{},\"cursor_segment_id\":{},\"cursor_offset\":{},\"exports_paused\":{}}}",
                    health.queued_bytes,
                    health.cursor_segment_id,
                    health.cursor_offset,
                    health.exports_paused
                ),
            ),
            Err(err) => (
                "503 Service Unavailable",
                "application/json",
                format!(
                    "{{\"status\":\"unhealthy\",\"error\":\"{}\"}}",
                    json_escape(&err.to_string())
                ),
            ),
        }
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    Ok(())
}

async fn read_request_path(stream: &mut TcpStream) -> Option<String> {
    let mut buffer = [0_u8; 1024];
    let read = stream.read(&mut buffer).await.ok()?;
    if read == 0 {
        return None;
    }
    parse_request_path(&String::from_utf8_lossy(&buffer[..read]))
}

fn parse_request_path(request: &str) -> Option<String> {
    let line = request.lines().next()?;
    let mut parts = line.split_whitespace();
    let _method = parts.next()?;
    let path = parts.next()?;
    Some(path.to_string())
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Args {
    tenant_id: Option<String>,
    config_path: Option<PathBuf>,
    queue_dir: PathBuf,
    listen: Option<String>,
    otlp_grpc: Option<String>,
    otlp_http: Option<String>,
    otlp_export_endpoint: Option<String>,
    health_listen: Option<String>,
    control_endpoint: Option<String>,
    agent_id: Option<String>,
    flush_batch_size: usize,
    flush_interval_ms: u64,
    control_heartbeat_interval_ms: u64,
    self_test: bool,
    help: bool,
}

impl Args {
    fn parse(values: impl Iterator<Item = String>) -> Result<Self, DynError> {
        let mut args = Self {
            tenant_id: None,
            config_path: None,
            queue_dir: PathBuf::from(".telemetry-fabric/queue"),
            listen: None,
            otlp_grpc: None,
            otlp_http: None,
            otlp_export_endpoint: None,
            health_listen: None,
            control_endpoint: None,
            agent_id: None,
            flush_batch_size: 128,
            flush_interval_ms: 1000,
            control_heartbeat_interval_ms: 5000,
            self_test: false,
            help: false,
        };

        let mut values = values.peekable();
        while let Some(value) = values.next() {
            match value.as_str() {
                "--tenant" => {
                    args.tenant_id = Some(next_value(&mut values, "--tenant")?);
                }
                "--config" => {
                    args.config_path = Some(PathBuf::from(next_value(&mut values, "--config")?));
                }
                "--queue-dir" => {
                    args.queue_dir = PathBuf::from(next_value(&mut values, "--queue-dir")?);
                }
                "--listen" => {
                    args.listen = Some(next_value(&mut values, "--listen")?);
                }
                "--otlp-grpc" => {
                    args.otlp_grpc = Some(next_value(&mut values, "--otlp-grpc")?);
                }
                "--otlp-http" => {
                    args.otlp_http = Some(next_value(&mut values, "--otlp-http")?);
                }
                "--otlp-export-endpoint" => {
                    args.otlp_export_endpoint =
                        Some(next_value(&mut values, "--otlp-export-endpoint")?);
                }
                "--health-listen" => {
                    args.health_listen = Some(next_value(&mut values, "--health-listen")?);
                }
                "--control-endpoint" => {
                    args.control_endpoint = Some(next_value(&mut values, "--control-endpoint")?);
                }
                "--agent-id" => {
                    args.agent_id = Some(next_value(&mut values, "--agent-id")?);
                }
                "--flush-batch-size" => {
                    args.flush_batch_size =
                        parse_positive_usize(&next_value(&mut values, "--flush-batch-size")?)?;
                }
                "--flush-interval-ms" => {
                    args.flush_interval_ms =
                        parse_positive_u64(&next_value(&mut values, "--flush-interval-ms")?)?;
                }
                "--control-heartbeat-interval-ms" => {
                    args.control_heartbeat_interval_ms = parse_positive_u64(&next_value(
                        &mut values,
                        "--control-heartbeat-interval-ms",
                    )?)?;
                }
                "--self-test" => {
                    args.self_test = true;
                }
                "--help" | "-h" => {
                    args.help = true;
                }
                other => {
                    return Err(format!("unknown argument: {other}").into());
                }
            }
        }

        let receiver_modes = [
            args.listen.is_some(),
            args.otlp_grpc.is_some(),
            args.otlp_http.is_some(),
        ]
        .into_iter()
        .filter(|enabled| *enabled)
        .count();
        if receiver_modes > 1 {
            return Err("choose only one receiver mode for this MVP agent process".into());
        }

        Ok(args)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReceiverMode {
    TfLine(String),
    OtlpGrpc(String),
    OtlpHttp { bind: String, tls: TlsConfig },
}

fn receiver_mode(args: &Args, runtime: &AgentRuntime) -> Result<Option<ReceiverMode>, DynError> {
    if let Some(bind) = args.listen.clone() {
        return Ok(Some(ReceiverMode::TfLine(bind)));
    }

    if let Some(bind) = args.otlp_grpc.clone() {
        return Ok(Some(ReceiverMode::OtlpGrpc(bind)));
    }

    if let Some(bind) = args.otlp_http.clone() {
        return Ok(Some(ReceiverMode::OtlpHttp {
            bind,
            tls: TlsConfig::disabled(),
        }));
    }

    if args.config_path.is_none() {
        return Ok(None);
    }

    let config = runtime.config();
    if let Some(receiver) = config
        .receivers
        .iter()
        .find(|receiver| receiver.protocol == ReceiverProtocol::OtlpGrpc)
    {
        return Ok(Some(ReceiverMode::OtlpGrpc(receiver.endpoint.clone())));
    }

    if let Some(receiver) = config
        .receivers
        .iter()
        .find(|receiver| receiver.protocol == ReceiverProtocol::OtlpHttp)
    {
        return Ok(Some(ReceiverMode::OtlpHttp {
            bind: receiver.endpoint.clone(),
            tls: receiver.tls.clone(),
        }));
    }

    if let Some(receiver) = config
        .receivers
        .iter()
        .find(|receiver| receiver.protocol == ReceiverProtocol::TfLine)
    {
        return Ok(Some(ReceiverMode::TfLine(receiver.endpoint.clone())));
    }

    Err("config has no supported receiver; supported receivers are otlp_grpc, otlp_http, and tf_line".into())
}

fn build_pipeline_config(args: &Args) -> Result<PipelineConfig, DynError> {
    let mut config = if let Some(path) = &args.config_path {
        load_pipeline_config(path)?
    } else {
        PipelineConfig::default()
    };

    if let Some(tenant_id) = &args.tenant_id {
        config.tenant_id = tenant_id.clone();
    }

    if let Some(endpoint) = &args.otlp_export_endpoint {
        config.exporters = vec![
            ExporterConfig {
                name: "otlp-upstream".to_string(),
                protocol: ExporterProtocol::OtlpGrpc,
                endpoint: endpoint.clone(),
                tls: if endpoint.starts_with("https://") {
                    TlsConfig::enabled()
                } else {
                    TlsConfig::disabled()
                },
            },
            ExporterConfig {
                name: "stdout".to_string(),
                protocol: ExporterProtocol::Stdout,
                endpoint: "stdout://local".to_string(),
                tls: TlsConfig::disabled(),
            },
        ];
        config.routes = vec![
            RouteConfig {
                signal: SignalKind::Trace,
                exporters: vec!["otlp-upstream".to_string()],
            },
            RouteConfig {
                signal: SignalKind::Metric,
                exporters: vec!["otlp-upstream".to_string()],
            },
            RouteConfig {
                signal: SignalKind::Log,
                exporters: vec!["otlp-upstream".to_string()],
            },
        ];
    }

    if args.config_path.is_none() && args.tenant_id.is_some() {
        config.tenant_id = args
            .tenant_id
            .clone()
            .unwrap_or_else(|| "default".to_string());
    }

    config.validate()?;
    Ok(config)
}

fn next_value(
    values: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    flag: &str,
) -> Result<String, DynError> {
    values
        .next()
        .ok_or_else(|| format!("missing value for {flag}").into())
}

fn parse_positive_usize(value: &str) -> Result<usize, DynError> {
    let parsed = value.parse::<usize>()?;
    if parsed == 0 {
        return Err("value must be greater than zero".into());
    }
    Ok(parsed)
}

fn parse_positive_u64(value: &str) -> Result<u64, DynError> {
    let parsed = value.parse::<u64>()?;
    if parsed == 0 {
        return Err("value must be greater than zero".into());
    }
    Ok(parsed)
}

fn default_agent_id() -> String {
    format!("telemetry-agent-{}", default_hostname())
}

fn default_hostname() -> String {
    env::var("HOSTNAME")
        .or_else(|_| env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown-host".to_string())
}

fn print_help() {
    println!(
        "telemetry-agent\n\
         \n\
         Usage:\n\
           telemetry-agent --self-test [--tenant TENANT] [--queue-dir DIR]\n\
           telemetry-agent --config FILE [--queue-dir DIR]\n\
           telemetry-agent --listen HOST:PORT [--queue-dir DIR]\n\
           telemetry-agent --otlp-grpc HOST:PORT [--queue-dir DIR]\n\
           telemetry-agent --otlp-http HOST:PORT [--queue-dir DIR]\n\
           telemetry-agent [--queue-dir DIR]\n\
         \n\
         Options:\n\
           --tenant TENANT     Tenant id to use for generated records.\n\
           --config FILE       Load pipeline configuration from YAML.\n\
           --queue-dir DIR     Durable queue directory.\n\
           --listen HOST:PORT  Start the minimal tf-line TCP receiver.\n\
           --otlp-grpc HOST:PORT Start the OTLP/gRPC trace, metrics, and logs receiver.\n\
           --otlp-http HOST:PORT Start the OTLP/HTTP protobuf trace, metrics, and logs receiver.\n\
           --otlp-export-endpoint HOST:PORT|URL Forward traces, metrics, and logs to an upstream OTLP/gRPC endpoint.\n\
           --health-listen HOST:PORT Start a JSON health endpoint for probes.\n\
           --control-endpoint URL Register and heartbeat with the MVP HTTP control plane.\n\
           --agent-id ID       Agent id used for control-plane registration.\n\
           --flush-batch-size N Flush up to N queued records per background cycle.\n\
           --flush-interval-ms N Background flush interval in milliseconds.\n\
           --control-heartbeat-interval-ms N Control-plane heartbeat interval in milliseconds.\n\
           --self-test         Enqueue and flush one synthetic record.\n"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_self_test_args() -> Result<(), DynError> {
        let args = Args::parse(
            [
                "--self-test",
                "--tenant",
                "payments-prod",
                "--queue-dir",
                "queue",
                "--otlp-export-endpoint",
                "127.0.0.1:4317",
                "--health-listen",
                "127.0.0.1:13133",
                "--control-endpoint",
                "http://127.0.0.1:4001",
                "--agent-id",
                "agent-1",
                "--flush-batch-size",
                "64",
                "--control-heartbeat-interval-ms",
                "2500",
            ]
            .into_iter()
            .map(String::from),
        )?;

        assert!(args.self_test);
        assert_eq!(args.tenant_id, Some("payments-prod".to_string()));
        assert_eq!(args.queue_dir, PathBuf::from("queue"));
        assert_eq!(
            args.otlp_export_endpoint,
            Some("127.0.0.1:4317".to_string())
        );
        assert_eq!(args.health_listen, Some("127.0.0.1:13133".to_string()));
        assert_eq!(
            args.control_endpoint,
            Some("http://127.0.0.1:4001".to_string())
        );
        assert_eq!(args.agent_id, Some("agent-1".to_string()));
        assert_eq!(args.flush_batch_size, 64);
        assert_eq!(args.control_heartbeat_interval_ms, 2500);
        Ok(())
    }

    #[test]
    fn otlp_export_endpoint_rewrites_trace_metric_and_log_routes() -> Result<(), DynError> {
        let args = Args::parse(
            ["--otlp-export-endpoint", "http://127.0.0.1:4317"]
                .into_iter()
                .map(String::from),
        )?;

        let config = build_pipeline_config(&args)?;

        assert_eq!(config.exporters[0].name, "otlp-upstream");
        assert_eq!(config.exporters[0].protocol, ExporterProtocol::OtlpGrpc);
        assert_eq!(config.routes[0].signal, SignalKind::Trace);
        assert_eq!(config.routes[0].exporters, vec!["otlp-upstream"]);
        assert_eq!(config.routes[1].signal, SignalKind::Metric);
        assert_eq!(config.routes[1].exporters, vec!["otlp-upstream"]);
        assert_eq!(config.routes[2].signal, SignalKind::Log);
        assert_eq!(config.routes[2].exporters, vec!["otlp-upstream"]);
        Ok(())
    }

    #[test]
    fn parses_otlp_http_receiver_args() -> Result<(), DynError> {
        let args = Args::parse(
            ["--otlp-http", "127.0.0.1:4318"]
                .into_iter()
                .map(String::from),
        )?;

        assert_eq!(args.otlp_http, Some("127.0.0.1:4318".to_string()));
        Ok(())
    }

    #[test]
    fn rejects_multiple_receiver_modes() {
        let result = Args::parse(
            [
                "--listen",
                "127.0.0.1:4319",
                "--otlp-http",
                "127.0.0.1:4318",
            ]
            .into_iter()
            .map(String::from),
        );

        assert!(result.is_err());
    }

    #[test]
    fn parses_http_request_paths() {
        assert_eq!(
            parse_request_path("GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n"),
            Some("/metrics".to_string())
        );
        assert_eq!(parse_request_path(""), None);
    }
}
