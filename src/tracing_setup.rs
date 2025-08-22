use anyhow::Result;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Initialize tracing with efficient async logging setup
/// Returns a WorkerGuard that must be kept alive for the duration of the program
pub fn init_tracing(log_level: tracing::Level) -> Result<WorkerGuard> {
    // Create a non-blocking appender for stdout with async performance
    let (non_blocking_stdout, guard) = tracing_appender::non_blocking(std::io::stdout());
    
    // Create the filter string for controlling log levels per crate
    let filter_string = format!(
        "mqtt_endpoint_tokio={},\
         mqtt_protocol_core={},\
         mqtt_broker_tokio={},\
         mqtt_broker={},\
         tokio=warn,\
         hyper=warn,\
         tungstenite=warn,\
         tokio_tungstenite=warn,\
         tokio_rustls=warn,\
         rustls=warn,\
         h2=warn,\
         tower=warn,\
         reqwest=warn",
        log_level.as_str().to_lowercase(),
        log_level.as_str().to_lowercase(),
        log_level.as_str().to_lowercase(),
        log_level.as_str().to_lowercase()
    );

    // Build the env filter
    let env_filter = EnvFilter::builder()
        .with_default_directive(tracing::Level::WARN.into())
        .parse_lossy(&filter_string);

    // Create a formatting layer with the non-blocking writer
    let formatting_layer = fmt::layer()
        .with_writer(non_blocking_stdout)
        .with_ansi(true)
        .with_level(true)
        .with_target(true)
        .with_thread_ids(false)
        .with_thread_names(false)
        .compact();

    // Initialize the global subscriber with the layers
    tracing_subscriber::registry()
        .with(env_filter)
        .with(formatting_layer)
        .init();

    Ok(guard)
}

/// Alternative initialization with file appender for production use
pub fn init_tracing_with_file(
    log_level: tracing::Level, 
    log_dir: &str, 
    log_file_prefix: &str
) -> Result<WorkerGuard> {
    // Create a daily rolling file appender
    let file_appender = tracing_appender::rolling::daily(log_dir, log_file_prefix);
    let (non_blocking_file, guard) = tracing_appender::non_blocking(file_appender);
    
    // Create the filter string
    let filter_string = format!(
        "mqtt_endpoint_tokio={},\
         mqtt_protocol_core={},\
         mqtt_broker_tokio={},\
         mqtt_broker={},\
         tokio=warn,\
         hyper=warn,\
         tungstenite=warn,\
         tokio_tungstenite=warn,\
         tokio_rustls=warn,\
         rustls=warn,\
         h2=warn,\
         tower=warn,\
         reqwest=warn",
        log_level.as_str().to_lowercase(),
        log_level.as_str().to_lowercase(),
        log_level.as_str().to_lowercase(),
        log_level.as_str().to_lowercase()
    );

    let env_filter = EnvFilter::builder()
        .with_default_directive(tracing::Level::WARN.into())
        .parse_lossy(&filter_string);

    // Create formatting layer for file output (no ANSI colors)
    let formatting_layer = fmt::layer()
        .with_writer(non_blocking_file)
        .with_ansi(false)
        .with_level(true)
        .with_target(true)
        .with_thread_ids(false)
        .with_thread_names(false)
        .compact();

    tracing_subscriber::registry()
        .with(env_filter)
        .with(formatting_layer)
        .init();

    Ok(guard)
}