// MIT License
//
// Copyright (c) 2025 Takatoshi Kondo
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
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
