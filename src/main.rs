/**
 * MIT License
 *
 * Copyright (c) 2025 Takatoshi Kondo
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
mod broker;
mod subscription_store;

use mqtt_endpoint_tokio::mqtt_ep;
use broker::{BrokerManager, SubscriptionMessage};
use clap::Parser;
use futures::future;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio_rustls::{TlsAcceptor, rustls};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tracing::{error, info, trace};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(name = "mqtt-broker")]
#[command(about = "MQTT Broker with configurable worker threads and logging")]
struct Args {
    /// Number of worker threads (defaults to CPU count)
    #[arg(long, default_value_t = num_cpus::get())]
    cpus: usize,

    /// Log level
    #[arg(long, default_value = "info")]
    #[arg(value_parser = ["error", "warn", "info", "debug", "trace"])]
    log_level: String,

    #[arg(long)]
    tcp_port: Option<u16>,
    #[arg(long)]
    tls_port: Option<u16>,
    #[arg(long)]
    ws_port: Option<u16>,
    #[arg(long)]
    ws_tls_port: Option<u16>,

    /// Path to server certificate file (required when tls_port or ws_tls_port is specified)
    #[arg(long)]
    server_crt: Option<String>,

    /// Path to server private key file (required when tls_port or ws_tls_port is specified)
    #[arg(long)]
    server_key: Option<String>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Parse log level
    let log_level = match args.log_level.to_lowercase().as_str() {
        "error" => tracing::Level::ERROR,
        "warn" => tracing::Level::WARN,
        "info" => tracing::Level::INFO,
        "debug" => tracing::Level::DEBUG,
        "trace" => tracing::Level::TRACE,
        _ => unreachable!(), // clap validates this
    };

    let worker_threads = if args.cpus > 0 {
        args.cpus
    } else {
        eprintln!("Worker thread count must be greater than 0. Using CPU count.");
        num_cpus::get()
    };

    // Build custom tokio runtime
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()?;

    runtime.block_on(async_main(log_level, worker_threads, args))
}

/// Load TLS configuration for the broker
fn load_tls_acceptor(cert_path: &str, key_path: &str) -> anyhow::Result<TlsAcceptor> {
    let cert_file = File::open(cert_path)
        .map_err(|e| anyhow::anyhow!("Failed to open certificate file '{}': {}", cert_path, e))?;
    let mut cert_reader = BufReader::new(cert_file);
    let cert_chain = rustls_pemfile::certs(&mut cert_reader)?
        .into_iter()
        .map(rustls::Certificate)
        .collect();

    let key_file = File::open(key_path)
        .map_err(|e| anyhow::anyhow!("Failed to open private key file '{}': {}", key_path, e))?;
    let mut key_reader = BufReader::new(key_file);

    // Try PKCS8 first, then PKCS1
    let private_keys = rustls_pemfile::pkcs8_private_keys(&mut key_reader)?;
    let private_key = if private_keys.is_empty() {
        // Reset reader and try PKCS1
        key_reader = BufReader::new(File::open(key_path)?);
        let rsa_keys = rustls_pemfile::rsa_private_keys(&mut key_reader)?;
        rustls::PrivateKey(rsa_keys[0].clone())
    } else {
        rustls::PrivateKey(private_keys[0].clone())
    };

    let config = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

async fn async_main(log_level: tracing::Level, threads: usize, args: Args) -> anyhow::Result<()> {
    // Initialize tracing with specified log level
    // Allow application logs at the specified level, suppress verbose logs from external crates
    let filter_string = format!(
        "mqtt_endpoint_tokio={},\
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
        log_level.as_str().to_lowercase()
    );

    tracing_subscriber::fmt()
        .with_max_level(log_level)
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing::Level::WARN.into())
                .parse_lossy(&filter_string),
        )
        .init();

    info!("Starting MQTT Broker with log level: {log_level}, worker threads: {threads}");

    // Validate TLS configuration if TLS ports are specified
    let tls_required = args.tls_port.is_some() || args.ws_tls_port.is_some();
    if tls_required {
        if args.server_crt.is_none() || args.server_key.is_none() {
            return Err(anyhow::anyhow!(
                "TLS certificate (--server-crt) and private key (--server-key) are required when TLS ports (--tls-port or --ws-tls-port) are specified"
            ));
        }
    }

    let broker = BrokerManager::new().await?;
    let mut tasks = Vec::new();

    // TCP listener
    if let Some(port) = args.tcp_port {
        info!("Starting TCP listener on port {port}");
        let bind_addr = format!("0.0.0.0:{port}");
        let tcp_listener = tokio::net::TcpListener::bind(&bind_addr).await?;
        info!("Listening on TCP port {port} for MQTT (dual-version support)");

        let broker_clone = broker.clone();
        let tcp_task = tokio::spawn(async move {
            loop {
                match tcp_listener.accept().await {
                    Ok((stream, addr)) => {
                        trace!("New TCP connection from: {addr}");
                        let broker = broker_clone.clone();
                        let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);
                        tokio::spawn(async move {
                            if let Err(e) = broker.handle_connection(transport).await {
                                error!("TCP connection error: {e}");
                            }
                        });
                    }
                    Err(e) => error!("Failed to accept TCP connection: {e}"),
                }
            }
        });
        tasks.push(tcp_task);
    }

    // TLS listener
    if let Some(port) = args.tls_port {
        info!("Starting TLS listener on port {port}");
        let cert_path = args.server_crt.as_ref().unwrap();
        let key_path = args.server_key.as_ref().unwrap();
        let acceptor = load_tls_acceptor(cert_path, key_path)?;
        let bind_addr = format!("0.0.0.0:{port}");
        let tls_listener = tokio::net::TcpListener::bind(&bind_addr).await?;
        info!("Listening on TLS port {port} for MQTT (dual-version support)");

        let broker_clone = broker.clone();
        let tls_task = tokio::spawn(async move {
            loop {
                match tls_listener.accept().await {
                    Ok((stream, addr)) => {
                        trace!("New TLS connection from: {addr}");
                        let broker = broker_clone.clone();
                        let acceptor = acceptor.clone();
                        tokio::spawn(async move {
                            match acceptor.accept(stream).await {
                                Ok(tls_stream) => {
                                    let transport =
                                        mqtt_ep::transport::TlsTransport::from_stream(tls_stream);
                                    if let Err(e) = broker.handle_connection(transport).await {
                                        error!("TLS connection error: {e}");
                                    }
                                }
                                Err(e) => error!("TLS handshake failed: {e}"),
                            }
                        });
                    }
                    Err(e) => error!("Failed to accept TLS connection: {e}"),
                }
            }
        });
        tasks.push(tls_task);
    }

    // WebSocket listener
    if let Some(port) = args.ws_port {
        info!("Starting WebSocket listener on port {port}");
        let bind_addr = format!("0.0.0.0:{port}");
        let ws_listener = tokio::net::TcpListener::bind(&bind_addr).await?;
        info!("Listening on WebSocket port {port} for MQTT (dual-version support)");

        let broker_clone = broker.clone();
        let ws_task = tokio::spawn(async move {
            loop {
                match ws_listener.accept().await {
                    Ok((stream, addr)) => {
                        trace!("New WebSocket connection from: {addr}");
                        let _broker = broker_clone.clone();
                        tokio::spawn(async move {
                            // Use accept_async for simpler handling, then check subprotocol
                            let callback = |req: &Request, mut response: Response| {
                                // Check if client requests MQTT subprotocol
                                if let Some(protocols) = req.headers().get("Sec-WebSocket-Protocol")
                                {
                                    if protocols.to_str().unwrap_or("").contains("mqtt") {
                                        // Accept MQTT subprotocol
                                        response.headers_mut().insert(
                                            "Sec-WebSocket-Protocol",
                                            HeaderValue::from_static("mqtt"),
                                        );
                                    }
                                }
                                Ok(response)
                            };

                            match tokio_tungstenite::accept_hdr_async(stream, callback).await {
                                Ok(ws_stream) => {
                                    trace!("Plain WebSocket connection established from: {addr}");

                                    // Use the new from_tcp_server_stream method for plain WebSocket
                                    let transport = mqtt_ep::transport::WebSocketTransport::from_tcp_server_stream(ws_stream);

                                    if let Err(e) = _broker.handle_connection(transport).await {
                                        error!("Plain WebSocket connection error: {e}");
                                    }
                                }
                                Err(e) => error!("Plain WebSocket handshake failed: {e}"),
                            }
                        });
                    }
                    Err(e) => error!("Failed to accept WebSocket connection: {e}"),
                }
            }
        });
        tasks.push(ws_task);
    }

    // WebSocket+TLS listener
    if let Some(port) = args.ws_tls_port {
        info!("Starting WebSocket+TLS listener on port {port}");
        let cert_path = args.server_crt.as_ref().unwrap();
        let key_path = args.server_key.as_ref().unwrap();
        let acceptor = load_tls_acceptor(cert_path, key_path)?;
        let bind_addr = format!("0.0.0.0:{port}");
        let ws_tls_listener = tokio::net::TcpListener::bind(&bind_addr).await?;
        info!("Listening on WebSocket+TLS port {port} for MQTT (dual-version support)");

        let broker_clone = broker.clone();
        let ws_tls_task = tokio::spawn(async move {
            loop {
                match ws_tls_listener.accept().await {
                    Ok((stream, addr)) => {
                        trace!("New WebSocket+TLS connection from: {addr}");
                        let broker = broker_clone.clone();
                        let acceptor = acceptor.clone();
                        tokio::spawn(async move {
                            match acceptor.accept(stream).await {
                                Ok(tls_stream) => {
                                    let callback = |req: &Request, mut response: Response| {
                                        // Check if client requests MQTT subprotocol
                                        if let Some(protocols) =
                                            req.headers().get("Sec-WebSocket-Protocol")
                                        {
                                            if protocols.to_str().unwrap_or("").contains("mqtt") {
                                                // Accept MQTT subprotocol
                                                response.headers_mut().insert(
                                                    "Sec-WebSocket-Protocol",
                                                    HeaderValue::from_static("mqtt"),
                                                );
                                            }
                                        }
                                        Ok(response)
                                    };

                                    match tokio_tungstenite::accept_hdr_async(tls_stream, callback)
                                        .await
                                    {
                                        Ok(ws_stream) => {
                                            let transport = mqtt_ep::transport::WebSocketTransport::from_tls_server_stream(ws_stream);
                                            if let Err(e) =
                                                broker.handle_connection(transport).await
                                            {
                                                error!("WebSocket+TLS connection error: {e}");
                                            }
                                        }
                                        Err(e) => error!("WebSocket+TLS handshake failed: {e}"),
                                    }
                                }
                                Err(e) => error!("TLS handshake failed for WebSocket+TLS: {e}"),
                            }
                        });
                    }
                    Err(e) => error!("Failed to accept WebSocket+TLS connection: {e}"),
                }
            }
        });
        tasks.push(ws_tls_task);
    }

    if tasks.is_empty() {
        return Err(anyhow::anyhow!(
            "No listeners configured. Please specify at least one port (--tcp-port, --tls-port, --ws-port, or --ws-tls-port)"
        ));
    }

    info!("All listeners started. Broker is ready to accept connections.");

    // Wait for any task to complete (they shouldn't under normal circumstances)
    let (result, _index, _remaining) = future::select_all(tasks).await;
    result?;

    Ok(())
}
