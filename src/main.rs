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
mod tracing_setup;

use broker::BrokerManager;
use clap::Parser;
use futures::future;
use mqtt_endpoint_tokio::mqtt_ep;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio_rustls::{TlsAcceptor, rustls};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tracing_setup::init_tracing;

use socket2::SockRef;
use tracing::{error, info, trace};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(name = "mqtt-broker")]
#[command(about = "MQTT Broker with configurable worker threads and logging")]
struct Args {
    /// Number of worker threads for async tasks
    #[arg(long)]
    worker_threads: Option<usize>,

    /// Number of blocking threads for blocking operations
    #[arg(long)]
    max_blocking_threads: Option<usize>,

    /// Enable thread keep alive for worker threads
    #[arg(long)]
    thread_keep_alive: Option<bool>,

    /// Thread stack size in bytes
    #[arg(long)]
    thread_stack_size: Option<usize>,

    /// Global queue interval for work stealing (microseconds)
    #[arg(long)]
    global_queue_interval: Option<u32>,

    /// Event loop interval for polling (microseconds)  
    #[arg(long)]
    event_interval: Option<u32>,

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

    /// Enable TCP_NODELAY socket option
    #[arg(long)]
    socket_no_delay: Option<bool>,

    /// TCP socket send buffer size in bytes
    #[arg(long)]
    socket_send_buf_size: Option<usize>,

    /// TCP socket receive buffer size in bytes
    #[arg(long)]
    socket_recv_buf_size: Option<usize>,

    /// MQTT endpoint receive buffer size for tokio stream reads in bytes
    #[arg(long)]
    ep_recv_buf_size: Option<usize>,

    /// Enable SO_REUSEPORT for load balancing across threads (Linux only)
    #[arg(long)]
    socket_reuseport: Option<bool>,

    /// TCP keepalive time in seconds (0 to disable)
    #[arg(long)]
    socket_keepalive_time: Option<u32>,
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

    let worker_threads = args.worker_threads.unwrap_or_else(num_cpus::get);

    // Build custom tokio runtime
    let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
    runtime_builder.worker_threads(worker_threads).enable_all();

    if let Some(max_blocking) = args.max_blocking_threads {
        runtime_builder.max_blocking_threads(max_blocking);
    }

    if let Some(keep_alive) = args.thread_keep_alive {
        runtime_builder.thread_keep_alive(std::time::Duration::from_secs(if keep_alive {
            10
        } else {
            0
        }));
    }

    if let Some(stack_size) = args.thread_stack_size {
        runtime_builder.thread_stack_size(stack_size);
    }

    if let Some(interval) = args.global_queue_interval {
        runtime_builder.global_queue_interval(interval);
    }

    if let Some(interval) = args.event_interval {
        runtime_builder.event_interval(interval);
    }

    let runtime = runtime_builder.build()?;

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

/// Configure individual socket options if specified
fn configure_individual_socket_options(
    stream: &tokio::net::TcpStream,
    addr: &std::net::SocketAddr,
    no_delay: Option<bool>,
    send_buf_size: Option<usize>,
    recv_buf_size: Option<usize>,
    keepalive_time: Option<u32>,
) {
    // Configure TCP_NODELAY if specified
    if let Some(no_delay) = no_delay {
        if let Err(e) = stream.set_nodelay(no_delay) {
            error!("Failed to set TCP_NODELAY for {addr}: {e}");
        }
    }

    let sock_ref = SockRef::from(stream);

    // Configure send buffer size if specified
    if let Some(send_buf_size) = send_buf_size {
        if send_buf_size > 0 {
            if let Err(e) = sock_ref.set_send_buffer_size(send_buf_size) {
                error!("Failed to set send buffer size for {addr}: {e}");
            }
        }
    }

    // Configure receive buffer size if specified
    if let Some(recv_buf_size) = recv_buf_size {
        if recv_buf_size > 0 {
            if let Err(e) = sock_ref.set_recv_buffer_size(recv_buf_size) {
                error!("Failed to set recv buffer size for {addr}: {e}");
            }
        }
    }

    // Configure keepalive if specified
    if let Some(keepalive_time) = keepalive_time {
        if keepalive_time > 0 {
            if let Err(e) = sock_ref.set_keepalive(true) {
                error!("Failed to enable keepalive for {addr}: {e}");
            }

            #[cfg(target_os = "linux")]
            {
                use std::os::unix::io::AsRawFd;
                let fd = stream.as_raw_fd();
                unsafe {
                    let optval = keepalive_time as libc::c_int;
                    if libc::setsockopt(
                        fd,
                        libc::IPPROTO_TCP,
                        libc::TCP_KEEPIDLE,
                        &optval as *const _ as *const libc::c_void,
                        std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                    ) != 0
                    {
                        error!("Failed to set TCP_KEEPIDLE for {addr}");
                    }
                }
            }
        }
    }
}

/// Configure listener socket options for optimal performance
fn configure_listener_socket(
    listener: &tokio::net::TcpListener,
    reuseport: bool,
) -> anyhow::Result<()> {
    let sock_ref = SockRef::from(listener);

    // Always set SO_REUSEADDR
    sock_ref
        .set_reuse_address(true)
        .map_err(|e| anyhow::anyhow!("Failed to set SO_REUSEADDR: {}", e))?;

    // Set SO_REUSEPORT (Linux only)
    #[cfg(target_os = "linux")]
    if reuseport {
        use std::os::unix::io::AsRawFd;
        let fd = listener.as_raw_fd();
        unsafe {
            let optval: libc::c_int = 1;
            if libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_REUSEPORT,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            ) != 0
            {
                return Err(anyhow::anyhow!("Failed to set SO_REUSEPORT"));
            }
        }
    }

    Ok(())
}

async fn async_main(log_level: tracing::Level, _threads: usize, args: Args) -> anyhow::Result<()> {
    // Initialize efficient async tracing
    let _guard = init_tracing(log_level)?;

    info!("Starting MQTT Broker with log level: {log_level}");
    info!("Tokio runtime configuration:");
    info!(
        "  --worker-threads        {}",
        args.worker_threads.unwrap_or_else(num_cpus::get)
    );
    info!(
        "  --max-blocking-threads  {}",
        args.max_blocking_threads
            .map_or("None".to_string(), |v| v.to_string())
    );
    info!(
        "  --thread-keep-alive     {}",
        args.thread_keep_alive
            .map_or("None".to_string(), |v| v.to_string())
    );
    info!(
        "  --thread-stack-size     {}",
        args.thread_stack_size
            .map_or("None".to_string(), |v| v.to_string())
    );
    info!(
        "  --global-queue-interval {}",
        args.global_queue_interval
            .map_or("None".to_string(), |v| v.to_string())
    );
    info!(
        "  --event-interval        {}",
        args.event_interval
            .map_or("None".to_string(), |v| v.to_string())
    );
    info!("Socket configuration:");
    info!(
        "  --socket-no-delay       {}",
        args.socket_no_delay
            .map_or("None".to_string(), |v| v.to_string())
    );
    info!(
        "  --socket-send-buf-size  {}",
        args.socket_send_buf_size
            .map_or("None".to_string(), |v| v.to_string())
    );
    info!(
        "  --socket-recv-buf-size  {}",
        args.socket_recv_buf_size
            .map_or("None".to_string(), |v| v.to_string())
    );
    info!(
        "  --socket-reuseport      {}",
        args.socket_reuseport
            .map_or("None".to_string(), |v| v.to_string())
    );
    info!(
        "  --socket-keepalive-time {}",
        args.socket_keepalive_time
            .map_or("None".to_string(), |v| v.to_string())
    );
    info!("Endpoint configuration:");
    info!(
        "  --ep-recv-buf-size      {}",
        args.ep_recv_buf_size
            .map_or("None".to_string(), |v| v.to_string())
    );

    if let Some(port) = args.tcp_port {
        info!("TCP port: {port}");
    }
    if let Some(port) = args.tls_port {
        info!("TLS port: {port}");
    }
    if let Some(port) = args.ws_port {
        info!("WebSocket port: {port}");
    }
    if let Some(port) = args.ws_tls_port {
        info!("WebSocket+TLS port: {port}");
    }

    // Validate TLS configuration if TLS ports are specified
    let tls_required = args.tls_port.is_some() || args.ws_tls_port.is_some();
    if tls_required {
        if args.server_crt.is_none() || args.server_key.is_none() {
            return Err(anyhow::anyhow!(
                "TLS certificate (--server-crt) and private key (--server-key) are required when TLS ports (--tls-port or --ws-tls-port) are specified"
            ));
        }
    }

    let broker = BrokerManager::new(args.ep_recv_buf_size).await?;
    let mut tasks = Vec::new();

    // TCP listener
    if let Some(port) = args.tcp_port {
        info!("Starting TCP listener on port {port}");
        let bind_addr = format!("0.0.0.0:{port}");
        let tcp_listener = tokio::net::TcpListener::bind(&bind_addr).await?;
        if let Some(reuseport) = args.socket_reuseport {
            configure_listener_socket(&tcp_listener, reuseport)?;
        }
        info!("Listening on TCP port {port} for MQTT (dual-version support)");

        let broker_clone = broker.clone();
        let socket_no_delay = args.socket_no_delay;
        let socket_send_buf_size = args.socket_send_buf_size;
        let socket_recv_buf_size = args.socket_recv_buf_size;
        let socket_keepalive_time = args.socket_keepalive_time;
        let tcp_task = tokio::spawn(async move {
            loop {
                match tcp_listener.accept().await {
                    Ok((stream, addr)) => {
                        trace!("New TCP connection from: {addr}");

                        // Configure socket options using shared function
                        configure_individual_socket_options(
                            &stream,
                            &addr,
                            socket_no_delay,
                            socket_send_buf_size,
                            socket_recv_buf_size,
                            socket_keepalive_time,
                        );
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
        if let Some(reuseport) = args.socket_reuseport {
            configure_listener_socket(&tls_listener, reuseport)?;
        }
        info!("Listening on TLS port {port} for MQTT (dual-version support)");

        let broker_clone = broker.clone();
        let socket_no_delay = args.socket_no_delay;
        let socket_send_buf_size = args.socket_send_buf_size;
        let socket_recv_buf_size = args.socket_recv_buf_size;
        let socket_keepalive_time = args.socket_keepalive_time;
        let tls_task = tokio::spawn(async move {
            loop {
                match tls_listener.accept().await {
                    Ok((stream, addr)) => {
                        trace!("New TLS connection from: {addr}");

                        // Configure socket options before TLS handshake
                        configure_individual_socket_options(
                            &stream,
                            &addr,
                            socket_no_delay,
                            socket_send_buf_size,
                            socket_recv_buf_size,
                            socket_keepalive_time,
                        );

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
        if let Some(reuseport) = args.socket_reuseport {
            configure_listener_socket(&ws_listener, reuseport)?;
        }
        info!("Listening on WebSocket port {port} for MQTT (dual-version support)");

        let broker_clone = broker.clone();
        let socket_no_delay = args.socket_no_delay;
        let socket_send_buf_size = args.socket_send_buf_size;
        let socket_recv_buf_size = args.socket_recv_buf_size;
        let socket_keepalive_time = args.socket_keepalive_time;
        let ws_task = tokio::spawn(async move {
            loop {
                match ws_listener.accept().await {
                    Ok((stream, addr)) => {
                        trace!("New WebSocket connection from: {addr}");

                        // Configure socket options before WebSocket handshake
                        configure_individual_socket_options(
                            &stream,
                            &addr,
                            socket_no_delay,
                            socket_send_buf_size,
                            socket_recv_buf_size,
                            socket_keepalive_time,
                        );

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
        if let Some(reuseport) = args.socket_reuseport {
            configure_listener_socket(&ws_tls_listener, reuseport)?;
        }
        info!("Listening on WebSocket+TLS port {port} for MQTT (dual-version support)");

        let broker_clone = broker.clone();
        let socket_no_delay = args.socket_no_delay;
        let socket_send_buf_size = args.socket_send_buf_size;
        let socket_recv_buf_size = args.socket_recv_buf_size;
        let socket_keepalive_time = args.socket_keepalive_time;
        let ws_tls_task = tokio::spawn(async move {
            loop {
                match ws_tls_listener.accept().await {
                    Ok((stream, addr)) => {
                        trace!("New WebSocket+TLS connection from: {addr}");

                        // Configure socket options before TLS handshake
                        configure_individual_socket_options(
                            &stream,
                            &addr,
                            socket_no_delay,
                            socket_send_buf_size,
                            socket_recv_buf_size,
                            socket_keepalive_time,
                        );

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
