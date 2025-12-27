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

use mqtt_endpoint_tokio::mqtt_ep;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;

#[allow(dead_code)]
pub const BROKER_PORT: u16 = 1883;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(10000);

pub struct BrokerProcess {
    child: Child,
    port: u16,
}

impl BrokerProcess {
    pub fn start() -> Self {
        // Ensure broker binary is built
        let broker_path = "target/debug/mqtt-broker";
        if !std::path::Path::new(broker_path).exists() {
            // Automatically build the broker binary
            // Inherit environment variables (including RUSTFLAGS for coverage)
            let output = Command::new("cargo")
                .args(["build", "--bin", "mqtt-broker"])
                .env_clear()
                .envs(std::env::vars())
                .output()
                .expect("Failed to execute cargo build");

            if !output.status.success() {
                panic!(
                    "Failed to build broker binary:\n{}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }

            #[cfg(test)]
            eprintln!(
                "Built mqtt-broker with RUSTFLAGS: {:?}",
                std::env::var("RUSTFLAGS")
            );
        }

        // Allocate unique port for this broker instance
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);

        // Run the built binary directly
        let mut cmd = Command::new(broker_path);
        cmd.args(["--tcp-port", &port.to_string(), "--log-level", "trace"]);

        // Set RUST_LOG
        cmd.env("RUST_LOG", "trace");

        // For coverage: make LLVM_PROFILE_FILE unique per subprocess
        if let Ok(llvm_profile) = std::env::var("LLVM_PROFILE_FILE") {
            // Make profile file unique per subprocess by inserting port number
            // Original: /path/mqtt-broker-tokio-%p-%4m.profraw
            // Modified: /path/mqtt-broker-tokio-port10000-%p-%4m.profraw
            let profile_file = llvm_profile.replace(
                "mqtt-broker-tokio-",
                &format!("mqtt-broker-tokio-port{port}-"),
            );

            // Debug: print coverage info (only in test builds)
            #[cfg(test)]
            eprintln!("Coverage: broker on port {port}: {profile_file}");

            cmd.env("LLVM_PROFILE_FILE", profile_file);
        } else {
            #[cfg(test)]
            eprintln!("Warning: LLVM_PROFILE_FILE not set for broker on port {port}");
        }

        let child = cmd.spawn().expect("Failed to start broker");

        BrokerProcess { child, port }
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub async fn wait_ready(&self) {
        // Wait for broker to start (max 30 seconds)
        for _ in 0..60 {
            if let Ok(stream) = TcpStream::connect(format!("127.0.0.1:{}", self.port)).await {
                drop(stream);
                // Wait a bit after connection check
                tokio::time::sleep(Duration::from_millis(100)).await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        panic!("Broker failed to start within 30 seconds");
    }
}

impl Drop for BrokerProcess {
    fn drop(&mut self) {
        // For coverage: we need to allow the process to flush coverage data gracefully

        #[cfg(unix)]
        {
            // On Unix, send SIGTERM first to allow graceful shutdown
            use std::process::Command;
            let pid = self.child.id();
            let _ = Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .output();

            // Wait up to 2 seconds for graceful shutdown
            for _ in 0..20 {
                match self.child.try_wait() {
                    Ok(Some(_)) => {
                        // Process exited gracefully
                        return;
                    }
                    Ok(None) => {
                        // Still running, wait a bit more
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                    Err(_) => break,
                }
            }

            // If still running after 2 seconds, force kill
            let _ = self.child.kill();
            let _ = self.child.wait();
        }

        #[cfg(not(unix))]
        {
            // On Windows, we don't have SIGTERM, so just wait a bit before killing
            // to give coverage runtime a chance to flush
            std::thread::sleep(std::time::Duration::from_millis(500));
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[allow(dead_code)]
pub async fn create_connected_endpoint(
    client_id: &str,
    port: u16,
) -> mqtt_ep::Endpoint<mqtt_ep::role::Client> {
    // Connect via TCP
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{port}"),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    // Create Endpoint
    let endpoint = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);

    // Create TcpTransport
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    // Attach transport
    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build ConnectionOption");

    endpoint
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    // Create and send CONNECT packet
    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id(client_id)
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");

    endpoint
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    let packet = endpoint.recv().await.expect("Failed to receive CONNACK");

    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert_eq!(
                connack.reason_code(),
                mqtt_ep::result_code::ConnectReasonCode::Success
            );
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    endpoint
}
